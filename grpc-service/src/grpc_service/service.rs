use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_stream::try_stream;
use futures::StreamExt;
use helius_laserstream::grpc::geyser_server::{Geyser, GeyserServer};
use helius_laserstream::grpc::{
    GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
    GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
    GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
    IsBlockhashValidResponse, PingRequest, PongResponse,
    SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
    SubscribeUpdate,
};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use super::convert::to_subscribe_update;
use super::dispatcher::{DispatcherHandle, TargetedSendResult};
use crate::domain::{AccountEvent, PubkeyFilter};
use crate::traits::{SnapshotStore, ValidatorSubscriptions};

type SubscribeStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<SubscribeUpdate, Status>>
            + Send
            + 'static,
    >,
>;

#[derive(Clone)]
pub(crate) struct GrpcSubscriptionService<
    P: SnapshotStore + Clone + Send + Sync + 'static,
    V: ValidatorSubscriptions + Clone + Send + Sync + 'static,
> {
    dispatcher: DispatcherHandle,
    snapshot_store: P,
    validator_subscriptions: V,
}

impl<
    P: SnapshotStore + Clone + Send + Sync + 'static,
    V: ValidatorSubscriptions + Clone + Send + Sync + 'static,
> GrpcSubscriptionService<P, V>
{
    pub(crate) fn new(
        dispatcher: DispatcherHandle,
        snapshot_store: P,
        validator_subscriptions: V,
    ) -> Self {
        Self {
            dispatcher,
            snapshot_store,
            validator_subscriptions,
        }
    }

    pub(crate) fn into_server(self) -> GeyserServer<Self> {
        GeyserServer::new(self)
    }
}

async fn bootstrap_new_pubkeys_impl<
    P: SnapshotStore + Send + Sync,
    V: ValidatorSubscriptions + Send + Sync,
>(
    dispatcher: &DispatcherHandle,
    snapshot_store: &P,
    validator_subscriptions: &V,
    client_id: u64,
    newly_added: HashSet<[u8; 32]>,
) {
    let mut pubkeys_to_whitelist = Vec::new();

    for pubkey_bytes in newly_added {
        let pubkey_b58 = bs58::encode(pubkey_bytes).into_string();
        let pubkey = match PubkeyFilter::parse(&pubkey_b58) {
            Ok(pubkey) => pubkey,
            Err(error) => {
                warn!(
                    client_id,
                    pubkey = %pubkey_b58,
                    error = %error,
                    "failed to normalize newly added pubkey"
                );
                continue;
            }
        };

        // If kSql already has materialized state for this pubkey, push that
        // snapshot immediately so the client sees the last known account state
        // on subscribe.
        //
        // If kSql does not have a row, do nothing here. That can simply mean
        // the validator has never produced an update for this pubkey into
        // Kafka yet.
        //
        // After we subscribe at the validator level and we are the first to
        // ever subscribe to the validator for this pubkey, then the validator
        // will publish one of two Kafka updates:
        // - the current account update if the account exists
        // - a MissingAccount update if the account does not exist
        let snapshot = match snapshot_store.fetch_one_by_pubkey(&pubkey).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(
                    client_id,
                    pubkey = %pubkey_b58,
                    error = %error,
                    "failed to fetch ksql snapshot for newly added pubkey"
                );
                continue;
            }
        };

        let Some(snapshot) = snapshot else {
            info!(
                client_id,
                pubkey = %pubkey_b58,
                "pubkey missing from ksql; scheduling validator whitelist"
            );
            pubkeys_to_whitelist.push(pubkey_b58);
            continue;
        };

        let update =
            match to_subscribe_update(&AccountEvent::Snapshot(snapshot)) {
                Ok(update) => update,
                Err(error) => {
                    warn!(
                        client_id,
                        pubkey = %pubkey_b58,
                        error = %error,
                        "failed to convert ksql snapshot into subscribe update"
                    );
                    continue;
                }
            };

        match dispatcher.send_to_client(client_id, update).await {
            Ok(TargetedSendResult::Delivered) => {}
            Ok(TargetedSendResult::ClientNotFound) => {
                warn!(
                    client_id,
                    pubkey = %pubkey_b58,
                    "targeted snapshot skipped because client is no longer registered"
                );
                break;
            }
            Ok(TargetedSendResult::FailedButRetained) => {
                warn!(
                    client_id,
                    pubkey = %pubkey_b58,
                    "targeted snapshot delivery failed but client was retained"
                );
                break;
            }
            Ok(TargetedSendResult::RemovedByPolicy) => {
                info!(
                    client_id,
                    pubkey = %pubkey_b58,
                    "targeted snapshot delivery removed client by dispatcher policy"
                );
                break;
            }
            Err(error) => {
                warn!(
                    client_id,
                    pubkey = %pubkey_b58,
                    error = %error,
                    "failed to queue targeted snapshot update"
                );
                break;
            }
        }
    }

    if pubkeys_to_whitelist.is_empty() {
        return;
    }

    info!(
        client_id,
        pubkey_count = pubkeys_to_whitelist.len(),
        "whitelisting ksql-missing pubkeys with validator"
    );

    if let Err(error) = validator_subscriptions
        .whitelist_pubkeys(&pubkeys_to_whitelist)
        .await
    {
        warn!(
            client_id,
            pubkey_count = pubkeys_to_whitelist.len(),
            error = %error,
            "failed to whitelist pubkeys with validator"
        );
    }
}

struct ClientGuardStream {
    inner: SubscribeStream,
    dispatcher: DispatcherHandle,
    client_id: u64,
}

impl tokio_stream::Stream for ClientGuardStream {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for ClientGuardStream {
    fn drop(&mut self) {
        let dispatcher = self.dispatcher.clone();
        let client_id = self.client_id;
        tokio::spawn(async move {
            dispatcher.remove_client(client_id).await;
        });
    }
}

#[allow(clippy::result_large_err)]
fn parse_accounts_filter(
    req: &SubscribeRequest,
) -> Result<HashSet<[u8; 32]>, Status> {
    let mut set = HashSet::new();
    for filter_accounts in req.accounts.values() {
        for account_b58 in &filter_accounts.account {
            let bytes = bs58::decode(account_b58).into_vec().map_err(|e| {
                Status::invalid_argument(format!("invalid base58 pubkey: {e}"))
            })?;
            let arr: [u8; 32] = bytes.try_into().map_err(|_| {
                Status::invalid_argument("pubkey must be 32 bytes")
            })?;
            set.insert(arr);
        }
    }
    Ok(set)
}

#[allow(clippy::result_large_err)]
fn parse_pubkey_list(accounts: &[String]) -> Result<HashSet<[u8; 32]>, Status> {
    let mut set = HashSet::new();
    for b58 in accounts {
        let bytes = bs58::decode(b58).into_vec().map_err(|e| {
            Status::invalid_argument(format!("invalid base58 pubkey: {e}"))
        })?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| Status::invalid_argument("pubkey must be 32 bytes"))?;
        set.insert(arr);
    }
    Ok(set)
}

enum FilterOp {
    Replace(HashSet<[u8; 32]>),
    Patch {
        add: HashSet<[u8; 32]>,
        remove: HashSet<[u8; 32]>,
    },
}

#[allow(clippy::result_large_err)]
fn parse_filter_op(req: &SubscribeRequest) -> Result<FilterOp, Status> {
    let has_add = req.accounts.contains_key("add");
    let has_remove = req.accounts.contains_key("remove");

    if has_add || has_remove {
        let add = match req.accounts.get("add") {
            Some(f) => parse_pubkey_list(&f.account)?,
            None => HashSet::new(),
        };
        let remove = match req.accounts.get("remove") {
            Some(f) => parse_pubkey_list(&f.account)?,
            None => HashSet::new(),
        };
        Ok(FilterOp::Patch { add, remove })
    } else {
        Ok(FilterOp::Replace(parse_accounts_filter(req)?))
    }
}

#[tonic::async_trait]
impl<
    P: SnapshotStore + Clone + Send + Sync + 'static,
    V: ValidatorSubscriptions + Clone + Send + Sync + 'static,
> Geyser for GrpcSubscriptionService<P, V>
{
    type SubscribeStream = SubscribeStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut request_stream = request.into_inner();

        // 1. Read + parse first message
        let first_req =
            request_stream.next().await.transpose()?.ok_or_else(|| {
                Status::invalid_argument("subscribe request stream was empty")
            })?;

        let initial_filter = parse_accounts_filter(&first_req)?;
        info!(
            filter_size = initial_filter.len(),
            "new gRPC subscriber connected"
        );

        // 2. Register with dispatcher using parsed filter
        let (client_id, mut receiver) = self
            .dispatcher
            .add_client(initial_filter, 256)
            .await
            .map_err(|_| Status::internal("dispatcher unavailable"))?;

        // 3. Spawn task to read subsequent messages
        let dispatcher = self.dispatcher.clone();
        let snapshot_store = self.snapshot_store.clone();
        let validator_subscriptions = self.validator_subscriptions.clone();
        tokio::spawn(async move {
            while let Some(result) = request_stream.next().await {
                match result {
                    Ok(req) => {
                        let op = match parse_filter_op(&req) {
                            Ok(op) => op,
                            Err(status) => {
                                warn!(client_id, error = %status, "invalid filter update");
                                continue;
                            }
                        };
                        let newly_added = match op {
                            FilterOp::Replace(filter) => {
                                dispatcher
                                    .update_filter(client_id, filter)
                                    .await
                            }
                            FilterOp::Patch { add, remove } => {
                                dispatcher
                                    .patch_filter(client_id, add, remove)
                                    .await
                            }
                        };
                        let newly_added = match newly_added {
                            Ok(newly_added) => newly_added,
                            Err(error) => {
                                warn!(
                                    client_id,
                                    error = %error,
                                    "failed to send filter command"
                                );
                                break;
                            }
                        };

                        // Note: self is not available in this spawned task,
                        // so we call the impl directly with the cloned fields
                        bootstrap_new_pubkeys_impl(
                            &dispatcher,
                            &snapshot_store,
                            &validator_subscriptions,
                            client_id,
                            newly_added,
                        )
                        .await;
                    }
                    Err(e) => {
                        debug!(
                            client_id,
                            error = %e,
                            "client request stream ended"
                        );
                        break;
                    }
                }
            }
        });

        // 4. Forward updates from dispatcher to client
        let dispatcher = self.dispatcher.clone();

        let stream = try_stream! {
            loop {
                match receiver.recv().await {
                    Some(update) => {
                        debug!("sending update to subscriber");
                        yield (*update).clone();
                    }
                    None => {
                        info!("dispatcher closed client channel, ending stream");
                        break;
                    }
                }
            }
        };

        let guarded = ClientGuardStream {
            inner: Box::pin(stream),
            dispatcher,
            client_id,
        };

        Ok(Response::new(Box::pin(guarded)))
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Err(Status::unimplemented(
            "SubscribeReplayInfo is not supported",
        ))
    }

    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> Result<Response<PongResponse>, Status> {
        Err(Status::unimplemented("Ping is not supported"))
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("GetLatestBlockhash is not supported"))
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("GetBlockHeight is not supported"))
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented("GetSlot is not supported"))
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("IsBlockhashValid is not supported"))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Err(Status::unimplemented("GetVersion is not supported"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use helius_laserstream::grpc::{
        SubscribeRequest, SubscribeRequestFilterAccounts,
        subscribe_update::UpdateOneof,
    };
    use tokio::time::timeout;

    use super::{
        FilterOp, bootstrap_new_pubkeys_impl, parse_accounts_filter,
        parse_filter_op, parse_pubkey_list,
    };
    use crate::domain::{AccountState, PubkeyFilter, bytes_to_base58};
    use crate::errors::{GeykagError, GeykagResult};
    use crate::grpc_service::dispatcher::DispatcherHandle;
    use crate::traits::{SnapshotStore, ValidatorSubscriptions};

    fn pubkey_bytes(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn pubkey_b58(byte: u8) -> String {
        bytes_to_base58(&pubkey_bytes(byte))
    }

    fn empty_request() -> SubscribeRequest {
        SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: HashMap::new(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        }
    }

    fn account_filter_request(key: &str, pubkeys: &[u8]) -> SubscribeRequest {
        let mut request = empty_request();
        request.accounts.insert(
            key.to_owned(),
            SubscribeRequestFilterAccounts {
                account: pubkeys.iter().map(|byte| pubkey_b58(*byte)).collect(),
                owner: Vec::new(),
                filters: Vec::new(),
                nonempty_txn_signature: None,
            },
        );
        request
    }

    fn replace_request(pubkeys: &[u8]) -> SubscribeRequest {
        account_filter_request("client", pubkeys)
    }

    fn add_request(pubkeys: &[u8]) -> SubscribeRequest {
        account_filter_request("add", pubkeys)
    }

    fn add_remove_request(add: &[u8], remove: &[u8]) -> SubscribeRequest {
        let mut request = add_request(add);
        request.accounts.insert(
            "remove".to_owned(),
            SubscribeRequestFilterAccounts {
                account: remove.iter().map(|byte| pubkey_b58(*byte)).collect(),
                owner: Vec::new(),
                filters: Vec::new(),
                nonempty_txn_signature: None,
            },
        );
        request
    }

    fn snapshot_state(byte: u8) -> AccountState {
        AccountState {
            pubkey_b58: pubkey_b58(byte),
            pubkey_bytes: pubkey_bytes(byte).to_vec(),
            owner_b58: bytes_to_base58(&pubkey_bytes(byte.wrapping_add(32))),
            slot: 10,
            lamports: 55,
            executable: false,
            rent_epoch: 2,
            write_version: 3,
            txn_signature_b58: Some(bytes_to_base58(&[7; 64])),
            data_len: 9,
        }
    }

    #[derive(Clone)]
    struct FakeSnapshotStore {
        state: Arc<Mutex<FakeSnapshotStoreState>>,
    }

    struct FakeSnapshotStoreState {
        fetch_filtered_result: Result<Vec<AccountState>, &'static str>,
        fetch_one_results:
            HashMap<String, Result<Option<AccountState>, &'static str>>,
        requested_pubkeys: Vec<String>,
    }

    impl FakeSnapshotStore {
        fn new(
            fetch_one_results: HashMap<
                String,
                Result<Option<AccountState>, &'static str>,
            >,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeSnapshotStoreState {
                    fetch_filtered_result: Ok(Vec::new()),
                    fetch_one_results,
                    requested_pubkeys: Vec::new(),
                })),
            }
        }

        fn requested_pubkeys(&self) -> Vec<String> {
            self.state.lock().unwrap().requested_pubkeys.clone()
        }
    }

    impl SnapshotStore for FakeSnapshotStore {
        fn fetch_filtered(
            &self,
            _filter: Option<&PubkeyFilter>,
        ) -> impl std::future::Future<Output = GeykagResult<Vec<AccountState>>> + Send
        {
            let result =
                self.state.lock().unwrap().fetch_filtered_result.clone();
            async move {
                result.map_err(|message| {
                    GeykagError::GrpcEventConversion(message.to_owned())
                })
            }
        }

        fn fetch_one_by_pubkey(
            &self,
            pubkey: &PubkeyFilter,
        ) -> impl std::future::Future<
            Output = GeykagResult<Option<AccountState>>,
        > + Send {
            let pubkey = pubkey.as_str().to_owned();
            let result = {
                let mut state = self.state.lock().unwrap();
                state.requested_pubkeys.push(pubkey.clone());
                state
                    .fetch_one_results
                    .get(&pubkey)
                    .cloned()
                    .unwrap_or(Ok(None))
            };

            async move {
                result.map_err(|message| {
                    GeykagError::GrpcEventConversion(message.to_owned())
                })
            }
        }
    }

    #[derive(Clone)]
    struct FakeValidatorSubscriptions {
        state: Arc<Mutex<FakeValidatorSubscriptionsState>>,
    }

    struct FakeValidatorSubscriptionsState {
        calls: Vec<Vec<String>>,
        result: Result<(), &'static str>,
    }

    impl FakeValidatorSubscriptions {
        fn succeed() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeValidatorSubscriptionsState {
                    calls: Vec::new(),
                    result: Ok(()),
                })),
            }
        }

        fn fail(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeValidatorSubscriptionsState {
                    calls: Vec::new(),
                    result: Err(message),
                })),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.state.lock().unwrap().calls.clone()
        }
    }

    impl ValidatorSubscriptions for FakeValidatorSubscriptions {
        fn whitelist_pubkeys(
            &self,
            pubkeys: &[String],
        ) -> impl std::future::Future<Output = GeykagResult<()>> + Send
        {
            let result = {
                let mut state = self.state.lock().unwrap();
                state.calls.push(pubkeys.to_vec());
                state.result
            };

            async move {
                result.map_err(|message| {
                    GeykagError::GrpcEventConversion(message.to_owned())
                })
            }
        }
    }

    #[test]
    fn test_parse_accounts_filter_parses_accounts() {
        let request = replace_request(&[1, 2]);

        let parsed = parse_accounts_filter(&request).unwrap();

        assert_eq!(
            parsed,
            [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect()
        );
    }

    #[test]
    fn test_parse_pubkey_list_parses_accounts() {
        let parsed =
            parse_pubkey_list(&[pubkey_b58(1), pubkey_b58(2)]).unwrap();

        assert_eq!(
            parsed,
            [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect()
        );
    }

    #[test]
    fn test_parse_filter_op_parses_replace_request() {
        match parse_filter_op(&replace_request(&[1, 2])).unwrap() {
            FilterOp::Replace(filter) => assert_eq!(
                filter,
                [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect()
            ),
            FilterOp::Patch { .. } => panic!("expected replace filter op"),
        }
    }

    #[test]
    fn test_parse_filter_op_parses_patch_request() {
        match parse_filter_op(&add_remove_request(&[1, 2], &[3])).unwrap() {
            FilterOp::Patch { add, remove } => {
                assert_eq!(
                    add,
                    [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect()
                );
                assert_eq!(remove, [pubkey_bytes(3)].into_iter().collect());
            }
            FilterOp::Replace(_) => panic!("expected patch filter op"),
        }
    }

    #[tokio::test]
    async fn test_existing_snapshot_sends_targeted_update_and_does_not_whitelist()
     {
        let dispatcher = DispatcherHandle::spawn(8, 8);
        let (client_id, mut rx) = dispatcher
            .add_client([pubkey_bytes(1)].into_iter().collect(), 8)
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let snapshot_store = FakeSnapshotStore::new(HashMap::from([(
            pubkey_b58(1),
            Ok(Some(snapshot_state(1))),
        )]));
        let validator = FakeValidatorSubscriptions::succeed();

        bootstrap_new_pubkeys_impl(
            &dispatcher,
            &snapshot_store,
            &validator,
            client_id,
            [pubkey_bytes(1)].into_iter().collect(),
        )
        .await;

        let delivered = timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match &delivered.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                assert_eq!(
                    account.account.as_ref().unwrap().pubkey,
                    pubkey_bytes(1).to_vec()
                );
            }
            other => panic!("expected account update, got {other:?}"),
        }
        assert!(validator.calls().is_empty());
    }

    #[tokio::test]
    async fn test_missing_snapshot_whitelists_pubkey_and_sends_no_targeted_update()
     {
        let dispatcher = DispatcherHandle::spawn(8, 8);
        let (client_id, mut rx) = dispatcher
            .add_client([pubkey_bytes(1)].into_iter().collect(), 8)
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let snapshot_store =
            FakeSnapshotStore::new(HashMap::from([(pubkey_b58(1), Ok(None))]));
        let validator = FakeValidatorSubscriptions::succeed();

        bootstrap_new_pubkeys_impl(
            &dispatcher,
            &snapshot_store,
            &validator,
            client_id,
            [pubkey_bytes(1)].into_iter().collect(),
        )
        .await;

        assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());
        assert_eq!(validator.calls(), vec![vec![pubkey_b58(1)]]);
    }

    #[tokio::test]
    async fn test_snapshot_fetch_error_skips_pubkey_and_continues_with_rest() {
        let dispatcher = DispatcherHandle::spawn(8, 8);
        let (client_id, _rx) = dispatcher
            .add_client(
                [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect(),
                8,
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let snapshot_store = FakeSnapshotStore::new(HashMap::from([
            (pubkey_b58(1), Err("fetch failed")),
            (pubkey_b58(2), Ok(None)),
        ]));
        let validator = FakeValidatorSubscriptions::succeed();

        bootstrap_new_pubkeys_impl(
            &dispatcher,
            &snapshot_store,
            &validator,
            client_id,
            [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect(),
        )
        .await;

        let requested: HashSet<_> =
            snapshot_store.requested_pubkeys().into_iter().collect();
        assert_eq!(
            requested,
            [pubkey_b58(1), pubkey_b58(2)].into_iter().collect()
        );
        assert_eq!(validator.calls(), vec![vec![pubkey_b58(2)]]);
    }

    #[tokio::test]
    async fn test_invalid_snapshot_conversion_skips_pubkey() {
        let dispatcher = DispatcherHandle::spawn(8, 8);
        let (client_id, mut rx) = dispatcher
            .add_client([pubkey_bytes(1)].into_iter().collect(), 8)
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let mut invalid_snapshot = snapshot_state(1);
        invalid_snapshot.owner_b58 = "0invalid-owner".to_owned();
        let snapshot_store = FakeSnapshotStore::new(HashMap::from([(
            pubkey_b58(1),
            Ok(Some(invalid_snapshot)),
        )]));
        let validator = FakeValidatorSubscriptions::succeed();

        bootstrap_new_pubkeys_impl(
            &dispatcher,
            &snapshot_store,
            &validator,
            client_id,
            [pubkey_bytes(1)].into_iter().collect(),
        )
        .await;

        assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());
        assert!(validator.calls().is_empty());
    }

    #[tokio::test]
    async fn test_client_not_found_stops_bootstrap_loop() {
        let dispatcher = DispatcherHandle::spawn(8, 8);
        let snapshot_store = FakeSnapshotStore::new(HashMap::from([
            (pubkey_b58(1), Ok(Some(snapshot_state(1)))),
            (pubkey_b58(2), Ok(Some(snapshot_state(2)))),
        ]));
        let validator = FakeValidatorSubscriptions::succeed();

        bootstrap_new_pubkeys_impl(
            &dispatcher,
            &snapshot_store,
            &validator,
            999,
            [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect(),
        )
        .await;

        assert_eq!(snapshot_store.requested_pubkeys().len(), 1);
        assert!(validator.calls().is_empty());
    }

    #[tokio::test]
    async fn test_validator_whitelist_errors_are_swallowed_after_collecting_missing_pubkeys()
     {
        let dispatcher = DispatcherHandle::spawn(8, 8);
        let (client_id, _rx) = dispatcher
            .add_client(
                [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect(),
                8,
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let snapshot_store = FakeSnapshotStore::new(HashMap::from([
            (pubkey_b58(1), Ok(None)),
            (pubkey_b58(2), Ok(None)),
        ]));
        let validator = FakeValidatorSubscriptions::fail("whitelist failed");

        bootstrap_new_pubkeys_impl(
            &dispatcher,
            &snapshot_store,
            &validator,
            client_id,
            [pubkey_bytes(1), pubkey_bytes(2)].into_iter().collect(),
        )
        .await;

        let calls = validator.calls();
        assert_eq!(calls.len(), 1);
        let whitelisted: HashSet<_> = calls[0].iter().cloned().collect();
        assert_eq!(
            whitelisted,
            [pubkey_b58(1), pubkey_b58(2)].into_iter().collect()
        );
    }
}
