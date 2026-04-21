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
use super::init_subs::InitSubsClient;
use crate::domain::{AccountEvent, PubkeyFilter};
use crate::ksql::KsqlAccountSnapshotClient;

type SubscribeStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<SubscribeUpdate, Status>>
            + Send
            + 'static,
    >,
>;

#[derive(Clone, Debug)]
pub(crate) struct GrpcSubscriptionService {
    dispatcher: DispatcherHandle,
    snapshot_client: KsqlAccountSnapshotClient,
    init_subs_client: InitSubsClient,
}

impl GrpcSubscriptionService {
    pub(crate) fn new(
        dispatcher: DispatcherHandle,
        snapshot_client: KsqlAccountSnapshotClient,
        init_subs_client: InitSubsClient,
    ) -> Self {
        Self {
            dispatcher,
            snapshot_client,
            init_subs_client,
        }
    }

    pub(crate) fn into_server(self) -> GeyserServer<Self> {
        GeyserServer::new(self)
    }
}

async fn bootstrap_new_pubkeys(
    dispatcher: &DispatcherHandle,
    snapshot_client: &KsqlAccountSnapshotClient,
    init_subs_client: &InitSubsClient,
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
        let snapshot = match snapshot_client.fetch_one_by_pubkey(&pubkey).await
        {
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

    if let Err(error) = init_subs_client
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
impl Geyser for GrpcSubscriptionService {
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
        let snapshot_client = self.snapshot_client.clone();
        let init_subs_client = self.init_subs_client.clone();
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

                        bootstrap_new_pubkeys(
                            &dispatcher,
                            &snapshot_client,
                            &init_subs_client,
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
