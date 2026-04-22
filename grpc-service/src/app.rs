use crate::config::Config;
use crate::domain::AccountEvent;
use crate::errors::GeykagResult;
use crate::grpc_service::{GrpcService, GrpcServiceHandle, GrpcSink};
use crate::kafka::KafkaAccountUpdateStream;
use crate::ksql::KsqlAccountSnapshotClient;
use crate::output::{ConsoleSink, TeeSink};
use crate::traits::{
    AccountSink, AccountUpdateSource, SnapshotStore, StatusSink,
};

pub struct App<
    P: SnapshotStore,
    K: AccountUpdateSource,
    A: AccountSink,
    S: StatusSink,
> {
    config: Config,
    snapshot_store: P,
    account_update_source: K,
    sink: A,
    status_sink: S,
}

impl
    App<
        KsqlAccountSnapshotClient,
        KafkaAccountUpdateStream,
        ConsoleSink,
        ConsoleSink,
    >
{
    #[allow(dead_code)]
    pub fn new(config: Config) -> GeykagResult<Self> {
        let snapshot_store =
            KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let account_update_source =
            KafkaAccountUpdateStream::new(config.kafka.clone());
        Ok(Self::build(
            config,
            snapshot_store,
            account_update_source,
            ConsoleSink::new(),
            ConsoleSink::new(),
        ))
    }
}

impl
    App<
        KsqlAccountSnapshotClient,
        KafkaAccountUpdateStream,
        GrpcSink,
        ConsoleSink,
    >
{
    #[allow(dead_code)]
    pub fn new_grpc(config: Config) -> GeykagResult<(Self, GrpcServiceHandle)> {
        let grpc = GrpcService::start(&config)?;
        let sink = grpc.sink();
        let snapshot_store =
            KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let account_update_source =
            KafkaAccountUpdateStream::new(config.kafka.clone());
        let app = Self::build(
            config,
            snapshot_store,
            account_update_source,
            sink,
            ConsoleSink::new(),
        );

        Ok((app, grpc))
    }
}

impl
    App<
        KsqlAccountSnapshotClient,
        KafkaAccountUpdateStream,
        TeeSink<GrpcSink, ConsoleSink>,
        ConsoleSink,
    >
{
    pub fn new_grpc_with_console(
        config: Config,
    ) -> GeykagResult<(Self, GrpcServiceHandle)> {
        let grpc = GrpcService::start(&config)?;
        let sink = TeeSink::new(grpc.sink(), ConsoleSink::new());
        let snapshot_store =
            KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let account_update_source =
            KafkaAccountUpdateStream::new(config.kafka.clone());
        let app = Self::build(
            config,
            snapshot_store,
            account_update_source,
            sink,
            ConsoleSink::new(),
        );

        Ok((app, grpc))
    }
}

impl<P: SnapshotStore, K: AccountUpdateSource, A: AccountSink, S: StatusSink>
    App<P, K, A, S>
{
    pub fn build(
        config: Config,
        snapshot_store: P,
        account_update_source: K,
        sink: A,
        status_sink: S,
    ) -> Self {
        Self {
            config,
            snapshot_store,
            account_update_source,
            sink,
            status_sink,
        }
    }

    pub async fn run(&self) -> GeykagResult<()> {
        let snapshots = self
            .snapshot_store
            .fetch_filtered(self.config.pubkey_filter.as_ref())
            .await?;

        if snapshots.is_empty() {
            if let Some(filter) = self.config.pubkey_filter.as_ref() {
                self.status_sink.write_status(&format!(
                    "No current ksql entry found for pubkey {}",
                    filter.as_str()
                ))?;
            } else {
                self.status_sink
                    .write_status("No current ksql account entries found.")?;
            }
        } else {
            for snapshot in snapshots {
                let event = AccountEvent::Snapshot(snapshot);
                self.sink.write_event(&event)?;
            }
        }

        self.account_update_source
            .run(self.config.pubkey_filter.as_ref(), |message| {
                let event = AccountEvent::Live(message);
                self.sink.write_event(&event)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::App;
    use crate::config::{
        Config, GrpcConfig, KafkaConfig, KsqlConfig, ValidatorConfig,
    };
    use crate::domain::{
        AccountEvent, AccountState, AccountUpdate, PubkeyFilter,
        bytes_to_base58,
    };
    use crate::errors::{GeykagError, GeykagResult};
    use crate::kafka::StreamMessage;
    use crate::traits::{
        AccountSink, AccountUpdateSource, SnapshotStore, StatusSink,
    };

    fn config(pubkey_filter: Option<PubkeyFilter>) -> Config {
        Config {
            kafka: KafkaConfig {
                bootstrap_servers: "localhost:9092".to_owned(),
                topic: "accounts".to_owned(),
                group_id: "tests".to_owned(),
                auto_offset_reset: "earliest".to_owned(),
                client: BTreeMap::new(),
            },
            ksql: KsqlConfig {
                url: "http://localhost:8088".to_owned(),
                table: "ACCOUNTS".to_owned(),
            },
            validator: ValidatorConfig {
                accounts_filter_url: "http://localhost:3000/filters/accounts"
                    .to_owned(),
            },
            grpc: GrpcConfig {
                bind_host: "127.0.0.1".to_owned(),
                port: 50051,
                dispatcher_capacity: 64,
            },
            pubkey_filter,
        }
    }

    fn account_state(byte: u8) -> AccountState {
        AccountState {
            pubkey_b58: bytes_to_base58(&[byte; 32]),
            pubkey_bytes: vec![byte; 32],
            owner_b58: bytes_to_base58(&[byte.wrapping_add(1); 32]),
            slot: 10,
            lamports: 100,
            executable: false,
            rent_epoch: 2,
            write_version: 3,
            txn_signature_b58: Some(bytes_to_base58(&[7; 64])),
            data_len: 5,
        }
    }

    fn stream_message(byte: u8) -> StreamMessage {
        StreamMessage {
            account: AccountUpdate {
                pubkey_b58: bytes_to_base58(&[byte; 32]),
                pubkey_bytes: vec![byte; 32],
                owner_b58: bytes_to_base58(&[byte.wrapping_add(1); 32]),
                slot: 20,
                lamports: 200,
                executable: true,
                rent_epoch: 4,
                write_version: 5,
                txn_signature_b58: Some(bytes_to_base58(&[8; 64])),
                data_len: 6,
                data_version: 7,
                account_age: 8,
            },
            partition: 1,
            offset: 2,
            timestamp: "ts".to_owned(),
        }
    }

    #[derive(Clone)]
    struct MockSnapshotStore {
        state: Arc<Mutex<Result<Vec<AccountState>, &'static str>>>,
    }

    impl MockSnapshotStore {
        fn new(
            fetch_filtered_result: Result<Vec<AccountState>, &'static str>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(fetch_filtered_result)),
            }
        }
    }

    impl SnapshotStore for MockSnapshotStore {
        fn fetch_filtered(
            &self,
            _filter: Option<&PubkeyFilter>,
        ) -> impl std::future::Future<Output = GeykagResult<Vec<AccountState>>> + Send
        {
            let result = self.state.lock().unwrap().clone();
            async move {
                result.map_err(|message| {
                    GeykagError::GrpcEventConversion(message.to_owned())
                })
            }
        }

        async fn fetch_one_by_pubkey(
            &self,
            _pubkey: &PubkeyFilter,
        ) -> GeykagResult<Option<AccountState>> {
            Ok(None)
        }
    }

    #[derive(Clone)]
    struct MockAccountUpdateSource {
        state: Arc<Mutex<MockAccountUpdateSourceState>>,
    }

    struct MockAccountUpdateSourceState {
        scripted: VecDeque<Result<StreamMessage, &'static str>>,
        called: bool,
    }

    impl MockAccountUpdateSource {
        fn new(scripted: Vec<Result<StreamMessage, &'static str>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockAccountUpdateSourceState {
                    scripted: scripted.into(),
                    called: false,
                })),
            }
        }

        fn called(&self) -> bool {
            self.state.lock().unwrap().called
        }
    }

    impl AccountUpdateSource for MockAccountUpdateSource {
        fn run<H>(
            &self,
            _filter: Option<&PubkeyFilter>,
            mut handler: H,
        ) -> impl std::future::Future<Output = GeykagResult<()>> + Send
        where
            H: FnMut(StreamMessage) -> GeykagResult<()> + Send,
        {
            let scripted = {
                let mut state = self.state.lock().unwrap();
                state.called = true;
                state.scripted.drain(..).collect::<Vec<_>>()
            };

            async move {
                for item in scripted {
                    match item {
                        Ok(message) => handler(message)?,
                        Err(message) => {
                            return Err(GeykagError::GrpcEventConversion(
                                message.to_owned(),
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }

    #[derive(Clone)]
    struct RecordingSink {
        state: Arc<Mutex<RecordingSinkState>>,
    }

    struct RecordingSinkState {
        events: Vec<AccountEvent>,
        fail_on_snapshot: bool,
        fail_on_live: bool,
    }

    impl RecordingSink {
        fn new(fail_on_snapshot: bool, fail_on_live: bool) -> Self {
            Self {
                state: Arc::new(Mutex::new(RecordingSinkState {
                    events: Vec::new(),
                    fail_on_snapshot,
                    fail_on_live,
                })),
            }
        }

        fn events(&self) -> Vec<AccountEvent> {
            self.state.lock().unwrap().events.clone()
        }
    }

    impl AccountSink for RecordingSink {
        fn write_event(&self, event: &AccountEvent) -> GeykagResult<()> {
            let mut state = self.state.lock().unwrap();
            match event {
                AccountEvent::Snapshot(_) if state.fail_on_snapshot => {
                    return Err(GeykagError::GrpcEventConversion(
                        "snapshot sink failure".to_owned(),
                    ));
                }
                AccountEvent::Live(_) if state.fail_on_live => {
                    return Err(GeykagError::GrpcEventConversion(
                        "live sink failure".to_owned(),
                    ));
                }
                _ => {}
            }

            state.events.push(event.clone());
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingStatusSink {
        state: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingStatusSink {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn statuses(&self) -> Vec<String> {
            self.state.lock().unwrap().clone()
        }
    }

    impl StatusSink for RecordingStatusSink {
        fn write_status(&self, status: &str) -> GeykagResult<()> {
            self.state.lock().unwrap().push(status.to_owned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_run_replays_snapshots_then_live_updates() {
        let snapshot_store = MockSnapshotStore::new(Ok(vec![account_state(1)]));
        let update_source =
            MockAccountUpdateSource::new(vec![Ok(stream_message(2))]);
        let sink = RecordingSink::new(false, false);
        let status_sink = RecordingStatusSink::new();
        let app = App::build(
            config(None),
            snapshot_store,
            update_source.clone(),
            sink.clone(),
            status_sink.clone(),
        );

        app.run().await.unwrap();

        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], AccountEvent::Snapshot(_)));
        assert!(matches!(events[1], AccountEvent::Live(_)));
        assert!(status_sink.statuses().is_empty());
        assert!(update_source.called());
    }

    #[tokio::test]
    async fn test_run_without_snapshots_and_without_filter_writes_generic_status()
     {
        let snapshot_store = MockSnapshotStore::new(Ok(Vec::new()));
        let update_source = MockAccountUpdateSource::new(Vec::new());
        let status_sink = RecordingStatusSink::new();
        let app = App::build(
            config(None),
            snapshot_store,
            update_source.clone(),
            RecordingSink::new(false, false),
            status_sink.clone(),
        );

        app.run().await.unwrap();

        assert_eq!(
            status_sink.statuses(),
            vec!["No current ksql account entries found.".to_owned()]
        );
        assert!(update_source.called());
    }

    #[tokio::test]
    async fn test_run_without_snapshots_and_with_filter_writes_specific_status()
    {
        let filter = PubkeyFilter::parse(&bytes_to_base58(&[9; 32])).unwrap();
        let snapshot_store = MockSnapshotStore::new(Ok(Vec::new()));
        let update_source = MockAccountUpdateSource::new(Vec::new());
        let status_sink = RecordingStatusSink::new();
        let app = App::build(
            config(Some(filter.clone())),
            snapshot_store,
            update_source.clone(),
            RecordingSink::new(false, false),
            status_sink.clone(),
        );

        app.run().await.unwrap();

        assert_eq!(
            status_sink.statuses(),
            vec![format!(
                "No current ksql entry found for pubkey {}",
                filter.as_str()
            )]
        );
        assert!(update_source.called());
    }

    #[tokio::test]
    async fn test_snapshot_fetch_error_returns_immediately_and_never_calls_update_source()
     {
        let snapshot_store =
            MockSnapshotStore::new(Err("snapshot fetch failed"));
        let update_source =
            MockAccountUpdateSource::new(vec![Ok(stream_message(1))]);
        let app = App::build(
            config(None),
            snapshot_store,
            update_source.clone(),
            RecordingSink::new(false, false),
            RecordingStatusSink::new(),
        );

        let error = app.run().await.unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
        assert!(!update_source.called());
    }

    #[tokio::test]
    async fn test_snapshot_sink_failure_returns_immediately_and_never_calls_update_source()
     {
        let snapshot_store = MockSnapshotStore::new(Ok(vec![account_state(1)]));
        let update_source =
            MockAccountUpdateSource::new(vec![Ok(stream_message(1))]);
        let app = App::build(
            config(None),
            snapshot_store,
            update_source.clone(),
            RecordingSink::new(true, false),
            RecordingStatusSink::new(),
        );

        let error = app.run().await.unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
        assert!(!update_source.called());
    }

    #[tokio::test]
    async fn test_live_sink_failure_propagates_from_update_source_callback() {
        let snapshot_store = MockSnapshotStore::new(Ok(Vec::new()));
        let update_source =
            MockAccountUpdateSource::new(vec![Ok(stream_message(1))]);
        let app = App::build(
            config(None),
            snapshot_store,
            update_source.clone(),
            RecordingSink::new(false, true),
            RecordingStatusSink::new(),
        );

        let error = app.run().await.unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
        assert!(update_source.called());
    }

    #[tokio::test]
    async fn test_update_source_error_propagates() {
        let snapshot_store = MockSnapshotStore::new(Ok(Vec::new()));
        let update_source =
            MockAccountUpdateSource::new(vec![Err("source failed")]);
        let app = App::build(
            config(None),
            snapshot_store,
            update_source.clone(),
            RecordingSink::new(false, false),
            RecordingStatusSink::new(),
        );

        let error = app.run().await.unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
        assert!(update_source.called());
    }
}
