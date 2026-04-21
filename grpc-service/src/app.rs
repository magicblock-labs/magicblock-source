use crate::config::Config;
use crate::domain::AccountEvent;
use crate::errors::GeykagResult;
use crate::grpc_service::{GrpcService, GrpcServiceHandle, GrpcSink};
use crate::kafka::KafkaAccountUpdateStream;
use crate::ksql::KsqlAccountSnapshotClient;
use crate::output::{ConsoleSink, TeeSink};
use crate::traits::{AccountSink, AccountUpdateSource, SnapshotStore, StatusSink};

pub struct App<P: SnapshotStore, K: AccountUpdateSource, A: AccountSink, S: StatusSink> {
    config: Config,
    snapshot_store: P,
    account_update_source: K,
    sink: A,
    status_sink: S,
}

impl App<KsqlAccountSnapshotClient, KafkaAccountUpdateStream, ConsoleSink, ConsoleSink> {
    #[allow(dead_code)]
    pub fn new(config: Config) -> GeykagResult<Self> {
        let snapshot_store = KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let account_update_source = KafkaAccountUpdateStream::new(config.kafka.clone());
        Ok(Self::build(
            config,
            snapshot_store,
            account_update_source,
            ConsoleSink::new(),
            ConsoleSink::new(),
        ))
    }
}

impl App<KsqlAccountSnapshotClient, KafkaAccountUpdateStream, GrpcSink, ConsoleSink> {
    #[allow(dead_code)]
    pub fn new_grpc(
        config: Config,
    ) -> GeykagResult<(Self, GrpcServiceHandle)> {
        let grpc = GrpcService::start(&config)?;
        let sink = grpc.sink();
        let snapshot_store = KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let account_update_source = KafkaAccountUpdateStream::new(config.kafka.clone());
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

impl App<
    KsqlAccountSnapshotClient,
    KafkaAccountUpdateStream,
    TeeSink<GrpcSink, ConsoleSink>,
    ConsoleSink,
> {
    pub fn new_grpc_with_console(
        config: Config,
    ) -> GeykagResult<(Self, GrpcServiceHandle)> {
        let grpc = GrpcService::start(&config)?;
        let sink = TeeSink::new(grpc.sink(), ConsoleSink::new());
        let snapshot_store = KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let account_update_source = KafkaAccountUpdateStream::new(config.kafka.clone());
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

impl<P: SnapshotStore, K: AccountUpdateSource, A: AccountSink, S: StatusSink> App<P, K, A, S> {
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
