use crate::config::Config;
use crate::domain::AccountEvent;
use crate::errors::GeykagResult;
use crate::grpc_service::{GrpcService, GrpcServiceHandle, GrpcSink};
use crate::kafka::KafkaAccountUpdateStream;
use crate::ksql::KsqlAccountSnapshotClient;
use crate::output::{ConsoleSink, TeeSink};
use crate::traits::{AccountSink, StatusSink};

pub struct App<A: AccountSink, S: StatusSink> {
    config: Config,
    snapshot_client: KsqlAccountSnapshotClient,
    kafka_stream: KafkaAccountUpdateStream,
    sink: A,
    status_sink: S,
}

impl App<ConsoleSink, ConsoleSink> {
    #[allow(dead_code)]
    pub fn new(config: Config) -> GeykagResult<Self> {
        Self::build(config, ConsoleSink::new(), ConsoleSink::new())
    }
}

impl App<GrpcSink, ConsoleSink> {
    #[allow(dead_code)]
    pub fn new_grpc(config: Config) -> GeykagResult<(Self, GrpcServiceHandle)> {
        let grpc = GrpcService::start(&config)?;
        let sink = grpc.sink();
        let app = Self::build(config, sink, ConsoleSink::new())?;

        Ok((app, grpc))
    }
}

impl App<TeeSink<GrpcSink, ConsoleSink>, ConsoleSink> {
    pub fn new_grpc_with_console(
        config: Config,
    ) -> GeykagResult<(Self, GrpcServiceHandle)> {
        let grpc = GrpcService::start(&config)?;
        let sink = TeeSink::new(grpc.sink(), ConsoleSink::new());
        let app = Self::build(config, sink, ConsoleSink::new())?;

        Ok((app, grpc))
    }
}

impl<A: AccountSink, S: StatusSink> App<A, S> {
    fn build(config: Config, sink: A, status_sink: S) -> GeykagResult<Self> {
        let snapshot_client =
            KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let kafka_stream = KafkaAccountUpdateStream::new(config.kafka.clone());

        Ok(Self {
            config,
            snapshot_client,
            kafka_stream,
            sink,
            status_sink,
        })
    }

    pub async fn run(&self) -> GeykagResult<()> {
        let snapshots = self
            .snapshot_client
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

        self.kafka_stream
            .run(self.config.pubkey_filter.as_ref(), |message| {
                let event = AccountEvent::Live(message);
                self.sink.write_event(&event)
            })
            .await
    }
}
