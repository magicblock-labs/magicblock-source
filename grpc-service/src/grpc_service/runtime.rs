use std::net::{SocketAddr, TcpListener};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tracing::info;

use crate::config::Config;
use crate::errors::{GeykagError, GeykagResult};
use crate::ksql::KsqlAccountSnapshotClient;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::dispatcher::DispatcherHandle;
use super::init_subs::InitSubsClient;
use super::service::GrpcSubscriptionService;
use super::sink::GrpcSink;

#[derive(Debug, Default)]
pub struct GrpcService;

impl GrpcService {
    pub fn start(config: &Config) -> GeykagResult<GrpcServiceHandle> {
        let bind_address =
            format!("{}:{}", config.grpc.bind_host, config.grpc.port);
        let socket_addr: SocketAddr =
            bind_address.parse().map_err(|source| {
                GeykagError::InvalidGrpcBindAddress {
                    address: bind_address.clone(),
                    source,
                }
            })?;

        let std_listener =
            TcpListener::bind(socket_addr).map_err(|source| {
                GeykagError::GrpcBind {
                    address: bind_address.clone(),
                    source,
                }
            })?;
        std_listener.set_nonblocking(true).map_err(|source| {
            GeykagError::GrpcBind {
                address: bind_address.clone(),
                source,
            }
        })?;
        let local_addr = std_listener.local_addr().map_err(|source| {
            GeykagError::GrpcBind {
                address: bind_address.clone(),
                source,
            }
        })?;

        let listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|source| GeykagError::GrpcBind {
                address: bind_address.clone(),
                source,
            })?;

        info!(address = %local_addr, "gRPC server listening");

        let dispatcher = DispatcherHandle::spawn(
            config.grpc.dispatcher_capacity, // update buffer
            64,                              // command buffer
        );
        let is_running = Arc::new(AtomicBool::new(true));
        let sink = GrpcSink::new(dispatcher.clone(), is_running.clone());
        let snapshot_store =
            KsqlAccountSnapshotClient::new(config.ksql.clone())?;
        let validator_subscriptions =
            InitSubsClient::new(config.validator.accounts_filter_url.clone())?;
        let service = GrpcSubscriptionService::new(
            dispatcher,
            snapshot_store,
            validator_subscriptions,
        )
        .into_server();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
        });

        Ok(GrpcServiceHandle {
            sink,
            is_running,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
            local_addr,
        })
    }
}

#[derive(Debug)]
pub struct GrpcServiceHandle {
    sink: GrpcSink,
    is_running: Arc<AtomicBool>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    local_addr: SocketAddr,
}

impl GrpcServiceHandle {
    pub fn sink(&self) -> GrpcSink {
        self.sink.clone()
    }

    #[allow(dead_code)]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(mut self) -> GeykagResult<()> {
        info!(address = %self.local_addr, "gRPC server shutting down");

        self.is_running.store(false, Ordering::Release);

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        let Some(task) = self.task.take() else {
            return Ok(());
        };

        let result = match task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(source)) => Err(GeykagError::GrpcServe { source }),
            Err(source) => Err(GeykagError::GrpcTaskJoin { source }),
        };

        info!(address = %self.local_addr, "gRPC server shut down");
        result
    }
}

impl Drop for GrpcServiceHandle {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Release);

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
