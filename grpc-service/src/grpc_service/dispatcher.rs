use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use helius_laserstream::grpc::{
    SubscribeUpdate, subscribe_update::UpdateOneof,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use super::utils::millis_since;

type ClientId = u64;

struct ClientEntry {
    filter: HashSet<[u8; 32]>,
    health: ClientHealth,
    tx: mpsc::Sender<Arc<SubscribeUpdate>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryFailureKind {
    ChannelFull,
    ChannelClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryOutcome {
    Delivered,
    Failed(DeliveryFailureKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientRemovalReason {
    ClosedChannel,
    ConsecutiveFailures,
    BackpressureTimeout,
}

#[derive(Clone, Debug, Default)]
struct ClientHealth {
    consecutive_failures: u32,
    last_success_at: Option<Instant>,
    last_failure_at: Option<Instant>,
    backpressure_since: Option<Instant>,
    last_failure_kind: Option<DeliveryFailureKind>,
}

impl ClientHealth {
    fn new() -> Self {
        Self::default()
    }
}

const MAX_CONSECUTIVE_DELIVERY_FAILURES: u32 = 8;
const MAX_BACKPRESSURE_AGE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientDeliveryResult {
    Delivered,
    FailedButRetained,
    RemovedByPolicy(ClientRemovalReason),
    ClientNotFound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TargetedSendResult {
    Delivered,
    ClientNotFound,
    FailedButRetained,
    RemovedByPolicy,
}

impl From<ClientDeliveryResult> for TargetedSendResult {
    fn from(value: ClientDeliveryResult) -> Self {
        match value {
            ClientDeliveryResult::Delivered => Self::Delivered,
            ClientDeliveryResult::ClientNotFound => Self::ClientNotFound,
            ClientDeliveryResult::FailedButRetained => Self::FailedButRetained,
            ClientDeliveryResult::RemovedByPolicy(_) => Self::RemovedByPolicy,
        }
    }
}

pub(crate) enum DispatcherCommand {
    AddClient {
        client_id: ClientId,
        filter: HashSet<[u8; 32]>,
        tx: mpsc::Sender<Arc<SubscribeUpdate>>,
    },
    RemoveClient {
        client_id: ClientId,
    },
    UpdateFilter {
        client_id: ClientId,
        filter: HashSet<[u8; 32]>,
        response_tx: oneshot::Sender<HashSet<[u8; 32]>>,
    },
    PatchFilter {
        client_id: ClientId,
        add: HashSet<[u8; 32]>,
        remove: HashSet<[u8; 32]>,
        response_tx: oneshot::Sender<HashSet<[u8; 32]>>,
    },
    SendToClient {
        client_id: ClientId,
        update: Arc<SubscribeUpdate>,
        response_tx: oneshot::Sender<TargetedSendResult>,
    },
}

struct Dispatcher {
    clients: Vec<(ClientId, ClientEntry)>,
    update_rx: mpsc::Receiver<Arc<SubscribeUpdate>>,
    command_rx: mpsc::Receiver<DispatcherCommand>,
}

impl Dispatcher {
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(update) = self.update_rx.recv() => {
                    self.fan_out(&update);
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_command(cmd);
                }
                else => break,
            }
        }
        info!("dispatcher shut down");
    }

    fn fan_out(&mut self, update: &Arc<SubscribeUpdate>) {
        let Some(pubkey) = extract_pubkey(update) else {
            return;
        };

        let target_client_ids = self
            .clients
            .iter()
            .filter_map(|(id, entry)| {
                entry.filter.contains(pubkey).then_some(*id)
            })
            .collect::<Vec<_>>();

        for client_id in target_client_ids {
            let _ = self.deliver_to_client(client_id, update);
        }
    }

    fn handle_command(&mut self, cmd: DispatcherCommand) {
        match cmd {
            DispatcherCommand::AddClient {
                client_id,
                filter,
                tx,
            } => {
                info!(
                    client_id,
                    filter_size = filter.len(),
                    "client registered"
                );
                self.clients.push((
                    client_id,
                    ClientEntry {
                        filter,
                        health: ClientHealth::new(),
                        tx,
                    },
                ));
            }
            DispatcherCommand::RemoveClient { client_id } => {
                let original_len = self.clients.len();
                self.clients.retain(|(id, _)| *id != client_id);
                if self.clients.len() < original_len {
                    info!(client_id, "client deregistered");
                } else {
                    debug!(client_id, "RemoveClient for unknown client");
                }
            }
            DispatcherCommand::UpdateFilter {
                client_id,
                filter,
                response_tx,
            } => {
                let mut newly_added = HashSet::new();

                if let Some((_id, entry)) =
                    self.clients.iter_mut().find(|(id, _)| *id == client_id)
                {
                    newly_added = filter
                        .difference(&entry.filter)
                        .copied()
                        .collect::<HashSet<_>>();
                    info!(
                        client_id,
                        filter_size = filter.len(),
                        "filter replaced"
                    );
                    entry.filter = filter;
                } else {
                    warn!(client_id, "UpdateFilter for unknown client");
                }

                let _ = response_tx.send(newly_added);
            }
            DispatcherCommand::PatchFilter {
                client_id,
                add,
                remove,
                response_tx,
            } => {
                let mut newly_added = HashSet::new();

                if let Some((_id, entry)) =
                    self.clients.iter_mut().find(|(id, _)| *id == client_id)
                {
                    newly_added = add
                        .difference(&entry.filter)
                        .copied()
                        .collect::<HashSet<_>>();
                    entry.filter.extend(newly_added.iter());
                    for key in &remove {
                        entry.filter.remove(key);
                    }
                    info!(
                        client_id,
                        filter_size = entry.filter.len(),
                        "filter patched"
                    );
                } else {
                    warn!(client_id, "PatchFilter for unknown client");
                }

                let _ = response_tx.send(newly_added);
            }
            DispatcherCommand::SendToClient {
                client_id,
                update,
                response_tx,
            } => {
                let result = self.deliver_to_client(client_id, &update);
                let _ = response_tx.send(result.into());
            }
        }
    }

    fn deliver_to_client(
        &mut self,
        client_id: ClientId,
        update: &Arc<SubscribeUpdate>,
    ) -> ClientDeliveryResult {
        let Some(index) =
            self.clients.iter().position(|(id, _)| *id == client_id)
        else {
            warn!(client_id, "SendToClient for unknown client");
            return ClientDeliveryResult::ClientNotFound;
        };

        let now = Instant::now();
        let delivery_outcome = {
            let (_, entry) = &mut self.clients[index];
            let outcome = try_deliver_update(&entry.tx, update);
            record_delivery_outcome(&mut entry.health, outcome, now);
            outcome
        };

        match delivery_outcome {
            DeliveryOutcome::Delivered => ClientDeliveryResult::Delivered,
            DeliveryOutcome::Failed(failure_kind) => {
                match failure_kind {
                    DeliveryFailureKind::ChannelFull => {
                        warn!(client_id, "client channel full");
                    }
                    DeliveryFailureKind::ChannelClosed => {
                        debug!(client_id, "client channel closed");
                    }
                }

                let removal_reason = {
                    let (_, entry) = &self.clients[index];
                    evaluate_client_health(&entry.health, now)
                };

                if let Some(reason) = removal_reason {
                    self.remove_client_with_reason(client_id, reason, now);
                    ClientDeliveryResult::RemovedByPolicy(reason)
                } else {
                    ClientDeliveryResult::FailedButRetained
                }
            }
        }
    }

    fn remove_client_with_reason(
        &mut self,
        client_id: ClientId,
        reason: ClientRemovalReason,
        now: Instant,
    ) {
        let Some(index) =
            self.clients.iter().position(|(id, _)| *id == client_id)
        else {
            debug!(client_id, ?reason, "client already removed");
            return;
        };

        let (_, entry) = self.clients.remove(index);
        info!(
            client_id,
            ?reason,
            consecutive_failures = entry.health.consecutive_failures,
            last_failure_kind = ?entry.health.last_failure_kind,
            since_last_success_ms = millis_since(now, entry.health.last_success_at),
            backpressure_age_ms = millis_since(now, entry.health.backpressure_since),
            "client removed by delivery policy"
        );
    }
}

#[derive(Debug, Error)]
pub(crate) enum DispatcherCommandError {
    #[error("dispatcher command channel closed")]
    CommandChannelClosed,
    #[error("dispatcher response channel closed")]
    ResponseChannelClosed,
}

fn try_deliver_update(
    tx: &mpsc::Sender<Arc<SubscribeUpdate>>,
    update: &Arc<SubscribeUpdate>,
) -> DeliveryOutcome {
    match tx.try_send(Arc::clone(update)) {
        Ok(()) => DeliveryOutcome::Delivered,
        Err(mpsc::error::TrySendError::Full(_)) => {
            DeliveryOutcome::Failed(DeliveryFailureKind::ChannelFull)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            DeliveryOutcome::Failed(DeliveryFailureKind::ChannelClosed)
        }
    }
}

fn record_delivery_outcome(
    health: &mut ClientHealth,
    outcome: DeliveryOutcome,
    now: Instant,
) {
    match outcome {
        DeliveryOutcome::Delivered => {
            health.consecutive_failures = 0;
            health.last_success_at = Some(now);
            health.backpressure_since = None;
            health.last_failure_kind = None;
        }
        DeliveryOutcome::Failed(DeliveryFailureKind::ChannelFull) => {
            health.consecutive_failures += 1;
            health.last_failure_at = Some(now);
            health.backpressure_since.get_or_insert(now);
            health.last_failure_kind = Some(DeliveryFailureKind::ChannelFull);
        }
        DeliveryOutcome::Failed(DeliveryFailureKind::ChannelClosed) => {
            health.consecutive_failures += 1;
            health.last_failure_at = Some(now);
            health.backpressure_since = None;
            health.last_failure_kind = Some(DeliveryFailureKind::ChannelClosed);
        }
    }
}

fn evaluate_client_health(
    health: &ClientHealth,
    now: Instant,
) -> Option<ClientRemovalReason> {
    if health.last_failure_kind == Some(DeliveryFailureKind::ChannelClosed) {
        return Some(ClientRemovalReason::ClosedChannel);
    }

    if health.consecutive_failures >= MAX_CONSECUTIVE_DELIVERY_FAILURES {
        return Some(ClientRemovalReason::ConsecutiveFailures);
    }

    if let Some(backpressure_since) = health.backpressure_since
        && now.duration_since(backpressure_since) >= MAX_BACKPRESSURE_AGE
    {
        return Some(ClientRemovalReason::BackpressureTimeout);
    }

    None
}

fn extract_pubkey(update: &SubscribeUpdate) -> Option<&[u8; 32]> {
    match &update.update_oneof {
        Some(UpdateOneof::Account(acct)) => acct
            .account
            .as_ref()
            .and_then(|info| info.pubkey.as_slice().try_into().ok()),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DispatcherHandle {
    update_tx: mpsc::Sender<Arc<SubscribeUpdate>>,
    command_tx: mpsc::Sender<DispatcherCommand>,
    next_client_id: Arc<AtomicU64>,
}

impl DispatcherHandle {
    pub(crate) fn spawn(update_buffer: usize, command_buffer: usize) -> Self {
        let (update_tx, update_rx) = mpsc::channel(update_buffer);
        let (command_tx, command_rx) = mpsc::channel(command_buffer);

        let dispatcher = Dispatcher {
            clients: Vec::new(),
            update_rx,
            command_rx,
        };
        tokio::spawn(dispatcher.run());

        Self {
            update_tx,
            command_tx,
            next_client_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Send an update into the dispatcher for fan-out (non-blocking, sync-safe).
    pub(crate) fn try_publish(
        &self,
        update: SubscribeUpdate,
    ) -> Result<(), mpsc::error::TrySendError<Arc<SubscribeUpdate>>> {
        self.update_tx.try_send(Arc::new(update))
    }

    /// Register a new client. Returns (client_id, mpsc::Receiver).
    pub(crate) async fn add_client(
        &self,
        filter: HashSet<[u8; 32]>,
        client_buffer: usize,
    ) -> Result<
        (ClientId, mpsc::Receiver<Arc<SubscribeUpdate>>),
        mpsc::error::SendError<DispatcherCommand>,
    > {
        let client_id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(client_buffer);
        self.command_tx
            .send(DispatcherCommand::AddClient {
                client_id,
                filter,
                tx,
            })
            .await?;
        Ok((client_id, rx))
    }

    /// Replace a client's filter entirely.
    pub(crate) async fn update_filter(
        &self,
        client_id: ClientId,
        filter: HashSet<[u8; 32]>,
    ) -> Result<HashSet<[u8; 32]>, DispatcherCommandError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DispatcherCommand::UpdateFilter {
                client_id,
                filter,
                response_tx,
            })
            .await
            .map_err(|_| DispatcherCommandError::CommandChannelClosed)?;

        response_rx
            .await
            .map_err(|_| DispatcherCommandError::ResponseChannelClosed)
    }

    /// Apply a delta patch to a client's filter.
    pub(crate) async fn patch_filter(
        &self,
        client_id: ClientId,
        add: HashSet<[u8; 32]>,
        remove: HashSet<[u8; 32]>,
    ) -> Result<HashSet<[u8; 32]>, DispatcherCommandError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DispatcherCommand::PatchFilter {
                client_id,
                add,
                remove,
                response_tx,
            })
            .await
            .map_err(|_| DispatcherCommandError::CommandChannelClosed)?;

        response_rx
            .await
            .map_err(|_| DispatcherCommandError::ResponseChannelClosed)
    }

    pub(crate) async fn send_to_client(
        &self,
        client_id: ClientId,
        update: SubscribeUpdate,
    ) -> Result<TargetedSendResult, DispatcherCommandError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DispatcherCommand::SendToClient {
                client_id,
                update: Arc::new(update),
                response_tx,
            })
            .await
            .map_err(|_| DispatcherCommandError::CommandChannelClosed)?;

        response_rx
            .await
            .map_err(|_| DispatcherCommandError::ResponseChannelClosed)
    }

    /// Deregister a client.
    pub(crate) async fn remove_client(&self, client_id: ClientId) {
        let _ = self
            .command_tx
            .send(DispatcherCommand::RemoveClient { client_id })
            .await;
    }
}
