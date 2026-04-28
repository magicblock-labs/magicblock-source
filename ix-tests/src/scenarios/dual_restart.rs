use anyhow::{Context, bail};

use crate::accounts::{NamedAccount, ScenarioAccounts};
use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::expectation::{
    CheckpointSpec, ClientCheckpoint, ClientCursor, ExpectedUpdate,
};
use crate::layout::ServiceInstance;
use crate::observation::ClientLog;
use crate::scenarios::ScenarioFailure;
use crate::service::{ManagedService, ServiceSpec};

pub async fn run(ctx: &ScenarioContext) -> Result<(), ScenarioFailure> {
    let spec_one = ServiceSpec::for_instance(ServiceInstance::One);
    let spec_two = ServiceSpec::for_instance(ServiceInstance::Two);
    let mut service_one = Some(
        ctx.service_controller
            .start(&spec_one, &ctx.artifacts)
            .await
            .map_err(scenario_failure_without_clients)?,
    );
    let mut service_two = Some(
        ctx.service_controller
            .start(&spec_two, &ctx.artifacts)
            .await
            .map_err(scenario_failure_without_clients)?,
    );
    let mut active_clients = Vec::new();
    let mut cursors = Vec::new();

    let result = run_inner(
        ctx,
        &spec_one,
        &spec_two,
        &mut service_one,
        &mut active_clients,
        &mut cursors,
    )
    .await;
    if let Err(error) = result {
        return Err(ScenarioFailure {
            error,
            clients: active_clients,
        });
    }

    shutdown_clients(active_clients)
        .await
        .map_err(scenario_failure_without_clients)?;
    shutdown_service(&ctx.service_controller, &mut service_one)
        .await
        .map_err(scenario_failure_without_clients)?;
    shutdown_service(&ctx.service_controller, &mut service_two)
        .await
        .map_err(scenario_failure_without_clients)?;
    Ok(())
}

async fn run_inner(
    ctx: &ScenarioContext,
    spec_one: &ServiceSpec,
    spec_two: &ServiceSpec,
    service_one: &mut Option<ManagedService>,
    active_clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
) -> anyhow::Result<()> {
    connect_service_one_clients(
        &ctx.accounts,
        active_clients,
        cursors,
        &spec_one.endpoint,
    )
    .await?;
    connect_service_two_clients(
        &ctx.accounts,
        active_clients,
        cursors,
        &spec_two.endpoint,
    )
    .await?;

    ctx.validator.fund_payer().await?;

    let restart_a_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::RestartA), 4_444_444)
        .await?;
    let restart_b_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::RestartB), 5_555_555)
        .await?;
    let shared_b_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::SharedB), 6_666_666)
        .await?;

    let pre_restart = CheckpointSpec {
        name: "pre-restart",
        clients: vec![
            repeated_checkpoint(
                0..5,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::RestartA),
                    4_444_444,
                    restart_a_sig,
                ),
            ),
            repeated_checkpoint(
                5..10,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    6_666_666,
                    shared_b_sig.clone(),
                ),
            ),
            repeated_checkpoint(
                10..15,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::RestartB),
                    5_555_555,
                    restart_b_sig,
                ),
            ),
            repeated_checkpoint(
                15..20,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    6_666_666,
                    shared_b_sig,
                ),
            ),
        ]
        .into_iter()
        .flatten()
        .collect(),
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&pre_restart, active_clients, cursors)
        .await?;

    let parked_logs =
        shutdown_service_one_clients(active_clients, cursors).await?;
    shutdown_service(&ctx.service_controller, service_one).await?;

    ctx.validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::RestartA), 7_777_777)
        .await?;
    let during_shared_b_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::SharedB), 8_888_888)
        .await?;

    assert_logs_unchanged(&parked_logs)?;

    let during_restart = CheckpointSpec {
        name: "during-restart",
        clients: vec![
            empty_checkpoints(10..15),
            repeated_checkpoint(
                15..20,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    8_888_888,
                    during_shared_b_sig,
                ),
            ),
        ]
        .into_iter()
        .flatten()
        .collect(),
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&during_restart, active_clients, cursors)
        .await?;

    *service_one = Some(
        ctx.service_controller
            .start(spec_one, &ctx.artifacts)
            .await?,
    );
    connect_service_one_clients(
        &ctx.accounts,
        active_clients,
        cursors,
        &spec_one.endpoint,
    )
    .await?;

    let post_restart_a_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::RestartA), 9_999_999)
        .await?;
    let post_shared_b_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::SharedB), 10_101_010)
        .await?;

    let post_restart = CheckpointSpec {
        name: "post-restart",
        clients: vec![
            repeated_checkpoint(
                0..5,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::RestartA),
                    9_999_999,
                    post_restart_a_sig,
                ),
            ),
            repeated_checkpoint(
                5..10,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    10_101_010,
                    post_shared_b_sig.clone(),
                ),
            ),
            empty_checkpoints(10..15),
            repeated_checkpoint(
                15..20,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    10_101_010,
                    post_shared_b_sig,
                ),
            ),
        ]
        .into_iter()
        .flatten()
        .collect(),
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&post_restart, active_clients, cursors)
        .await
}

async fn connect_service_one_clients(
    accounts: &ScenarioAccounts,
    active_clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
    endpoint: &str,
) -> anyhow::Result<()> {
    for id in 0..10 {
        let client = TestGrpcClient::connect(
            id,
            ServiceInstance::One,
            endpoint.to_owned(),
        )
        .await
        .with_context(|| {
            format!("failed to connect service-one client {id}")
        })?;
        let subscription = if id < 5 {
            vec![NamedAccount::RestartA]
        } else {
            vec![NamedAccount::SharedB]
        };
        let pubkeys = subscription
            .into_iter()
            .map(|account| accounts.pubkey_b58(account))
            .collect::<Vec<_>>();
        client.replace_subscription(&pubkeys).await?;
        upsert_client(active_clients, cursors, client);
    }
    Ok(())
}

async fn connect_service_two_clients(
    accounts: &ScenarioAccounts,
    active_clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
    endpoint: &str,
) -> anyhow::Result<()> {
    for id in 10..20 {
        let client = TestGrpcClient::connect(
            id,
            ServiceInstance::Two,
            endpoint.to_owned(),
        )
        .await
        .with_context(|| {
            format!("failed to connect service-two client {id}")
        })?;
        let subscription = if id < 15 {
            vec![NamedAccount::RestartB]
        } else {
            vec![NamedAccount::SharedB]
        };
        let pubkeys = subscription
            .into_iter()
            .map(|account| accounts.pubkey_b58(account))
            .collect::<Vec<_>>();
        client.replace_subscription(&pubkeys).await?;
        upsert_client(active_clients, cursors, client);
    }
    Ok(())
}

fn upsert_client(
    active_clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
    client: TestGrpcClient,
) {
    let client_id = client.id;
    if let Some(position) =
        active_clients.iter().position(|c| c.id == client_id)
    {
        active_clients[position] = client;
    } else {
        active_clients.push(client);
    }

    if let Some(cursor) = cursors
        .iter_mut()
        .find(|cursor| cursor.client_id == client_id)
    {
        cursor.next_index = 0;
    } else {
        cursors.push(ClientCursor {
            client_id,
            next_index: 0,
        });
    }
}

async fn shutdown_service_one_clients(
    active_clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
) -> anyhow::Result<Vec<ParkedClientLog>> {
    let mut parked = Vec::new();
    let mut remaining = Vec::new();

    for client in active_clients.drain(..) {
        if client.service == ServiceInstance::One {
            let log = client.log().clone();
            let len = log.len();
            parked.push(ParkedClientLog {
                client_id: client.id,
                log,
                len,
            });
            client.shutdown().await?;
        } else {
            remaining.push(client);
        }
    }

    *active_clients = remaining;
    cursors.retain(|cursor| cursor.client_id >= 10);
    Ok(parked)
}

fn assert_logs_unchanged(
    parked_logs: &[ParkedClientLog],
) -> anyhow::Result<()> {
    for parked in parked_logs {
        if parked.log.len() != parked.len {
            bail!(
                "service-one client {} received updates after shutdown",
                parked.client_id
            );
        }
    }
    Ok(())
}

fn expected_update(
    pubkey_b58: String,
    lamports: u64,
    txn_signature_b58: String,
) -> ExpectedUpdate {
    ExpectedUpdate {
        pubkey_b58: Some(pubkey_b58),
        lamports: Some(lamports),
        txn_signature_b58: Some(Some(txn_signature_b58)),
        ..Default::default()
    }
}

fn repeated_checkpoint(
    range: std::ops::Range<usize>,
    expected: ExpectedUpdate,
) -> Vec<ClientCheckpoint> {
    range
        .map(|client_id| ClientCheckpoint {
            client_id,
            required: vec![expected.clone()],
        })
        .collect()
}

fn empty_checkpoints(range: std::ops::Range<usize>) -> Vec<ClientCheckpoint> {
    range
        .map(|client_id| ClientCheckpoint {
            client_id,
            required: Vec::new(),
        })
        .collect()
}

async fn shutdown_clients(clients: Vec<TestGrpcClient>) -> anyhow::Result<()> {
    for client in clients {
        client.shutdown().await?;
    }
    Ok(())
}

async fn shutdown_service(
    controller: &crate::service::ServiceController,
    service: &mut Option<ManagedService>,
) -> anyhow::Result<()> {
    if let Some(service) = service.take() {
        controller.shutdown(service).await?;
    }
    Ok(())
}

struct ParkedClientLog {
    client_id: usize,
    log: ClientLog,
    len: usize,
}

fn scenario_failure_without_clients(error: anyhow::Error) -> ScenarioFailure {
    ScenarioFailure {
        error,
        clients: Vec::new(),
    }
}
