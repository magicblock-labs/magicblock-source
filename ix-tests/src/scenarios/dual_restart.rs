use anyhow::{Context, bail};

use crate::accounts::{NamedAccount, ScenarioAccounts};
use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::expectation::{CheckpointSpec, ClientCheckpoint, ExpectedUpdate};
use crate::layout::ServiceInstance;
use crate::observation::ClientLog;
use crate::scenarios::ScenarioFailure;
use crate::service::{ServiceHandle, ServiceSpec};

const PRE_RESTART_A_AIRDROP_LAMPORTS: u64 = 4_444_444;
const PRE_RESTART_B_AIRDROP_LAMPORTS: u64 = 5_555_555;
const PRE_SHARED_B_AIRDROP_LAMPORTS: u64 = 6_666_666;

const DURING_RESTART_A_AIRDROP_LAMPORTS: u64 = 7_777_777;
const DURING_SHARED_B_AIRDROP_LAMPORTS: u64 = 8_888_888;

const POST_RESTART_A_AIRDROP_LAMPORTS: u64 = 9_999_999;
const POST_SHARED_B_AIRDROP_LAMPORTS: u64 = 10_101_010;

// Account updates report the account's resulting balance, not the
// individual airdrop delta. The first airdrops below target fresh random
// accounts, so their resulting balances are equal to their airdrop amounts.
const PRE_RESTART_A_EXPECTED_BALANCE: u64 = PRE_RESTART_A_AIRDROP_LAMPORTS;
const PRE_RESTART_B_EXPECTED_BALANCE: u64 = PRE_RESTART_B_AIRDROP_LAMPORTS;
const PRE_SHARED_B_EXPECTED_BALANCE: u64 = PRE_SHARED_B_AIRDROP_LAMPORTS;

// During restart, service one is offline but the validator still applies both
// airdrops. SharedB already has the pre-restart balance when this update is
// emitted, so the expected lamports are cumulative.
const DURING_RESTART_A_EXPECTED_BALANCE: u64 =
    PRE_RESTART_A_EXPECTED_BALANCE + DURING_RESTART_A_AIRDROP_LAMPORTS;
const DURING_SHARED_B_EXPECTED_BALANCE: u64 =
    PRE_SHARED_B_EXPECTED_BALANCE + DURING_SHARED_B_AIRDROP_LAMPORTS;

// After service one restarts, live updates again carry full account balances.
// These expectations include all earlier airdrops to the same account.
const POST_RESTART_A_EXPECTED_BALANCE: u64 =
    DURING_RESTART_A_EXPECTED_BALANCE + POST_RESTART_A_AIRDROP_LAMPORTS;
const POST_SHARED_B_EXPECTED_BALANCE: u64 =
    DURING_SHARED_B_EXPECTED_BALANCE + POST_SHARED_B_AIRDROP_LAMPORTS;

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

    let result = run_inner(
        ctx,
        &spec_one,
        &spec_two,
        &mut service_one,
        &mut active_clients,
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
    service_one: &mut Option<ServiceHandle>,
    active_clients: &mut Vec<TestGrpcClient>,
) -> anyhow::Result<()> {
    connect_service_one_clients(
        &ctx.accounts,
        active_clients,
        &spec_one.endpoint,
    )
    .await?;
    connect_service_two_clients(
        &ctx.accounts,
        active_clients,
        &spec_two.endpoint,
    )
    .await?;

    ctx.validator.fund_payer().await?;

    let sigs = ctx
        .validator
        .airdrops(vec![
            (
                ctx.accounts.pubkey(NamedAccount::RestartA),
                PRE_RESTART_A_AIRDROP_LAMPORTS,
            ),
            (
                ctx.accounts.pubkey(NamedAccount::RestartB),
                PRE_RESTART_B_AIRDROP_LAMPORTS,
            ),
            (
                ctx.accounts.pubkey(NamedAccount::SharedB),
                PRE_SHARED_B_AIRDROP_LAMPORTS,
            ),
        ])
        .await?;
    let [restart_a_sig, restart_b_sig, shared_b_sig]: [String; 3] =
        sigs.try_into().expect("expected three airdrop signatures");

    let pre_restart = CheckpointSpec {
        name: "pre-restart",
        checkpoints: vec![
            repeated_checkpoint(
                0..5,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::RestartA),
                    PRE_RESTART_A_EXPECTED_BALANCE,
                    restart_a_sig,
                ),
            ),
            repeated_checkpoint(
                5..10,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    PRE_SHARED_B_EXPECTED_BALANCE,
                    shared_b_sig.clone(),
                ),
            ),
            repeated_checkpoint(
                10..15,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::RestartB),
                    PRE_RESTART_B_EXPECTED_BALANCE,
                    restart_b_sig,
                ),
            ),
            repeated_checkpoint(
                15..20,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    PRE_SHARED_B_EXPECTED_BALANCE,
                    shared_b_sig,
                ),
            ),
        ]
        .into_iter()
        .flatten()
        .collect(),
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&pre_restart, active_clients)
        .await?;

    let parked_logs = shutdown_service_one_clients(active_clients).await?;
    shutdown_service(&ctx.service_controller, service_one).await?;

    let sigs = ctx
        .validator
        .airdrops(vec![
            (
                ctx.accounts.pubkey(NamedAccount::RestartA),
                DURING_RESTART_A_AIRDROP_LAMPORTS,
            ),
            (
                ctx.accounts.pubkey(NamedAccount::SharedB),
                DURING_SHARED_B_AIRDROP_LAMPORTS,
            ),
        ])
        .await?;
    let [_during_restart_a_sig, during_shared_b_sig]: [String; 2] =
        sigs.try_into().expect("expected two airdrop signatures");

    assert_logs_unchanged(&parked_logs)?;

    // SharedB remains subscribed on service two while service one is down;
    // lamports are the cumulative balance (6_666_666 + 8_888_888), not just the second airdrop.
    let during_restart = CheckpointSpec {
        name: "during-restart",
        checkpoints: vec![
            empty_checkpoints(10..15),
            repeated_checkpoint(
                15..20,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    DURING_SHARED_B_EXPECTED_BALANCE,
                    during_shared_b_sig,
                ),
            ),
        ]
        .into_iter()
        .flatten()
        .collect(),
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&during_restart, active_clients)
        .await?;

    *service_one = Some(
        ctx.service_controller
            .start(spec_one, &ctx.artifacts)
            .await?,
    );
    connect_service_one_clients(
        &ctx.accounts,
        active_clients,
        &spec_one.endpoint,
    )
    .await?;

    let sigs = ctx
        .validator
        .airdrops(vec![
            (
                ctx.accounts.pubkey(NamedAccount::RestartA),
                POST_RESTART_A_AIRDROP_LAMPORTS,
            ),
            (
                ctx.accounts.pubkey(NamedAccount::SharedB),
                POST_SHARED_B_AIRDROP_LAMPORTS,
            ),
        ])
        .await?;
    let [post_restart_a_sig, post_shared_b_sig]: [String; 2] =
        sigs.try_into().expect("expected two airdrop signatures");

    // Reconnected live updates still report full balances: RestartA includes all
    // three RestartA airdrops and SharedB includes all three SharedB airdrops.
    let post_restart = CheckpointSpec {
        name: "post-restart",
        checkpoints: vec![
            repeated_checkpoint(
                0..5,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::RestartA),
                    POST_RESTART_A_EXPECTED_BALANCE,
                    post_restart_a_sig,
                ),
            ),
            repeated_checkpoint(
                5..10,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    POST_SHARED_B_EXPECTED_BALANCE,
                    post_shared_b_sig.clone(),
                ),
            ),
            empty_checkpoints(10..15),
            repeated_checkpoint(
                15..20,
                expected_update(
                    ctx.accounts.pubkey_b58(NamedAccount::SharedB),
                    POST_SHARED_B_EXPECTED_BALANCE,
                    post_shared_b_sig,
                ),
            ),
        ]
        .into_iter()
        .flatten()
        .collect(),
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&post_restart, active_clients)
        .await
}

async fn connect_service_one_clients(
    accounts: &ScenarioAccounts,
    active_clients: &mut Vec<TestGrpcClient>,
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
        upsert_client(active_clients, client);
    }
    Ok(())
}

async fn connect_service_two_clients(
    accounts: &ScenarioAccounts,
    active_clients: &mut Vec<TestGrpcClient>,
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
        upsert_client(active_clients, client);
    }
    Ok(())
}

fn upsert_client(
    active_clients: &mut Vec<TestGrpcClient>,
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
}

async fn shutdown_service_one_clients(
    active_clients: &mut Vec<TestGrpcClient>,
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
    service: &mut Option<ServiceHandle>,
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
