use anyhow::Context;

use crate::accounts::NamedAccount;
use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::expectation::{CheckpointSpec, ClientCheckpoint, ExpectedUpdate};
use crate::layout::ServiceInstance;
use crate::scenarios::ScenarioFailure;
use crate::service::{ServiceHandle, ServiceSpec};

pub async fn run(ctx: &ScenarioContext) -> Result<(), ScenarioFailure> {
    let spec = ServiceSpec::for_instance(ServiceInstance::One);
    let mut service = Some(
        ctx.service_controller
            .start(&spec, &ctx.artifacts)
            .await
            .map_err(scenario_failure_without_clients)?,
    );
    let mut clients = Vec::new();

    let outcome = run_inner(ctx, &spec.endpoint, &mut clients).await;
    let shutdown_clients_result = shutdown_clients(clients).await;
    let shutdown_service_result =
        shutdown_service(&ctx.service_controller, &mut service).await;

    if let Err(error) = outcome {
        return Err(ScenarioFailure {
            error,
            clients: Vec::new(),
        });
    }

    shutdown_clients_result.map_err(scenario_failure_without_clients)?;
    shutdown_service_result.map_err(scenario_failure_without_clients)?;
    Ok(())
}

async fn run_inner(
    ctx: &ScenarioContext,
    endpoint: &str,
    clients: &mut Vec<TestGrpcClient>,
) -> anyhow::Result<()> {
    let shared_a = ctx.accounts.pubkey_b58(NamedAccount::SharedA);
    let shared_b = ctx.accounts.pubkey_b58(NamedAccount::SharedB);

    for id in 0..100 {
        let client = TestGrpcClient::connect(
            id,
            ServiceInstance::One,
            endpoint.to_owned(),
        )
        .await
        .with_context(|| format!("failed to connect client {id}"))?;
        client
            .replace_subscription(&[shared_a.clone(), shared_b.clone()])
            .await
            .with_context(|| {
                format!("failed to set subscriptions for client {id}")
            })?;
        clients.push(client);
    }

    ctx.validator.fund_payer().await?;

    let rent_exempt_lamports = ctx.validator.rent_exempt_balance(0).await?;

    let airdrop_specs = (1..=25u64)
        .map(|index| {
            let (account, lamports) = if index % 2 == 1 {
                (NamedAccount::SharedA, rent_exempt_lamports + 10_000 + index)
            } else {
                (NamedAccount::SharedB, rent_exempt_lamports + 20_000 + index)
            };
            (account, ctx.accounts.pubkey(account), lamports)
        })
        .collect::<Vec<_>>();
    let airdrop_requests = airdrop_specs
        .iter()
        .map(|(_, pubkey, lamports)| (*pubkey, *lamports))
        .collect();
    ctx.validator.airdrops(airdrop_requests).await?;

    let (shared_a_balance, shared_b_balance) = airdrop_specs.iter().fold(
        (0, 0),
        |(shared_a_balance, shared_b_balance), (account, _pubkey, lamports)| {
            match account {
                NamedAccount::SharedA => {
                    (shared_a_balance + lamports, shared_b_balance)
                }
                NamedAccount::SharedB => {
                    (shared_a_balance, shared_b_balance + lamports)
                }
                _ => unreachable!("single-load only airdrops shared accounts"),
            }
        },
    );
    let expected_updates = vec![
        ExpectedUpdate {
            pubkey_b58: Some(ctx.accounts.pubkey_b58(NamedAccount::SharedA)),
            lamports: Some(shared_a_balance),
            ..Default::default()
        },
        ExpectedUpdate {
            pubkey_b58: Some(ctx.accounts.pubkey_b58(NamedAccount::SharedB)),
            lamports: Some(shared_b_balance),
            ..Default::default()
        },
    ];

    let client_specs = (0..100)
        .map(|client_id| ClientCheckpoint {
            client_id,
            required: expected_updates.clone(),
        })
        .collect();
    let checkpoint = CheckpointSpec {
        name: "single-load-fanout",
        checkpoints: client_specs,
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&checkpoint, clients)
        .await
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

fn scenario_failure_without_clients(error: anyhow::Error) -> ScenarioFailure {
    ScenarioFailure {
        error,
        clients: Vec::new(),
    }
}
