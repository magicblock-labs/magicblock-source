use anyhow::Context;

use crate::accounts::NamedAccount;
use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::expectation::{
    CheckpointSpec, ClientCheckpoint, ClientCursor, ExpectedUpdate,
};
use crate::layout::ServiceInstance;
use crate::scenarios::ScenarioFailure;
use crate::service::{ManagedService, ServiceSpec};

pub async fn run(ctx: &ScenarioContext) -> Result<(), ScenarioFailure> {
    let spec = ServiceSpec::for_instance(ServiceInstance::One);
    let mut service = Some(
        ctx.service_controller
            .start(&spec, &ctx.artifacts)
            .await
            .map_err(scenario_failure_without_clients)?,
    );
    let mut clients = Vec::new();
    let mut cursors = Vec::new();

    let result =
        run_inner(ctx, &spec.endpoint, &mut clients, &mut cursors).await;
    if let Err(error) = result {
        return Err(ScenarioFailure { error, clients });
    }

    shutdown_clients(clients)
        .await
        .map_err(scenario_failure_without_clients)?;
    shutdown_service(&ctx.service_controller, &mut service)
        .await
        .map_err(scenario_failure_without_clients)?;
    Ok(())
}

async fn run_inner(
    ctx: &ScenarioContext,
    endpoint: &str,
    clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
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
        cursors.push(ClientCursor {
            client_id: id,
            next_index: 0,
        });
        clients.push(client);
    }

    ctx.validator.fund_payer().await?;

    let mut expected_updates = Vec::new();
    for index in 1..=25u64 {
        let (account, lamports) = if index % 2 == 1 {
            (NamedAccount::SharedA, 10_000 + index)
        } else {
            (NamedAccount::SharedB, 20_000 + index)
        };
        let pubkey_b58 = ctx.accounts.pubkey_b58(account);
        let sig = ctx
            .validator
            .airdrop(&ctx.accounts.pubkey(account), lamports)
            .await?;
        expected_updates.push(ExpectedUpdate {
            pubkey_b58: Some(pubkey_b58),
            lamports: Some(lamports),
            txn_signature_b58: Some(Some(sig)),
            ..Default::default()
        });
    }

    let client_specs = (0..100)
        .map(|client_id| ClientCheckpoint {
            client_id,
            required: expected_updates.clone(),
        })
        .collect();
    let checkpoint = CheckpointSpec {
        name: "single-load-fanout",
        clients: client_specs,
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&checkpoint, clients, cursors)
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
    service: &mut Option<ManagedService>,
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
