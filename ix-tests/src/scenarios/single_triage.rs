use anyhow::Context;
use solana_keypair::{Keypair, Signer};
use tracing::info;

use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::expectation::{CheckpointSpec, ClientCheckpoint, ExpectedUpdate};
use crate::layout::ServiceInstance;
use crate::scenarios::ScenarioFailure;
use crate::service::{ServiceHandle, ServiceSpec};

pub async fn run(ctx: &ScenarioContext) -> Result<(), ScenarioFailure> {
    let spec = ServiceSpec::for_instance(ServiceInstance::One);
    let service = ctx
        .service_controller
        .start(&spec, &ctx.artifacts)
        .await
        .map_err(scenario_failure_without_clients)?;

    if service.is_external() {
        info!(
            endpoint = %service.endpoint,
            "single-triage attached to already-running external grpc-service"
        );
    } else {
        info!(
            endpoint = %service.endpoint,
            "single-triage launched managed grpc-service"
        );
    }

    let mut service = Some(service);
    let mut clients = Vec::new();

    let result = run_inner(ctx, &spec.endpoint, &mut clients).await;
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
) -> anyhow::Result<()> {
    info!(endpoint = %endpoint, "single-triage targeting endpoint");

    let client =
        TestGrpcClient::connect(0, ServiceInstance::One, endpoint.to_owned())
            .await
            .with_context(|| "failed to connect client 0")?;
    clients.push(client);

    let random_pubkey = Keypair::new().pubkey();
    info!(pubkey = %random_pubkey, "single-triage generated random pubkey");
    clients[0]
        .replace_subscription(&[random_pubkey.to_string()])
        .await?;

    let bootstrap_checkpoint = CheckpointSpec {
        name: "single-triage-bootstrap",
        checkpoints: vec![ClientCheckpoint {
            client_id: 0,
            required: vec![ExpectedUpdate {
                pubkey_b58: Some(random_pubkey.to_string()),
                lamports: Some(0),
                txn_signature_b58: Some(None),
                ..Default::default()
            }],
        }],
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&bootstrap_checkpoint, clients)
        .await?;
    info!(
        pubkey = %random_pubkey,
        "single-triage bootstrap lamports=0 checkpoint passed"
    );

    ctx.validator.fund_payer().await?;

    let airdrop_signature =
        ctx.validator.airdrop(&random_pubkey, 1_000_000).await?;

    let airdrop_checkpoint = CheckpointSpec {
        name: "single-triage-airdrop",
        checkpoints: vec![ClientCheckpoint {
            client_id: 0,
            required: vec![ExpectedUpdate {
                pubkey_b58: Some(random_pubkey.to_string()),
                lamports: Some(1_000_000),
                txn_signature_b58: Some(Some(airdrop_signature)),
                ..Default::default()
            }],
        }],
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&airdrop_checkpoint, clients)
        .await?;
    info!(
        pubkey = %random_pubkey,
        "single-triage post-airdrop checkpoint passed"
    );

    Ok(())
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
