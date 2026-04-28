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
    let spec_one = ServiceSpec::for_instance(ServiceInstance::One);
    let spec_two = ServiceSpec::for_instance(ServiceInstance::Two);
    let mut services = Vec::new();
    let mut clients = Vec::new();
    let mut cursors = Vec::new();

    let result = run_inner(
        ctx,
        &spec_one,
        &spec_two,
        &mut services,
        &mut clients,
        &mut cursors,
    )
    .await;
    if let Err(error) = result {
        return Err(ScenarioFailure { error, clients });
    }

    shutdown_clients(clients)
        .await
        .map_err(scenario_failure_without_clients)?;
    shutdown_services(&ctx.service_controller, services)
        .await
        .map_err(scenario_failure_without_clients)?;
    Ok(())
}

async fn run_inner(
    ctx: &ScenarioContext,
    spec_one: &ServiceSpec,
    spec_two: &ServiceSpec,
    services: &mut Vec<ManagedService>,
    clients: &mut Vec<TestGrpcClient>,
    cursors: &mut Vec<ClientCursor>,
) -> anyhow::Result<()> {
    services.push(
        ctx.service_controller
            .start(spec_one, &ctx.artifacts)
            .await?,
    );
    services.push(
        ctx.service_controller
            .start(spec_two, &ctx.artifacts)
            .await?,
    );

    let simple_a = ctx.accounts.pubkey_b58(NamedAccount::SimpleA);
    let simple_b = ctx.accounts.pubkey_b58(NamedAccount::SimpleB);
    let shared_a = ctx.accounts.pubkey_b58(NamedAccount::SharedA);

    for id in 0..10 {
        let client = TestGrpcClient::connect(
            id,
            ServiceInstance::One,
            spec_one.endpoint.clone(),
        )
        .await
        .with_context(|| {
            format!("failed to connect service-one client {id}")
        })?;
        let subscription = if id < 5 {
            vec![simple_a.clone()]
        } else {
            vec![shared_a.clone()]
        };
        client.replace_subscription(&subscription).await?;
        clients.push(client);
        cursors.push(ClientCursor {
            client_id: id,
            next_index: 0,
        });
    }

    for id in 10..20 {
        let client = TestGrpcClient::connect(
            id,
            ServiceInstance::Two,
            spec_two.endpoint.clone(),
        )
        .await
        .with_context(|| {
            format!("failed to connect service-two client {id}")
        })?;
        let subscription = if id < 15 {
            vec![simple_b.clone()]
        } else {
            vec![shared_a.clone()]
        };
        client.replace_subscription(&subscription).await?;
        clients.push(client);
        cursors.push(ClientCursor {
            client_id: id,
            next_index: 0,
        });
    }

    ctx.validator.fund_payer().await?;

    let simple_a_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::SimpleA), 1_111_111)
        .await?;
    let simple_b_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::SimpleB), 2_222_222)
        .await?;
    let shared_a_sig = ctx
        .validator
        .airdrop(&ctx.accounts.pubkey(NamedAccount::SharedA), 3_333_333)
        .await?;

    let simple_a_expected = ExpectedUpdate {
        pubkey_b58: Some(simple_a),
        lamports: Some(1_111_111),
        txn_signature_b58: Some(Some(simple_a_sig)),
        ..Default::default()
    };
    let simple_b_expected = ExpectedUpdate {
        pubkey_b58: Some(simple_b),
        lamports: Some(2_222_222),
        txn_signature_b58: Some(Some(simple_b_sig)),
        ..Default::default()
    };
    let shared_a_expected = ExpectedUpdate {
        pubkey_b58: Some(shared_a),
        lamports: Some(3_333_333),
        txn_signature_b58: Some(Some(shared_a_sig)),
        ..Default::default()
    };

    let mut checkpoint_clients = Vec::new();
    for client_id in 0..5 {
        checkpoint_clients.push(single_update_checkpoint(
            client_id,
            simple_a_expected.clone(),
        ));
    }
    for client_id in 5..10 {
        checkpoint_clients.push(single_update_checkpoint(
            client_id,
            shared_a_expected.clone(),
        ));
    }
    for client_id in 10..15 {
        checkpoint_clients.push(single_update_checkpoint(
            client_id,
            simple_b_expected.clone(),
        ));
    }
    for client_id in 15..20 {
        checkpoint_clients.push(single_update_checkpoint(
            client_id,
            shared_a_expected.clone(),
        ));
    }

    let checkpoint = CheckpointSpec {
        name: "dual-concurrent-routing",
        clients: checkpoint_clients,
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&checkpoint, clients, cursors)
        .await
}

fn single_update_checkpoint(
    client_id: usize,
    expected: ExpectedUpdate,
) -> ClientCheckpoint {
    ClientCheckpoint {
        client_id,
        required: vec![expected],
    }
}

async fn shutdown_clients(clients: Vec<TestGrpcClient>) -> anyhow::Result<()> {
    for client in clients {
        client.shutdown().await?;
    }
    Ok(())
}

async fn shutdown_services(
    controller: &crate::service::ServiceController,
    services: Vec<ManagedService>,
) -> anyhow::Result<()> {
    for service in services {
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
