use anyhow::Context;
use solana_pubkey::Pubkey;
use tracing::debug;

use crate::accounts::NamedAccount;
use crate::client::TestGrpcClient;
use crate::context::ScenarioContext;
use crate::expectation::{CheckpointSpec, ClientCheckpoint, ExpectedUpdate};
use crate::layout::ServiceInstance;
use crate::scenarios::ScenarioFailure;
use crate::service::{ServiceHandle, ServiceSpec};

const OWNER_DATA_SPACE: u64 = 64;
const SYNTHETIC_OWNER_BYTES: [u8; 32] = [
    0x31, 0x22, 0x13, 0x04, 0xF5, 0xE6, 0xD7, 0xC8, 0xB9, 0xAA, 0x9B, 0x8C,
    0x7D, 0x6E, 0x5F, 0x40, 0x11, 0x32, 0x53, 0x74, 0x95, 0xB6, 0xD7, 0xF8,
    0x18, 0x29, 0x3A, 0x4B, 0x5C, 0x6D, 0x7E, 0x8F,
];

pub async fn run(ctx: &ScenarioContext) -> Result<(), ScenarioFailure> {
    let spec = ServiceSpec::for_instance(ServiceInstance::One);
    let mut service = Some(
        ctx.service_controller
            .start(&spec, &ctx.artifacts)
            .await
            .map_err(scenario_failure_without_clients)?,
    );
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
    for id in 0..4 {
        let client = TestGrpcClient::connect(
            id,
            ServiceInstance::One,
            endpoint.to_owned(),
        )
        .await
        .with_context(|| format!("failed to connect client {id}"))?;
        clients.push(client);
    }

    let simple_a = ctx.accounts.pubkey(NamedAccount::SimpleA);
    debug!("Client 0 subscribing to SimpleA: {simple_a}");
    clients[0]
        .replace_subscription(&[simple_a.to_string()])
        .await?;

    let simple_b = ctx.accounts.pubkey(NamedAccount::SimpleB);
    debug!("Client 1 subscribing to SimpleB: {simple_b}");
    clients[1]
        .replace_subscription(&[simple_b.to_string()])
        .await?;

    let simple_c = ctx.accounts.pubkey(NamedAccount::SimpleC);
    debug!("Client 2 subscribing to SimpleC: {simple_c}");
    clients[2]
        .replace_subscription(&[simple_c.to_string()])
        .await?;

    let owner_data = ctx.accounts.pubkey(NamedAccount::OwnerData);
    debug!("Client 3 subscribing to OwnerData: {owner_data}");
    clients[3]
        .replace_subscription(&[owner_data.to_string()])
        .await?;

    // Right after we made the subscriptions we expect to get an _empty_ account update for
    // each account
    let empty_checkpoint = CheckpointSpec {
        name: "initial-empty-accounts",
        checkpoints: vec![
            lamport_client_checkpoint(0, simple_a.to_string(), 0, None),
            lamport_client_checkpoint(1, simple_b.to_string(), 0, None),
            lamport_client_checkpoint(2, simple_c.to_string(), 0, None),
            lamport_client_checkpoint(3, owner_data.to_string(), 0, None),
        ],
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&empty_checkpoint, clients)
        .await?;

    debug!("✅ initial empty accounts");

    // Then we airdrop some lamports to each account and expect to see the updates with the
    // correct lamports and signatures
    ctx.validator.fund_payer().await?;

    let sigs = ctx
        .validator
        .airdrops(vec![
            (simple_a, 1_000_000),
            (simple_b, 2_000_000),
            (simple_c, 3_000_000),
        ])
        .await?;
    let [simple_a_sig, simple_b_sig, simple_c_sig]: [String; 3] =
        sigs.try_into().expect("expected three airdrop signatures");

    let basic_checkpoint = CheckpointSpec {
        name: "basic-lamports",
        checkpoints: vec![
            lamport_client_checkpoint(
                0,
                simple_a.to_string(),
                1_000_000,
                Some(simple_a_sig),
            ),
            lamport_client_checkpoint(
                1,
                simple_b.to_string(),
                2_000_000,
                Some(simple_b_sig),
            ),
            lamport_client_checkpoint(
                2,
                simple_c.to_string(),
                3_000_000,
                Some(simple_c_sig),
            ),
        ],
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&basic_checkpoint, clients)
        .await?;

    debug!("✅ basic lamports updates");

    let rent_lamports =
        ctx.validator.rent_exempt_balance(OWNER_DATA_SPACE).await?;
    let owner_data_airdrop_sig =
        ctx.validator.airdrop(&owner_data, rent_lamports).await?;

    let owner_data_funding_checkpoint = CheckpointSpec {
        name: "owner-data-funding",
        checkpoints: vec![ClientCheckpoint {
            client_id: 3,
            required: vec![ExpectedUpdate {
                pubkey_b58: Some(owner_data.to_string()),
                lamports: Some(rent_lamports),
                txn_signature_b58: Some(Some(owner_data_airdrop_sig)),
                ..Default::default()
            }],
        }],
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&owner_data_funding_checkpoint, clients)
        .await?;

    let synthetic_owner = Pubkey::new_from_array(SYNTHETIC_OWNER_BYTES);
    let owner_data_sig = ctx
        .validator
        .allocate_and_assign(
            ctx.accounts.keypair(NamedAccount::OwnerData),
            OWNER_DATA_SPACE,
            synthetic_owner,
        )
        .await?;

    let owner_data_expected = ExpectedUpdate {
        pubkey_b58: Some(owner_data.to_string()),
        owner_b58: Some(synthetic_owner.to_string()),
        lamports: Some(rent_lamports),
        txn_signature_b58: Some(Some(owner_data_sig)),
        data: None,
        ..Default::default()
    };
    let owner_data_checkpoint = CheckpointSpec {
        name: "owner-data-change",
        checkpoints: vec![ClientCheckpoint {
            client_id: 3,
            required: vec![owner_data_expected],
        }],
    };
    ctx.checkpoint_runner
        .wait_until_satisfied(&owner_data_checkpoint, clients)
        .await?;

    debug!("✅ owner and data updates");

    Ok(())
}

fn lamport_client_checkpoint(
    client_id: usize,
    pubkey_b58: String,
    lamports: u64,
    txn_signature_b58: Option<String>,
) -> ClientCheckpoint {
    let expected = ExpectedUpdate {
        pubkey_b58: Some(pubkey_b58),
        lamports: Some(lamports),
        txn_signature_b58: Some(txn_signature_b58),
        ..Default::default()
    };
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
