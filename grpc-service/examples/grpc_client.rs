use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use helius_laserstream::grpc::{
    SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeUpdateAccountInfo, geyser_client::GeyserClient,
    subscribe_update::UpdateOneof,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

const REST_PORT: u16 = 3030;

type ReqSender = mpsc::Sender<SubscribeRequest>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let endpoint = parse_endpoint();
    tracing::info!(endpoint, "connecting to gRPC server");

    let mut client = GeyserClient::connect(endpoint.clone()).await?;
    tracing::info!("connected, subscribing…");

    let initial_request = SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: None,
        entry: HashMap::new(),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
        transactions_status: HashMap::new(),
    };

    let (tx, rx) = mpsc::channel(4);
    tx.send(initial_request).await?;

    let response = client.subscribe(ReceiverStream::new(rx)).await?;
    let mut update_stream = response.into_inner();

    tracing::info!("subscribed, waiting for updates…");

    // Start REST server for filter management
    let tx = Arc::new(tx);
    let app = Router::new()
        .route("/add/{pubkey}", post(handle_add))
        .route("/remove/{pubkey}", post(handle_remove))
        .with_state(tx);

    let listener =
        tokio::net::TcpListener::bind(("0.0.0.0", REST_PORT)).await?;
    tracing::info!(port = REST_PORT, "REST server listening");

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "REST server error");
        }
    });

    loop {
        tokio::select! {
            item = update_stream.next() => {
                match item {
                    Some(Ok(update)) => {
                        if let Some(UpdateOneof::Account(account_update)) =
                            update.update_oneof
                            && let Some(info) = account_update.account
                        {
                            println!(
                                "--- update slot={} ---\n{}",
                                account_update.slot,
                                format_account_info(&info),
                            );
                        }
                    }
                    Some(Err(status)) => {
                        tracing::error!(%status, "stream error");
                        break;
                    }
                    None => {
                        tracing::info!("stream ended");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received Ctrl-C, shutting down");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_add(
    State(tx): State<Arc<ReqSender>>,
    Path(pubkey): Path<String>,
) -> StatusCode {
    send_filter_update(&tx, "add", &pubkey).await
}

async fn handle_remove(
    State(tx): State<Arc<ReqSender>>,
    Path(pubkey): Path<String>,
) -> StatusCode {
    send_filter_update(&tx, "remove", &pubkey).await
}

async fn send_filter_update(
    tx: &ReqSender,
    key: &str,
    pubkey: &str,
) -> StatusCode {
    let req = SubscribeRequest {
        accounts: HashMap::from([(
            key.to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![pubkey.to_string()],
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    match tx.send(req).await {
        Ok(()) => {
            tracing::info!("sent /{key} {pubkey}");
            StatusCode::OK
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to send filter update");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn format_account_info(info: &SubscribeUpdateAccountInfo) -> String {
    let pubkey = bytes_to_base58(&info.pubkey);
    let owner = bytes_to_base58(&info.owner);
    let txn_signature = info
        .txn_signature
        .as_ref()
        .map(|b| bytes_to_base58(b))
        .unwrap_or_else(|| "N/A".to_owned());

    [
        format!("pubkey:        {}", format_identifier(&pubkey)),
        format!("owner:         {}", format_identifier(&owner)),
        format!("lamports:      {}", info.lamports),
        format!("data_len:      {} bytes", info.data.len()),
        format!("executable:    {}", info.executable),
        format!("rent_epoch:    {}", info.rent_epoch),
        format!("write_version: {}", info.write_version),
        format!("txn_signature: {}", format_identifier(&txn_signature)),
    ]
    .join("\n")
}

fn bytes_to_base58(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    bs58::encode(bytes).into_string()
}

fn format_identifier(value: &str) -> &str {
    if value.is_empty() { "(empty)" } else { value }
}

fn parse_endpoint() -> String {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--endpoint"
            && let Some(val) = args.get(i + 1)
        {
            return val.clone();
        }
        i += 1;
    }
    "http://127.0.0.1:50051".to_owned()
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "grpc_client=info".into()),
        )
        .with_target(false)
        .init();
}
