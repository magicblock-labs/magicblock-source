use {
    crate::{
        initial_account_backfill::{
            INITIAL_BACKFILL_INITIAL_BACKOFF_MS, INITIAL_BACKFILL_MAX_ATTEMPTS,
            INITIAL_BACKFILL_MAX_BACKOFF_MS,
            INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST,
        },
        metrics::{
            INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL,
            INITIAL_BACKFILL_RPC_FAILURES_TOTAL,
        },
        wire::UpdateAccountEvent,
    },
    log::*,
    solana_account::Account,
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::{Pubkey, pubkey},
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    std::io,
    std::time::Duration,
    tokio::time::sleep,
};

pub(crate) async fn fetch_account_events_for_request(
    client: &RpcClient,
    local_rpc_url: &str,
    pubkeys: &[[u8; 32]],
) -> io::Result<Vec<UpdateAccountEvent>> {
    let mut events = Vec::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST) {
        events.extend(
            fetch_account_events_for_chunk(client, local_rpc_url, chunk)
                .await?,
        );
    }
    Ok(events)
}

async fn fetch_account_events_for_chunk(
    client: &RpcClient,
    local_rpc_url: &str,
    pubkeys: &[[u8; 32]],
) -> io::Result<Vec<UpdateAccountEvent>> {
    let keys = pubkeys
        .iter()
        .map(|pubkey| Pubkey::new_from_array(*pubkey))
        .collect::<Vec<_>>();

    let mut backoff_ms = INITIAL_BACKFILL_INITIAL_BACKOFF_MS;
    let mut last_error = None;

    for attempt in 1..=INITIAL_BACKFILL_MAX_ATTEMPTS {
        INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
            .with_label_values(&["started"])
            .inc();
        info!(
            "Starting initial account backfill RPC request for {} pubkeys, attempt={}/{}",
            pubkeys.len(),
            attempt,
            INITIAL_BACKFILL_MAX_ATTEMPTS
        );

        match client
            .get_multiple_accounts_with_commitment(
                &keys,
                CommitmentConfig::confirmed(),
            )
            .await
        {
            Ok(response) => {
                info!(
                    "Initial account backfill RPC request succeeded for {} pubkeys at slot {}",
                    pubkeys.len(),
                    response.context.slot
                );

                if response.value.len() != pubkeys.len() {
                    INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                        .with_label_values(&["failed"])
                        .inc();
                    INITIAL_BACKFILL_RPC_FAILURES_TOTAL
                        .with_label_values(&["length_mismatch"])
                        .inc();
                    return Err(io::Error::other(format!(
                        "rpc returned {} accounts for {} requested pubkeys",
                        response.value.len(),
                        pubkeys.len()
                    )));
                }

                INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                    .with_label_values(&["succeeded"])
                    .inc();
                return Ok(pubkeys
                    .iter()
                    .zip(response.value.into_iter())
                    .map(|(pubkey, maybe_account)| match maybe_account {
                        Some(account) => map_existing_account(
                            account,
                            response.context.slot,
                            *pubkey,
                        ),
                        None => {
                            map_missing_account(response.context.slot, *pubkey)
                        }
                    })
                    .collect());
            }
            Err(error) => {
                INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                    .with_label_values(&["failed"])
                    .inc();
                warn!(
                    "Initial account backfill RPC request failed for {} pubkeys via {}, \
                     attempt={}/{}: {error}",
                    pubkeys.len(),
                    local_rpc_url,
                    attempt,
                    INITIAL_BACKFILL_MAX_ATTEMPTS
                );
                last_error = Some(error);

                if attempt < INITIAL_BACKFILL_MAX_ATTEMPTS {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms =
                        (backoff_ms * 2).min(INITIAL_BACKFILL_MAX_BACKOFF_MS);
                }
            }
        }
    }

    let last_error_message = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "unknown error".to_owned());
    Err(io::Error::other(format_exhausted_error(
        local_rpc_url,
        INITIAL_BACKFILL_MAX_ATTEMPTS,
        &last_error_message,
    )))
}

pub(crate) fn format_exhausted_error(
    local_rpc_url: &str,
    max_attempts: usize,
    last_error_message: &str,
) -> String {
    format!(
        "initial account backfill RPC failed after {max_attempts} attempts via {local_rpc_url}: {last_error_message}"
    )
}

pub(crate) const SYSTEM_PROGRAM_ID: Pubkey =
    pubkey!("11111111111111111111111111111111");

pub(crate) fn map_existing_account(
    account: Account,
    slot: u64,
    pubkey: [u8; 32],
) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: pubkey.to_vec(),
        lamports: account.lamports,
        owner: account.owner.to_bytes().to_vec(),
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: account.data,
        write_version: 0,
        txn_signature: None,
        data_version: 0,
        is_startup: false,
        account_age: 0,
    }
}

pub(crate) fn map_missing_account(
    slot: u64,
    pubkey: [u8; 32],
) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: pubkey.to_vec(),
        lamports: 0,
        owner: SYSTEM_PROGRAM_ID.to_bytes().to_vec(),
        executable: false,
        rent_epoch: 0,
        data: Vec::new(),
        write_version: 0,
        txn_signature: None,
        data_version: 0,
        is_startup: false,
        account_age: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_exhausted_error_includes_url_and_attempts() {
        let message = format_exhausted_error(
            "http://127.0.0.1:8899",
            INITIAL_BACKFILL_MAX_ATTEMPTS,
            "connection refused",
        );

        assert!(message.starts_with(&format!(
            "initial account backfill RPC failed after {INITIAL_BACKFILL_MAX_ATTEMPTS} attempts via http://127.0.0.1:8899: "
        )));
        assert!(message.contains("connection refused"));
    }
}
