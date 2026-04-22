use std::time::SystemTime;

use helius_laserstream::grpc::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
    subscribe_update,
};
use prost_types::Timestamp;

use crate::domain::{AccountEvent, AccountState, AccountUpdate};
use crate::errors::{GeykagError, GeykagResult};

pub(crate) fn to_subscribe_update(
    event: &AccountEvent,
) -> GeykagResult<SubscribeUpdate> {
    match event {
        AccountEvent::Snapshot(account) => Ok(SubscribeUpdate {
            filters: Vec::new(),
            created_at: Some(Timestamp::from(SystemTime::now())),
            update_oneof: Some(subscribe_update::UpdateOneof::Account(
                SubscribeUpdateAccount {
                    account: Some(to_account_info_from_snapshot(account)?),
                    slot: account.slot,
                    is_startup: Default::default(),
                },
            )),
        }),
        AccountEvent::Live(message) => Ok(SubscribeUpdate {
            filters: Vec::new(),
            created_at: Some(Timestamp::from(SystemTime::now())),
            update_oneof: Some(subscribe_update::UpdateOneof::Account(
                SubscribeUpdateAccount {
                    account: Some(to_account_info_from_live(&message.account)?),
                    slot: message.account.slot,
                    is_startup: Default::default(),
                },
            )),
        }),
    }
}

fn to_account_info_from_snapshot(
    account: &AccountState,
) -> GeykagResult<SubscribeUpdateAccountInfo> {
    Ok(SubscribeUpdateAccountInfo {
        pubkey: decode_base58_field("pubkey", &account.pubkey_b58)?,
        lamports: account.lamports,
        owner: decode_base58_field("owner", &account.owner_b58)?,
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: Vec::new(),
        write_version: account.write_version,
        txn_signature: decode_optional_base58_field(
            "txn_signature",
            account.txn_signature_b58.as_deref(),
        )?,
    })
}

fn to_account_info_from_live(
    account: &AccountUpdate,
) -> GeykagResult<SubscribeUpdateAccountInfo> {
    Ok(SubscribeUpdateAccountInfo {
        pubkey: decode_base58_field("pubkey", &account.pubkey_b58)?,
        lamports: account.lamports,
        owner: decode_base58_field("owner", &account.owner_b58)?,
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: Vec::new(),
        write_version: account.write_version,
        txn_signature: decode_optional_base58_field(
            "txn_signature",
            account.txn_signature_b58.as_deref(),
        )?,
    })
}

fn decode_base58_field(
    field: &'static str,
    value: &str,
) -> GeykagResult<Vec<u8>> {
    bs58::decode(value).into_vec().map_err(|source| {
        GeykagError::GrpcEventConversion(format!(
            "failed to decode {field} as base58: {source}"
        ))
    })
}

fn decode_optional_base58_field(
    field: &'static str,
    value: Option<&str>,
) -> GeykagResult<Option<Vec<u8>>> {
    value
        .map(|value| decode_base58_field(field, value))
        .transpose()
}

#[cfg(test)]
mod tests {
    use helius_laserstream::grpc::subscribe_update::UpdateOneof;

    use super::to_subscribe_update;
    use crate::domain::{
        AccountEvent, AccountState, AccountUpdate, bytes_to_base58,
    };
    use crate::errors::GeykagError;
    use crate::kafka::StreamMessage;

    fn sample_pubkey_bytes() -> Vec<u8> {
        (1..=32).collect()
    }

    fn sample_owner_bytes() -> Vec<u8> {
        (32..64).collect()
    }

    fn sample_signature_bytes() -> Vec<u8> {
        vec![9; 64]
    }

    fn sample_account_state() -> AccountState {
        AccountState {
            pubkey_b58: bytes_to_base58(&sample_pubkey_bytes()),
            pubkey_bytes: sample_pubkey_bytes(),
            owner_b58: bytes_to_base58(&sample_owner_bytes()),
            slot: 88,
            lamports: 777,
            executable: true,
            rent_epoch: 12,
            write_version: 33,
            txn_signature_b58: Some(bytes_to_base58(&sample_signature_bytes())),
            data_len: 19,
        }
    }

    fn sample_account_update() -> AccountUpdate {
        AccountUpdate {
            pubkey_b58: bytes_to_base58(&sample_pubkey_bytes()),
            pubkey_bytes: sample_pubkey_bytes(),
            owner_b58: bytes_to_base58(&sample_owner_bytes()),
            slot: 99,
            lamports: 1234,
            executable: false,
            rent_epoch: 15,
            write_version: 44,
            txn_signature_b58: Some(bytes_to_base58(&sample_signature_bytes())),
            data_len: 64,
            data_version: 6,
            account_age: 17,
        }
    }

    fn sample_stream_message() -> StreamMessage {
        StreamMessage {
            account: sample_account_update(),
            partition: 2,
            offset: 10,
            timestamp: "now".to_owned(),
        }
    }

    #[test]
    fn snapshot_events_convert_to_account_updates() {
        let update = to_subscribe_update(&AccountEvent::Snapshot(
            sample_account_state(),
        ))
        .unwrap();

        match update.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                assert_eq!(account.slot, 88);
                let info = account.account.unwrap();
                assert_eq!(info.pubkey, sample_pubkey_bytes());
                assert_eq!(info.owner, sample_owner_bytes());
            }
            other => panic!("expected account update, got {other:?}"),
        }
    }

    #[test]
    fn live_events_convert_expected_account_fields() {
        let update =
            to_subscribe_update(&AccountEvent::Live(sample_stream_message()))
                .unwrap();

        match update.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                assert_eq!(account.slot, 99);
                let info = account.account.unwrap();
                assert_eq!(info.pubkey, sample_pubkey_bytes());
                assert_eq!(info.owner, sample_owner_bytes());
                assert_eq!(info.lamports, 1234);
                assert!(!info.executable);
                assert_eq!(info.rent_epoch, 15);
                assert_eq!(info.write_version, 44);
                assert_eq!(info.txn_signature, Some(sample_signature_bytes()));
            }
            other => panic!("expected account update, got {other:?}"),
        }
    }

    #[test]
    fn live_events_leave_txn_signature_empty_when_absent() {
        let mut message = sample_stream_message();
        message.account.txn_signature_b58 = None;

        let update = to_subscribe_update(&AccountEvent::Live(message)).unwrap();

        match update.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                let info = account.account.unwrap();
                assert_eq!(info.txn_signature, None);
            }
            other => panic!("expected account update, got {other:?}"),
        }
    }

    #[test]
    fn invalid_snapshot_pubkey_base58_returns_conversion_error() {
        let mut state = sample_account_state();
        state.pubkey_b58 = "0badpubkey".to_owned();

        let error =
            to_subscribe_update(&AccountEvent::Snapshot(state)).unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
    }

    #[test]
    fn invalid_snapshot_owner_base58_returns_conversion_error() {
        let mut state = sample_account_state();
        state.owner_b58 = "0badowner".to_owned();

        let error =
            to_subscribe_update(&AccountEvent::Snapshot(state)).unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
    }

    #[test]
    fn invalid_live_txn_signature_base58_returns_conversion_error() {
        let mut message = sample_stream_message();
        message.account.txn_signature_b58 = Some("0badsignature".to_owned());

        let error =
            to_subscribe_update(&AccountEvent::Live(message)).unwrap_err();

        assert!(matches!(error, GeykagError::GrpcEventConversion(_)));
    }
}
