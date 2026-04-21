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
