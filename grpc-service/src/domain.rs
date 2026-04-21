use crate::errors::{GeykagError, GeykagResult};
use crate::kafka::StreamMessage;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PubkeyFilter {
    raw: String,
    bytes: Vec<u8>,
}

impl PubkeyFilter {
    pub fn parse(value: &str) -> GeykagResult<Self> {
        let bytes = bs58::decode(value).into_vec().map_err(|source| {
            GeykagError::InvalidPubkey {
                value: value.to_owned(),
                source,
            }
        })?;

        if bytes.len() != 32 {
            return Err(GeykagError::InvalidPubkeyLength {
                actual: bytes.len(),
            });
        }

        Ok(Self {
            raw: value.to_owned(),
            bytes,
        })
    }

    pub fn matches(&self, bytes: &[u8]) -> bool {
        self.bytes == bytes
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

#[derive(Clone, Debug)]
pub struct AccountUpdate {
    pub pubkey_b58: String,
    pub pubkey_bytes: Vec<u8>,
    pub owner_b58: String,
    pub slot: u64,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub txn_signature_b58: Option<String>,
    pub data_len: usize,
    pub data_version: u32,
    pub account_age: u64,
}

#[derive(Clone, Debug)]
pub struct AccountState {
    pub pubkey_b58: String,
    pub pubkey_bytes: Vec<u8>,
    pub owner_b58: String,
    pub slot: u64,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub txn_signature_b58: Option<String>,
    pub data_len: usize,
}

#[derive(Clone, Debug)]
pub enum AccountEvent {
    Snapshot(AccountState),
    Live(StreamMessage),
}

#[allow(dead_code)]
impl AccountEvent {
    pub fn get_account_state(&self) -> AccountState {
        match self {
            Self::Snapshot(state) => state.clone(),
            Self::Live(message) => AccountState::from(&message.account),
        }
    }

    pub fn into_account_state(self) -> AccountState {
        match self {
            Self::Snapshot(state) => state,
            Self::Live(message) => AccountState::from(message.account),
        }
    }
}

impl AccountState {
    pub fn matches_filter(&self, filter: Option<&PubkeyFilter>) -> bool {
        filter.is_none_or(|pubkey| pubkey.matches(&self.pubkey_bytes))
    }
}

impl AccountUpdate {
    pub fn matches_filter(&self, filter: Option<&PubkeyFilter>) -> bool {
        filter.is_none_or(|pubkey| pubkey.matches(&self.pubkey_bytes))
    }
}

impl From<&AccountUpdate> for AccountState {
    fn from(update: &AccountUpdate) -> Self {
        Self {
            pubkey_b58: update.pubkey_b58.clone(),
            pubkey_bytes: update.pubkey_bytes.clone(),
            owner_b58: update.owner_b58.clone(),
            slot: update.slot,
            lamports: update.lamports,
            executable: update.executable,
            rent_epoch: update.rent_epoch,
            write_version: update.write_version,
            txn_signature_b58: update.txn_signature_b58.clone(),
            data_len: update.data_len,
        }
    }
}

impl From<AccountUpdate> for AccountState {
    fn from(update: AccountUpdate) -> Self {
        Self {
            pubkey_b58: update.pubkey_b58,
            pubkey_bytes: update.pubkey_bytes,
            owner_b58: update.owner_b58,
            slot: update.slot,
            lamports: update.lamports,
            executable: update.executable,
            rent_epoch: update.rent_epoch,
            write_version: update.write_version,
            txn_signature_b58: update.txn_signature_b58,
            data_len: update.data_len,
        }
    }
}

pub fn bytes_to_base58(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    bs58::encode(bytes).into_string()
}
