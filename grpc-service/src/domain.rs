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

#[cfg(test)]
mod tests {
    use super::{AccountState, AccountUpdate, PubkeyFilter, bytes_to_base58};
    use crate::errors::GeykagError;

    fn valid_pubkey_bytes() -> Vec<u8> {
        (1..=32).collect()
    }

    fn other_pubkey_bytes() -> Vec<u8> {
        (33..=64).collect()
    }

    fn valid_pubkey_b58() -> String {
        bytes_to_base58(&valid_pubkey_bytes())
    }

    fn valid_owner_b58() -> String {
        bytes_to_base58(&other_pubkey_bytes())
    }

    fn sample_account_update() -> AccountUpdate {
        AccountUpdate {
            pubkey_b58: valid_pubkey_b58(),
            pubkey_bytes: valid_pubkey_bytes(),
            owner_b58: valid_owner_b58(),
            slot: 42,
            lamports: 500,
            executable: true,
            rent_epoch: 7,
            write_version: 99,
            txn_signature_b58: Some(bytes_to_base58(&[9; 64])),
            data_len: 128,
            data_version: 3,
            account_age: 11,
        }
    }

    fn sample_account_state() -> AccountState {
        AccountState {
            pubkey_b58: valid_pubkey_b58(),
            pubkey_bytes: valid_pubkey_bytes(),
            owner_b58: valid_owner_b58(),
            slot: 42,
            lamports: 500,
            executable: true,
            rent_epoch: 7,
            write_version: 99,
            txn_signature_b58: Some(bytes_to_base58(&[9; 64])),
            data_len: 128,
        }
    }

    #[test]
    fn test_pubkey_filter_parse_accepts_valid_base58_key() {
        let filter = PubkeyFilter::parse(&valid_pubkey_b58()).unwrap();

        assert_eq!(filter.as_str(), valid_pubkey_b58());
        assert!(filter.matches(&valid_pubkey_bytes()));
    }

    #[test]
    fn test_pubkey_filter_parse_rejects_invalid_base58() {
        let error = PubkeyFilter::parse("not_base58_0OIl").unwrap_err();

        assert!(matches!(error, GeykagError::InvalidPubkey { .. }));
    }

    #[test]
    fn test_pubkey_filter_parse_rejects_non_32_byte_keys() {
        let short_key = bytes_to_base58(&[7; 31]);
        let error = PubkeyFilter::parse(&short_key).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::InvalidPubkeyLength { actual: 31 }
        ));
    }

    #[test]
    fn test_pubkey_filter_matches_only_identical_bytes() {
        let filter = PubkeyFilter::parse(&valid_pubkey_b58()).unwrap();

        assert!(filter.matches(&valid_pubkey_bytes()));
        assert!(!filter.matches(&other_pubkey_bytes()));
    }

    #[test]
    fn test_account_state_matches_filter_returns_true_for_none() {
        assert!(sample_account_state().matches_filter(None));
    }

    #[test]
    fn test_account_update_matches_filter_returns_false_for_non_matching_filter()
     {
        let update = sample_account_update();
        let filter = PubkeyFilter::parse(&valid_owner_b58()).unwrap();

        assert!(!update.matches_filter(Some(&filter)));
    }

    #[test]
    fn test_account_state_from_account_update_ref_copies_all_fields() {
        let update = sample_account_update();
        let state = AccountState::from(&update);

        assert_eq!(state.pubkey_b58, update.pubkey_b58);
        assert_eq!(state.pubkey_bytes, update.pubkey_bytes);
        assert_eq!(state.owner_b58, update.owner_b58);
        assert_eq!(state.slot, update.slot);
        assert_eq!(state.lamports, update.lamports);
        assert_eq!(state.executable, update.executable);
        assert_eq!(state.rent_epoch, update.rent_epoch);
        assert_eq!(state.write_version, update.write_version);
        assert_eq!(state.txn_signature_b58, update.txn_signature_b58);
        assert_eq!(state.data_len, update.data_len);
    }

    #[test]
    fn test_account_state_from_account_update_moves_all_fields() {
        let update = sample_account_update();
        let expected_pubkey_b58 = update.pubkey_b58.clone();
        let expected_pubkey_bytes = update.pubkey_bytes.clone();
        let expected_owner_b58 = update.owner_b58.clone();
        let expected_slot = update.slot;
        let expected_lamports = update.lamports;
        let expected_executable = update.executable;
        let expected_rent_epoch = update.rent_epoch;
        let expected_write_version = update.write_version;
        let expected_txn_signature_b58 = update.txn_signature_b58.clone();
        let expected_data_len = update.data_len;

        let state = AccountState::from(update);

        assert_eq!(state.pubkey_b58, expected_pubkey_b58);
        assert_eq!(state.pubkey_bytes, expected_pubkey_bytes);
        assert_eq!(state.owner_b58, expected_owner_b58);
        assert_eq!(state.slot, expected_slot);
        assert_eq!(state.lamports, expected_lamports);
        assert_eq!(state.executable, expected_executable);
        assert_eq!(state.rent_epoch, expected_rent_epoch);
        assert_eq!(state.write_version, expected_write_version);
        assert_eq!(state.txn_signature_b58, expected_txn_signature_b58);
        assert_eq!(state.data_len, expected_data_len);
    }

    #[test]
    fn test_bytes_to_base58_empty_bytes_returns_empty_string() {
        assert_eq!(bytes_to_base58(&[]), "");
    }

    #[test]
    fn test_bytes_to_base58_round_trips_valid_bytes() {
        let bytes = valid_pubkey_bytes();
        let encoded = bytes_to_base58(&bytes);
        let decoded = bs58::decode(encoded).into_vec().unwrap();

        assert_eq!(decoded, bytes);
    }
}
