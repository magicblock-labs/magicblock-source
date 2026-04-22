use crate::domain::{AccountEvent, AccountState, AccountUpdate};
use crate::errors::GeykagResult;
use crate::traits::{AccountSink, StatusSink};

pub struct ConsoleSink;

impl ConsoleSink {
    pub fn new() -> Self {
        Self
    }
}

impl AccountSink for ConsoleSink {
    fn write_event(&self, event: &AccountEvent) -> GeykagResult<()> {
        match event {
            AccountEvent::Snapshot(account) => {
                println!(
                    "--- snapshot pubkey={} ---\n{}",
                    account.pubkey_b58,
                    format_account_event(event)
                );
            }
            AccountEvent::Live(message) => {
                println!(
                    "--- stream partition={} offset={} timestamp={} ---\n{}",
                    message.partition,
                    message.offset,
                    message.timestamp,
                    format_account_event(event)
                );
            }
        }

        Ok(())
    }
}

impl StatusSink for ConsoleSink {
    fn write_status(&self, status: &str) -> GeykagResult<()> {
        println!("{status}");
        Ok(())
    }
}

pub struct TeeSink<S1, S2> {
    left: S1,
    right: S2,
}

impl<S1, S2> TeeSink<S1, S2> {
    #[allow(dead_code)]
    pub fn new(left: S1, right: S2) -> Self {
        Self { left, right }
    }
}

impl<S1: AccountSink, S2: AccountSink> AccountSink for TeeSink<S1, S2> {
    fn write_event(&self, event: &AccountEvent) -> GeykagResult<()> {
        self.left.write_event(event)?;
        self.right.write_event(event)
    }
}

fn format_account_event(event: &AccountEvent) -> String {
    match event {
        AccountEvent::Snapshot(account) => format_snapshot(account),
        AccountEvent::Live(message) => format_update(&message.account),
    }
}

fn format_snapshot(record: &AccountState) -> String {
    [
        format!("slot:          {}", record.slot),
        format!("pubkey:        {}", record.pubkey_b58),
        format!("owner:         {}", format_identifier(&record.owner_b58)),
        format!("lamports:      {}", record.lamports),
        format!("data_len:      {} bytes", record.data_len),
        format!("executable:    {}", record.executable),
        format!("rent_epoch:    {}", record.rent_epoch),
        format!("write_version: {}", record.write_version),
        format!(
            "txn_signature: {}",
            record
                .txn_signature_b58
                .as_deref()
                .map(format_identifier)
                .unwrap_or("N/A")
        ),
        "data_version:  N/A".to_owned(),
        "account_age:   N/A".to_owned(),
    ]
    .join("\n")
}

fn format_update(record: &AccountUpdate) -> String {
    [
        format!("slot:          {}", record.slot),
        format!("pubkey:        {}", record.pubkey_b58),
        format!("owner:         {}", format_identifier(&record.owner_b58)),
        format!("lamports:      {}", record.lamports),
        format!("data_len:      {} bytes", record.data_len),
        format!("executable:    {}", record.executable),
        format!("rent_epoch:    {}", record.rent_epoch),
        format!("write_version: {}", record.write_version),
        format!(
            "txn_signature: {}",
            record
                .txn_signature_b58
                .as_deref()
                .map(format_identifier)
                .unwrap_or("N/A")
        ),
        format!("data_version:  {}", record.data_version),
        format!("account_age:   {}", record.account_age),
    ]
    .join("\n")
}

fn format_identifier(value: &str) -> &str {
    if value.is_empty() { "(empty)" } else { value }
}

#[cfg(test)]
mod tests {
    use super::{format_identifier, format_snapshot, format_update};
    use crate::domain::{AccountState, AccountUpdate};

    fn snapshot() -> AccountState {
        AccountState {
            pubkey_b58: "pubkey123".to_owned(),
            pubkey_bytes: vec![1; 32],
            owner_b58: String::new(),
            slot: 42,
            lamports: 500,
            executable: false,
            rent_epoch: 7,
            write_version: 9,
            txn_signature_b58: None,
            data_len: 64,
        }
    }

    fn update() -> AccountUpdate {
        AccountUpdate {
            pubkey_b58: "pubkey456".to_owned(),
            pubkey_bytes: vec![2; 32],
            owner_b58: "owner456".to_owned(),
            slot: 52,
            lamports: 700,
            executable: true,
            rent_epoch: 8,
            write_version: 10,
            txn_signature_b58: Some("sig456".to_owned()),
            data_len: 96,
            data_version: 3,
            account_age: 11,
        }
    }

    #[test]
    fn format_identifier_returns_empty_marker_for_empty_string() {
        assert_eq!(format_identifier(""), "(empty)");
    }

    #[test]
    fn format_identifier_returns_non_empty_value_unchanged() {
        assert_eq!(format_identifier("owner123"), "owner123");
    }

    #[test]
    fn format_snapshot_renders_expected_fields() {
        let rendered = format_snapshot(&snapshot());

        assert!(rendered.contains("slot:          42"));
        assert!(rendered.contains("pubkey:        pubkey123"));
        assert!(rendered.contains("owner:         (empty)"));
        assert!(rendered.contains("txn_signature: N/A"));
        assert!(rendered.contains("data_version:  N/A"));
        assert!(rendered.contains("account_age:   N/A"));
    }

    #[test]
    fn format_update_renders_expected_fields() {
        let rendered = format_update(&update());

        assert!(rendered.contains("slot:          52"));
        assert!(rendered.contains("pubkey:        pubkey456"));
        assert!(rendered.contains("owner:         owner456"));
        assert!(rendered.contains("txn_signature: sig456"));
        assert!(rendered.contains("data_version:  3"));
        assert!(rendered.contains("account_age:   11"));
    }
}
