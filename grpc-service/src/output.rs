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
