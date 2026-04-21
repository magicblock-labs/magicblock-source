use crate::domain::{AccountEvent, AccountState, PubkeyFilter};
use crate::errors::GeykagResult;
use crate::kafka::StreamMessage;

pub trait AccountSink {
    fn write_event(&self, event: &AccountEvent) -> GeykagResult<()>;
}

pub trait StatusSink {
    fn write_status(&self, status: &str) -> GeykagResult<()>;
}

#[allow(dead_code)]
pub trait SnapshotStore: Send + Sync {
    async fn fetch_filtered(
        &self,
        filter: Option<&PubkeyFilter>,
    ) -> GeykagResult<Vec<AccountState>>;

    async fn fetch_one_by_pubkey(
        &self,
        pubkey: &PubkeyFilter,
    ) -> GeykagResult<Option<AccountState>>;
}

#[allow(dead_code)]
pub trait ValidatorSubscriptions: Send + Sync {
    async fn whitelist_pubkeys(&self, pubkeys: &[String]) -> GeykagResult<()>;
}

#[allow(dead_code)]
pub trait AccountUpdateSource: Send + Sync {
    async fn run<H>(
        &self,
        filter: Option<&PubkeyFilter>,
        handler: H,
    ) -> GeykagResult<()>
    where
        H: FnMut(StreamMessage) -> GeykagResult<()>;
}
