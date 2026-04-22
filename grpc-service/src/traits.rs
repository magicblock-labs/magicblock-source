use crate::domain::{AccountEvent, AccountState, PubkeyFilter};
use crate::errors::GeykagResult;
use crate::kafka::StreamMessage;

pub trait AccountSink: Send + Sync {
    fn write_event(&self, event: &AccountEvent) -> GeykagResult<()>;
}

pub trait StatusSink: Send + Sync {
    fn write_status(&self, status: &str) -> GeykagResult<()>;
}

#[allow(dead_code)]
pub trait SnapshotStore: Send + Sync {
    fn fetch_filtered(
        &self,
        filter: Option<&PubkeyFilter>,
    ) -> impl std::future::Future<Output = GeykagResult<Vec<AccountState>>> + Send;

    fn fetch_one_by_pubkey(
        &self,
        pubkey: &PubkeyFilter,
    ) -> impl std::future::Future<Output = GeykagResult<Option<AccountState>>> + Send;
}

#[allow(dead_code)]
pub trait ValidatorSubscriptions: Send + Sync {
    fn whitelist_pubkeys(
        &self,
        pubkeys: &[String],
    ) -> impl std::future::Future<Output = GeykagResult<()>> + Send;
}

#[allow(dead_code)]
pub trait AccountUpdateSource: Send + Sync {
    fn run<H>(
        &self,
        filter: Option<&PubkeyFilter>,
        handler: H,
    ) -> impl std::future::Future<Output = GeykagResult<()>> + Send
    where
        H: FnMut(StreamMessage) -> GeykagResult<()> + Send;
}
