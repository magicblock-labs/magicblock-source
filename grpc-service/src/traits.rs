use crate::domain::AccountEvent;
use crate::errors::GeykagResult;

pub trait AccountSink {
    fn write_event(&self, event: &AccountEvent) -> GeykagResult<()>;
}

pub trait StatusSink {
    fn write_status(&self, status: &str) -> GeykagResult<()>;
}
