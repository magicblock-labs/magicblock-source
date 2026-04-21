use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tracing::{debug, warn};

use crate::domain::AccountEvent;
use crate::errors::{GeykagError, GeykagResult};
use crate::traits::AccountSink;

use super::convert::to_subscribe_update;
use super::dispatcher::DispatcherHandle;

#[derive(Clone, Debug)]
pub struct GrpcSink {
    dispatcher: DispatcherHandle,
    is_running: Arc<AtomicBool>,
}

impl GrpcSink {
    pub(crate) fn new(
        dispatcher: DispatcherHandle,
        is_running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            dispatcher,
            is_running,
        }
    }
}

impl AccountSink for GrpcSink {
    fn write_event(&self, event: &AccountEvent) -> GeykagResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            warn!("gRPC sink write attempted after shutdown");
            return Err(GeykagError::GrpcSinkWriteAfterShutdown);
        }

        let update = match to_subscribe_update(event) {
            Ok(update) => update,
            Err(e) => {
                warn!(error = %e, "failed to convert account event to SubscribeUpdate");
                return Err(e);
            }
        };

        debug!("publishing update to dispatcher");
        match self.dispatcher.try_publish(update) {
            Ok(()) => Ok(()),
            Err(_) => {
                warn!("dispatcher update channel full, dropping update");
                Ok(())
            }
        }
    }
}
