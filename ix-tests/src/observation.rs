use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::layout::ServiceInstance;

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize)]
pub struct ObservedUpdate {
    pub client_id: usize,
    pub service: ServiceInstance,
    pub pubkey_b58: String,
    pub slot: u64,
    pub lamports: u64,
    pub owner_b58: String,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub txn_signature_b58: Option<String>,
    pub data: Vec<u8>,
    pub received_at_millis: u128,
}

#[derive(Clone)]
pub struct ClientLog {
    entries: Arc<Mutex<Vec<ObservedUpdate>>>,
}

#[allow(dead_code)]
impl ClientLog {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn push(&self, update: ObservedUpdate) {
        self.entries.lock().unwrap().push(update);
    }

    pub fn snapshot(&self) -> Vec<ObservedUpdate> {
        self.entries.lock().unwrap().clone()
    }

    pub fn snapshot_from(&self, start_index: usize) -> Vec<ObservedUpdate> {
        let guard = self.entries.lock().unwrap();
        guard[start_index..].to_vec()
    }

    /// Takes the next update in the order it came in
    /// Removes and returns it from the log
    pub fn consume_next_update(&self) -> Option<ObservedUpdate> {
        if self.entries.lock().unwrap().is_empty() {
            None
        } else {
            Some(self.entries.lock().unwrap().remove(0))
        }
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}
