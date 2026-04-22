use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use tokio::time::sleep;

use crate::client::TestGrpcClient;
use crate::config::SuiteConfig;
use crate::observation::ObservedUpdate;

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct ExpectedUpdate {
    pub pubkey_b58: Option<String>,
    pub slot: Option<u64>,
    pub lamports: Option<u64>,
    pub owner_b58: Option<String>,
    pub executable: Option<bool>,
    pub rent_epoch: Option<u64>,
    pub write_version: Option<u64>,
    pub txn_signature_b58: Option<Option<String>>,
    pub data: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct ClientCheckpoint {
    pub client_id: usize,
    pub allowed: Vec<ExpectedUpdate>,
    pub required: Vec<ExpectedUpdate>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct CheckpointSpec {
    pub name: &'static str,
    pub clients: Vec<ClientCheckpoint>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct ClientCursor {
    pub client_id: usize,
    pub next_index: usize,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CheckpointRunner {
    timeout: Duration,
}

#[allow(dead_code)]
impl ExpectedUpdate {
    pub fn matches(&self, observed: &ObservedUpdate) -> bool {
        self.pubkey_b58
            .as_ref()
            .is_none_or(|expected| observed.pubkey_b58 == *expected)
            && self.slot.is_none_or(|expected| observed.slot == expected)
            && self
                .lamports
                .is_none_or(|expected| observed.lamports == expected)
            && self
                .owner_b58
                .as_ref()
                .is_none_or(|expected| observed.owner_b58 == *expected)
            && self
                .executable
                .is_none_or(|expected| observed.executable == expected)
            && self
                .rent_epoch
                .is_none_or(|expected| observed.rent_epoch == expected)
            && self
                .write_version
                .is_none_or(|expected| observed.write_version == expected)
            && self.txn_signature_b58.as_ref().is_none_or(|expected| {
                observed.txn_signature_b58.as_ref() == expected.as_ref()
            })
            && self
                .data
                .as_ref()
                .is_none_or(|expected| observed.data == *expected)
    }
}

#[allow(dead_code)]
impl CheckpointRunner {
    pub fn new(config: &SuiteConfig) -> Self {
        Self {
            timeout: Duration::from_millis(config.checkpoint_timeout_ms),
        }
    }

    pub async fn wait_until_satisfied(
        &self,
        spec: &CheckpointSpec,
        clients: &[TestGrpcClient],
        cursors: &mut [ClientCursor],
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.timeout;

        loop {
            let mut all_required_seen = true;

            for client_spec in &spec.clients {
                let client = clients
                    .iter()
                    .find(|client| client.id == client_spec.client_id)
                    .with_context(|| {
                        format!(
                            "checkpoint '{}' references unknown client {}",
                            spec.name, client_spec.client_id
                        )
                    })?;
                let cursor = cursors
                    .iter()
                    .find(|cursor| cursor.client_id == client_spec.client_id)
                    .with_context(|| {
                        format!(
                            "checkpoint '{}' is missing cursor for client {}",
                            spec.name, client_spec.client_id
                        )
                    })?;
                let observed = client.log().snapshot_from(cursor.next_index);

                if let Some(unexpected) = observed.iter().find(|update| {
                    !client_spec
                        .allowed
                        .iter()
                        .any(|expected| expected.matches(update))
                }) {
                    bail!(
                        "checkpoint '{}' failed for client {}: unexpected update for pubkey {}",
                        spec.name,
                        client_spec.client_id,
                        unexpected.pubkey_b58
                    );
                }

                let missing_required =
                    client_spec.required.iter().any(|expected| {
                        !observed.iter().any(|update| expected.matches(update))
                    });
                if missing_required {
                    all_required_seen = false;
                }
            }

            if all_required_seen {
                for cursor in cursors.iter_mut() {
                    let client = clients
                        .iter()
                        .find(|client| client.id == cursor.client_id)
                        .with_context(|| {
                            format!(
                                "checkpoint '{}' cannot advance missing client {}",
                                spec.name, cursor.client_id
                            )
                        })?;
                    cursor.next_index = client.log().len();
                }
                return Ok(());
            }

            if Instant::now() >= deadline {
                bail!(
                    "checkpoint '{}' timed out after {:?}",
                    spec.name,
                    self.timeout
                );
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::layout::ServiceInstance;
    use crate::observation::ObservedUpdate;

    use super::ExpectedUpdate;

    fn observed_update() -> ObservedUpdate {
        ObservedUpdate {
            client_id: 7,
            service: ServiceInstance::One,
            pubkey_b58: "pubkey".to_owned(),
            slot: 42,
            lamports: 99,
            owner_b58: "owner".to_owned(),
            executable: false,
            rent_epoch: 5,
            write_version: 6,
            txn_signature_b58: Some("sig".to_owned()),
            data: vec![1, 2, 3],
            received_at_millis: 123,
        }
    }

    #[test]
    fn matches_ignores_none_fields() {
        let expected = ExpectedUpdate {
            lamports: Some(99),
            ..Default::default()
        };

        assert!(expected.matches(&observed_update()));
    }

    #[test]
    fn matches_rejects_mismatched_fields() {
        let expected = ExpectedUpdate {
            lamports: Some(100),
            ..Default::default()
        };

        assert!(!expected.matches(&observed_update()));
    }
}
