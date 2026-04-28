use std::time::{Duration, Instant};
use tracing::*;

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
    #[allow(dead_code)]
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
        let mut mismatches = Vec::new();
        if let Some(expected) = &self.pubkey_b58 {
            if observed.pubkey_b58 != *expected {
                mismatches.push(format!(
                    "pubkey_b58: expected {}, got {}",
                    expected, observed.pubkey_b58
                ));
            }
        }
        if let Some(expected) = self.slot {
            if observed.slot != expected {
                mismatches.push(format!(
                    "slot: expected {}, got {}",
                    expected, observed.slot
                ));
            }
        }
        if let Some(expected) = self.lamports {
            if observed.lamports != expected {
                mismatches.push(format!(
                    "lamports: expected {}, got {}",
                    expected, observed.lamports
                ));
            }
        }
        if let Some(expected) = &self.owner_b58 {
            if observed.owner_b58 != *expected {
                mismatches.push(format!(
                    "owner_b58: expected {}, got {}",
                    expected, observed.owner_b58
                ));
            }
        }

        if let Some(expected) = self.executable {
            if observed.executable != expected {
                mismatches.push(format!(
                    "executable: expected {}, got {}",
                    expected, observed.executable
                ));
            }
        }

        if let Some(expected) = self.rent_epoch {
            if observed.rent_epoch != expected {
                mismatches.push(format!(
                    "rent_epoch: expected {}, got {}",
                    expected, observed.rent_epoch
                ));
            }
        }

        if let Some(expected) = self.write_version {
            if observed.write_version != expected {
                mismatches.push(format!(
                    "write_version: expected {}, got {}",
                    expected, observed.write_version
                ));
            }
        }

        if let Some(expected) = &self.txn_signature_b58 {
            if observed.txn_signature_b58.as_ref() != expected.as_ref() {
                mismatches.push(format!(
                    "txn_signature_b58: expected {:?}, got {:?}",
                    expected, observed.txn_signature_b58
                ));
            }
        }

        if let Some(expected) = &self.data {
            if observed.data != *expected {
                mismatches.push(format!(
                    "data: expected {:?}, got {:?}",
                    expected, observed.data
                ));
            }
        }

        if !mismatches.is_empty() {
            warn!("Mismatches:\n {}", mismatches.join("\n  "));
        }
        mismatches.is_empty()
    }
}

fn unmatched_required<'a>(
    required: &'a [ExpectedUpdate],
    observed: &[ObservedUpdate],
) -> Vec<&'a ExpectedUpdate> {
    required
        .iter()
        .filter(|expected| {
            !observed.iter().any(|update| expected.matches(update))
        })
        .collect()
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

                if !unmatched_required(&client_spec.required, &observed)
                    .is_empty()
                {
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
                // Build a useful diagnostic per client and bail.
                for client_spec in &spec.clients {
                    let Some(client) = clients
                        .iter()
                        .find(|client| client.id == client_spec.client_id)
                    else {
                        continue;
                    };
                    let Some(cursor) = cursors.iter().find(|cursor| {
                        cursor.client_id == client_spec.client_id
                    }) else {
                        continue;
                    };
                    let observed =
                        client.log().snapshot_from(cursor.next_index);
                    let missing =
                        unmatched_required(&client_spec.required, &observed);
                    if missing.is_empty() {
                        continue;
                    }
                    error!(
                        checkpoint = spec.name,
                        client_id = client_spec.client_id,
                        "Missing required: {:#?}",
                        missing
                    );
                    error!(
                        checkpoint = spec.name,
                        client_id = client_spec.client_id,
                        "Observed in window: {:#?}",
                        observed
                    );
                }
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

    use super::{ExpectedUpdate, unmatched_required};

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

    fn observed_with(
        lamports: u64,
        txn_signature_b58: Option<String>,
    ) -> ObservedUpdate {
        ObservedUpdate {
            client_id: 7,
            service: ServiceInstance::One,
            pubkey_b58: "pubkey".to_owned(),
            slot: 42,
            lamports,
            owner_b58: "owner".to_owned(),
            executable: false,
            rent_epoch: 5,
            write_version: 6,
            txn_signature_b58,
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

    #[test]
    fn unmatched_required_returns_empty_when_all_required_match_with_noise() {
        let noise = observed_with(0, None);
        let matching = observed_with(1_000_000, Some("real-sig".to_owned()));
        let observed = vec![noise, matching];

        let required = vec![ExpectedUpdate {
            lamports: Some(1_000_000),
            txn_signature_b58: Some(Some("real-sig".to_owned())),
            ..Default::default()
        }];

        assert!(unmatched_required(&required, &observed).is_empty());
    }

    #[test]
    fn unmatched_required_reports_missing_when_required_never_arrives() {
        let noise = observed_with(0, None);
        let observed = vec![noise];

        let required = vec![ExpectedUpdate {
            lamports: Some(1_000_000),
            ..Default::default()
        }];

        let missing = unmatched_required(&required, &observed);
        assert!(!missing.is_empty());
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].lamports, Some(1_000_000));
    }
}
