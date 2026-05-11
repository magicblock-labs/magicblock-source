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
    pub required: Vec<ExpectedUpdate>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct CheckpointSpec {
    pub name: &'static str,
    pub checkpoints: Vec<ClientCheckpoint>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CheckpointRunner {
    timeout: Duration,
}

#[allow(dead_code)]
impl ExpectedUpdate {
    pub fn matches(&self, observed: &ObservedUpdate) -> bool {
        let mismatches = self.mismatches(observed);
        if !mismatches.is_empty() {
            warn!("Mismatches:\n {}", mismatches.join("\n  "));
        }
        mismatches.is_empty()
    }

    fn matches_quietly(&self, observed: &ObservedUpdate) -> bool {
        self.mismatches(observed).is_empty()
    }

    fn mismatches(&self, observed: &ObservedUpdate) -> Vec<String> {
        let mut mismatches = Vec::new();
        if let Some(expected) = &self.pubkey_b58
            && observed.pubkey_b58 != *expected
        {
            mismatches.push(format!(
                "pubkey_b58: expected {}, got {}",
                expected, observed.pubkey_b58
            ));
        }
        if let Some(expected) = self.slot
            && observed.slot != expected
        {
            mismatches.push(format!(
                "slot: expected {}, got {}",
                expected, observed.slot
            ));
        }
        if let Some(expected) = self.lamports
            && observed.lamports != expected
        {
            mismatches.push(format!(
                "lamports: expected {}, got {}",
                expected, observed.lamports
            ));
        }
        if let Some(expected) = &self.owner_b58
            && observed.owner_b58 != *expected
        {
            mismatches.push(format!(
                "owner_b58: expected {}, got {}",
                expected, observed.owner_b58
            ));
        }

        if let Some(expected) = self.executable
            && observed.executable != expected
        {
            mismatches.push(format!(
                "executable: expected {}, got {}",
                expected, observed.executable
            ));
        }

        if let Some(expected) = self.rent_epoch
            && observed.rent_epoch != expected
        {
            mismatches.push(format!(
                "rent_epoch: expected {}, got {}",
                expected, observed.rent_epoch
            ));
        }

        if let Some(expected) = self.write_version
            && observed.write_version != expected
        {
            mismatches.push(format!(
                "write_version: expected {}, got {}",
                expected, observed.write_version
            ));
        }

        if let Some(expected) = &self.txn_signature_b58
            && observed.txn_signature_b58.as_ref() != expected.as_ref()
        {
            mismatches.push(format!(
                "txn_signature_b58: expected {:?}, got {:?}",
                expected, observed.txn_signature_b58
            ));
        }

        if let Some(expected) = &self.data
            && observed.data != *expected
        {
            mismatches.push(format!(
                "data: expected {:?}, got {:?}",
                expected, observed.data
            ));
        }

        mismatches
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
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.timeout;

        // For each spec we take the next (in order of arrival) state from
        // the matching client and compare them
        for check_point in &spec.checkpoints {
            let client = clients
                .iter()
                .find(|client| client.id == check_point.client_id)
                .with_context(|| {
                    format!(
                        "checkpoint '{}' references unknown client {}",
                        spec.name, check_point.client_id
                    )
                })?;

            let mut matched = vec![false; check_point.required.len()];
            while matched.iter().any(|is_matched| !*is_matched) {
                let client_state = client.log().consume_next_update();
                if let Some(observed) = client_state {
                    let matched_idx =
                        check_point.required.iter().enumerate().find_map(
                            |(idx, expected)| {
                                (!matched[idx]
                                    && expected.matches_quietly(&observed))
                                .then_some(idx)
                            },
                        );

                    if let Some(idx) = matched_idx {
                        matched[idx] = true;
                        trace!(
                            checkpoint = spec.name,
                            idx,
                            client_id = check_point.client_id,
                            "matched expected update: {:#?}",
                            check_point.required[idx]
                        );
                    } else {
                        debug!(
                            checkpoint = spec.name,
                            client_id = check_point.client_id,
                            observed = ?observed,
                            "skipping non-required update while waiting for checkpoint"
                        );
                    }
                } else if Instant::now() > deadline {
                    let missing = matched
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, is_matched)| {
                            (!*is_matched).then_some(idx)
                        })
                        .collect::<Vec<_>>();
                    bail!(
                        "checkpoint '{}' timed out waiting for client {}; missing required update indexes {:?}",
                        spec.name,
                        check_point.client_id,
                        missing
                    );
                } else {
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        Ok(())
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
