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

        if !mismatches.is_empty() {
            warn!("Mismatches:\n {}", mismatches.join("\n  "));
        }
        mismatches.is_empty()
    }
}

/// Per-client state tracked across loop iterations within a single
/// `wait_until_satisfied` call. We consume each required exactly once
/// (so subsequent iterations only compare unmatched required against
/// newly-arrived observations) and we track how far into the log we
/// have already scanned so we never re-examine an observation.
struct ClientMatchState {
    /// Whether each required entry (by index) has already been matched.
    matched: Vec<bool>,
    /// Index into `client.log()` from which we still need to scan.
    /// Starts at `cursor.next_index` for the checkpoint.
    scan_cursor: usize,
}

impl ClientMatchState {
    fn all_matched(&self) -> bool {
        self.matched.iter().all(|matched| *matched)
    }
}

/// Try to consume each new observation against any still-unmatched
/// required entry. Each observation may satisfy at most one required
/// entry, and once a required has been matched it stays matched for
/// the remainder of the checkpoint (so subsequent observations are
/// only compared against the still-unmatched required entries).
fn consume_observations(
    required: &[ExpectedUpdate],
    matched: &mut [bool],
    observations: &[ObservedUpdate],
) {
    for observation in observations {
        if matched.iter().all(|m| *m) {
            return;
        }
        for (req_idx, expected) in required.iter().enumerate() {
            if !matched[req_idx] && expected.matches(observation) {
                matched[req_idx] = true;
                break;
            }
        }
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

        // Initialize per-client matching state seeded from the
        // existing cursors so we only consider observations newer than
        // anything a previous checkpoint already advanced past.
        let mut states: Vec<ClientMatchState> = spec
            .clients
            .iter()
            .map(|client_spec| {
                let scan_cursor = cursors
                    .iter()
                    .find(|cursor| cursor.client_id == client_spec.client_id)
                    .map(|cursor| cursor.next_index)
                    .unwrap_or(0);
                ClientMatchState {
                    matched: vec![false; client_spec.required.len()],
                    scan_cursor,
                }
            })
            .collect();

        loop {
            // For each client, fetch only the observations that have
            // arrived since we last scanned and try to consume each one
            // against any still-unmatched required entry.
            for (idx, client_spec) in spec.clients.iter().enumerate() {
                let client = clients
                    .iter()
                    .find(|client| client.id == client_spec.client_id)
                    .with_context(|| {
                        format!(
                            "checkpoint '{}' references unknown client {}",
                            spec.name, client_spec.client_id
                        )
                    })?;
                let state = &mut states[idx];
                let new_observations =
                    client.log().snapshot_from(state.scan_cursor);
                state.scan_cursor += new_observations.len();

                consume_observations(
                    &client_spec.required,
                    &mut state.matched,
                    &new_observations,
                );
            }

            let all_satisfied = states.iter().all(|state| state.all_matched());

            if all_satisfied {
                // Advance public cursors for clients participating in
                // this checkpoint to the end of their log so the next
                // checkpoint starts from fresh observations only.
                for client_spec in &spec.clients {
                    let cursor = cursors
                        .iter_mut()
                        .find(|cursor| {
                            cursor.client_id == client_spec.client_id
                        })
                        .with_context(|| {
                            format!(
                                "checkpoint '{}' is missing cursor for client {}",
                                spec.name, client_spec.client_id
                            )
                        })?;
                    let client = clients
                        .iter()
                        .find(|client| client.id == client_spec.client_id)
                        .with_context(|| {
                            format!(
                                "checkpoint '{}' cannot advance missing client {}",
                                spec.name, client_spec.client_id
                            )
                        })?;
                    cursor.next_index = client.log().len();
                }
                return Ok(());
            }

            if Instant::now() >= deadline {
                // Build a useful diagnostic per client and bail.
                for (idx, client_spec) in spec.clients.iter().enumerate() {
                    let state = &states[idx];
                    if state.all_matched() {
                        continue;
                    }
                    let missing: Vec<&ExpectedUpdate> = client_spec
                        .required
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !state.matched[*i])
                        .map(|(_, expected)| expected)
                        .collect();
                    let Some(client) = clients
                        .iter()
                        .find(|client| client.id == client_spec.client_id)
                    else {
                        continue;
                    };
                    let observation_start = cursors
                        .iter()
                        .find(|cursor| {
                            cursor.client_id == client_spec.client_id
                        })
                        .map(|cursor| cursor.next_index)
                        .unwrap_or(0);
                    let observed =
                        client.log().snapshot_from(observation_start);
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

    use super::{ExpectedUpdate, consume_observations};

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
    fn consume_observations_marks_required_as_matched_with_noise() {
        let noise = observed_with(0, None);
        let matching = observed_with(1_000_000, Some("real-sig".to_owned()));
        let observed = vec![noise, matching];

        let required = vec![ExpectedUpdate {
            lamports: Some(1_000_000),
            txn_signature_b58: Some(Some("real-sig".to_owned())),
            ..Default::default()
        }];
        let mut matched = vec![false; required.len()];

        consume_observations(&required, &mut matched, &observed);

        assert_eq!(matched, vec![true]);
    }

    #[test]
    fn consume_observations_leaves_required_unmatched_when_never_arrives() {
        let noise = observed_with(0, None);
        let observed = vec![noise];

        let required = vec![ExpectedUpdate {
            lamports: Some(1_000_000),
            ..Default::default()
        }];
        let mut matched = vec![false; required.len()];

        consume_observations(&required, &mut matched, &observed);

        assert_eq!(matched, vec![false]);
    }

    #[test]
    fn consume_observations_consumes_each_observation_at_most_once() {
        // Two near-identical required entries; a single observation
        // matching both should only consume one of them.
        let observed =
            vec![observed_with(1_000_000, Some("real-sig".to_owned()))];

        let required = vec![
            ExpectedUpdate {
                lamports: Some(1_000_000),
                ..Default::default()
            },
            ExpectedUpdate {
                lamports: Some(1_000_000),
                ..Default::default()
            },
        ];
        let mut matched = vec![false; required.len()];

        consume_observations(&required, &mut matched, &observed);

        assert_eq!(matched, vec![true, false]);
    }

    #[test]
    fn consume_observations_is_incremental_across_calls() {
        // Simulate two iterations of the wait loop: first only noise
        // arrives, then the matching observation arrives. The second
        // call only sees the new observation and still satisfies the
        // required entry.
        let required = vec![ExpectedUpdate {
            lamports: Some(1_000_000),
            ..Default::default()
        }];
        let mut matched = vec![false; required.len()];

        let first_batch = vec![observed_with(0, None)];
        consume_observations(&required, &mut matched, &first_batch);
        assert_eq!(matched, vec![false]);

        let second_batch =
            vec![observed_with(1_000_000, Some("real-sig".to_owned()))];
        consume_observations(&required, &mut matched, &second_batch);
        assert_eq!(matched, vec![true]);
    }
}
