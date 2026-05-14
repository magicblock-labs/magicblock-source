use crate::scenario::ScenarioName;

pub fn ordered_scenarios(requested: ScenarioName) -> Vec<ScenarioName> {
    match requested {
        ScenarioName::All => vec![
            ScenarioName::SingleTriage,
            ScenarioName::SingleBasic,
            ScenarioName::SingleLoad,
            ScenarioName::DualConcurrent,
            ScenarioName::DualRestart,
        ],
        concrete => vec![concrete],
    }
}
