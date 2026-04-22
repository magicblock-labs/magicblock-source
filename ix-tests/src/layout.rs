use crate::scenario::ScenarioName;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceInstance {
    One,
    Two,
}

pub struct ScenarioLayout {
    pub services: Vec<ServiceInstance>,
    pub client_count: usize,
}

impl ScenarioLayout {
    pub fn for_scenario(name: ScenarioName) -> Self {
        match name {
            ScenarioName::SingleBasic => Self {
                services: vec![ServiceInstance::One],
                client_count: 4,
            },
            ScenarioName::SingleLoad => Self {
                services: vec![ServiceInstance::One],
                client_count: 100,
            },
            ScenarioName::DualConcurrent | ScenarioName::DualRestart => Self {
                services: vec![ServiceInstance::One, ServiceInstance::Two],
                client_count: 20,
            },
            ScenarioName::All => {
                unreachable!("All is expanded before layout")
            }
        }
    }
}
