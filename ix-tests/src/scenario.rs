use anyhow::bail;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioName {
    All,
    SingleTriage,
    SingleBasic,
    SingleLoad,
    DualConcurrent,
    DualRestart,
}

impl ScenarioName {
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        match input {
            "all" => Ok(Self::All),
            "single-triage" => Ok(Self::SingleTriage),
            "single-basic" => Ok(Self::SingleBasic),
            "single-load" => Ok(Self::SingleLoad),
            "dual-concurrent" => Ok(Self::DualConcurrent),
            "dual-restart" => Ok(Self::DualRestart),
            _ => bail!("unknown scenario: {input}"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::SingleTriage => "single-triage",
            Self::SingleBasic => "single-basic",
            Self::SingleLoad => "single-load",
            Self::DualConcurrent => "dual-concurrent",
            Self::DualRestart => "dual-restart",
        }
    }
}
