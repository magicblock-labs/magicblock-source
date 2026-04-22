use std::path::PathBuf;

use anyhow::{Context, bail};

pub struct Cli {
    pub config_path: PathBuf,
    pub scenario: String,
}

impl Cli {
    pub fn parse() -> anyhow::Result<Self> {
        let mut cli = Self {
            config_path: PathBuf::from("ix-tests/configs/suite.toml"),
            scenario: "all".to_owned(),
        };

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let path =
                        args.next().context("missing value for --config")?;
                    cli.config_path = PathBuf::from(path);
                }
                "--scenario" => {
                    cli.scenario =
                        args.next().context("missing value for --scenario")?;
                }
                _ => bail!("invalid CLI argument: {arg}"),
            }
        }

        Ok(cli)
    }
}
