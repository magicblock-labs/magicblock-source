// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    std::{error::Error, fmt, fs, path::PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupError {
    pub subsystem: &'static str,
    pub field: Option<&'static str>,
    pub target: Option<String>,
    pub cause: String,
    pub action: String,
}

impl StartupError {
    pub fn new(
        subsystem: &'static str,
        field: Option<&'static str>,
        target: Option<impl Into<String>>,
        cause: impl Into<String>,
        action: impl Into<String>,
    ) -> Self {
        Self {
            subsystem,
            field,
            target: target.map(Into::into),
            cause: cause.into(),
            action: action.into(),
        }
    }
}

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "ERROR {} startup check failed", self.subsystem)?;
        if let Some(field) = self.field {
            writeln!(f, "  field: {field}")?;
        }
        if let Some(target) = &self.target {
            writeln!(f, "  target: {target}")?;
        }
        writeln!(f, "  cause: {}", self.cause)?;
        write!(f, "  action: {}", self.action)
    }
}

impl Error for StartupError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorConfigPaths {
    pub wrapper_path: PathBuf,
    pub libpath: PathBuf,
    pub runtime_config_path: PathBuf,
}

#[derive(Debug)]
pub struct LoadedPluginConfig {
    pub paths: Option<ValidatorConfigPaths>,
    pub config: crate::config::Config,
}

pub(crate) fn run_static_startup_checks(
    config_path: impl AsRef<std::path::Path>,
) -> Result<LoadedPluginConfig, StartupError> {
    let loaded = load_config_with_paths(config_path)?;
    if let Some(paths) = &loaded.paths
        && !paths.libpath.exists()
    {
        return Err(StartupError::new(
            "config",
            Some("libpath"),
            Some(paths.libpath.display().to_string()),
            "plugin shared library does not exist",
            "run make geyser-plugin-build or update libpath in the validator JSON wrapper",
        ));
    }
    Ok(loaded)
}

pub fn load_config_with_paths(
    config_path: impl AsRef<std::path::Path>,
) -> Result<LoadedPluginConfig, StartupError> {
    let config_path = config_path.as_ref();
    let contents = fs::read_to_string(config_path).map_err(|error| {
        StartupError::new(
            "config",
            None,
            Some(config_path.display().to_string()),
            format!("failed to read config file: {error}"),
            "check that the file exists and is readable",
        )
    })?;

    match serde_json::from_str::<crate::config::ValidatorConfig>(&contents) {
        Ok(wrapper) => {
            let libpath =
                resolve_wrapper_relative_path(config_path, &wrapper.libpath);
            let runtime_config_path =
                crate::config::resolve_runtime_config_path(
                    config_path,
                    &wrapper.config_file,
                );
            if !runtime_config_path.exists() {
                return Err(StartupError::new(
                    "config",
                    Some("config_file"),
                    Some(runtime_config_path.display().to_string()),
                    "runtime TOML config does not exist",
                    "create the runtime TOML config or update config_file in the validator JSON wrapper",
                ));
            }

            let config =
                read_parse_validate_runtime_config(&runtime_config_path)?;
            Ok(LoadedPluginConfig {
                paths: Some(ValidatorConfigPaths {
                    wrapper_path: config_path.to_path_buf(),
                    libpath,
                    runtime_config_path,
                }),
                config,
            })
        }
        Err(error) => {
            let looks_like_json = config_path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
                || matches!(
                    contents.trim_start().as_bytes().first(),
                    Some(b'{') | Some(b'[')
                );
            if looks_like_json {
                return Err(StartupError::new(
                    "config",
                    None,
                    Some(config_path.display().to_string()),
                    format!("invalid validator config JSON: {error}"),
                    "fix the validator JSON wrapper; it must contain libpath and config_file",
                ));
            }

            let config = parse_validate_runtime_config(&contents, config_path)?;
            Ok(LoadedPluginConfig {
                paths: None,
                config,
            })
        }
    }
}

fn resolve_wrapper_relative_path(
    wrapper_path: &std::path::Path,
    path: &std::path::Path,
) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        wrapper_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(path)
    }
}

fn read_parse_validate_runtime_config(
    path: &std::path::Path,
) -> Result<crate::config::Config, StartupError> {
    let contents = fs::read_to_string(path).map_err(|error| {
        config_error_to_startup_error(
            path,
            GeyserPluginError::ConfigFileReadError {
                msg: format!("failed to read runtime TOML config: {error}"),
            },
        )
    })?;
    parse_validate_runtime_config(&contents, path)
}

fn parse_validate_runtime_config(
    contents: &str,
    path: &std::path::Path,
) -> Result<crate::config::Config, StartupError> {
    let mut config: crate::config::Config = toml::from_str(contents)
        .map_err(|error| toml_error_to_startup_error(path, error))?;
    config.fill_defaults();
    config
        .validate()
        .map_err(|error| config_error_to_startup_error(path, error))?;
    Ok(config)
}

fn toml_error_to_startup_error(
    path: &std::path::Path,
    error: toml::de::Error,
) -> StartupError {
    StartupError::new(
        "config",
        None,
        Some(path.display().to_string()),
        error.to_string(),
        "fix the runtime TOML config",
    )
}

fn config_error_to_startup_error(
    path: &std::path::Path,
    error: GeyserPluginError,
) -> StartupError {
    let cause = match error {
        GeyserPluginError::ConfigFileReadError { msg } => msg,
        other => other.to_string(),
    };
    StartupError::new(
        "config",
        None,
        Some(path.display().to_string()),
        cause,
        "fix the runtime TOML config",
    )
}

#[cfg(test)]
mod tests {
    use super::{load_config_with_paths, run_static_startup_checks};
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_dir(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "geyser-plugin-preflight-{test_name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn valid_runtime_config() -> &'static str {
        r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#
    }

    #[test]
    fn malformed_validator_json_reports_invalid_validator_config_json() {
        let base = temp_dir("malformed-json");
        fs::create_dir_all(&base).unwrap();
        let wrapper_path = base.join("plugin-config.json");
        fs::write(&wrapper_path, r#"{ "libpath": "plugin.so", "#).unwrap();

        let error = load_config_with_paths(&wrapper_path).unwrap_err();
        assert_eq!(error.subsystem, "config");
        assert!(error.cause.contains("invalid validator config JSON"));

        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn missing_config_file_target_reports_config_file_field() {
        let base = temp_dir("missing-config-file");
        fs::create_dir_all(&base).unwrap();
        let wrapper_path = base.join("plugin-config.json");
        fs::write(
            &wrapper_path,
            r#"{
  "libpath": "plugin.so",
  "config_file": "missing.toml"
}"#,
        )
        .unwrap();

        let error = load_config_with_paths(&wrapper_path).unwrap_err();
        assert_eq!(error.subsystem, "config");
        assert_eq!(error.field, Some("config_file"));
        assert!(error.target.as_deref().unwrap().ends_with("missing.toml"));

        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn malformed_runtime_toml_reports_config_subsystem() {
        let base = temp_dir("malformed-runtime-toml");
        fs::create_dir_all(&base).unwrap();
        let runtime_path = base.join("runtime.toml");
        fs::write(&runtime_path, "[kafka\nbootstrap_servers = nope").unwrap();
        let wrapper_path = base.join("plugin-config.json");
        fs::write(
            &wrapper_path,
            r#"{
  "libpath": "plugin.so",
  "config_file": "runtime.toml"
}"#,
        )
        .unwrap();

        let error = load_config_with_paths(&wrapper_path).unwrap_err();
        assert_eq!(error.subsystem, "config");
        assert!(error.action.contains("fix the runtime TOML config"));

        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn direct_toml_config_loads_successfully() {
        let base = temp_dir("direct-toml");
        fs::create_dir_all(&base).unwrap();
        let runtime_path = base.join("runtime.toml");
        fs::write(&runtime_path, valid_runtime_config()).unwrap();

        let loaded = load_config_with_paths(&runtime_path).unwrap();
        assert!(loaded.paths.is_none());
        assert_eq!(loaded.config.kafka.topic, "solana.testnet.account_updates");

        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn static_startup_checks_report_missing_libpath() {
        let base = temp_dir("missing-libpath");
        fs::create_dir_all(&base).unwrap();
        let runtime_path = base.join("runtime.toml");
        fs::write(&runtime_path, valid_runtime_config()).unwrap();
        let wrapper_path = base.join("plugin-config.json");
        fs::write(
            &wrapper_path,
            r#"{
  "libpath": "missing-plugin.so",
  "config_file": "runtime.toml"
}"#,
        )
        .unwrap();

        let error = run_static_startup_checks(&wrapper_path).unwrap_err();
        assert_eq!(error.subsystem, "config");
        assert_eq!(error.field, Some("libpath"));
        assert!(
            error
                .target
                .as_deref()
                .unwrap()
                .ends_with("missing-plugin.so")
        );
        assert_eq!(error.cause, "plugin shared library does not exist");
        assert!(error.action.contains("make geyser-plugin-build"));

        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn static_startup_checks_accept_existing_libpath() {
        let base = temp_dir("existing-libpath");
        fs::create_dir_all(&base).unwrap();
        let runtime_path = base.join("runtime.toml");
        fs::write(&runtime_path, valid_runtime_config()).unwrap();
        let libpath = base.join("plugin.so");
        fs::write(&libpath, "").unwrap();
        let wrapper_path = base.join("plugin-config.json");
        fs::write(
            &wrapper_path,
            r#"{
  "libpath": "plugin.so",
  "config_file": "runtime.toml"
}"#,
        )
        .unwrap();

        let loaded = run_static_startup_checks(&wrapper_path).unwrap();
        let paths = loaded.paths.unwrap();
        assert_eq!(paths.libpath, libpath);

        fs::remove_dir_all(&base).unwrap();
    }
}
