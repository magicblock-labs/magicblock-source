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
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    reqwest::Url,
    serde::Deserialize,
    std::{
        collections::BTreeMap,
        fs::File,
        io::Read,
        net::SocketAddr,
        path::{Path, PathBuf},
    },
};

/// Plugin config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[allow(dead_code)]
    #[serde(default)]
    libpath: Option<String>,

    /// Kafka config.
    pub kafka: KafkaConfig,

    /// Optional ksqlDB startup restore config.
    #[serde(default)]
    pub ksql: KsqlConfig,

    /// Plugin-specific runtime config.
    pub plugin: PluginConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    #[serde(default)]
    pub client: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KsqlConfig {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_ksql_table")]
    pub table: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
    pub local_rpc_url: String,
    pub admin: SocketAddr,
    #[serde(default)]
    pub metrics: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ValidatorConfig {
    #[allow(dead_code)]
    pub(crate) libpath: PathBuf,
    pub(crate) config_file: PathBuf,
}

fn default_shutdown_timeout_ms() -> u64 {
    30_000
}

fn default_ksql_table() -> String {
    "accounts".to_owned()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            libpath: None,
            kafka: KafkaConfig {
                bootstrap_servers: String::new(),
                topic: String::new(),
                client: BTreeMap::new(),
            },
            ksql: KsqlConfig {
                url: None,
                table: default_ksql_table(),
            },
            plugin: PluginConfig {
                shutdown_timeout_ms: default_shutdown_timeout_ms(),
                local_rpc_url: String::new(),
                admin: SocketAddr::from(([127, 0, 0, 1], 0)),
                metrics: false,
            },
        }
    }
}

impl Default for KsqlConfig {
    fn default() -> Self {
        Self {
            url: None,
            table: default_ksql_table(),
        }
    }
}

impl Config {
    /// Read plugin config from either a validator JSON wrapper or a TOML runtime config.
    #[allow(dead_code)]
    pub fn read_from<P: AsRef<Path>>(config_path: P) -> PluginResult<Self> {
        let config_path = config_path.as_ref();
        let contents = read_to_string(config_path)?;
        let runtime_path =
            match serde_json::from_str::<ValidatorConfig>(&contents) {
                Ok(wrapper) => resolve_runtime_config_path(
                    config_path,
                    &wrapper.config_file,
                ),
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
                        return Err(GeyserPluginError::ConfigFileReadError {
                            msg: error.to_string(),
                        });
                    }
                    config_path.to_path_buf()
                }
            };
        let runtime_contents = if runtime_path == config_path {
            contents
        } else {
            read_to_string(&runtime_path)?
        };
        let mut this: Self =
            toml::from_str(&runtime_contents).map_err(|e| {
                GeyserPluginError::ConfigFileReadError { msg: e.to_string() }
            })?;
        this.fill_defaults();
        this.validate()?;
        Ok(this)
    }

    fn set_default(&mut self, k: &'static str, v: &'static str) {
        if !self.kafka.client.contains_key(k) {
            self.kafka.client.insert(k.to_owned(), v.to_owned());
        }
    }

    pub(crate) fn fill_defaults(&mut self) {
        self.set_default("request.required.acks", "1");
        self.set_default("message.timeout.ms", "30000");
        self.set_default("compression.type", "lz4");
        self.set_default("partitioner", "murmur2_random");
    }

    pub(crate) fn validate(&self) -> PluginResult<()> {
        if self.kafka.bootstrap_servers.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `kafka.bootstrap_servers`"
                    .to_owned(),
            });
        }

        if self.kafka.topic.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `kafka.topic`".to_owned(),
            });
        }

        let trimmed_local_rpc_url = self.plugin.local_rpc_url.trim();
        if trimmed_local_rpc_url.is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg:
                    "invalid config field `plugin.local_rpc_url`: URL must not be empty"
                        .to_owned(),
            });
        }

        let parsed_local_rpc_url =
            Url::parse(trimmed_local_rpc_url).map_err(|error| {
                GeyserPluginError::ConfigFileReadError {
                    msg: format!(
                        "invalid config field `plugin.local_rpc_url`: {error}"
                    ),
                }
            })?;

        match parsed_local_rpc_url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: format!(
                        "invalid config field `plugin.local_rpc_url`: unsupported scheme `{scheme}`"
                    ),
                });
            }
        }

        if !parsed_local_rpc_url.has_host() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg:
                    "invalid config field `plugin.local_rpc_url`: host is required"
                        .to_owned(),
            });
        }

        if self.plugin.admin.port() == 0 {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "invalid admin address: port 0 is not allowed".to_owned(),
            });
        }

        if let Some(url) = &self.ksql.url {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg:
                        "invalid config field `ksql.url`: URL must not be empty"
                            .to_owned(),
                });
            }

            let parsed = Url::parse(trimmed).map_err(|error| {
                GeyserPluginError::ConfigFileReadError {
                    msg: format!("invalid config field `ksql.url`: {error}"),
                }
            })?;

            match parsed.scheme() {
                "http" | "https" => {}
                scheme => {
                    return Err(GeyserPluginError::ConfigFileReadError {
                        msg: format!(
                            "invalid config field `ksql.url`: unsupported scheme `{scheme}`"
                        ),
                    });
                }
            }

            if !parsed.has_host() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `ksql.url`: host is required"
                        .to_owned(),
                });
            }
        }

        validate_ksql_identifier(&self.ksql.table).map_err(|error| {
            GeyserPluginError::ConfigFileReadError {
                msg: format!("invalid config field `ksql.table`: {error}"),
            }
        })?;

        Ok(())
    }
}

/// Validates that `identifier` is a safe ksqlDB identifier suitable for
/// direct interpolation into a SQL statement. The identifier must start with
/// an ASCII letter or `_` and may otherwise contain only ASCII alphanumeric
/// characters or `_`.
pub(crate) fn validate_ksql_identifier(
    identifier: &str,
) -> std::io::Result<&str> {
    let mut chars = identifier.chars();
    let first = chars.next().ok_or_else(|| {
        std::io::Error::other("ksql identifier must not be empty")
    })?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(std::io::Error::other(format!(
            "invalid ksql identifier `{identifier}`: must start with an ASCII letter or `_`"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(std::io::Error::other(format!(
                "invalid ksql identifier `{identifier}`: only ASCII alphanumeric characters and `_` are allowed"
            )));
        }
    }
    Ok(identifier)
}

#[allow(dead_code)]
fn read_to_string(path: &Path) -> PluginResult<String> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

pub(crate) fn resolve_runtime_config_path(
    wrapper_path: &Path,
    runtime_path: &Path,
) -> PathBuf {
    if runtime_path.is_absolute() {
        runtime_path.to_path_buf()
    } else {
        wrapper_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(runtime_path)
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, validate_ksql_identifier};
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn test_validates_simple_identifier() {
        assert_eq!(validate_ksql_identifier("accounts").unwrap(), "accounts");
        assert_eq!(validate_ksql_identifier("_x").unwrap(), "_x");
        assert_eq!(validate_ksql_identifier("A1_b2").unwrap(), "A1_b2");
    }

    #[test]
    fn test_rejects_empty_identifier() {
        let error = validate_ksql_identifier("").unwrap_err().to_string();
        assert!(error.contains("must not be empty"));
    }

    #[test]
    fn test_rejects_identifier_starting_with_digit() {
        let error = validate_ksql_identifier("1bad").unwrap_err().to_string();
        assert!(error.contains("must start with an ASCII letter"));
    }

    #[test]
    fn test_rejects_identifier_with_invalid_characters() {
        let error = validate_ksql_identifier("accounts; DROP TABLE x")
            .unwrap_err()
            .to_string();
        assert!(error.contains("only ASCII alphanumeric"));
    }

    #[test]
    fn test_rejects_identifier_with_quote() {
        let error = validate_ksql_identifier("a\"b").unwrap_err().to_string();
        assert!(error.contains("only ASCII alphanumeric"));
    }

    fn parse_config(toml: &str) -> Result<Config, String> {
        let mut config: Config =
            toml::from_str(toml).map_err(|error| error.to_string())?;
        config.fill_defaults();
        config.validate().map_err(|error| format!("{error:?}"))?;
        Ok(config)
    }

    #[test]
    fn test_parses_valid_minimal_config() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.kafka.topic, "solana.testnet.account_updates");
        assert_eq!(config.plugin.local_rpc_url, "http://127.0.0.1:8899");
    }

    #[test]
    fn test_parses_valid_minimal_config_without_libpath() {
        let config = parse_config(
            r#"
[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.kafka.topic, "solana.testnet.account_updates");
        assert_eq!(config.plugin.local_rpc_url, "http://127.0.0.1:8899");
    }

    #[test]
    fn test_reads_runtime_toml_via_validator_json_wrapper() {
        let base = std::env::temp_dir().join(format!(
            "geyser-plugin-config-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&base).unwrap();

        let runtime_path = base.join("runtime.toml");
        fs::write(
            &runtime_path,
            r#"
[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        let wrapper_path = base.join("plugin-config.json");
        fs::write(
            &wrapper_path,
            r#"{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "config_file": "runtime.toml"
}"#,
        )
        .unwrap();

        let config = Config::read_from(&wrapper_path).unwrap();
        assert_eq!(config.kafka.topic, "solana.testnet.account_updates");
        assert_eq!(config.plugin.admin.port(), 8080);

        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn test_rejects_missing_admin() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
"#,
        )
        .unwrap_err();

        assert!(error.contains("missing field `admin`"));
    }

    #[test]
    fn test_rejects_missing_kafka_topic() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("missing field `topic`"));
    }

    #[test]
    fn test_rejects_missing_bootstrap_servers() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "   "
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains(
            "missing required config field `kafka.bootstrap_servers`"
        ));
    }

    #[test]
    fn test_rejects_legacy_filter_fields() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"
filters = [{ update_account_topic = "legacy" }]

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("unknown field `filters`"));
    }

    #[test]
    fn test_parses_config_with_metrics_enabled() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
metrics = true
"#,
        )
        .unwrap();

        assert!(config.plugin.metrics);
    }

    #[test]
    fn test_metrics_defaults_to_false() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert!(!config.plugin.metrics);
    }

    #[test]
    fn test_parses_config_without_ksql_startup_restore_url() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.ksql.url, None);
    }

    #[test]
    fn test_parses_config_with_valid_ksql_startup_restore_url() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "https://127.0.0.1:8088"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.ksql.url.as_deref(), Some("https://127.0.0.1:8088"));
    }

    #[test]
    fn test_rejects_empty_ksql_startup_restore_url() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "   "

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("URL must not be empty"));
    }

    #[test]
    fn test_rejects_ksql_startup_restore_url_without_scheme() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "127.0.0.1:8088"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("relative URL without a base"));
    }

    #[test]
    fn test_rejects_ksql_startup_restore_url_without_host() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "http://:8088"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(
            error.contains("empty host") || error.contains("host is required")
        );
    }

    #[test]
    fn test_rejects_invalid_ksql_table_identifier() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
url = "http://127.0.0.1:8088"
table = "bad-name"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("invalid config field `ksql.table`"));
        assert!(error.contains("only ASCII alphanumeric"));
    }

    #[test]
    fn test_rejects_ksql_table_starting_with_digit() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[ksql]
table = "1bad"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("invalid config field `ksql.table`"));
        assert!(error.contains("must start with an ASCII letter"));
    }

    #[test]
    fn test_rejects_empty_local_rpc_url() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "   "
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("invalid config field `plugin.local_rpc_url`"));
        assert!(error.contains("URL must not be empty"));
    }

    #[test]
    fn test_rejects_local_rpc_url_without_scheme() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("invalid config field `plugin.local_rpc_url`"));
        assert!(error.contains("relative URL without a base"));
    }

    #[test]
    fn test_rejects_local_rpc_url_with_unsupported_scheme() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "ftp://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("invalid config field `plugin.local_rpc_url`"));
        assert!(error.contains("unsupported scheme `ftp`"));
    }

    #[test]
    fn test_rejects_local_rpc_url_without_host() {
        let error = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap_err();

        assert!(error.contains("invalid config field `plugin.local_rpc_url`"));
        assert!(
            error.contains("empty host") || error.contains("host is required")
        );
    }

    #[test]
    fn test_passes_through_kafka_client_overrides() {
        let config = parse_config(
            r#"
libpath = "target/release/libsolana_accountsdb_plugin_kafka.so"

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[kafka.client]
"linger.ms" = "5"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        assert_eq!(config.kafka.client.get("linger.ms"), Some(&"5".to_owned()));
    }
}
