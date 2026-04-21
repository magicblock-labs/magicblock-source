use base64::Engine;
use reqwest::Client;
use serde_json::Value;
use std::fmt::Write;

use crate::config::KsqlConfig;
use crate::domain::{AccountState, PubkeyFilter, bytes_to_base58};
use crate::errors::{GeykagError, GeykagResult};

#[derive(Clone, Debug)]
pub struct KsqlAccountSnapshotClient {
    client: Client,
    config: KsqlConfig,
}

impl KsqlAccountSnapshotClient {
    pub fn new(config: KsqlConfig) -> GeykagResult<Self> {
        let client = Client::builder()
            .build()
            .map_err(|source| GeykagError::KsqlClientBuild { source })?;

        Ok(Self { client, config })
    }

    pub async fn fetch_filtered(
        &self,
        filter: Option<&PubkeyFilter>,
    ) -> GeykagResult<Vec<AccountState>> {
        let sql = format!(
            "SELECT PUBKEY, SLOT, LAMPORTS, OWNER, EXECUTABLE, RENT_EPOCH, DATA, WRITE_VERSION, TXN_SIGNATURE FROM {} LIMIT 1000;",
            self.config.table
        );
        let body = self.execute_query(&sql).await?;

        parse_accounts_response(&body, filter)
    }

    #[allow(dead_code)]
    pub async fn fetch_one_by_pubkey(
        &self,
        pubkey: &PubkeyFilter,
    ) -> GeykagResult<Option<AccountState>> {
        let sql = format!(
            "SELECT PUBKEY, SLOT, LAMPORTS, OWNER, EXECUTABLE, RENT_EPOCH, DATA, WRITE_VERSION, TXN_SIGNATURE FROM {} WHERE PUBKEY = {};",
            self.config.table,
            pubkey_bytes_literal(pubkey)
        );
        let body = self.execute_query(&sql).await?;
        let mut rows = parse_accounts_response(&body, None)?;

        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.pop()),
            actual => Err(GeykagError::UnexpectedKsqlSingleRowCount { actual }),
        }
    }

    async fn execute_query(&self, sql: &str) -> GeykagResult<String> {
        let url = format!(
            "{}/query-stream",
            self.config.server_url.trim_end_matches('/')
        );

        let response = self
            .client
            .post(url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/vnd.ksql.v1+json; charset=utf-8",
            )
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await
            .map_err(|source| GeykagError::KsqlQuery { source })?
            .error_for_status()
            .map_err(|source| GeykagError::KsqlQueryStatus { source })?;

        response
            .text()
            .await
            .map_err(|source| GeykagError::KsqlResponseBody { source })
    }
}

fn pubkey_bytes_literal(pubkey: &PubkeyFilter) -> String {
    let pubkey_bytes = bs58::decode(pubkey.as_str())
        .into_vec()
        .expect("PubkeyFilter should always contain normalized base58 bytes");
    let mut hex = String::with_capacity(pubkey_bytes.len() * 2);

    for byte in pubkey_bytes {
        let _ = write!(&mut hex, "{byte:02x}");
    }

    format!("TO_BYTES('{hex}', 'hex')")
}

fn parse_accounts_response(
    body: &str,
    filter: Option<&PubkeyFilter>,
) -> GeykagResult<Vec<AccountState>> {
    let mut accounts = Vec::new();

    for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(|source| {
            GeykagError::KsqlJsonLine {
                line: line.to_owned(),
                source,
            }
        })?;

        match value {
            Value::Object(mut object) => {
                if object.contains_key("queryId") {
                    continue;
                }

                if let Some(error_type) = object.remove("@type") {
                    return Err(GeykagError::KsqlErrorResponse {
                        error_type: error_type.to_string(),
                        details: format!("{object:?}"),
                    });
                }
            }
            Value::Array(row) => {
                let account = parse_account_row(&row)?;
                if account.matches_filter(filter) {
                    accounts.push(account);
                }
            }
            _ => {
                return Err(GeykagError::UnexpectedKsqlResponseLine {
                    line: line.to_owned(),
                });
            }
        }
    }

    Ok(accounts)
}

fn parse_account_row(row: &[Value]) -> GeykagResult<AccountState> {
    if row.len() != 9 {
        return Err(GeykagError::UnexpectedKsqlColumnCount {
            actual: row.len(),
        });
    }

    let pubkey = decode_base64_field(&row[0]).map_err(|source| {
        GeykagError::KsqlDecodeBase64Field {
            field: "PUBKEY",
            source: Box::new(source),
        }
    })?;
    let slot = parse_u64_field(&row[1]).map_err(|source| {
        GeykagError::KsqlParseField {
            field: "SLOT",
            source: Box::new(source),
        }
    })?;
    let lamports = parse_u64_field(&row[2]).map_err(|source| {
        GeykagError::KsqlParseField {
            field: "LAMPORTS",
            source: Box::new(source),
        }
    })?;
    let owner = decode_base64_field(&row[3]).map_err(|source| {
        GeykagError::KsqlDecodeBase64Field {
            field: "OWNER",
            source: Box::new(source),
        }
    })?;
    let executable = row[4].as_bool().ok_or(GeykagError::KsqlParseField {
        field: "EXECUTABLE",
        source: Box::new(GeykagError::InvalidJsonBool {
            value: row[4].clone(),
        }),
    })?;
    let rent_epoch = parse_u64_field(&row[5]).map_err(|source| {
        GeykagError::KsqlParseField {
            field: "RENT_EPOCH",
            source: Box::new(source),
        }
    })?;
    let data = decode_base64_field(&row[6]).map_err(|source| {
        GeykagError::KsqlDecodeBase64Field {
            field: "DATA",
            source: Box::new(source),
        }
    })?;
    let write_version = parse_u64_field(&row[7]).map_err(|source| {
        GeykagError::KsqlParseField {
            field: "WRITE_VERSION",
            source: Box::new(source),
        }
    })?;
    let txn_signature =
        decode_optional_base64_field(&row[8]).map_err(|source| {
            GeykagError::KsqlDecodeTxnSignature {
                source: Box::new(source),
            }
        })?;

    Ok(AccountState {
        pubkey_b58: bytes_to_base58(&pubkey),
        pubkey_bytes: pubkey,
        owner_b58: bytes_to_base58(&owner),
        slot,
        lamports,
        executable,
        rent_epoch,
        write_version,
        txn_signature_b58: txn_signature.as_deref().map(bytes_to_base58),
        data_len: data.len(),
    })
}

fn parse_u64_field(value: &Value) -> GeykagResult<u64> {
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }

    if let Some(number) = value.as_i64() {
        return Ok(number as u64);
    }

    Err(GeykagError::InvalidJsonInteger {
        value: value.clone(),
    })
}

fn decode_base64_field(value: &Value) -> GeykagResult<Vec<u8>> {
    let encoded = value.as_str().ok_or(GeykagError::ExpectedBase64String)?;

    if encoded.is_empty() {
        return Ok(Vec::new());
    }

    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|source| GeykagError::InvalidBase64Field {
            encoded: encoded.to_owned(),
            source,
        })
}

fn decode_optional_base64_field(
    value: &Value,
) -> GeykagResult<Option<Vec<u8>>> {
    let encoded = value
        .as_str()
        .ok_or(GeykagError::ExpectedOptionalBase64String)?;

    if encoded.is_empty() {
        return Ok(None);
    }

    decode_base64_field(value).map(Some)
}
