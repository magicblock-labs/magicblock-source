use base64::Engine;
use reqwest::Client;
use serde_json::Value;
use std::fmt::Write;

use crate::config::KsqlConfig;
use crate::domain::{AccountState, PubkeyFilter, bytes_to_base58};
use crate::errors::{GeykagError, GeykagResult};
use crate::traits::SnapshotStore;

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
        let url =
            format!("{}/query-stream", self.config.url.trim_end_matches('/'));

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

impl SnapshotStore for KsqlAccountSnapshotClient {
    async fn fetch_filtered(
        &self,
        filter: Option<&PubkeyFilter>,
    ) -> GeykagResult<Vec<AccountState>> {
        KsqlAccountSnapshotClient::fetch_filtered(self, filter).await
    }

    async fn fetch_one_by_pubkey(
        &self,
        pubkey: &PubkeyFilter,
    ) -> GeykagResult<Option<AccountState>> {
        KsqlAccountSnapshotClient::fetch_one_by_pubkey(self, pubkey).await
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use serde_json::{Value, json};

    use super::{
        decode_base64_field, decode_optional_base64_field, parse_account_row,
        parse_accounts_response, parse_u64_field, pubkey_bytes_literal,
    };
    use crate::domain::{PubkeyFilter, bytes_to_base58};
    use crate::errors::GeykagError;

    fn valid_pubkey_bytes() -> Vec<u8> {
        (1..=32).collect()
    }

    fn other_pubkey_bytes() -> Vec<u8> {
        (32..64).collect()
    }

    fn valid_base64_pubkey_field() -> String {
        base64::engine::general_purpose::STANDARD.encode(valid_pubkey_bytes())
    }

    fn base64_field(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn valid_ksql_row(pubkey_bytes: &[u8]) -> Value {
        Value::Array(vec![
            json!(base64_field(pubkey_bytes)),
            json!(123_u64),
            json!(456_u64),
            json!(base64_field(&other_pubkey_bytes())),
            json!(true),
            json!(789_u64),
            json!(base64_field(b"payload")),
            json!(321_u64),
            json!(base64_field(&[8; 64])),
        ])
    }

    fn response_body(lines: &[Value]) -> String {
        let mut body = lines
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        body.push('\n');
        body
    }

    #[test]
    fn pubkey_bytes_literal_formats_expected_hex() {
        let pubkey =
            PubkeyFilter::parse(&bytes_to_base58(&valid_pubkey_bytes()))
                .unwrap();

        assert_eq!(
            pubkey_bytes_literal(&pubkey),
            "TO_BYTES('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20', 'hex')"
        );
    }

    #[test]
    fn parse_accounts_response_ignores_metadata_query_id_lines() {
        let body = response_body(&[
            json!({ "queryId": "transient_1" }),
            valid_ksql_row(&valid_pubkey_bytes()),
        ]);

        let rows = parse_accounts_response(&body, None).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pubkey_bytes, valid_pubkey_bytes());
    }

    #[test]
    fn parse_accounts_response_parses_one_valid_row() {
        let body = response_body(&[valid_ksql_row(&valid_pubkey_bytes())]);

        let rows = parse_accounts_response(&body, None).unwrap();

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.pubkey_bytes, valid_pubkey_bytes());
        assert_eq!(row.pubkey_b58, bytes_to_base58(&valid_pubkey_bytes()));
        assert_eq!(row.slot, 123);
        assert_eq!(row.lamports, 456);
        assert_eq!(row.owner_b58, bytes_to_base58(&other_pubkey_bytes()));
        assert!(row.executable);
        assert_eq!(row.rent_epoch, 789);
        assert_eq!(row.write_version, 321);
        assert_eq!(row.txn_signature_b58, Some(bytes_to_base58(&[8; 64])));
        assert_eq!(row.data_len, b"payload".len());
    }

    #[test]
    fn parse_accounts_response_parses_multiple_valid_rows() {
        let another_pubkey: Vec<u8> = (64..96).collect();
        let body = response_body(&[
            valid_ksql_row(&valid_pubkey_bytes()),
            valid_ksql_row(&another_pubkey),
        ]);

        let rows = parse_accounts_response(&body, None).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].pubkey_bytes, valid_pubkey_bytes());
        assert_eq!(rows[1].pubkey_bytes, another_pubkey);
    }

    #[test]
    fn parse_accounts_response_applies_pubkey_filter() {
        let another_pubkey: Vec<u8> = (64..96).collect();
        let filter =
            PubkeyFilter::parse(&bytes_to_base58(&valid_pubkey_bytes()))
                .unwrap();
        let body = response_body(&[
            valid_ksql_row(&valid_pubkey_bytes()),
            valid_ksql_row(&another_pubkey),
        ]);

        let rows = parse_accounts_response(&body, Some(&filter)).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pubkey_bytes, valid_pubkey_bytes());
    }

    #[test]
    fn parse_accounts_response_returns_empty_for_empty_or_whitespace_body() {
        assert!(parse_accounts_response("", None).unwrap().is_empty());
        assert!(parse_accounts_response(" \n\t\n", None).unwrap().is_empty());
    }

    #[test]
    fn parse_accounts_response_rejects_invalid_top_level_json_shapes() {
        let body = response_body(&[json!("not an array")]);
        let error = parse_accounts_response(&body, None).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::UnexpectedKsqlResponseLine { .. }
        ));
    }

    #[test]
    fn parse_accounts_response_returns_ksql_error_response_for_type_lines() {
        let body = response_body(&[json!({
            "@type": "statement_error",
            "message": "bad query"
        })]);
        let error = parse_accounts_response(&body, None).unwrap_err();

        match error {
            GeykagError::KsqlErrorResponse {
                error_type,
                details,
            } => {
                assert_eq!(error_type, "\"statement_error\"");
                assert!(details.contains("bad query"));
            }
            other => panic!("expected KsqlErrorResponse, got {other:?}"),
        }
    }

    #[test]
    fn valid_pubkey_field_helper_returns_encoded_32_byte_pubkey() {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(valid_base64_pubkey_field())
            .unwrap();

        assert_eq!(decoded, valid_pubkey_bytes());
    }

    #[test]
    fn parse_account_row_rejects_wrong_column_count() {
        let error = parse_account_row(&[json!(valid_base64_pubkey_field())])
            .unwrap_err();

        assert!(matches!(
            error,
            GeykagError::UnexpectedKsqlColumnCount { actual: 1 }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_slot_integer() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[1] = json!("bad-slot");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlParseField { field: "SLOT", .. }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_lamports_integer() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[2] = json!("bad-lamports");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlParseField {
                field: "LAMPORTS",
                ..
            }
        ));
    }

    #[test]
    fn parse_account_row_rejects_non_boolean_executable() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[4] = json!("true");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlParseField {
                field: "EXECUTABLE",
                ..
            }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_rent_epoch_integer() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[5] = json!(false);

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlParseField {
                field: "RENT_EPOCH",
                ..
            }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_write_version_integer() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[7] = json!(true);

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlParseField {
                field: "WRITE_VERSION",
                ..
            }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_pubkey_base64() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[0] = json!("%%%");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlDecodeBase64Field {
                field: "PUBKEY",
                ..
            }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_owner_base64() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[3] = json!("%%%");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlDecodeBase64Field { field: "OWNER", .. }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_data_base64() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[6] = json!("%%%");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::KsqlDecodeBase64Field { field: "DATA", .. }
        ));
    }

    #[test]
    fn parse_account_row_rejects_invalid_txn_signature_base64() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[8] = json!("%%%");

        let error = parse_account_row(row.as_array().unwrap()).unwrap_err();

        assert!(matches!(error, GeykagError::KsqlDecodeTxnSignature { .. }));
    }

    #[test]
    fn parse_account_row_accepts_empty_data() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[6] = json!("");

        let account = parse_account_row(row.as_array().unwrap()).unwrap();

        assert_eq!(account.data_len, 0);
    }

    #[test]
    fn parse_account_row_accepts_empty_optional_txn_signature() {
        let mut row = valid_ksql_row(&valid_pubkey_bytes());
        row.as_array_mut().unwrap()[8] = json!("");

        let account = parse_account_row(row.as_array().unwrap()).unwrap();

        assert_eq!(account.txn_signature_b58, None);
    }

    #[test]
    fn parse_u64_field_accepts_u64_values() {
        assert_eq!(parse_u64_field(&json!(42_u64)).unwrap(), 42);
    }

    #[test]
    fn parse_u64_field_accepts_non_negative_i64_values() {
        assert_eq!(parse_u64_field(&json!(42_i64)).unwrap(), 42);
    }

    #[test]
    fn parse_u64_field_rejects_string_values() {
        let error = parse_u64_field(&json!("42")).unwrap_err();

        assert!(matches!(error, GeykagError::InvalidJsonInteger { .. }));
    }

    #[test]
    fn parse_u64_field_rejects_boolean_values() {
        let error = parse_u64_field(&json!(true)).unwrap_err();

        assert!(matches!(error, GeykagError::InvalidJsonInteger { .. }));
    }

    #[test]
    fn decode_base64_field_rejects_non_string_json() {
        let error = decode_base64_field(&json!(123)).unwrap_err();

        assert!(matches!(error, GeykagError::ExpectedBase64String));
    }

    #[test]
    fn decode_optional_base64_field_rejects_non_string_json() {
        let error = decode_optional_base64_field(&json!(123)).unwrap_err();

        assert!(matches!(error, GeykagError::ExpectedOptionalBase64String));
    }
}
