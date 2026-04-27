use {
    base64::Engine,
    log::debug,
    reqwest::{Url, blocking::Client, header::CONTENT_TYPE},
    serde_json::Value,
    std::io::{self, BufRead, BufReader},
};

pub(crate) const INIT_TRACKING_RESTORE_CHUNK_SIZE: usize = 256;

pub(crate) struct KsqlPubkeyRestoreClient {
    client: Client,
    base_url: String,
    table: String,
}

impl KsqlPubkeyRestoreClient {
    pub(crate) fn new(base_url: &str, table: &str) -> io::Result<Self> {
        let parsed = Url::parse(base_url).map_err(|error| {
            io::Error::other(format!("invalid ksql base URL: {error}"))
        })?;
        let normalized = parsed.as_str().trim_end_matches('/').to_owned();
        let client = Client::builder().build().map_err(|error| {
            io::Error::other(format!("failed to build ksql client: {error}"))
        })?;

        Ok(Self {
            client,
            base_url: normalized,
            table: table.to_owned(),
        })
    }

    pub(crate) fn fetch_pubkeys(&self) -> io::Result<Vec<[u8; 32]>> {
        let table = validate_ksql_identifier(&self.table)?;
        let sql = format!("SELECT PUBKEY FROM {table};");
        let query_url = format!("{}/query-stream", self.base_url);
        debug!(
            "Querying ksql for startup restore, url={}, sql={}",
            query_url, sql
        );

        let response = self
            .client
            .post(&query_url)
            .header(CONTENT_TYPE, "application/vnd.ksql.v1+json; charset=utf-8")
            .json(&serde_json::json!({
                "sql": sql,
            }))
            .send()
            .map_err(|error| {
                io::Error::other(format!("failed to query ksqlDB: {error}"))
            })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_else(|error| {
                format!("<failed to read ksql error body: {error}>")
            });
            return Err(io::Error::other(format!(
                "ksqlDB query failed with HTTP status {status}: {body}"
            )));
        }

        let reader = BufReader::new(response);
        let pubkeys = parse_pubkeys_stream(reader)?;
        debug!(
            "Parsed ksql startup restore response, found_pubkeys={}",
            pubkeys.len()
        );
        Ok(pubkeys)
    }
}

/// Validates that `identifier` is a safe ksqlDB identifier suitable for
/// direct interpolation into a SQL statement. The identifier must start with
/// an ASCII letter or `_` and may otherwise contain only ASCII alphanumeric
/// characters or `_`.
pub(crate) fn validate_ksql_identifier(identifier: &str) -> io::Result<&str> {
    let mut chars = identifier.chars();
    let first = chars
        .next()
        .ok_or_else(|| io::Error::other("ksql identifier must not be empty"))?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(io::Error::other(format!(
            "invalid ksql identifier `{identifier}`: must start with an ASCII letter or `_`"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(io::Error::other(format!(
                "invalid ksql identifier `{identifier}`: only ASCII alphanumeric characters and `_` are allowed"
            )));
        }
    }
    Ok(identifier)
}

pub(crate) fn parse_pubkeys_stream(
    reader: impl BufRead,
) -> io::Result<Vec<[u8; 32]>> {
    let mut pubkeys = Vec::new();

    for line_result in reader.lines() {
        let line = line_result?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line).map_err(|error| {
            io::Error::other(format!(
                "invalid ksql response line `{line}`: {error}"
            ))
        })?;

        match value {
            Value::Object(mut object) => {
                if object.contains_key("queryId") {
                    continue;
                }

                if let Some(error_type) = object.remove("@type") {
                    return Err(io::Error::other(format!(
                        "ksql error response {error_type}: {object:?}"
                    )));
                }

                return Err(io::Error::other(format!(
                    "unexpected ksql object line: {object:?}"
                )));
            }
            Value::Array(row) => {
                if row.len() != 1 {
                    return Err(io::Error::other(format!(
                        "unexpected ksql column count: expected 1, got {}",
                        row.len()
                    )));
                }

                let encoded = row[0].as_str().ok_or_else(|| {
                    io::Error::other(format!(
                        "expected PUBKEY column to be base64 string, got {}",
                        row[0]
                    ))
                })?;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|error| {
                        io::Error::other(format!(
                            "invalid PUBKEY base64 value: {error}"
                        ))
                    })?;
                let pubkey: [u8; 32] =
                    decoded.try_into().map_err(|bytes: Vec<u8>| {
                        io::Error::other(format!(
                            "expected 32 decoded PUBKEY bytes, got {}",
                            bytes.len()
                        ))
                    })?;
                pubkeys.push(pubkey);
            }
            _ => {
                return Err(io::Error::other(format!(
                    "unexpected ksql response line: {line}"
                )));
            }
        }
    }

    Ok(pubkeys)
}

#[cfg(test)]
mod tests {
    use super::{parse_pubkeys_stream, validate_ksql_identifier};

    fn pubkey(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn test_parses_header_and_valid_rows() {
        let body = concat!(
            "{\"queryId\":\"query_1\"}\n",
            "[\"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=\"]\n"
        );

        let parsed = parse_pubkeys_stream(body.as_bytes()).unwrap();

        assert_eq!(parsed, vec![pubkey(1)]);
    }

    #[test]
    fn test_parses_multiple_valid_rows() {
        let body = concat!(
            "[\"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=\"]\n",
            "[\"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=\"]\n"
        );

        let parsed = parse_pubkeys_stream(body.as_bytes()).unwrap();

        assert_eq!(parsed, vec![pubkey(1), pubkey(2)]);
    }

    #[test]
    fn test_rejects_ksql_error_rows() {
        let error = parse_pubkeys_stream(
            "{\"@type\":\"error\",\"message\":\"boom\"}\n".as_bytes(),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("ksql error response"));
    }

    #[test]
    fn test_rejects_non_array_data_rows() {
        let error = parse_pubkeys_stream("\"nope\"\n".as_bytes())
            .unwrap_err()
            .to_string();

        assert!(error.contains("unexpected ksql response line"));
    }

    #[test]
    fn test_rejects_wrong_column_count() {
        let error = parse_pubkeys_stream("[\"a\",\"b\"]\n".as_bytes())
            .unwrap_err()
            .to_string();

        assert!(error.contains("expected 1, got 2"));
    }

    #[test]
    fn test_rejects_invalid_base64() {
        let error = parse_pubkeys_stream("[\"not-base64\"]\n".as_bytes())
            .unwrap_err()
            .to_string();

        assert!(error.contains("invalid PUBKEY base64"));
    }

    #[test]
    fn test_rejects_wrong_pubkey_length() {
        let error = parse_pubkeys_stream("[\"AQ==\"]\n".as_bytes())
            .unwrap_err()
            .to_string();

        assert!(error.contains("expected 32 decoded PUBKEY bytes"));
    }

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
}
