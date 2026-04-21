use crate::errors::{GeykagError, GeykagResult};

#[derive(Clone, Debug)]
pub(crate) struct InitSubsClient {
    http: reqwest::Client,
    accounts_filter_url: String,
}

impl InitSubsClient {
    pub(crate) fn new(accounts_filter_url: String) -> GeykagResult<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|source| GeykagError::InitSubsClientBuild { source })?;

        Ok(Self {
            http,
            accounts_filter_url,
        })
    }

    pub(crate) async fn whitelist_pubkeys(
        &self,
        pubkeys: &[String],
    ) -> GeykagResult<()> {
        if pubkeys.is_empty() {
            return Ok(());
        }

        let pubkeys_json = pubkeys
            .iter()
            .map(|pubkey| format!("\"{pubkey}\""))
            .collect::<Vec<_>>()
            .join(",");
        let body = format!(r#"{{"pubkeys":[{pubkeys_json}]}}"#);

        self.http
            .post(&self.accounts_filter_url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|source| GeykagError::InitSubsRequest { source })?
            .error_for_status()
            .map(|_| ())
            .map_err(|source| GeykagError::InitSubsRequestStatus { source })
    }
}
