use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::*;

use anyhow::Context;
use helius_laserstream::grpc::geyser_client::GeyserClient;
use helius_laserstream::grpc::subscribe_update::UpdateOneof;
use helius_laserstream::grpc::{
    SubscribeRequest, SubscribeRequestFilterAccounts,
};
use solana_keypair::Signature;
use solana_pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::layout::ServiceInstance;
use crate::observation::{ClientLog, ObservedUpdate};

#[allow(dead_code)]
pub struct TestGrpcClient {
    pub id: usize,
    pub service: ServiceInstance,
    pub endpoint: String,
    log: ClientLog,
    request_tx: mpsc::Sender<SubscribeRequest>,
    receive_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

#[allow(dead_code)]
impl TestGrpcClient {
    pub async fn connect(
        id: usize,
        service: ServiceInstance,
        endpoint: String,
    ) -> anyhow::Result<Self> {
        let mut client =
            GeyserClient::connect(endpoint.clone()).await.with_context(
                || format!("client {id}: failed to connect to {endpoint}"),
            )?;

        let (tx, rx) = mpsc::channel::<SubscribeRequest>(16);

        tx.send(SubscribeRequest::default())
            .await
            .context("failed to send initial empty subscribe request")?;

        let response = client
            .subscribe(ReceiverStream::new(rx))
            .await
            .with_context(|| format!("client {id}: subscribe call failed"))?;
        let mut update_stream = response.into_inner();

        let log = ClientLog::new();
        let log_clone = log.clone();

        fn pubkey_str(bytes: &[u8]) -> Option<String> {
            if bytes.is_empty() {
                return Some(String::new());
            }
            match Pubkey::try_from(bytes.to_vec()) {
                Ok(pubkey) => Some(pubkey.to_string()),
                Err(err) => {
                    error!(
                        bytes = ?bytes,
                        err = ?err,
                        "failed to parse pubkey"
                    );
                    None
                }
            }
        }
        fn txn_signature_str(bytes: &[u8]) -> Option<String> {
            if bytes.is_empty() {
                return Some(String::new());
            }
            match Signature::try_from(bytes.to_vec()) {
                Ok(signature) => Some(signature.to_string()),
                Err(err) => {
                    error!(
                        bytes = ?bytes,
                        err = ?err,
                        "failed to parse txn signature"
                    );
                    None
                }
            }
        }

        let receive_task = tokio::spawn(async move {
            while let Some(item) = update_stream.next().await {
                match item {
                    Ok(update) => {
                        if let Some(UpdateOneof::Account(account_update)) =
                            update.update_oneof
                            && let Some(info) = account_update.account
                        {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis();

                            let Some(pubkey_b58) = pubkey_str(&info.pubkey) else {
                                continue;
                            };
                            let Some(owner_b58) = pubkey_str(&info.owner) else {
                                continue;
                            };
                            let txn_signature_b58 = match info.txn_signature.as_ref() {
                                Some(signature) => {
                                    let Some(signature_b58) = txn_signature_str(signature) else {
                                        continue;
                                    };
                                    Some(signature_b58)
                                }
                                None => None,
                            };

                            let observed = ObservedUpdate {
                                client_id: id,
                                service,
                                pubkey_b58,
                                slot: account_update.slot,
                                lamports: info.lamports,
                                owner_b58,
                                executable: info.executable,
                                rent_epoch: info.rent_epoch,
                                write_version: info.write_version,
                                txn_signature_b58,
                                data: info.data,
                                received_at_millis: now,
                            };

                            trace!(
                                client_id = id,
                                pubkey = %observed.pubkey_b58,
                                slot = observed.slot,
                                lamports = observed.lamports,
                                owner = %observed.owner_b58,
                                executable = observed.executable,
                                rent_epoch = observed.rent_epoch,
                                write_version = observed.write_version,
                                txn_signature = ?observed.txn_signature_b58.as_deref(),
                                data_len = observed.data.len(),
                                "received account update"
                            );

                            log_clone.push(observed);
                        }
                    }
                    Err(status) => {
                        tracing::warn!(
                            client_id = id,
                            %status,
                            "stream error"
                        );
                        break;
                    }
                }
            }
            Ok(())
        });

        Ok(Self {
            id,
            service,
            endpoint,
            log,
            request_tx: tx,
            receive_task,
        })
    }

    pub async fn replace_subscription(
        &self,
        pubkeys: &[String],
    ) -> anyhow::Result<()> {
        let req = SubscribeRequest {
            accounts: HashMap::from([(
                "replace".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: pubkeys.to_vec(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        self.request_tx
            .send(req)
            .await
            .context("failed to send replace subscription request")
    }

    pub async fn patch_subscription(
        &self,
        add: &[String],
        remove: &[String],
    ) -> anyhow::Result<()> {
        let mut accounts = HashMap::new();
        if !add.is_empty() {
            accounts.insert(
                "add".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: add.to_vec(),
                    ..Default::default()
                },
            );
        }
        if !remove.is_empty() {
            accounts.insert(
                "remove".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: remove.to_vec(),
                    ..Default::default()
                },
            );
        }
        let req = SubscribeRequest {
            accounts,
            ..Default::default()
        };
        self.request_tx
            .send(req)
            .await
            .context("failed to send patch subscription request")
    }

    pub fn log(&self) -> &ClientLog {
        &self.log
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.request_tx);
        self.receive_task.abort();
        match self.receive_task.await {
            Ok(result) => result,
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
