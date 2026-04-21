use futures::StreamExt;
use magigblock_event_proto::{
    MessageWrapper, UpdateAccountEvent, message_wrapper,
};
use prost::Message;
use rdkafka::Message as KafkaMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tracing::{error, info, warn};

use crate::config::KafkaConfig;
use crate::domain::{AccountUpdate, PubkeyFilter, bytes_to_base58};
use crate::errors::{GeykagError, GeykagResult};
use crate::traits::AccountUpdateSource;
pub struct KafkaAccountUpdateStream {
    config: KafkaConfig,
}

impl KafkaAccountUpdateStream {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    pub async fn run<H>(
        &self,
        filter: Option<&PubkeyFilter>,
        mut handler: H,
    ) -> GeykagResult<()>
    where
        H: FnMut(StreamMessage) -> GeykagResult<()>,
    {
        let mut client_config = ClientConfig::new();
        for (key, value) in &self.config.client {
            client_config.set(key, value);
        }
        client_config
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", &self.config.group_id)
            .set("auto.offset.reset", &self.config.auto_offset_reset)
            .set("enable.auto.commit", "true");

        let consumer: StreamConsumer =
            client_config.create().map_err(|source| {
                GeykagError::KafkaConsumerCreate {
                    broker: self.config.bootstrap_servers.clone(),
                    source,
                }
            })?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|source| GeykagError::KafkaSubscribe {
                topic: self.config.topic.clone(),
                source,
            })?;

        info!(
            broker = self.config.bootstrap_servers,
            topic = self.config.topic,
            group_id = self.config.group_id,
            auto_offset_reset = self.config.auto_offset_reset,
            pubkey_filter =
                filter.map(PubkeyFilter::as_str).unwrap_or("(none)"),
            "listening for Kafka messages"
        );

        let mut stream = consumer.stream();
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    let Some(payload) = msg.payload() else {
                        warn!(
                            partition = msg.partition(),
                            offset = msg.offset(),
                            "skipping empty payload"
                        );
                        continue;
                    };

                    match decode_account_update(payload) {
                        Ok(account) => {
                            if !account.matches_filter(filter) {
                                continue;
                            }

                            handler(StreamMessage {
                                account,
                                partition: msg.partition(),
                                offset: msg.offset(),
                                timestamp: format!("{:?}", msg.timestamp()),
                            })?;
                        }
                        Err(err) => {
                            warn!(
                                partition = msg.partition(),
                                offset = msg.offset(),
                                error = %err,
                                "failed to decode message payload"
                            );
                        }
                    }
                }
                Err(err) => error!(error = %err, "Kafka consumer error"),
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StreamMessage {
    pub account: AccountUpdate,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: String,
}

fn decode_account_update(payload: &[u8]) -> GeykagResult<AccountUpdate> {
    if let Ok(account) = decode_raw_account_update(payload) {
        return Ok(account);
    }

    if let Some(stripped_payload) = strip_confluent_protobuf_framing(payload)
        && let Ok(account) = decode_raw_account_update(stripped_payload)
    {
        return Ok(account);
    }

    Err(GeykagError::UnsupportedPayloadEncoding)
}

fn decode_raw_account_update(payload: &[u8]) -> GeykagResult<AccountUpdate> {
    if let Ok(wrapper) = MessageWrapper::decode(payload) {
        return match wrapper.event_message {
            Some(message_wrapper::EventMessage::Account(account)) => {
                Ok(AccountUpdate::from_proto(*account))
            }
            None => Err(GeykagError::MissingMessageWrapperPayload),
        };
    }

    Ok(AccountUpdate::from_proto(
        UpdateAccountEvent::decode(payload).map_err(|source| {
            GeykagError::InvalidAccountUpdatePayload { source }
        })?,
    ))
}

fn strip_confluent_protobuf_framing(payload: &[u8]) -> Option<&[u8]> {
    if payload.len() < 6 || payload[0] != 0 {
        return None;
    }

    if payload[5] == 0 {
        return Some(&payload[6..]);
    }

    None
}

impl AccountUpdate {
    fn from_proto(account: UpdateAccountEvent) -> Self {
        Self {
            pubkey_b58: bytes_to_base58(&account.pubkey),
            pubkey_bytes: account.pubkey,
            owner_b58: bytes_to_base58(&account.owner),
            slot: account.slot,
            lamports: account.lamports,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            write_version: account.write_version,
            txn_signature_b58: account
                .txn_signature
                .as_deref()
                .map(bytes_to_base58),
            data_len: account.data.len(),
            data_version: account.data_version,
            account_age: account.account_age,
        }
    }
}

impl AccountUpdateSource for KafkaAccountUpdateStream {
    async fn run<H>(
        &self,
        filter: Option<&PubkeyFilter>,
        handler: H,
    ) -> GeykagResult<()>
    where
        H: FnMut(StreamMessage) -> GeykagResult<()>,
    {
        self.run(filter, handler).await
    }
}
