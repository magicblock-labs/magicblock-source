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
        H: FnMut(StreamMessage) -> GeykagResult<()> + Send,
    {
        KafkaAccountUpdateStream::run(self, filter, handler).await
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        decode_account_update, decode_raw_account_update,
        strip_confluent_protobuf_framing,
    };
    use crate::domain::AccountUpdate;
    use crate::errors::GeykagError;
    use magigblock_event_proto::{
        MessageWrapper, UpdateAccountEvent, message_wrapper,
    };

    fn sample_account_event() -> UpdateAccountEvent {
        UpdateAccountEvent {
            slot: 42,
            pubkey: (1..=32).collect(),
            lamports: 500,
            owner: (32..64).collect(),
            executable: true,
            rent_epoch: 7,
            data: b"payload".to_vec(),
            write_version: 88,
            txn_signature: Some(vec![7; 64]),
            data_version: 3,
            is_startup: false,
            account_age: 11,
        }
    }

    fn wrapped_account_event() -> MessageWrapper {
        MessageWrapper {
            event_message: Some(message_wrapper::EventMessage::Account(
                Box::new(sample_account_event()),
            )),
        }
    }

    fn bare_payload_bytes() -> Vec<u8> {
        sample_account_event().encode_to_vec()
    }

    fn wrapped_payload_bytes() -> Vec<u8> {
        wrapped_account_event().encode_to_vec()
    }

    fn confluent_framed_payload_bytes(payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn strip_confluent_protobuf_framing_returns_stripped_payload() {
        let payload = bare_payload_bytes();
        let framed = confluent_framed_payload_bytes(&payload);

        assert_eq!(
            strip_confluent_protobuf_framing(&framed),
            Some(payload.as_slice())
        );
    }

    #[test]
    fn strip_confluent_protobuf_framing_rejects_short_payloads() {
        assert_eq!(strip_confluent_protobuf_framing(&[0, 0, 0, 0, 0]), None);
    }

    #[test]
    fn strip_confluent_protobuf_framing_rejects_wrong_magic_byte() {
        let payload = bare_payload_bytes();
        let mut framed = confluent_framed_payload_bytes(&payload);
        framed[0] = 1;

        assert_eq!(strip_confluent_protobuf_framing(&framed), None);
    }

    #[test]
    fn strip_confluent_protobuf_framing_rejects_wrong_message_index_byte() {
        let payload = bare_payload_bytes();
        let mut framed = confluent_framed_payload_bytes(&payload);
        framed[5] = 1;

        assert_eq!(strip_confluent_protobuf_framing(&framed), None);
    }

    #[test]
    fn decode_raw_account_update_decodes_bare_update_account_event() {
        let account = decode_raw_account_update(&bare_payload_bytes()).unwrap();

        assert_eq!(account.slot, 42);
        assert_eq!(account.lamports, 500);
        assert_eq!(account.data_len, 7);
    }

    #[test]
    fn decode_raw_account_update_decodes_wrapped_message_wrapper_account() {
        let account =
            decode_raw_account_update(&wrapped_payload_bytes()).unwrap();

        assert_eq!(account.slot, 42);
        assert_eq!(account.write_version, 88);
        assert_eq!(account.account_age, 11);
    }

    #[test]
    fn decode_raw_account_update_rejects_wrapper_without_payload() {
        let payload = MessageWrapper {
            event_message: None,
        }
        .encode_to_vec();
        let error = decode_raw_account_update(&payload).unwrap_err();

        assert!(matches!(error, GeykagError::MissingMessageWrapperPayload));
    }

    #[test]
    fn decode_raw_account_update_rejects_invalid_protobuf_bytes() {
        let error = decode_raw_account_update(&[0xff, 0x01, 0x02]).unwrap_err();

        assert!(matches!(
            error,
            GeykagError::InvalidAccountUpdatePayload { .. }
        ));
    }

    #[test]
    fn decode_account_update_prefers_direct_raw_decode() {
        let account = decode_account_update(&bare_payload_bytes()).unwrap();

        assert_eq!(account.slot, 42);
        assert_eq!(account.pubkey_bytes, (1..=32).collect::<Vec<_>>());
    }

    #[test]
    fn decode_account_update_falls_back_to_stripped_confluent_frame() {
        let framed = confluent_framed_payload_bytes(&wrapped_payload_bytes());
        let account = decode_account_update(&framed).unwrap();

        assert_eq!(
            account.owner_b58,
            "3ARMH9zfVCnU2TKiphU4xcEyWdA45fc1sjKEtYMdf3gr"
        );
    }

    #[test]
    fn decode_account_update_rejects_unknown_payload_encoding() {
        let error = decode_account_update(&[1, 2, 3, 4, 5, 6, 7]).unwrap_err();

        assert!(matches!(error, GeykagError::UnsupportedPayloadEncoding));
    }

    #[test]
    fn account_update_from_proto_maps_all_fields() {
        let account = AccountUpdate::from_proto(sample_account_event());

        assert_eq!(
            account.pubkey_b58,
            "4wBqpZM9xaSheZzJSMawUKKwhdpChKbZ5eu5ky4Vigw"
        );
        assert_eq!(account.pubkey_bytes, (1..=32).collect::<Vec<_>>());
        assert_eq!(
            account.owner_b58,
            "3ARMH9zfVCnU2TKiphU4xcEyWdA45fc1sjKEtYMdf3gr"
        );
        assert_eq!(account.slot, 42);
        assert_eq!(account.lamports, 500);
        assert!(account.executable);
        assert_eq!(account.rent_epoch, 7);
        assert_eq!(account.write_version, 88);
        assert_eq!(
            account.txn_signature_b58,
            Some(
                "99eUso3aSbE9tqGSTXzo3TLfKb9RkMTURrHKQ1K7Zh3BbeqPevr5E1iCbpTjqHuTFLtfxTTD5ekfVuZFzQyEQf8"
                    .to_owned()
            )
        );
        assert_eq!(account.data_len, 7);
        assert_eq!(account.data_version, 3);
        assert_eq!(account.account_age, 11);
    }
}
