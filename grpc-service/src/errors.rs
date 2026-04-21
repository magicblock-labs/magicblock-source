use base64::DecodeError as Base64DecodeError;
use prost::DecodeError as ProstDecodeError;
use rdkafka::error::KafkaError;
use serde_json::Value;
use std::{env::VarError, io, net::AddrParseError};
use thiserror::Error;
use tokio::task::JoinError;
use tonic::transport::Error as TonicTransportError;

pub type GeykagResult<T> = Result<T, GeykagError>;

#[derive(Debug, Error)]
pub enum GeykagError {
    #[error("usage: cargo run -- [PUBKEY]")]
    InvalidCliUsage,
    #[error("invalid base58 pubkey: {value}")]
    InvalidPubkey {
        value: String,
        source: bs58::decode::Error,
    },
    #[error(
        "invalid Solana pubkey length: expected 32 bytes after base58 decode, got {actual}"
    )]
    InvalidPubkeyLength { actual: usize },
    #[error("failed to read environment variable {key}")]
    InvalidEnvRead { key: &'static str, source: VarError },
    #[error("invalid value for environment variable {key}: {value}")]
    InvalidEnvValue {
        key: &'static str,
        value: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("failed to create HTTP client for ksqlDB")]
    KsqlClientBuild {
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to query ksqlDB")]
    KsqlQuery {
        #[source]
        source: reqwest::Error,
    },
    #[error("ksqlDB returned an error status for snapshot query")]
    KsqlQueryStatus {
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to read ksqlDB response body")]
    KsqlResponseBody {
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to create HTTP client for validator init subscriptions")]
    InitSubsClientBuild {
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to whitelist pubkeys with validator")]
    InitSubsRequest {
        #[source]
        source: reqwest::Error,
    },
    #[error("validator whitelist endpoint returned an error status")]
    InitSubsRequestStatus {
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to parse ksqlDB response line as JSON: {line}")]
    KsqlJsonLine {
        line: String,
        source: serde_json::Error,
    },
    #[error("ksqlDB returned {error_type}: {details}")]
    KsqlErrorResponse { error_type: String, details: String },
    #[error("unexpected ksqlDB response line: {line}")]
    UnexpectedKsqlResponseLine { line: String },
    #[allow(dead_code)]
    #[error(
        "expected at most one row from ksqlDB single-pubkey lookup, got {actual}"
    )]
    UnexpectedKsqlSingleRowCount { actual: usize },
    #[error("expected 9 columns from ksqlDB row, got {actual}")]
    UnexpectedKsqlColumnCount { actual: usize },
    #[error("failed to decode {field} from ksqlDB")]
    KsqlDecodeBase64Field {
        field: &'static str,
        #[source]
        source: Box<GeykagError>,
    },
    #[error("failed to parse {field} from ksqlDB")]
    KsqlParseField {
        field: &'static str,
        #[source]
        source: Box<GeykagError>,
    },
    #[error("failed to decode TXN_SIGNATURE")]
    KsqlDecodeTxnSignature {
        #[source]
        source: Box<GeykagError>,
    },
    #[error("expected integer JSON value, got {value}")]
    InvalidJsonInteger { value: Value },
    #[error("expected boolean JSON value, got {value}")]
    InvalidJsonBool { value: Value },
    #[error("expected base64 string for BYTES column")]
    ExpectedBase64String,
    #[error("expected optional base64 string for BYTES column")]
    ExpectedOptionalBase64String,
    #[error("invalid base64 BYTES field: {encoded}")]
    InvalidBase64Field {
        encoded: String,
        source: Base64DecodeError,
    },
    #[error("failed to create Kafka consumer for broker {broker}")]
    KafkaConsumerCreate { broker: String, source: KafkaError },
    #[error("failed to subscribe to topic {topic}")]
    KafkaSubscribe { topic: String, source: KafkaError },
    #[error(
        "payload was neither raw protobuf nor Confluent-framed protobuf for MessageWrapper.account or UpdateAccountEvent"
    )]
    UnsupportedPayloadEncoding,
    #[error("received MessageWrapper without event payload")]
    MissingMessageWrapperPayload,
    #[error(
        "payload was neither a MessageWrapper.account nor a bare UpdateAccountEvent"
    )]
    InvalidAccountUpdatePayload {
        #[source]
        source: ProstDecodeError,
    },
    #[error("invalid gRPC bind address: {address}")]
    InvalidGrpcBindAddress {
        address: String,
        #[source]
        source: AddrParseError,
    },
    #[allow(dead_code)]
    #[error("failed to start gRPC service")]
    GrpcStartup {
        #[source]
        source: TonicTransportError,
    },
    #[error("failed to bind gRPC listener at {address}")]
    GrpcBind {
        address: String,
        #[source]
        source: io::Error,
    },
    #[error("gRPC service failed while serving requests")]
    GrpcServe {
        #[source]
        source: TonicTransportError,
    },
    #[error("gRPC service task failed to join")]
    GrpcTaskJoin {
        #[source]
        source: JoinError,
    },
    #[error("gRPC sink write attempted after shutdown")]
    GrpcSinkWriteAfterShutdown,
    #[allow(dead_code)]
    #[error(
        "failed to publish update because gRPC runtime is no longer running"
    )]
    GrpcPublishAfterShutdown,
    #[error("failed to convert account event into SubscribeUpdate")]
    GrpcEventConversion(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
