#!/usr/bin/env bash
set -euo pipefail

# Create a ksqlDB STREAM over the Solana account-updates Kafka topic.
# The topic carries raw Protobuf MessageWrapper messages (no Schema Registry
# wire format). We use PROTOBUF_NOSR with an inline schema definition.
#
# Env overrides:
# - TOPIC=solana.testnet.account_updates
# - STREAM=account_updates_stream
# - KSQL_SERVER_URL=http://ksqldb-server:8088

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TOPIC="${TOPIC:-solana.testnet.account_updates}"
STREAM="${STREAM:-account_updates_stream}"
KSQL_SERVER_URL="${KSQL_SERVER_URL:-http://ksqldb-server:8088}"

# Pick docker compose command (supports v1 and v2)
if command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  DC="docker compose"
fi

echo "Using compose command: $DC"

# 1. Wait for ksqlDB
echo "Waiting for ksqlDB server to be ready (via CLI)..."
for i in $(seq 1 60); do
  if $DC run --rm ksqldb-cli ksql ${KSQL_SERVER_URL} -e 'SHOW STREAMS;' >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ksqlDB not ready after 60 seconds" >&2
    exit 1
  fi
done
echo "ksqlDB is ready."

# 2. Ensure Kafka topic exists
echo "Ensuring Kafka topic '$TOPIC' exists..."
if ! $DC exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list | grep -xq "$TOPIC"; then
  echo "Topic '$TOPIC' not found. Creating it..."
  $DC exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --replication-factor 1 \
    --partitions 1 >/dev/null 2>&1 || true
fi

# 3. Create stream using PROTOBUF_NOSR (raw protobuf, no Schema Registry wire format).
#    The topic carries MessageWrapper messages with a oneof; field 1 is UpdateAccountEvent.
#    We define the schema inline matching the MessageWrapper.account structure.
echo "Creating ksqlDB stream '${STREAM}' (PROTOBUF_NOSR)..."
$DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e \
  "CREATE STREAM IF NOT EXISTS ${STREAM} (
    account STRUCT<
      slot BIGINT,
      pubkey BYTES,
      lamports BIGINT,
      owner BYTES,
      executable BOOLEAN,
      rent_epoch BIGINT,
      data BYTES,
      write_version BIGINT,
      txn_signature BYTES
    >
  ) WITH (
    kafka_topic='${TOPIC}',
    key_format='KAFKA',
    value_format='PROTOBUF_NOSR'
  );"

echo "Done creating stream '${STREAM}'."
