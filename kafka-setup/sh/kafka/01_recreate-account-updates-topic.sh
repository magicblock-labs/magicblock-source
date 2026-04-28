#!/usr/bin/env bash
set -euo pipefail

# Delete and recreate the Kafka source topic used by the account updates flow.

TOPIC="${TOPIC:-solana.testnet.account_updates}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"

if command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  DC="docker compose"
fi

echo "Using compose command: $DC"
echo "Checking Kafka readiness..."
for i in $(seq 1 60); do
  if $DC exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "Kafka not ready after 60 seconds" >&2
    exit 1
  fi
done
echo "Kafka is ready."

if $DC exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep -Fxq "$TOPIC"; then
  echo "Deleting Kafka topic '$TOPIC'..."
  $DC exec -T kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --delete \
    --topic "$TOPIC"

  for i in $(seq 1 60); do
    if ! $DC exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep -Fxq "$TOPIC"; then
      break
    fi
    sleep 1
    if [[ $i -eq 60 ]]; then
      echo "Kafka topic '$TOPIC' was not deleted after 60 seconds" >&2
      exit 1
    fi
  done
else
  echo "Kafka topic '$TOPIC' does not exist; skipping delete."
fi

echo "Recreating Kafka topic '$TOPIC'..."
$DC exec -T kafka kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --replication-factor "$REPLICATION_FACTOR" \
  --partitions "$PARTITIONS"

echo "Done recreating Kafka topic '$TOPIC'."
