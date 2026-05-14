#!/usr/bin/env bash
set -euo pipefail

# Rebuild Kafka topic state and the dependent ksqlDB stream/table state without
# restarting the Docker stack.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Starting Kafka reset workflow..."

echo "Resetting ksqlDB state..."
"$SCRIPT_DIR/ksql/03_reset-accounts-state.sh"

echo "Resetting Kafka source topic..."
"$SCRIPT_DIR/kafka/01_recreate-account-updates-topic.sh"

echo "Recreating ksqlDB stream and table..."
"$SCRIPT_DIR/ksql/01_setup-streams.sh"
"$SCRIPT_DIR/ksql/02_create-accounts-table.sh"

echo "Done resetting Kafka and ksqlDB state."
