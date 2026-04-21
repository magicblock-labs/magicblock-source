#!/usr/bin/env bash
set -euo pipefail

# Create a persistent TABLE storing the latest account update per pubkey.
# Uses write_version to keep only the most recent update.
# Env overrides:
# - STREAM=account_updates_stream
# - TABLE=accounts
# - KSQL_SERVER_URL=http://ksqldb-server:8088

STREAM="${STREAM:-account_updates_stream}"
TABLE="${TABLE:-accounts}"
KSQL_SERVER_URL="${KSQL_SERVER_URL:-http://ksqldb-server:8088}"

if command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  DC="docker compose"
fi

echo "Using compose command: $DC"

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

SQL="SET 'auto.offset.reset'='earliest';"
SQL="${SQL} CREATE TABLE IF NOT EXISTS ${TABLE} AS"
SQL="${SQL} SELECT account->pubkey AS PUBKEY,"
SQL="${SQL} LATEST_BY_OFFSET(account->slot) AS SLOT,"
SQL="${SQL} LATEST_BY_OFFSET(account->lamports) AS LAMPORTS,"
SQL="${SQL} LATEST_BY_OFFSET(account->owner) AS OWNER,"
SQL="${SQL} LATEST_BY_OFFSET(account->executable) AS EXECUTABLE,"
SQL="${SQL} LATEST_BY_OFFSET(account->rent_epoch) AS RENT_EPOCH,"
SQL="${SQL} LATEST_BY_OFFSET(account->data) AS DATA,"
SQL="${SQL} LATEST_BY_OFFSET(account->write_version) AS WRITE_VERSION,"
SQL="${SQL} LATEST_BY_OFFSET(account->txn_signature) AS TXN_SIGNATURE"
SQL="${SQL} FROM ${STREAM}"
SQL="${SQL} GROUP BY account->pubkey EMIT CHANGES;"

echo "Creating TABLE '${TABLE}' with latest update per pubkey (if not exists) via CLI..."
$DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "$SQL"

echo "Done creating table '${TABLE}'."
