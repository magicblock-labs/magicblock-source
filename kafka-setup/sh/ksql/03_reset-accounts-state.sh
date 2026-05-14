#!/usr/bin/env bash
set -euo pipefail

# Tear down the ksqlDB objects that derive account state so they can be rebuilt
# from a fresh source topic.

STREAM="${STREAM:-account_updates_stream}"
TABLE="${TABLE:-accounts}"
STREAM_UPPER="$(printf '%s' "$STREAM" | tr '[:lower:]' '[:upper:]')"
TABLE_UPPER="$(printf '%s' "$TABLE" | tr '[:lower:]' '[:upper:]')"
KSQL_SERVER_URL="${KSQL_SERVER_URL:-http://ksqldb-server:8088}"

if command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  DC="docker compose"
fi

echo "Using compose command: $DC"
echo "Waiting for ksqlDB server to be ready (via CLI)..."
for i in $(seq 1 60); do
  if $DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e 'SHOW STREAMS;' >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ksqlDB not ready after 60 seconds" >&2
    exit 1
  fi
done
echo "ksqlDB is ready."

QUERIES_OUTPUT="$($DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "SHOW QUERIES;")"
ACCOUNT_QUERIES="$(printf '%s\n' "$QUERIES_OUTPUT" | grep -Eo 'CTAS_[^[:space:]|;]*ACCOUNTS[^[:space:]|;]*' | sort -u || true)"
if [[ -n "$ACCOUNT_QUERIES" ]]; then
  while IFS= read -r query_name; do
    [[ -z "$query_name" ]] && continue
    echo "Terminating persistent ACCOUNTS query '${query_name}'..."
    $DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "TERMINATE ${query_name};"
  done <<< "$ACCOUNT_QUERIES"
else
  echo "No persistent ACCOUNTS query found; skipping terminate."
fi

sleep 2

TABLES_OUTPUT="$($DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "SHOW TABLES;")"
if printf '%s\n' "$TABLES_OUTPUT" | grep -q "${TABLE_UPPER}"; then
  echo "Dropping table '${TABLE_UPPER}'..."
  $DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "DROP TABLE ${TABLE_UPPER} DELETE TOPIC;"
else
  echo "Table '${TABLE_UPPER}' does not exist; skipping drop."
fi

STREAMS_OUTPUT="$($DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "SHOW STREAMS;")"
if printf '%s\n' "$STREAMS_OUTPUT" | grep -q "${STREAM_UPPER}"; then
  echo "Dropping stream '${STREAM}'..."
  $DC run --rm ksqldb-cli ksql "${KSQL_SERVER_URL}" -e "DROP STREAM ${STREAM};"
else
  echo "Stream '${STREAM}' does not exist; skipping drop."
fi
