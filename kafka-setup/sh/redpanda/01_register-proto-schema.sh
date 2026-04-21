#!/usr/bin/env bash
set -euo pipefail

# Registers the UpdateAccountEvent protobuf schema with Confluent Schema Registry
# so that Redpanda Console can deserialize messages from the topic.

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TOPIC="${TOPIC:-solana.testnet.account_updates}"
SUBJECT="${TOPIC}-value"

PROTO_FILE="$(cd "$(dirname "$0")/../../" && pwd)/../event-proto/proto/event.proto"

if [[ ! -f "$PROTO_FILE" ]]; then
  echo "ERROR: Proto file not found at $PROTO_FILE" >&2
  echo "Expected the event-proto crate alongside this directory." >&2
  exit 1
fi

echo "Waiting for Schema Registry at $SCHEMA_REGISTRY_URL ..."
for i in $(seq 1 30); do
  if curl -sf "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [[ $i -eq 30 ]]; then
    echo "Schema Registry not ready after 30 seconds" >&2
    exit 1
  fi
done
echo "Schema Registry is ready."

# Delete existing schema so we always overwrite
if curl -sf "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/latest" >/dev/null 2>&1; then
  echo "Subject '$SUBJECT' already exists — deleting to overwrite ..."
  curl -sf -X DELETE "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT" >/dev/null
  curl -sf -X DELETE "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT?permanent=true" >/dev/null 2>&1 || true
fi

# Read the proto file content, escape it for JSON
SCHEMA_STR=$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    print(json.dumps(f.read()))
" "$PROTO_FILE")

# Build the registration payload
PAYLOAD=$(cat <<EOF
{
  "schemaType": "PROTOBUF",
  "schema": $SCHEMA_STR
}
EOF
)

echo "Registering protobuf schema for subject '$SUBJECT' ..."

HTTP_CODE=$(curl -s -o /tmp/sr-response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "$PAYLOAD" \
  "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions")

if [[ "$HTTP_CODE" -ge 200 && "$HTTP_CODE" -lt 300 ]]; then
  SCHEMA_ID=$(python3 -c "import json; print(json.load(open('/tmp/sr-response.json'))['id'])")
  echo "Schema registered successfully (id=$SCHEMA_ID)."
else
  echo "ERROR: Schema registration failed (HTTP $HTTP_CODE):" >&2
  cat /tmp/sr-response.json >&2
  echo >&2
  exit 1
fi

echo ""
echo "Redpanda Console should now deserialize messages from '$TOPIC'."
echo "Open http://localhost:8080 and navigate to the topic to see decoded protobuf."
