KAFKA_BROKER ?= localhost:9092
KAFKA_TOPIC ?= solana.testnet.account_updates
KAFKA_GROUP_ID ?= kafka2grpc-dev
KAFKA_AUTO_OFFSET_RESET ?= latest

GRPC_ENDPOINT ?= http://127.0.0.1:50051
CLIENT_REST ?= http://127.0.0.1:3030
PUBKEY ?=

.PHONY: help run client client-add-sub client-remove-sub

help:
	@echo "Available targets:"
	@echo "  run               - Query ksql snapshot, then consume live Kafka updates"
	@echo "  client            - Start gRPC example client with REST API on port 3030"
	@echo "  client-add-sub    - Add a subscription via CLIENT_REST (PUBKEY=...)"
	@echo "  client-remove-sub - Remove a subscription via CLIENT_REST (PUBKEY=...)"

run:
	KAFKA_BROKER="$(KAFKA_BROKER)" \
	KAFKA_TOPIC="$(KAFKA_TOPIC)" \
	KAFKA_GROUP_ID="$(KAFKA_GROUP_ID)" \
	KAFKA_AUTO_OFFSET_RESET="$(KAFKA_AUTO_OFFSET_RESET)" \
	cargo run

client:
	cargo run --example grpc_client -- --endpoint "$(GRPC_ENDPOINT)"

client-add-sub:
	@test -n "$(PUBKEY)" || (echo "Provide PUBKEY=..." >&2; exit 1)
	curl -s -X POST "$(CLIENT_REST)/add/$(PUBKEY)"

client-remove-sub:
	@test -n "$(PUBKEY)" || (echo "Provide PUBKEY=..." >&2; exit 1)
	curl -s -X POST "$(CLIENT_REST)/remove/$(PUBKEY)"
