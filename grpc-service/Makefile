CONFIG ?= configs/config.toml
GRPC_ENDPOINT ?= http://127.0.0.1:50051
CLIENT_REST ?= http://127.0.0.1:3030
PUBKEY ?=

.PHONY: help init-config run client client-add-sub client-remove-sub

help:
	@echo "Available targets:"
	@echo "  init-config       - Create configs/config.toml from configs/config.example.toml if missing"
	@echo "  run               - Run the service using CONFIG=..."
	@echo "  client            - Start gRPC example client with REST API on port 3030"
	@echo "  client-add-sub    - Add a subscription via CLIENT_REST (PUBKEY=...)"
	@echo "  client-remove-sub - Remove a subscription via CLIENT_REST (PUBKEY=...)"

init-config:
	@if [ -f "$(CONFIG)" ]; then \
		echo "$(CONFIG) already exists"; \
	else \
		cp configs/config.example.toml "$(CONFIG)"; \
		echo "Created $(CONFIG)"; \
	fi

run:
	cargo run -- --config "$(CONFIG)"

client:
	cargo run --example grpc_client -- --endpoint "$(GRPC_ENDPOINT)"

client-add-sub:
	@test -n "$(PUBKEY)" || (echo "Provide PUBKEY=..." >&2; exit 1)
	curl -s -X POST "$(CLIENT_REST)/add/$(PUBKEY)"

client-remove-sub:
	@test -n "$(PUBKEY)" || (echo "Provide PUBKEY=..." >&2; exit 1)
	curl -s -X POST "$(CLIENT_REST)/remove/$(PUBKEY)"
