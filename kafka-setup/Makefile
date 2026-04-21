DC := $(shell if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo docker compose; fi)

.PHONY: help up down setup-stream create-table register-schema ready ui ui-down

help:
	@echo "Available targets:"
	@echo "  up              - Start the stack in detached mode"
	@echo "  down            - Stop and remove the stack (volumes too)"
	@echo "  setup-stream    - Create the ksqlDB stream over account_updates topic"
	@echo "  create-table    - Create the accounts table (latest update per pubkey)"
	@echo "  register-schema - Register the protobuf schema for Redpanda Console"
	@echo "  ready           - Start stack, setup stream/table, and register schema"
	@echo "  ui              - Start Redpanda Console UI (http://localhost:8080)"
	@echo "  ui-down         - Stop Redpanda Console"

up:
	$(DC) up -d

down:
	$(DC) down -v

register-schema:
	sh/redpanda/01_register-proto-schema.sh

setup-stream:
	sh/ksql/01_setup-streams.sh

create-table:
	sh/ksql/02_create-accounts-table.sh

ready: up setup-stream create-table register-schema

ui:
	$(DC) up -d redpanda-console

ui-down:
	$(DC) stop redpanda-console
