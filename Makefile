PUBKEY ?=
CLIENT_REST ?= http://127.0.0.1:3030

.PHONY: \
	help \
	build \
	check \
	kafka-up \
	kafka-down \
	kafka-ready \
	kafka-ui \
	kafka-ui-down \
	ix-tests-build \
	ix-tests-run \
	ix-tests-scenario \
	grpc-service-run \
	grpc-service-build \
	grpc-service-client \
	grpc-service-client-add-sub \
	grpc-service-client-remove-sub \
	geyser-plugin-build \
	geyser-plugin-launch

help:
	@echo "Available targets:"
	@echo "  build                         - Build all Rust workspace packages"
	@echo "  check                         - Run workspace fmt, clippy, build, and test"
	@echo "  kafka-up                      - Start the Kafka/ksqlDB stack"
	@echo "  kafka-down                    - Stop and remove the Kafka/ksqlDB stack"
	@echo "  kafka-ready                   - Start the stack and initialize stream/table/schema"
	@echo "  kafka-ui                      - Start Redpanda Console"
	@echo "  kafka-ui-down                 - Stop Redpanda Console"
	@echo "  ix-tests-build                 - Build the gRPC service binary and the ix-tests harness"
	@echo "  ix-tests-run                   - Run the full local integration suite"
	@echo "  ix-tests-scenario              - Run one integration scenario (SCENARIO=...)"
	@echo "  grpc-service-run              - Run the gRPC service"
	@echo "  grpc-service-build            - Build the gRPC service package"
	@echo "  grpc-service-client           - Run the example gRPC client"
	@echo "  grpc-service-client-add-sub   - Add a client subscription (PUBKEY=...)"
	@echo "  grpc-service-client-remove-sub - Remove a client subscription (PUBKEY=...)"
	@echo "  geyser-plugin-build           - Build the geyser plugin"
	@echo "  geyser-plugin-launch          - Launch the validator with the plugin"

build:
	cargo build -p magigblock-event-proto
	cargo build -p magigblock-grpc-service
	cargo build -p solana-accountsdb-plugin-kafka

check:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --no-deps -- -D warnings
	cargo build --workspace
	cargo test --workspace -- --test-threads=16

kafka-up:
	$(MAKE) -C kafka-setup up

kafka-down:
	$(MAKE) -C kafka-setup down

kafka-ready:
	$(MAKE) -C kafka-setup ready

kafka-ui:
	$(MAKE) -C kafka-setup ui

kafka-ui-down:
	$(MAKE) -C kafka-setup ui-down

ix-tests-build:
	cargo build -p magigblock-grpc-service
	cargo build -p ix-tests

ix-tests-run:
	cargo run -p ix-tests -- --config ix-tests/configs/suite.toml --scenario all

ix-tests-scenario:
	@test -n "$(SCENARIO)" || (echo "Provide SCENARIO=..." >&2; exit 1)
	cargo run -p ix-tests -- --config ix-tests/configs/suite.toml --scenario "$(SCENARIO)"

grpc-service-run:
	$(MAKE) -C grpc-service run

grpc-service-build:
	cargo build -p magigblock-grpc-service

grpc-service-client:
	$(MAKE) -C grpc-service client

grpc-service-client-add-sub:
	$(MAKE) -C grpc-service client-add-sub PUBKEY="$(PUBKEY)" CLIENT_REST="$(CLIENT_REST)"

grpc-service-client-remove-sub:
	$(MAKE) -C grpc-service client-remove-sub PUBKEY="$(PUBKEY)" CLIENT_REST="$(CLIENT_REST)"

geyser-plugin-build:
	$(MAKE) -C geyser-plugin build-plugin

geyser-plugin-launch:
	$(MAKE) -C geyser-plugin launch
