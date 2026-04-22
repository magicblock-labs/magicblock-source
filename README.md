# magicblock-source

This repo contains the MagicBlock account update pipeline:

- `event-proto` owns the shared protobuf schema and generated wire types.
- `geyser-plugin` publishes confirmed account updates to Kafka.
- `kafka-setup` stands up Kafka, ksqlDB, Schema Registry, and Redpanda Console and prepares stream/table state.
- `grpc-service` serves snapshot and live account updates to clients.

## Layout

- `event-proto/`: shared Rust crate `magigblock-event-proto`
- `grpc-service/`: Rust crate `magigblock-grpc-service`
- `geyser-plugin/`: Solana Geyser plugin crate
- `kafka-setup/`: minimal Kafka/ksqlDB local environment
- `Makefile`: top-level operator entrypoint

## CI Contract

The repository-level CI in `.github/workflows/test.yml` runs the
same workspace checks on every push and pull request:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --no-deps -- -D warnings
cargo build --workspace
cargo test --workspace -- --test-threads=16
```

## Common Root Workflows

- `make build`
- `make check`
- `make kafka-up`
- `make kafka-down`
- `make kafka-ready`
- `make kafka-ui`
- `make kafka-ui-down`
- `make grpc-service-run`
- `make grpc-service-build`
- `make grpc-service-client`
- `make grpc-service-client-add-sub PUBKEY=<base58-pubkey>`
- `make grpc-service-client-remove-sub PUBKEY=<base58-pubkey>`
- `make geyser-plugin-build`
- `make geyser-plugin-launch`
