# magicblock-source

This repo contains the MagicBlock account update pipeline:

- `event-proto` owns the shared protobuf schema and generated wire types.
- `geyser-plugin` publishes confirmed account updates to Kafka.
- `kafka-setup` stands up Kafka, ksqlDB, Schema Registry, and Redpanda Console and prepares stream/table state.
- `grpc-service` serves snapshot and live account updates to clients.

Common root workflows:

- `make build`
- `make kafka-up`
- `make kafka-ready`
- `make grpc-service-run`
- `make grpc-service-client`
- `make grpc-service-client-add-sub PUBKEY=<base58-pubkey>`
- `make grpc-service-client-remove-sub PUBKEY=<base58-pubkey>`
- `make geyser-plugin-build`
