# magigblock-grpc-service

Queries ksqlDB snapshots, consumes Kafka account updates, and serves a Geyser-compatible gRPC stream.

## Configuration

Configuration is loaded from TOML.

CLI syntax:

```shell
cargo run -- [--config PATH] [PUBKEY]
```

Example config:

```toml
[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"
group_id = "kafka2grpc-dev"
auto_offset_reset = "latest"

[ksql]
url = "http://localhost:8088"
table = "ACCOUNTS"

[validator]
accounts_filter_url = "http://localhost:3000/filters/accounts"

[grpc]
bind_host = "0.0.0.0"
port = 50051
dispatcher_capacity = 1024
```

The default config path is `configs/config.toml`. Use `--config PATH` to point at a different file. Supplying a trailing `PUBKEY` filters the startup snapshot and live update stream to that one account.

## Make Targets

- `make init-config` creates `configs/config.toml` from `configs/config.example.toml` if it does not already exist
- `make run` starts the service with `CONFIG=configs/config.toml`
- override `CONFIG=...` to run with a different TOML file
