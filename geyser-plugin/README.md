# Solana AccountsDB Plugin for Kafka

Kafka publisher for Solana's [Geyser plugin framework](https://docs.solana.com/developing/plugins/geyser-plugins).

## What This Plugin Does

This plugin publishes confirmed Solana account updates to Kafka.

- account updates are the only published event type
- accounts are included only after they are added through `POST /filters/accounts`
- slot notifications are consumed internally so buffered updates can be released once confirmed
- Kafka payloads always use `MessageWrapper::Account(UpdateAccountEvent)`

## Architecture

- `plugin/` is the Geyser adapter. It reads config, wires runtime services, maps validator callbacks into internal events, and releases buffered updates when slots become confirmed.
- `confirmation_buffer.rs` owns the slot graph and confirmed-update buffering so account callbacks stay separate from confirmation policy.
- `initial_account_backfill/` owns subscription bootstrap. It enqueues requested pubkeys, fetches current account state from local RPC, and suppresses stale snapshots when a live update wins the race.
- `server/` owns the admin HTTP API. `server/mod.rs` boots the listener, `server/accounts.rs` handles `POST /filters/accounts`, and `server/prom.rs` exposes `GET /metrics`.
- `account_update_publisher.rs` owns publication policy, while `publisher.rs` only encodes and hands approved account updates to Kafka.

## Quick Start

```json
{
  "libpath": "../target/release/libsolana_accountsdb_plugin_kafka.so",
  "config_file": "plugin-config.toml"
}
```

```toml

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[kafka.client]
"request.required.acks" = "1"
"message.timeout.ms" = "30000"
"compression.type" = "lz4"
"partitioner" = "murmur2_random"

[plugin]
shutdown_timeout_ms = 30000
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:3000"
metrics = false
```

After the plugin is running, add accounts to the whitelist:

```shell
curl -X POST http://127.0.0.1:3000/filters/accounts \
  -H 'content-type: application/json' \
  -d '{"pubkeys":["YourAccountPubkey111111111111111111111111111111"]}'
```

## Installation

### Binary releases

Find binary releases [on GitHub](https://github.com/Blockdaemon/solana-accountsdb-plugin-kafka/releases).

### Building from source

Prerequisites:

- Rust 1.93.1 from `rust-toolchain.toml`
- workspace checkout including `event-proto/`

Build:

```shell
cargo build --release
```

- Linux: `./target/release/libsolana_accountsdb_plugin_kafka.so`
- macOS: `./target/release/libsolana_accountsdb_plugin_kafka.dylib`

The Solana validator and this plugin must be built against matching Solana and Rust toolchains.

## Configuration

Config is split into two files:

- `plugin-config.json`: validator-facing JSON wrapper passed to `--geyser-plugin-config`
- `plugin-config.toml`: plugin runtime config loaded by this crate

The validator wrapper must contain:

- `libpath`: path to the plugin shared library
- `config_file`: path to the plugin TOML config, relative to the JSON file or absolute

The runtime config is TOML.

Supported fields:

- `[kafka]`
  - `bootstrap_servers`: Kafka bootstrap broker list
  - `topic`: Kafka topic for wrapped account updates
- `[kafka.client]`: optional raw `librdkafka` producer settings using quoted keys such as `"compression.type"` or `"linger.ms"`
- `[ksql]`
  - `url`: optional ksqlDB base URL; when set, startup restores tracked pubkeys from `accounts` and fails fast if restore cannot complete
  - `table`: ksqlDB table name for startup restore, default `accounts`
- `[plugin]`
  - `shutdown_timeout_ms`: producer flush timeout on shutdown
  - `local_rpc_url`: local validator RPC endpoint used for initial account backfill
  - `admin`: required listen address for the admin HTTP API
  - `metrics`: optional boolean, default `false`; set to `true` to enable the `/metrics` endpoint

Minimal config:

```toml

[kafka]
bootstrap_servers = "localhost:9092"
topic = "solana.testnet.account_updates"

[plugin]
local_rpc_url = "http://127.0.0.1:8899"
admin = "127.0.0.1:3000"
```

`kafka.bootstrap_servers`, `kafka.topic`, `plugin.local_rpc_url`, and `plugin.admin` are required. The `admin` bind address serves `POST /filters/accounts` and, when `plugin.metrics` is `true`, also `GET /metrics`. If `ksql.url` is set, it must be a valid absolute `http` or `https` base URL and startup will fail if the restore query cannot complete. Legacy filter arrays and legacy transaction, slot-status, block, and wrapping options are rejected during config parsing.

## Startup checks

Startup checks run from `KafkaPlugin::on_load` during normal validator/plugin startup. They run whether the validator is launched through `make geyser-plugin-launch` or directly with `solana-test-validator --geyser-plugin-config ...`; no separate preflight binary or Makefile target is required.

Startup validates:

- validator JSON wrapper
- runtime TOML config
- plugin library path
- admin bind address
- ksqlDB startup restore when `ksql.url` is configured
- Kafka bootstrap/topic readiness
- local RPC URL syntax only, not local RPC liveness

The local RPC endpoint is not required to be reachable during startup checks because it belongs to the validator being launched.

### Safe-start manual test matrix

| Scenario | Temporary change | Expected prefix |
| --- | --- | --- |
| malformed validator JSON | invalid JSON in wrapper copy | Agave rejects the wrapper before plugin load with `FailedToLoadPlugin`; no segfault |
| missing runtime TOML | JSON `config_file` points to missing TOML | `ERROR config startup check failed` |
| malformed runtime TOML | invalid TOML in runtime copy | `ERROR config startup check failed` |
| missing Kafka bootstrap | empty `kafka.bootstrap_servers` | `ERROR config startup check failed` |
| Kafka down | no Kafka on configured bootstrap | `ERROR kafka startup check failed` |
| ksqlDB down | `ksql.url = "http://127.0.0.1:1"` | `ERROR ksql startup check failed` |
| invalid ksql table | `table = "bad-name"` | `ERROR config startup check failed` |
| admin port in use | keep listener on configured port | `ERROR admin startup check failed` |
| malformed local RPC URL | `local_rpc_url = "127.0.0.1:8899"` | `ERROR config startup check failed` |

Proof command for the original issue:

```shell
make geyser-plugin-launch
```

Expected with no dependencies running:

- exits non-zero
- prints a Kafka startup check error with action `make kafka-ready`
- validator/plugin startup exits gracefully
- does not print `Segmentation fault`

Direct validator path that must receive the same checks:

```shell
cd geyser-plugin
solana-test-validator --log --reset --geyser-plugin-config plugin-config.json
```

Expected failures and messages must match the Makefile path because the checks live in `KafkaPlugin::on_load`.

Success path:

```shell
make kafka-ready
make geyser-plugin-launch
```

## Whitelist Management

Account inclusion is managed through the HTTP API:

- `POST /filters/accounts` adds accounts to the active whitelist
- newly added accounts are queued for initial backfill from the local RPC endpoint
- live updates and backfill snapshots are both gated by the same whitelist
- optional startup restore can replay previously tracked pubkeys from ksqlDB through the same whitelist and backfill path

Startup replay updates are not used as a second bootstrap publication path; subscribed bootstrap delivery comes from the initial RPC backfill flow.

If initial backfill enqueue fails because the queue is full, the keys are kept for retry on a later request.

## Message Format

The plugin publishes one wire format only:

- Kafka key: raw account pubkey bytes
- Kafka value: protobuf `MessageWrapper` with the `account` variant populated

`UpdateAccountEvent` contains the account fields, slot, write version, optional transaction signature, and the additional account metadata already present in the schema.

## Buffering

The Kafka producer is non-blocking so validator progress is not stalled by broker latency. If the producer queue fills, additional events may be dropped.

Useful `librdkafka` settings for `[kafka.client]` include:

- `"queue.buffering.max.messages"`
- `"queue.buffering.max.kbytes"`
- `"statistics.interval.ms"`

## Migration

Older configs must be reduced to the account-only TOML shape above.

Remove:

- `filters`
- `block_events_topic`
- `slot_status_topic`
- `transaction_topic`
- `program_filters`
- `program_ignores`
- `account_filters`
- `publish_all_accounts`
- `include_vote_transactions`
- `include_failed_transactions`
- `wrap_messages`
