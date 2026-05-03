# kafka-setup

Minimal local environment support for the MagicBlock pipeline.

Available workflows:

- `make up`
- `make ready`
- `make reset-state`
- `make down`
- `make ui`
- `make ui-down`

`make reset-state` is the narrower option for an already-running
environment. It rebuilds the Kafka topic and the dependent ksqlDB
state without restarting Docker or re-running the broader `make ready`
workflow.

The local compose stack sets `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0`
to reduce Kafka consumer cold-start delay during development and
integration tests.
