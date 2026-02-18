# Examples

This directory contains examples demonstrating franz-go features. Each
subdirectory is a standalone Go module that can be run with `go run .`.

All examples accept `-brokers` to override the default `localhost:9092`.

## admin_and_requests

Demonstrates two ways to perform admin operations: the high-level `kadm` admin
client and raw `kmsg` protocol requests. Creates a topic and inspects metadata.

- `-mode kadm` - use the kadm admin client (default)
- `-mode kmsg` - use raw kmsg request/response structs
- `-topic` - topic name to create

## bench

A benchmarking tool for measuring produce and consume throughput. Also serves
as a general example of TLS, SASL, compression, and other client tuning.

- `-consume` - consume instead of produce
- `-group` - consume as part of a consumer group
- `-record-bytes` - size of produced records
- `-compression` - compression algorithm (none, gzip, snappy, lz4, zstd)
- `-linger` - producer linger duration
- `-tls` - dial over TLS
- `-sasl-method` - SASL mechanism (plain, scram-sha-256, scram-sha-512, aws_msk_iam)

The `compare/` subdirectory contains equivalent benchmarks for other Go Kafka
clients (confluent-kafka-go, sarama, segment).

## consumer_group_lag

Monitors consumer group lag using `kadm.Lag`. Periodically prints per-topic,
per-partition lag, committed offsets, and end offsets for the specified groups.

- `-groups` - comma-delimited consumer groups to monitor (required)
- `-interval` - polling interval (default 5s)

## consumer_runtime_control

Demonstrates dynamically adding/removing topics and partitions, and
pausing/resuming consumption at runtime.

- `-mode add-topics` - add a topic to the consumer after a delay (default)
- `-mode add-partitions` - add and remove specific partitions at runtime
- `-mode pause` - pause and resume consumption based on record count

## dlq

Demonstrates a dead letter queue pattern. Records that fail processing after
retries are forwarded to a DLQ topic with error metadata in headers.

## goroutine_per_partition_consuming

Three sub-examples that each start a goroutine per assigned partition for
concurrent processing within a consumer group.

- **autocommit_normal** - simplest: autocommit with mutex-based partition
  management. Prone to duplicate processing on rebalance.
- **autocommit_marks** - uses `BlockRebalanceOnPoll` + `AutoCommitMarks` for
  tighter control. Marked offsets are flushed on revoke.
- **manual_commit** - like autocommit_marks but each partition goroutine
  commits synchronously after processing.

Flags (same for all three): `-b` brokers, `-t` topic, `-g` group.

## group_committing

Demonstrates five different strategies for consuming and committing offsets.

- `-commit-style autocommit` - offsets committed automatically after each poll (default)
- `-commit-style records` - disable autocommit, commit specific records
- `-commit-style uncommitted` - disable autocommit, commit all uncommitted offsets per poll
- `-commit-style marks` - `AutoCommitMarks` + `BlockRebalanceOnPoll`, build a mark map and commit marked offsets
- `-commit-style kadm` - bypass the consumer group protocol, fetch/commit offsets via kadm with direct partition assignment
- `-group` - consumer group name
- `-topic` - topic to consume
- `-logger` - enable info-level logging

## manual_flushing

Demonstrates `ManualFlushing` for batch-oriented producers. Records are
buffered and only sent when `Flush` is explicitly called.

- `-topic` - topic to produce to
- `-batch-size` - number of records per flush

## partitioners

Demonstrates the various partitioning strategies available for producers.

- `-strategy sticky` - pin to a random partition per batch (default)
- `-strategy sticky-key` - hash keys for consistent partitioning
- `-strategy round-robin` - cycle through partitions evenly
- `-strategy least-backup` - pick the partition with fewest buffered records
- `-strategy manual` - use the Partition field set on the Record
- `-strategy uniform-bytes` - pin until a byte threshold, then switch (KIP-794)
- `-records` - number of records to produce

## plugin_kotel

Demonstrates OpenTelemetry tracing and metrics using the `kotel` plugin.
Includes end-to-end trace propagation through produce and consume, with spans
linked by trace ID. Metrics are exported periodically.

- `-topic` - topic to produce/consume

## plugins

Demonstrates the logging and metrics plugin ecosystem. Configures one of three
logging backends plus optional Prometheus metrics. You can use one logger and
one metrics plugin together.

- `-logger slog` - use kslog with log/slog (default)
- `-logger zap` - use kzap with go.uber.org/zap
- `-logger basic` - use the built-in kgo.BasicLogger
- `-metrics-port` - port for Prometheus metrics (0 to disable, default 9999)
- `-produce` - produce instead of consume

## record_formatter

Demonstrates `RecordFormatter` and `RecordReader` for printf-style record
formatting. Useful for building CLI tools and log processing.

- `-format` - record format layout using percent verbs (`%t` topic, `%k` key, `%v` value, `%p` partition, `%o` offset, etc.)
- `-topic` - topic to produce to and consume from

## sasl

Demonstrates SASL authentication with TLS using different mechanisms.

- `-method plain` - SASL/PLAIN (default)
- `-method scram-sha-256` - SCRAM-SHA-256
- `-method scram-sha-512` - SCRAM-SHA-512
- `-method aws-msk-iam` - AWS MSK IAM (uses AWS SDK credential chain)
- `-user` / `-pass` - credentials for plain/scram methods

## schema_registry

Demonstrates using the `sr` (schema registry) client with Avro serialization.
Registers a schema, produces encoded records, and decodes them on consume using
`sr.Serde`.

- `-topic` - topic to produce to and consume from
- `-registry` - schema registry URL (default localhost:8081)

## testing_with_kfake

Demonstrates `kfake`, a fake in-memory Kafka cluster for testing without a
real broker. Shows three control function patterns for intercepting, observing,
and coordinating Kafka protocol requests. Unlike other examples, this one runs
entirely in-process with no external dependencies.

- `-mode inject-error` - one-shot `ControlKey` to inject a produce error; kgo
  retries and succeeds because the control is consumed after one interception (default)
- `-mode observe` - `KeepControl` to persistently count every request type
  during a produce-and-consume cycle without intercepting
- `-mode sleep` - `SleepControl` with `SleepOutOfOrder` to delay a fetch until
  a produce arrives, coordinating between connections

## transactions

Demonstrates Kafka transactions in two modes.

- `-mode produce` - standalone transactional producer that atomically commits
  or aborts batches of records, alternating to show both paths (default)
- `-mode eos` - exactly-once consume-transform-produce pipeline using
  `NewGroupTransactSession`, with consumer offsets committed atomically in the
  same transaction
- `-produce-to` - input topic (required)
- `-eos-to` - output topic for EOS mode
- `-group` - consumer group for EOS mode
