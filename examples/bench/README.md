# Bench

This example allows for benchmarking producing or consuming against a local
cluster, and prints the producing and consuming byte and record rates.

This example is also a good general example if you want to look at plugging
in TLS or SASL.

## Examples

```
go run . -topic foo
go run . -topic foo -consume

go run . -topic foo -no-compression

go run . -tls -sasl-method scram-sha-256 -sasl-user user -sasl-pass pass -consume -group group -topic foo
```

## Comparisons

The [compare](./compare) directory has comparisons to other clients. For
simplicity, not every aspect that is supported in this benchmark is ported to
comparisons: tls support and sasl support may be missing, and for the
confluent-kafka-go comparison, it does not seem possible to consume outside of
a group.

All flags that _can_ be easily supported in comparisons are the same.

## `rdkafka_performance`

This client has a few different default flags in comparison to librdkafka's
`rdkafka_performance` utility. For starters, this prints less stats, but stats
can be added if requested.

To operate similarly to librdkafka's benchmarker, use `-linger 1s -compression none -static-record`.

This will match `rdkafka_performance` flags of `./rdkafka_performance -P -t <topic> -b <brokers> -s 100 -i 1000`.

librdkafka itself does not handle disabling lingering too well, but there is
negligible performance impact in franz-go, so feel free to leave lingering
disabled.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` specifies the topic to produce to or consume from.

`-pprof` sets a port to bind to to enable the default pprof handlers.

`-prometheus` adds a /metrics handler to the default handler (requires -pprof)

`-log-level` sets the log level to use, overriding the default of no client-level logs (can be debug, info, warn, error).

### Producing (only relevant if producing)

`-record-bytes` specifies the size of records to produce.

`-batch-max-bytes` specifies the maximum amount of bytes per partition when producing. This must be less than Kafka's max.message.bytes value.

`-no-compression` disables compression.

`-compression` sets the compression to use, overriding the default of snappy. Supports "", "none", "snappy", "lz4", and "zstd".

`-pool` enables using a `sync.Pool` to reuse records and value slices, reducing
garbage as a factor of the benchmark.

`-static-record` configures the benchmarking to use a single, static value for
all messages produced. This implies `-pool`, and completely eliminates any
record-value-creation overhead from benchmarking so that you can specifically
bench the performance of the client itself.

`-disable-idempotency` disables producing idempotently, which limits the throughput to 1rps

`-linger` sets an amount of milliseconds to linger before producing, overriding the default 0.

### Consuming (only relevant if consuming)

`-consume` opts in to consuming.

`-group` switches from consuming partitions directly to consuming as a part of
a consumer group.


### Connecting (optional tls, sasl)

`-tls` if true, sets the benchmark to dial over tls

`-ca-cert` specifies a custom CA to use when dialing (implies `-tls`)

`-client-cert` specifies a client cert to use when dialing (implies `-tls`, requires `-client-key`)

`-client-key` specifies a client key to use when dialing (implies `-tls`, requires `-client-cert`)

`-sasl-method` specifies a SASL method to use when connecting. This supports
`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `AWS_MSK_IAM` (any casing, with
or without dashes or underscores).

`-sasl-user` specifies the username to use for SASL. If using `AWS_MSK_IAM`,
this is the access key.

`-sasl-pass` specifies the password to use for SASL. If using `AWS_MSK_IAM`,
this is the secret key.
