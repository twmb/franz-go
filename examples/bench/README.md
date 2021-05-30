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

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` specifies the topic to produce to or consume from.

`-pprof` sets a port to bind to to enable the default pprof handlers.

### Producing (only relevant if producing)

`-record-bytes` specifies the size of records to produce.

`-no-compression` disables snappy compression.

`-pool` enables using a `sync.Pool` to reuse records and value slices, reducing
garbage as a factor of the benchmark.

### Consuming (only relevant if consuming)

`-consume` opts in to consuming.

`-group` switches from consuming partitions directly to consuming as a part of
a consumer group.


### Connecting (optional tls, sasl)

`-tls` if true, sets the benchmark to dial over tls

`-sasl-method` specifies a SASL method to use when connecting. This supports
`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `AWS_MSK_IAM` (any casing, with
or without dashes or underscores).

`-sasl-user` specifies the username to use for SASL. If using `AWS_MSK_IAM`,
this is the access key.

`-sasl-pass` specifies the password to use for SASL. If using `AWS_MSK_IAM`,
this is the secret key.
