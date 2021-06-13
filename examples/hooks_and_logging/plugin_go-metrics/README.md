go-metrics hooks & basic logging
===

This example shows how to use the plug-in kgmetrics package to easily povide
go-metrics metrics.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see logs and run `curl localhost:9999/metrics` in a separate terminal to see
metrics printed.

## Flags

`-debug-port` can be specified to change the metrics port from 9999.

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to consume from an existing topic on your local
broker, which will make the metrics and logs more meaningful.

`-produce`, if used, configures this example to produce "foo" to the topic
once per second, rather than the default of consuming from the topic.
