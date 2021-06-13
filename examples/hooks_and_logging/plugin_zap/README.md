zap logging
===

This example shows how to use the plug-in kzap package to hook in uber's zap
logging.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see logs.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to consume from an existing topic on your local
broker, which will make the metrics and logs more meaningful.

`-produce`, if used, configures this example to produce "foo" to the topic
once per second, rather than the default of consuming from the topic.
