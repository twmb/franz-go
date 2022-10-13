kotel hooks
===

This example shows how to use the plug-in kotel package to easily export
open telemetry traces/metrics.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see traces/metrics printed in the console.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to consume from an existing topic on your local
broker.
