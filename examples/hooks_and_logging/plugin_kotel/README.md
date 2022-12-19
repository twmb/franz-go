kotel hooks
===

This example shows how to use the plug-in kotel package to easily export
open telemetry traces/metrics.

The following spans will be linked with a trace id:

1) my-topic send
2) my-topic receive
3) request-span
4) process-span

Metrics will be printed every 60 seconds.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see traces/metrics printed in the console.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to producer/consume from an existing topic on your local
broker.
