kotel hooks
===

This example demonstrates how to use the kotel package to easily export
OpenTelemetry traces and metrics.

The example includes end-to-end tracing, where spans are injected into produce
records and extracted from consume records. The following spans will be linked
with a trace ID:

1) request
2) topic publish
3) topic receive
4) topic process

The example also includes how metrics are exported, they will be displayed
every 60 seconds.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see traces/metrics printed in the console.

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to any
comma delimited set of brokers.

`-topic` can be specified to producer/consume from an existing topic on your
local broker.
