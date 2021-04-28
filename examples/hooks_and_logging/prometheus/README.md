Prometheus hooks & basic logging
===

This contains a complete example for how to integrate prometheus metrics
through read/write/connect/disconnect/throttle hooks, as well as how to
use a logger with the kgo provided `BasicLogger`.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see logs and run `curl localhost:9999/metrics` in a separate terminal to see
metrics printed.

## Flags

`-debug-port` can be specified to change the metrics port from 9999.

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to consume from an existing topic on your local
broker, which will make the metrics and logs more meaningful.
