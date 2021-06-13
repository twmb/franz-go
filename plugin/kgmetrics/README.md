kgmetrics
===

kgmetrics is a plug-in package to provide
[rcrowley/go-metrics](https://github.com/rcrowley/go-metrics) metrics through a
[`kgo.Hook`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Hook).

This package tracks the following metrics under the following names, all
metrics are meters:

```
    broker.<id>.connects
    broker.<id>.connect_errors
    broker.<id>.disconnects
    broker.<id>.write_errors
    broker.<id>.write_bytes
    broker.<id>.read_errors
    broker.<id>.read_bytes
    broker.<id>.topic.<topic>.produce_bytes
    broker.<id>.topic.<topic>.fetch_bytes
```

Note that seed brokers use broker IDs starting at math.MinInt32.

To use,

```go
m := kgmetrics.NewMetrics()
cl, err := kgo.NewClient(
	kgo.WithHooks(m),
	// ...other opts
)
```

You can use your own metrics registry, as well as specify a prefix on metrics.
See the package [documentation](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kgmetrics) for more info!

