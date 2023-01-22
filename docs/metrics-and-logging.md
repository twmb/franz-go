Metrics and logging
===

The `kgo` package supports both metrics and logging through options. By
default, both are disabled.

## Logging

The [`WithLogger`][1] option can be used to enable internal logging. The
default [`Logger`][2] interface is small but easy to implement, and there is a
simple [`BasicLogger`][3] provided that you can use while developing or even in
production. In production, it is recommended that you use a more "real" logger
such as [zap][4], and to aid this, the franz-go repo provides a drop-in
[`kzap`][5]. There also exists a drop-in [`zerolog`][6] [`kzerolog`][7]
package. If you have another relatively standard logger that would be good to
provide a drop-in package for, please open an issue and we can add it. It is
recommended to use an info logging level: if you find that too noisy, please
open an issue and we can figure out if some logs need to be changed.

[1]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#WithLogger
[2]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Logger
[3]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#BasicLogger
[4]: https://github.com/uber-go/zap
[5]: https://pkg.go.dev/github.com/twmb/franz-go/plugin/kzap
[6]: https://pkg.go.dev/github.com/rs/zerolog
[7]: https://pkg.go.dev/github.com/twmb/franz-go/plugin/kzerolog

## Metrics

`kgo` takes an unopinionated stance on metrics, instead supporting ["hooks"][8]
that you can provide functions for to implement your own metrics. You can
provide an interface that hooks into any behavior you wish to monitor and
provide yourself extremely coarse monitoring or extremely detailed monitoring.
If there are angles you would like to monitor do not have a hook, please open
an issue and we can figure out what hook to add where.

Similar to logging, franz-go provides drop-in packages that provide some
opinion of which metrics may be useful to monitoring: [`kprom`][9] for
prometheus, and [`kgmetrics`][10] for gmetrics.

[8]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Hook
[9]: https://pkg.go.dev/github.com/twmb/franz-go/plugin/kprom
[10]: https://pkg.go.dev/github.com/twmb/franz-go/plugin/kgmetrics

## Latency: brokers, requests, records

The hooks mentioned just above can be used to glean insight into client <=>
broker latency, request latency, and per-record consume & produce latency.
Latencies are not provided by default in the plugin packages within franz-go
because latencies can easily result in cardinality explosion, and it is
difficult to know ahead of time which latency bucketing a user is interested
in.

The `OnBroker` hooks (write, read, e2e) can be used for broker latency. The
hook also has a function parameter indicating the request key that was written
or read; this key can be used to track per-request request kind latency. For
example, if you are interested in produce request latency, you can hook into
request key 0 and monitor it. Similar thought for fetch, with request key 1.

Per-record latency is more difficult to track. When a record is produced, its
`Timestamp` field is set. You can use `time.Since(r.Timestamp)` when the
record's promise is called to track the e2e latency for an _individual_ record.
Internally, records are produced in batches, so a chunk of records will be
promised in a row all at once. The per-record latency for all of these is
likely to be roughly the same; each `time.Since` to track latency is a
_discrete_ observation: per-record latencies should not be added together in an
attempt to learn something. For more thoughts on record & batch latency when
producing, skim [#130][130]. Per-record fetch latency can be measured similarly
to producing: it is more beneficial to measure fetch request latency, but
per-record timestamp delta's can be used to glean producer <=> consumer e2e
latency.

[130]: https://github.com/twmb/franz-go/issues/130

