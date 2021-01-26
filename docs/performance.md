# Performance

This client avoids spinning up more goroutines than necessary and avoids
lock contention as much as possible.

For simplicity, this client **does** buffer records before writing to Kafka.
The assumption here is that modern software is fast, and buffering will be of
minimal concern.

Producer latency can be tuned by adding a linger. By default, there is no
linger and records are sent as soon as they are published. In a high
throughput scenario, this is fine and will not lead to single-record batches,
but in a low throughput scenario it may be worth it to add lingering.
As well, it is possible to completely disable auto-flushing and instead
only have manual flushes. This allows you to buffer as much as you want
before flushing in one go (however, with this option, you must consider
the max buffered records option).