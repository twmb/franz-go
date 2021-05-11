Producing transactionally, and consuming
===

This contains an example that will produce 10 records in succession as a part
of a single transaction, and will then commit those records. After every commit,
the consumer is able to consume what was just produced.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see the consumer output!

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to override the default topic produced to from 'test'

`-group` can be specified to override the default group that is used for consuming
