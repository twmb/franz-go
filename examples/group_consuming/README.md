Group consuming, committing three different ways
===

This contains an example that consumes records from a topic as a group consumer,
and then depending on the commit style chosen, prints a log of what was consumed
and the risks with the commit style.

If your broker is running on `localhost:9092`, run `go run .` in this directory
to see the consumer output!

## Flags

`-brokers` can be specified to override the default localhost:9092 broker to
any comma delimited set of brokers.

`-topic` can be specified to override the default topic produced to from 'test'

`-group` can be specified to override the default group that is used for consuming

`-style` can be specified as either "autocommit", "records", or "uncommitted" to
change whether to use autocommitting, committing by record, or committing
everything uncommitted.
