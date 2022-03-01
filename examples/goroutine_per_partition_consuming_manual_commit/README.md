Group consuming, using a goroutine per partition and manual committing
===

This example consumes from a group and starts a goroutine to process each
partition concurrently, and committing manually after doing some work, on the slice of records.
This type of code may be useful if processing each
record per partition is slow, such that processing records in a single
`PollRecords` loop is not as fast as you want it to be.

This is just one example of how to process messages concurrently. A simpler
solution would be just to have a group of record consumers selecting from a
channel, and to send all records down this channel in your `PollRecords` loop.
However, that simple solution does not preserve per-partition ordering.

## Flags

`-b` can be specified to override the default localhost:9092 broker to any
comma delimited set of brokers.

`-t` specifies the topic to consume (required)

`-g` specifies the group to consume in (required)

