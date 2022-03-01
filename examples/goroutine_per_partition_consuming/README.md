Group consuming, using a goroutine per partition
===

This directory contains three examples that demonstrate different ways to
have per-partition processing as a group consumer. Because each file is
invoked the same way, this one readme serves all three examples.

These examples consume from a group and start a goroutine to process each
partition concurrently. This type of code may be useful if processing each
record per partition is slow, such that processing records in a single
`PollFetches` loop is not as fast as you want it to be.

A simpler solution would be to have a pool of goroutines selecting from a
channel and then sending all records from your `PollFetches` loop down this
channel. However, the simple solution does not preserve per-partition ordering.

## Auto committing

The autocommitting example is the simplest, but is the most prone to duplicate
consuming due to rebalances. This solution consumes and processes each
partition individually, but does nothing about a behind-the-scenes rebalance.
If a rebalance happens after records are sent to the partition goroutines,
those partition goroutines will process records for partitions that may have
been lost.

## Auto committing marks

This example adds a few things to the simpler auto-committing example. First,
we switch to `BlockRebalanceOnPoll` and uses some locking to avoid rebalances
while the partition goroutines are processing, and we switch to
`AutoCommitMarks` to have more control over what will actually be committed.
This example uses `CommitUncommittedOffsets` at the end of being revoked to
ensure that marked records are committed before revoking is allowed to
continue. Lastly, we use `EachPartition` rather than `EachTopic` to avoid the
internal allocations that `EachTopic` may do.

Blocking rebalance while polling allows for a lot of simplifications in
comparison to plain autocommitting. Compare the differences: we worry less
about whether partition consumers have gone away, and we are more sure of what
is actually happening. These simplifications are commented within the file.

The main downside with `BlockRebalanceOnPoll` is that your application is more
at risk of blocking the rebalance so long that the member is booted from the
group. You must ensure that your goroutine workers are fast enough to not block
rebalancing for all of `RebalanceTimeout`.

## Manually commit

This example is a small extension of the autocommit marks example: rather than
marking records for commit and forcing a commit when revoked, we issue a
synchronous commit in each partition consumer whenever a partition batch is
processed.

This example will have more blocking commits, but has even tighter guarantees
around what is committed when. Because this also uses `BlockRebalanceOnPoll`,
like above, you must ensure that your partition processing is fast enough to
not block a rebalance too long.

## Flags

The flags in each example are the same:

`-b` can be specified to override the default localhost:9092 broker to any
comma delimited set of brokers.

`-t` specifies the topic to consume (required)

`-g` specifies the group to consume in (required)

