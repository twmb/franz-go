Producing and consuming
===

This document describes at a high level how producing and consuming works, and
links to pkg.go.dev documentation for you to understand more on the mentioned
functions/methods and how they can be used.

Code for both producing and consuming can be seen in the examples directory,
particularly in the transaction examples.

## Producing

The client provides three methods to produce, [`Produce`][1],
[`ProduceSync`][2], and [`TryProduce`][TryProduce]. The first allows for
asynchronous production, the second for synchronous, and the third for async
while also failing a record immediately if the [maximum records][max_records]
are buffered. These methods are also available on [`GroupTransactSession`][3]
if you are using that for EOS.

Everything is produced through a [`Record`][4]. You can produce to multiple
topics by creating a record in full (i.e., with a `Value` or `Key` or
`Headers`) and then setting the `Topic` field, but if you are only ever
producing to one topic, you can use the client's `DefaultProduceTopic` option.
You can still use this option even when producing to multiple topics; the
option only applies to records that have an empty topic.

There exist a few small helpers to create records out of slices or strings:

* [`kgo.StringRecord`][5]
* [`kgo.KeyStringRecord`][6]
* [`kgo.SliceRecord`][7]
* [`kgo.KeySliceRecord`][8]

The string functions should only be used if you do not touch the `Key` and
`Value` fields after the record is created, because the string functions use
the `unsafe` package to convert the strings to slices without allocating.

Lastly, if you are producing asynchronously in batches and only want to know
whether the batch errored at all, there exists a [`FirstErrPromise`][9] type to
help eliminate promise boilerplate.

[1]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Produce
[2]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.ProduceSync
[3]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#GroupTransactSession
[4]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Record
[5]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#StringRecord
[6]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#KeyStringRecord
[7]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#SliceRecord
[8]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#KeySliceRecord
[9]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#FirstErrPromise
[TryProduce]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.TryProduce
[max_records]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#MaxBufferedRecords

### Record reliability

By default, kgo uses idempotent production. This can be disabled with the
[`DisableIdempotentWrite`][10] option, but this should really only be necessary
if you want to produce with no ack required or with only leader acks required.

[10]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#DisableIdempotentWrite

The default is to always retry records forever, but this can be dropped with
the [`RecordRetries`][11] and [`RecordDeliveryTimeout`][12] options, as well as with
the context that you use for producing a record. A record will only be aborted
if it is safe to do so without messing up the client's sequence numbers. Thus,
a record can only be aborted if it has never been produced or if it knows that
it received a successful response from its last produce attempt (even if that
response indicated an error on that partition). If a record is ever failed, all
records buffered on the same partition are failed.

[11]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#RecordRetries
[12]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#RecordDeliveryTimeout

### Exactly once semantics

As mentioned above, kgo supports EOS. Because there are a lot of corner cases
around transactions, this client favors a "if we maybe should abort, abort"
approach. This client provides a `GroupTransactSession` type that is used
to manage consume-modify-produce transactions. Any time it is possible that
the transaction should be aborted, the session sets its internal abort state.
This may mean you will end up re-processing records more than necessary, but
in general this should only happen on group rebalances, which should be rare.

Producer-only transactions are also supported. This is just a simple extension
of the idempotent producer except with a manual begin transaction and end
transaction call whenever appropriate.

To see more documentation about transactions and EOS, see the
[transactions](./transactions.md) page.

### Latency

Producer latency can be modified by adding a linger. By default, there is no
linger and records are sent as soon as they are published. In a high throughput
scenario, this is fine and will not lead to single-record batches, but in a low
throughput scenario it may be worth it to add lingering.

As well, it is possible to completely disable auto-flushing and instead only
have manual flushes with the
[`ManualFlushing`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#ManualFlushing)
option. This allows you to buffer as much as you want before flushing in one
go. However, with this option, you likely want to consider the
[`MaxBufferedRecords`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#MaxBufferedRecords)
option.

## Consuming

franz-go supports consuming partitions directly, consuming as a part of a
consumer group, and consuming in a group for EOS. Consuming can also be done
via regex to match certain topics to consume.

To consume partitions directly, use [`ConsumeTopics`][13] or [`ConsumePartitions`][a]. Otherwise, use
`ConsumeTopics` with [`ConsumerGroup`][14] for group consuming or [`NewGroupTransactSession`][15]
for group consuming for EOS.

[13]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#ConsumeTopics
[a]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#ConsumePartitions
[14]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#ConsumerGroup
[15]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#NewGroupTransactSession

### Consumer groups

The default consumer group balancer is the new "cooperative-sticky" balancer.
This is **not compatible** with historical balancers (sticky, range, roundrobin).
If you wish to use this client with another client that uses a historical balancer,
you must set the balancers option.

By default, the group consumer will autocommit every 5s, commit whenever a
rebalance happens (in [`OnPartitionsRevoked`][16]), and will issue a blocking
commit when leaving the group. For most purposes, this can suffice. The default
commit logs any errors encountered, but this can be overridden with the
[`AutoCommitCallback`][17] option or by disabling autocommit and instead committing
yourself.

[16]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#OnPartitionsRevoked
[17]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#AutoCommitCallback

#### Offset management

Unlike Sarama or really most Kafka clients, this client manages the consumer
group **completely independently** from consuming itself. More to the point, a
revoke can happen **at any time** and if you need to stop consuming or do some
cleanup on a revoke, you must set a callback that will **not return** until you
are ready for the group to be rejoined. Even more to the point, if you are
manually committing offsets, you **must** commit in your `OnPartitionsRevoked`,
or you must abandon your work after the revoke finishes, because otherwise you
may be working on partitions that moved to another client. When you are **done
consuming**, before you shut down, you must perform a blocking commit. If you
rely on the default options and do not commit yourself, all of this is
automatically handled.

Alternatively, you can use the [`BlockRebalanceOnPoll`][BROP] option in
combination with [`AllowRebalance`][AR] to ensure rebalance cannot happen after
you poll until you explicitly allow it. This option is much easier to reason
about, but has a risk if processing your poll takes so long that a rebalance
started and finished and you were kicked from the group. If you use this option
and this API, it is recommended to take care and use [`PollRecords`][PR] and
ensure your [`RebalanceTimeout`][RT] is long enough to encompass any processing you
do between polls. My recommendation is to block rebalance on poll, ensure your
processing is quick, and to use [`CommitUncommittedOffsets`][CUO].

[BROP]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#BlockRebalanceOnPoll
[AR]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.AllowRebalance
[PR]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.PollRecords
[RT]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#RebalanceTimeout
[CUO]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.CommitUncommittedOffsets

##### Direct offset management outside of a group

You can use the `ConsumePartitions` option to assign partitions manually and
consume outside of the context of a group. If you want to use Kafka to manage
group offsets even with direct partition assignment, this repo provides a
`kadm` package to easily manage offsets via an admin interface. Check the
[`manual_committing`](../examples/manual_committing) example to see some
example code for how to do this.

##### Without transactions

There are two easy patterns to success for offset management in a normal
consumer group.

First and the most recommended option, you can just rely on the default
autocommitting behavior and the default blocking commit on leave. At most, you
may want to use your own custom commit callback.

Alternatively, you can disable autocommitting with [`DisableAutoCommit`][19]
and instead use a custom `OnPartitionsRevoked`.

[19]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#DisableAutoCommit

In your custom revoke, you can guard a revoked variable with a mutex. Before
committing, check this revoked variable and do not commit if it has been set.
For some hints as to how to do this properly, check how
[`GroupTransactSession`][20] is implemented (albeit it is more complicated due
to handling transactions).

[20]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#GroupTransactSession

##### With transactions

Because an EOS consumer is difficult to implement correctly, all details have
been abstracted away to a [`GroupTransactSession`][20] type. See the
[transactions](./transactions.md) page for more details.

### The cooperative balancer

Kafka 2.4.0 introduced support for [KIP-429][21], the incremental rebalancing
protocol. This allows consumers to continue fetching records **during** a
rebalance, effectively eliminating the stop the world aspect of rebalancing.
However, while the Java client introduced this in Kafka 2.4.0, the balancer
is not actually dependent on that Kafka version.

[21]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol

This client has support for KIP-429 and in fact defaults to cooperative
consuming. Cooperative consuming is not compatible with clients using the
historical consumer group strategies, and if you plan to use kgo with these
historical clients, you need to set the balancers appropriately.

Cooperative rebalancing allows a client to continue fetching during rebalances,
even during transactions. For transactions, a transact session will only be
aborted if the member has partitions revoked.

### Static membership

Kafka 2.4.0 also introduced support for [KIP-345][22], the "static" member
concept for consumer group members. This is a relatively simple concept that
basically just means that group members must be managed out of band from the
client, whereas historically, member IDs were newly determined every time a
client connected.

[22]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances

Static membership avoids unnecessary partition migration during rebalances and
conveys a host of other benefits; see the KIP for more details. To use static
membership, your cluster must be at least 2.4.0, and you can use the
[`InstanceID`][23] option.

[23]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#InstanceID
