# Consumer Groups

The default consumer group balancer is the new "cooperative-sticky" balancer.
This is **not compatible** with historical balancers (sticky, range, roundrobin).
If you wish to use this client with another client that uses a historical balancer,
you must set the balancers option.

## Offset Management

Unlike Sarama or really most Kafka clients, this client manages the consumer group
**completely independently** from consuming itself. More to the point, a revoke
can happen **at any time** and if you need to stop consuming or do some cleanup
on a revoke, you must set a callback that will **not return** until you are ready
for the group to be rejoined. Even more to the point, if you are manually committing
offsets, you **must** commit in on revoke, or you must abandon your work after the
revoke finishes, because otherwise you may be working on partitions that moved to
another client.

The default consumer group options autocommit every five seconds and automatically
commit in on revoke. Thus, be default, you do not need to do anything. Assuming
committing itself is working (i.e. you are not committing for deleted topics),
then everything will work properly.

As well, when you are **done** consuming, before you shut down, you must perform
a blocking commit. This may become optional in the future if an option is added
to do this for you, but I figured that for the time being, it is worth it to have
at least one instance of you potentially checking if the commit succeeded.

### Without Transactions

There are two easy patterns to success for offset management in a normal consumer
group.

For one, you can rely on simple autocommitting and then a blocking commit on shutdown.
This is the pattern I described just above.

Alternatively, you can guard a revoked variable with a mutex, have it be set in
a custom on revoked callback, and check it every time before committing. This is more
manual but allows for committing whenever desired. I may introduce a GroupSession
struct to abstract this flow in the future, similar to how a GroupTransactSession
type exists today.

### Within Transactions

As mentioned above, because consume-modify-produce loops within transactions have
a higher bar for success, and because this flow itself has about a million footguns,
I have abstracted this flow into a GroupTransactSession type.

With it, you can consume as much as you want, and before you begin producing,
begin a transact session. When you are ready to commit, end the session (you
can also specify to abort here). The session will manage whether an abort needs
to happen or not depending on whether partitions were revoked or anything else.

## The Cooperative Balancer

Kafka 2.4.0 introduced support for [KIP-429][3], the incremental rebalancing
protocol. This allows consumers to continue fetching records **during** a
rebalance, effectively eliminating the stop the world aspect of rebalancing.

[3]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol

This client has support for KIP-429 and in fact default to cooperative consuming.
Cooperative consuming is not compatible with clients using the historical
consumer group strategies, and if you plan to use kgo with these historical
clients, you need to set the balancers appropriately.

This client allows fetching to continue during rebalances with a cooperative
consumer, even in transactions. For transactions, a session will only be
aborted if the cooperative consumer has some partitions revoked. For
non-transactions with autocommitting enabled, a commit only happens if some
transactions are being revoked.

## Static Members

Kafka 2.4.0 introduced support for [KIP-345][4], the "static" member concept
for consumer group members. This is a relatively simple concept that basically
just means that group members must be managed out of band from the client,
whereas historically, member IDs were newly determined every time a client
connected.

[4]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances

Static membership avoids unnecessary partition migration during rebalances
and conveys a host of other benefits; see the KIP for more details.