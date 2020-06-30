kgo
===

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)][godev]

[godev]: https://pkg.go.dev/github.com/twmb/kafka-go/pkg/kgo

## Contents

- [Introduction](#introduction)
- [Stability Status](#stability-status)
- [Performance](#performance)
- [Record Reliability](#record-reliability)
- [EOS](#eos)
- [Consumer Groups](#consumer-groups)
    - [Offset Management](#offset-management)
        - [Without Transactions](#without-transactions)
        - [Within Transactions](#within-transactions)
    - [The Cooperative Balancer](#the-cooperative-balancer)
    - [Static Members](#static-members)
- [Version Pinning](#version-pinning)
- [General Admin Requests](#general-admin-requests)
- [TLS](#tls)
- [Logging](#logging)
- [Metrics](#metrics)
- [Supported KIPs](#supported-kips)

## Introduction

kgo is a high performance, pure Go library for interacting with Kafka.
This library aims to provide every Kafka feature from 0.8.0+.

kgo has support for transactions, regex topic consuming, the latest partitioning
strategies, data loss detection, closest replica fetching, and more. If a client
[KIP][1] exists, this library aims to support it.

[1]: https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals

This library attempts to provide an intuitive API _while_ interacting with Kafka
the way Kafka expects (timeouts, etc.). In some cases, this makes the tricky
corners of Kafka more explicit and manual to deal with correctly. In the general
case, though, I hope this library is satisfactory.

## Stability Status

The current release is a 0.x version, meaning I am not guaranteeing API
stability. Once I have some feedback on whether things need changing or not,
I plan to bump to a 1.x release. As much as possible, I personally consider
the current API stable.

Some features in this client only **theoretically** are implemented, but have
not been tested. The main untested feature is nearest replica fetching.

I have manually tested pausing and unpausing partitions (forcing a leader epoch
bump) and manually moving partitions between brokers while producing and
consuming (with alter partition assignments on Kafka 2.5.0). I aim to add
partition migration to the integration test suite in the code soon.

I have integration tested a chain of producing and consuming within groups
with and without transactions for all balancers. These integration tests rely
on my [`kcl`][2] tool. In the long term, I plan to have tests that spin up
containers and trigger every relevant scenario as appropriate, and I plan to
remove the dependency on kcl.

[2]: https://github.com/twmb/kcl

In effect, consider this **beta**. I would love confirmation that this client
has been used succesfully, and would love to start a "Users" section below.

## Performance

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

## Record Reliability

By default, kgo uses idempotent production. This is automatic and cannot
(currently) be disabled. As well, the default is to always retry records
forever, and to have acks from all in sync replicas. Thus, the default is
highly reliable.

The required acks can be dropped, as can the max record retries, but this
is not recommended.

## EOS

As mentioned above, kgo supports EOS. Because there are a lot of corner cases
around transactions, this client favors a "if we maybe should abort, abort"
approach. This client provides a `GroupTransactSession` type that is used
to manage consume-modify-produce transactions. Any time it is possible that
the transaction should be aborted, the session sets its internal abort state.
This may mean you will end up re-processing records more than necessary, but
in general this should only happen on group rebalances, which should be rare.

By proxy, producer-only transactions are also supported. This is just a simple
extension of the idempotent producer except with a manual begin transaction and
end transaction call whenever appropriate.

EOS has been integration tested, but I would love more verification of it.

## Consumer Groups

The default consumer group balancer is the new "cooperative-sticky" balancer.
This is **not compatible** with historical balancers (sticky, range, roundrobin).
If you wish to use this client with another client that uses a historical balancer,
you must set the balancers option.

### Offset Management

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

#### Without Transactions

There are two easy patterns to success for offset management in a normal consumer
group.

For one, you can rely on simple autocommitting and then a blocking commit on shutdown.
This is the pattern I described just above.

Alternatively, you can guard a revoked variable with a mutex, have it be set in
a custom on revoked callback, and check it every time before committing. This is more
manual but allows for committing whenever desired. I may introduce a GroupSession
struct to abstract this flow in the future, similar to how a GroupTransactSession
type exists today.

#### Within Transactions

As mentioned above, because consume-modify-produce loops within transactions have
a higher bar for success, and because this flow itself has about a million footguns,
I have abstracted this flow into a GroupTransactSession type.

With it, you can consume as much as you want, and before you begin producing,
begin a transact session. When you are ready to commit, end the session (you
can also specify to abort here). The session will manage whether an abort needs
to happen or not depending on whether partitions were revoked or anything else.

### The Cooperative Balancer

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

### Static Members

Kafka 2.4.0 introduced support for [KIP-345][4], the "static" member concept
for consumer group members. This is a relatively simple concept that basically
just means that group members must be managed out of band from the client,
whereas historically, member IDs were newly determined every time a client
connected.

[4]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances

Static membership avoids unnecessary partition migration during rebalances
and conveys a host of other benefits; see the KIP for more details.

## Version Pinning

By default, the client issues an ApiVersions request on connect to brokers
and defaults to using the maximum supported version for requests that each
broker supports.

The ApiVersions request was introduced in Kafka 0.10.0; if you are working
with brokers older than that, you must use the kversions package so explicitly
set the MaxVersions option for the client to support.

As well, it is recommended to set the MaxVersions to the version of your
broker cluster. Until [KIP-584][5] is implemented, it is possible that
if you do not pin a max version, this client will speak with some features
to one broker while not to another when you are in the middle of a broker
update roll.

[5]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features

## General Admin Requests

All Kafka requests and responses are supported through generated code in the
kmsg package. The package aims to provide some relatively comprehensive
documentation (at least more than Kafka itself provides), but may be lacking
in some areas.

If you are using the kmsg package manually, it is **strongly** recommended to
set the MaxVersions option so that you do not accidentally have new fields
added across client versions erroneously have zero value defaults sent to
Kafka.

It is recommended to always set all fields of a request. If you are talking
to a broker that does not support all fields you intend to use, those fields
are silently not written to that broker. It is recommended to ensure your
brokers support what you are expecting to send them.

To issue a kmsg request, use the client's `Request` function. This function
is a bit overpowered, as specified in its documentation.

## TLS

This client does not provide any TLS on its own, however it does provide
a Dialer option to set how connections should be dialed to brokers. You
can use the dialer to dial TLS as necessary, with any specific custom
TLS configuration you desire.

## Logging

The client supports some relatively primitive logging currently that will
be enhanced and improved as requests for improvement come in.

Do not expect the logging messages to be stable across library versions.

## Metrics

This client does not currently have metrics, but aims to support metrics
through function callbacks in the future. I would like some input on
what metrics are desired. The usage of callbacks will ensure that users
can plug in which metric libraries they would like to use as desired.

## Supported KIPs

Theoretically, every (non-Java-specific) client facing KIP is supported.
Most are tested, some need testing. Any KIP that simply adds or modifies
a protocol is supported by code generation.

- KIP-12 (sasl & ssl; 0.9.0)
- KIP-13 (throttling; supported but not obeyed)
- KIP-31 (relative offsets in message set; 0.10.0)
- KIP-32 (timestamps in message set v1; 0.10.0)
- KIP-35 (adds ApiVersion; 0.10.0)
- KIP-36 (rack aware replica assignment; 0.10.0)
- KIP-40 (ListGroups and DescribeGroup v0; 0.9.0)
- KIP-43 (sasl enhancements & handshake; 0.10.0)
- KIP-54 (sticky group assignment)
- KIP-62 (join group rebalnce timeout, background thread heartbeats; v0.10.1)
- KIP-74 (fetch response size limit; 0.10.1)
- KIP-78 (cluster id in metadata; 0.10.1)
- KIP-79 (list offset req/resp timestamp field; 0.10.1)
- KIP-84 (sasl scram; 0.10.2)
- KIP-98 (EOS; 0.11.0)
- KIP-101 (offset for leader epoch introduced; broker usage yet; 0.11.0)
- KIP-107 (delete records; 0.11.0)
- KIP-108 (validate create topic; 0.10.2)
- KIP-110 (zstd; 2.1.0)
- KIP-112 (JBOD disk failure, protocol changes; 1.0.0)
- KIP-113 (JBOD log dir movement, protocol additions; 1.0.0)
- KIP-124 (request rate quotas; 0.11.0)
- KIP-133 (describe & alter configs; 0.11.0)
- KIP-152 (more sasl, introduce sasl authenticate; 1.0.0)
- KIP-183 (elect preferred leaders; 2.2.0)
- KIP-185 (idempotent is default; 1.0.0)
- KIP-195 (create partitions request; 1.0.0)
- KIP-207 (new error in list offset request; 2.2.0)
- KIP-219 (throttling happens after response; 2.0.0)
- KIP-226 (describe configs v1; 1.1.0)
- KIP-227 (incremental fetch requests; 1.1.0)
- KIP-229 (delete groups request; 1.1.0)
- KIP-255 (oauth via sasl/oauthbearer; 2.0.0)
- KIP-279 (leader / follower failover; changed offsets for leader epoch; 2.0.0)
- KIP-320 (fetcher log truncation detection; 2.1.0)
- KIP-322 (new error when delete topics is disabled; 2.1.0)
- KIP-339 (incremental alter configs; 2.3.0)
- KIP-341 (sticky group bug fix)
- KIP-342 (oauth extensions; 2.1.0)
- KIP-345 (static group membership, see KAFKA-8224)
- KIP-360 (safe epoch bumping for `UNKNOWN_PRODUCER_ID`; 2.5.0)
- KIP-368 (periodically reauth sasl; 2.2.0)
- KIP-369 (always round robin produce partitioner; 2.4.0)
- KIP-380 (inter-broker command changes; 2.2.0)
- KIP-392 (fetch request from closest replica w/ rack; 2.2.0)
- KIP-394 (require member.id for initial join; 2.2.0)
- KIP-412 (dynamic log levels with incremental alter configs; 2.4.0)
- KIP-429 (incremental rebalance, see KAFKA-8179; 2.4.0)
- KIP-430 (include authorized ops in describe groups; 2.3.0)
- KIP-447 (transaction changes to better support group changes; 2.5.0)
- KIP-455 (admin replica reassignment; 2.4.0)
- KIP-460 (admin leader election; 2.4.0)
- KIP-464 (defaults for create topic, KIP-464; 2.4.0)
- KIP-467 (produce response error change for per-record errors; 2.4.0)
- KIP-480 (sticky partition producing; 2.4.0)
- KIP-482 (tagged fields; KAFKA-8885; 2.4.0)
- KIP-496 (offset delete admin command; 2.4.0)
- KIP-511 (add client name / version in apiversions req; 2.4.0)
- KIP-518 (list groups by state; 2.6.0)
- KIP-525 (create topics v5 returns configs; 2.4.0)
- KIP-526 (reduce metadata lookups; done minus part 2, which we wont do)
- KIP-546 (client quota APIs; 2.5.0)
- KIP-559 (protocol info in sync / join; 2.5.0)
- KIP-569 (doc/type in describe configs; 2.6.0)
- KIP-570 (leader epoch in stop replica; 2.6.0)
- KIP-580 (exponential backoff; 2.6.0)
