v1.2.4
===

This patch release fixes handling forgotten topics in fetch sessions, and
allows disabling fetch sessions. Previously, if a topic or partition was paused
with fetch sessions active, the partition would continually be fetched and
skipped behind the scenes. As well, the client now allows disabling fetch
sessions entirely if they are not desired.

This also fixes one bug in the `RecordReader`: using an empty layout would mean
that any `io.Reader` would result in infinite messages.

Lastly, some log messages have been improved.

- [`db90100`](https://github.com/twmb/franz-go/commit/db90100) consuming: improve error messages
- [`101d6bd`](https://github.com/twmb/franz-go/commit/101d6bd) consuming: log added/skipped when consuming by regex
- [`b6759bc`](https://github.com/twmb/franz-go/commit/b6759bc) **improvement** consumer: allow disabling fetch sessions with a config opt
- [`7cd959c`](https://github.com/twmb/franz-go/commit/7cd959c) **bugfix** source: use forgotten topics for sessions
- [`cfb4a7f`](https://github.com/twmb/franz-go/commit/cfb4a7f) **bugfix** kgo: error if RecordReader layout is empty

v1.2.3
===

This patch fixes the client not supporting brokers that start from large node
IDs. The client previously assumed that brokers started at node ID 0 or 1 and
went upwards from there. Starting at node 1000 would not always work.

This also adds two very tiny new APIs, `FetchTopic.Each{Partition,Record}`,
which are so tiny they are not worth a minor release.

More work on kadm has been done: the package now supports ACLs.

SASL no longer has a hard coded 30s write timeout; instead, we use the
ConnTimeoutOverhead same as everything else. The default produce request
timeout has been dropped to 10s from 30s, which is still ~10x more time than
Kafka usually needs at the p99.9 level. This will help speed up producing to
hung brokers. As well, the default backoff has been changed to 250ms-2.5s
rather than 100ms-1s. The old backoff retried too quickly in all cases.

Logs for assigned partitions have been improved, as have logs for inner
metadata request / fetch request failures.

- [`07a38bc`](https://github.com/twmb/franz-go/commit/07a38bc) **bugfix** client: support non-zero/one node IDs
- [`3cbaa5f`](https://github.com/twmb/franz-go/commit/3cbaa5f) add more context to metadata reloads on inner partition errors
- [`1bc1156`](https://github.com/twmb/franz-go/commit/1bc1156) **feature** FetchTopic: add EachRecord, Records helper methods
- [`0779837`](https://github.com/twmb/franz-go/commit/0779837) consuming, group: improve logging, simplify code
- [`d378b32`](https://github.com/twmb/franz-go/commit/d378b32) config: edit comment for FetchMaxWait (about Java setting) (thanks @dwagin!)
- [`df80a52`](https://github.com/twmb/franz-go/commit/df80a52) **behavior change** config: drop ProduceRequestTimeout to 10s, doc more
- [`6912cfe`](https://github.com/twmb/franz-go/commit/6912cfe) **behavior change** config: change default backoff from 100ms-1s to 250ms-2.5s
- [`c197efd`](https://github.com/twmb/franz-go/commit/c197efd) **behavior change** client: remove 30s default write timeout for SASL
- [`10ff785`](https://github.com/twmb/franz-go/commit/10ff785) metadata: drop timer based info log, reword

v1.2.2
===

This patch release fixes a bug with cooperative group consuming. If a
cooperative group member rebalances before offset fetching returns, then it
will not resume offset fetching partitions it kept across the rebalance. It
will only offset fetch new partitions.

This release fixes that by tracking a bit of fetch state across rebalances for
cooperative consumers. See the embedded comments in the commit for more
details.

- [`4f2e7fe`](https://github.com/twmb/franz-go/commit/4f2e7fe) **bugfix** consumer group: fix cooperative rebalancing losing offset fetches


v1.2.1
===

This patch release fixes a panic that can occur in the following sequence of
events:

1) a LeaveGroup occurs (which always happens in Close)
2) the cluster moves a partition from one broker to another
3) a metadata refresh occurs and sees the partition has moved

Whenever a client closes, `Close` calls `LeaveGroup` (even if not in a group),
and there is a small window of time before the metadata loop quits.

- [`864526a`](https://github.com/twmb/franz-go/commit/864526a) **bugfix** consuming: avoid setting cursors to nil on LeaveGroup


v1.2.0
===

This release contains new formatting features and sets the stage for a new
admin administration package to be added in v1.3.0. For now, the `kadm` package
is in a separate unversioned module. The API is relatively stable, but I'd like
to play with it some more to figure out what needs changing or not. Any
external experimenting and feedback is welcome.

There are two new types introduced in the kgo package: `RecordFormatter` and
`RecordReader`. Both of these extract and improve logic used in the
[`kcl`](github.com/twmb/kcl) repo. These are effectively powerful ways to
format records into bytes, and read bytes into records. They are not _exactly_
opposites of each other due to reasons mentioned in the docs. I expect
formatting to be more useful than reading, and formatting is actually
surprisingly fast if the formatter is reused. The one off `Record.AppendFormat`
builds a new formatter on each use and should only be used if needed
infrequently, because building the formatter is the slow part.

I am considering removing the default of snappy compression for the v1.3
release. I generally find it useful: many people forget to add compression
which increases network costs and makes broker disks fill faster, and snappy
compression is cheap cpu-wise and fast. However, some people are confused when
their bandwidth metrics look lower than they expect, and snappy _does_ mean
throughput is not _as_ high as it could be. Removal of snappy by default may or
may not happen, but if anybody agrees or disagrees with it, please mention so
in an issue or on discord.

There are a few minor behavior changes in this release that should make default
clients safer (lowering max buffered memory) and faster on request failures.
These behavior changes should not affect anything. As well, the `bench` utility
no longer uses snappy by default.

Non-formatter/reader/kadm commits worth mentioning:

- [`a8dbd2f`](https://github.com/twmb/franz-go/commit/a8dbd2f) broker: add context to responses that look like HTTP
- [`ec0d81f`](https://github.com/twmb/franz-go/commit/ec0d81f) broker: avoid non-debug logs on ErrClientClosed
- [`af4fce4`](https://github.com/twmb/franz-go/commit/af4fce4) **beahvior change** bench: use no compression by default, remove -no-compression flag
- [`d368d11`](https://github.com/twmb/franz-go/commit/d368d11) bench: allow custom certs with -ca-cert, -client-cert, -client-key
- [`fbf9239`](https://github.com/twmb/franz-go/commit/fbf9239) broker: add two new connection types, cxnGroup and cxnSlow
- [`2d6c1a8`](https://github.com/twmb/franz-go/commit/2d6c1a8) **behavior change** client: lower ConnTimeoutOverhead and RetryTimeout defaults
- [`9bcfc98`](https://github.com/twmb/franz-go/commit/9bcfc98) kgo: only run req/resp handling goroutines when needed
- [`b9b592e`](https://github.com/twmb/franz-go/commit/b9b592e) **behavior change** kgo: change default MaxBufferedRecords from unlimited to 10,000
- [`19f4e9b`](https://github.com/twmb/franz-go/commit/19f4e9b) and [`58bf74a`](https://github.com/twmb/franz-go/commit/58bf74a) client: collapse shard errors
- [`126778a`](https://github.com/twmb/franz-go/commit/126778a) client: avoid sharding to partitions with no leader
- [`254764a`](https://github.com/twmb/franz-go/commit/254764a) **behavior change** kversion: skip WriteTxnMarkers for version guessing

v1.1.4
===

This includes on more patch for the prior tag to fully make `InitProducerID`
retriable when it encounters a retriable error.

- [`d623ffe`](https://github.com/twmb/franz-go/commit/d623ffe) errors: make errProducerIDLoadFail retriable


v1.1.3
===

This patch allows `BrokerNotAvailable` to be retried. As well, it contains some
small improvements under the hood. When sharding certain requests, we now avoid
issuing requests to partitions that have no leader. We also better collapse
per-partition sharding error messages.

Dial errors are now not retriable. Now, if a produce request fails because the
broker cannot be dialed, we refresh metadata. Previously, we just accepted the
error as retriable and tried producing again.

The SoftwareVersion used in ApiVersions requests now includes your current kgo
version.

This tag is based on a 1.1.x branch in the repo. There is some feature work
that is yet to be stabilized on the main branch.

Notable commits:

- [`747ab0c`](https://github.com/twmb/franz-go/commit/747ab0c) kerr: make BrokerNotAvailable retriable
- [`f494301`](https://github.com/twmb/franz-go/commit/f494301) errors: make dial errors non-retriable
- [`4a76861`](https://github.com/twmb/franz-go/commit/4a76861) config: default to kgo dep version in ApiVersions

v1.1.2
===

This patch release fixes processing the `LogAppendTime` timestamp for message
set v1 formatted records. Message set v1 records were used only during the
Kafka v0.10 releases.

- [`5fee169..688d6fb`](https://github.com/twmb/franz-go/compare/5fee169..688d6fb): **bugfix** message set v1: fix bit 4 as the timestamp bit

v1.1.1
===

This patch release fixes a bug in `PollRecords` that could cause a panic during
cluster instability. This also defines Kafka 3.0 in kversion.

- [`847aeb9`](https://github.com/twmb/franz-go/commit/847aeb9) **bugfix** consumer: do not buffer partitions we reload
- [`49f82b9`](https://github.com/twmb/franz-go/commit/49f82b9) kversion: cut 3.0

v1.1.0
===

This minor release contains a few useful features, an incredibly minor bugfix
(fixing an error message), and a behavior change around buffering produced
records for unknown topics. As well, all plugins have been tagged v1.0.0.

- [`cf04997`](https://github.com/twmb/franz-go/commit/cf04997) errors: export ErrRecordRetries, ErrRecordTimeout
- [`ee8b12d`](https://github.com/twmb/franz-go/commit/ee8b12d) producer: fail topics that repeatedly fail to load after 5 tries
- [`f6a8c9a`](https://github.com/twmb/franz-go/commit/f6a8c9a) **very minor bugfix** ErrDataLoss: use the new offset **after** we create the error
- [`8216b7c`](https://github.com/twmb/franz-go/commit/8216b7c) and [`c153b9a`](https://github.com/twmb/franz-go/commit/c153b9a) **feature** kmsg: add EnumStrings(), ParseEnum() functions
- [`e44dde9`](https://github.com/twmb/franz-go/commit/e44dde9) add `goroutine_per_partition` consuming example
- [`cedffb7`](https://github.com/twmb/franz-go/commit/cedffb7) **feature** plugins: Add kzerolog logging adapter (thanks @fsaintjacques!)
- [`563e016`](https://github.com/twmb/franz-go/commit/563e016) **feature** FetchTopic: add EachPartition; FetchPartition: add EachRecord
- [`8f648e7`](https://github.com/twmb/franz-go/commit/8f648e7) consumer group: document actual behavior for on revoked / lost

v1.0.0
===

This is a significant release and is a commitment to not changing the API.
There is no change in this release from v0.11.1. This release has been a long
time coming!

The kmsg package is a separate dedicated module and its API can still change if
Kafka changes the protocol in a way that breaks kmsg. However, the odds are
increasingly likely that kmsg will switch to bumping major versions as Kafka
makes incompatible changes, so that the module does not have breaking updates.

v0.11.1
===

This is a patch release to a bug introduced in v0.10.3 / v0.11.0.

The intent is to tag v1.0 either Friday (tomorrow) or Monday.

- [`1469495`](https://github.com/twmb/franz-go/commit/1469495) **bugfix** isRetriableBrokerErr: nil error is **not** retriable (thanks @Neal!)
- [`33635e2`](https://github.com/twmb/franz-go/commit/33635e2) **feature** consumer: add pausing / unpausing topics and partitions
- [`082db89`](https://github.com/twmb/franz-go/commit/082db89) **bugfix** add locking to g.uncommitted MarkCommitRecords (thanks @vtolstov!)
- [`3684df2`](https://github.com/twmb/franz-go/commit/3684df2) fetches: rename CollectRecords to Records

v0.11.0
===

This release is an rc2 for a v1 release, and contains breaking API changes
related to renaming some options. The point of this release is to allow any
chance for final API suggestions before stabilization.

The following configuration options were changed:

* `ConnTimeoutOverhead` to `RequestTimeoutOverhead`
* `CommitCallback` to `AutoCommitCallback`
* `On{Assigned,Revoked,Lost}` to `OnPartitions{Assigned,LostRevoked}`
* `BatchCompression` to `ProducerBatchCompression`
* `Linger` to `ProducerLinger`
* `BatchMaxBytes` to `ProducerBatchMaxBytes`
* `{,Stop}OnDataLoss` to `{,Stop}ProducerOnDataLossDetected`
* `RecordTimeout` to `RecordDeliveryTimeout`

The point of these renames is to make the options a bit clearer as to their
purpose and to hopefully be slightly less confusing.

**If you have any API suggestions that should be addressed before a 1.0 API
stabilization release, please open an issue.**

- [`577c73a`](https://github.com/twmb/franz-go/commit/577c73a) **breaking**: rename RecordTimeout to RecordDeliveryTimeout
- [`76ae8f5`](https://github.com/twmb/franz-go/commit/76ae8f5) **breaking**: rename {,Stop}OnDataLoss to {,Stop}ProducerOnDataLossDetected
- [`6272a3b`](https://github.com/twmb/franz-go/commit/6272a3b) **breaking**: rename BatchMaxBytes to ProducerBatchMaxBytes
- [`9a76213`](https://github.com/twmb/franz-go/commit/9a76213) **breaking**: rename Linger to ProducerLinger
- [`3d354bc`](https://github.com/twmb/franz-go/commit/3d354bc) **breaking**: rename BatchCompression to ProducerBatchCompression
- [`8c3eb3c`](https://github.com/twmb/franz-go/commit/8c3eb3c) **breaking**: renaming OnXyz to OnPartitionsXyz
- [`525b2d2`](https://github.com/twmb/franz-go/commit/525b2d2) **breaking**: rename CommitCallback to AutoCommitCallback
- [`ac9fd1c`](https://github.com/twmb/franz-go/commit/ac9fd1c) docs: clarify Balancer is equivalent to Java's PartitionAssignor
- [`2109ed4`](https://github.com/twmb/franz-go/commit/2109ed4) **breaking**: rename ConnTimeoutOverhead to RequestTimeoutOverhead

v0.10.3
===

This is a small release intended to be a minor release for anybody that does
not want to update the API. The next release, v0.11.0, is going to contain
breaking config option changes. The point of the intermediate release is to
allow a few days for anybody to raise issues for better suggestions.

- [`31ed46b`](https://github.com/twmb/franz-go/commit/31ed46b) **feature** consuming: add autocommitting marked records
- [`c973268`](https://github.com/twmb/franz-go/commit/c973268) **feature** fetches: add CollectRecords convenience function
- [`a478251`](https://github.com/twmb/franz-go/commit/a478251) **bugfix** source: advance offsets even if we return no records
- [`307c22e`](https://github.com/twmb/franz-go/commit/307c22e) **minor bugfix** client retriable req: fix err bug
- [`fcaaf3f`](https://github.com/twmb/franz-go/commit/fcaaf3f) breaking kmsg: update per protocol changes
- [`262afb4`](https://github.com/twmb/franz-go/commit/262afb4) **bugfix** offset commit: use -1 for RetentionTimeMillis
- [`c6df11d`](https://github.com/twmb/franz-go/commit/c6df11d) consumer: commit all dirty offsets when _entering_ poll
- [`e185676`](https://github.com/twmb/franz-go/commit/e185676) (minor) breaking change: `AllowedConcurrentFetches` => `MaxConcurrentFetches`
- [`d5e80b3`](https://github.com/twmb/franz-go/commit/d5e80b3) bump min go version to go1.15

v0.10.2
===

This release contains everything currently outstanding for a v1.0.0 release.
Primarily, this release addresses #57, meaning that by default, auto committing
no longer has the potential to lose records during crashes. It is now highly
recommended to have a manual `CommitUncommittedOffsets` before shutting down if
autocommitting, just to prevent any unnecessary duplicate messages.

The aim is to let this release bake for a day / a few days / a week so that if
anybody tries it out, I can receive any bug reports.

- [`31754a9`](https://github.com/twmb/franz-go/commit/31754a9) **feature-ish** compression: switch snappy to @klauspost's s2
- [`28bba43`](https://github.com/twmb/franz-go/commit/28bba43) autocommitting: only commit previously polled fetches
- [`4fb0de2`](https://github.com/twmb/franz-go/commit/4fb0de2) `isRetriableBrokerErr`: opt in `net.ErrClosed`, restructure
- [`0d01f74`](https://github.com/twmb/franz-go/commit/0d01f74) **feature** add `RoundRobinPartitioner`
- [`5be804d`](https://github.com/twmb/franz-go/commit/5be804d) `LeastBackupPartitioner`: fix, speedup
- [`733848b`](https://github.com/twmb/franz-go/commit/733848b) **bugfix** CommitRecords: commit next offset to move forward
- [`4c20135`](https://github.com/twmb/franz-go/commit/4c20135) **bugfix/simplification** sink: simplify decInflight for batches in req

v0.10.1
===

This fix contains an important bugfix for some behavior introduced in v0.9.1,
as well as extra debug logging messages, and a few other general improvements.
Some more errors are now retriable in more cases, and the client can now
tolerate some input seeds being invalid when others are not invalid.

It is highly recommended that any use of v0.9.1 switch to v0.10.1. If on v0.9.1
and a produced batch has a retriable error, and then some cluster maintenance
causes the partition to move to a different broker, the client will be unable
to continue producing to the partition.

- [`029f5e3`](https://github.com/twmb/franz-go/commit/029f5e3) **bugfix** sink: fix decInflight double decrement bug
- [`2cf62e2`](https://github.com/twmb/franz-go/commit/2cf62e2) sink: properly debug log the number of records written
- [`370c18e`](https://github.com/twmb/franz-go/commit/370c18e) and [`caadb8b`](https://github.com/twmb/franz-go/commit/caadb8b) **feature**: add LeastBackupPartitioner
- [`0fee54c`](https://github.com/twmb/franz-go/commit/0fee54c) and [`589c5e5`](https://github.com/twmb/franz-go/commit/589c5e5) add errUnknownBroker and io.EOF to be retriable errors in sink; add errUnknownBroker to be skippable in the client
- [`ec0c992`](https://github.com/twmb/franz-go/commit/ec0c992) client improvement: retry more when input brokers are invalid

v0.10.0
===

#62 now tracks the v1.0.0 stabilization status. This release contains
everything but addressing #57, which I would like to address before
stabilizing.

After using this client some more in a different program, I've run into some
things that are better to break now. Particularly, a few options are awkward.
The goal is for this release to make these options less awkward.

Three options have been renamed in this release, the fixes are very simple (and
ideally, if you used `RetryTimeout`, you may find the new `RetryTimeout` even
simpler).

- [`c1d62a7`](https://github.com/twmb/franz-go/commit/c1d62a7) **feature** config: add DialTLSConfig option
- [`4e5eca8`](https://github.com/twmb/franz-go/commit/4e5eca8) **breaking**: rename ProduceRetries to RecordRetries
- [`12808d5`](https://github.com/twmb/franz-go/commit/12808d5) **breaking**: rename RetryBackoff to RetryBackoffFn
- [`8199f5b`](https://github.com/twmb/franz-go/commit/8199f5b) **breaking**: split RetryTimeout function

v0.9.1
===

v0.9.0 is to be stabilized, but I would like to address #57 first, which may
slightly change how autocommitting works. This intermediate release pulls in
some decent fixes.

- [`fd889cc`](https://github.com/twmb/franz-go/commit/fd889cc) all: work around brokers that have inconsistent request versions
- [`f591593`](https://github.com/twmb/franz-go/commit/f591593) broker: permanently store the initial ApiVersions response
- [`8da4eaa`](https://github.com/twmb/franz-go/commit/8da4eaa) and [`37ecd21`](https://github.com/twmb/franz-go/commit/37ecd21) **feature** client: enable ipv6 support in seed brokers (thanks @vtolstov and @SteadBytes!)
- [`aecaf27`](https://github.com/twmb/franz-go/commit/aecaf27) **bugfix** client: force a coordinator refresh if the coordinator is unknown
- [`f29fb7f`](https://github.com/twmb/franz-go/commit/f29fb7f) **bugfix** sink: fix out-of-order response handling across partition rebalances
- [`4fadcde`](https://github.com/twmb/franz-go/commit/4fadcde) **rare bugfix** consuming: fix logic race, simplify some logic

v0.9.0
===

This is a v1.0 release candidate. The only breaking change in this release is
that the kmsg package is now a dedicated module.

### Why have a module for kmsg?

The kmsg package is almost entirely autogenerated from the Kafka protocol.
Unfortunately, the Kafka protocol is mostly designed for an object oriented
language (i.e., Java), so some changes to the Kafka protocol that are
non-breaking are breaking in the kmsg package. In particular, the Kafka changes
a non-nullable field to a nullable field, the kmsg package must change from a
non-pointer to a pointer. This is a breaking change. Even more rarely,
sometimes Kafka changes a field's type by renaming its old version to
"SomethingOld" and having a new version take the place of "Something". I would
prefer the kmsg avoid "SomethingNew" to avoid deprecating the old field, so I
also choose to rename fields.

One option is to have absolutely everything be a pointer in the kmsg package,
but this would create a lot of garbage and make using the API even worse. As
well, I could just add newly named fields, but this makes things worse as time
goes on, because the newer fields would be the ones with the worse names.

In Go, if kmsg were stabilized, any protocol change would need to be a major
version bump. We cannot get away with minor bumps and a big doc comment saying
"deal with it", because we run into a transitive dependency issue: if user A
uses franz-go at v1.1.0, and they also use a library that uses franz-go at
v1.0.0, and there is an incompatible kmsg change, then the user's compilation
breaks.

So, we choose to have a dedicated module for kmsg which is unversioned. This
avoids some transitive dependency issues.

If you use the kmsg package directly, the only update you should need to do is:

```
go get -u github.com/twmb/franz-go/pkg/kmsg@latest
```

### Other potentially notable changes

- [`f82e5c6`](https://github.com/twmb/franz-go/commit/f82e5c6) consumer: document that a nil / closed context is supported for polling
- [`8770662`](https://github.com/twmb/franz-go/commit/8770662) very minor bugfix for producing: only opt in to more than 1req if idempotent
- [`5c0a2c7`](https://github.com/twmb/franz-go/commit/5c0a2c7) bench: add -prometheus flag

v0.8.7
===

This release contains commits for upcoming Kafka protocol changes, new features
to gain insight into buffered records (and intercept & modify them before being
produced or consumed), a few minor other changes, and one minor breaking
change.

The minor breaking change should not affect anybody due to its rather niche use
when better methods exist, but if it does, the fix is to delete the
`Partitions` field that was removed (the field is now embedded).

This field was removed just for ease of use purposes: there was no reason to
have a separate named field in `FetchTopicPartition` for the `Partition`,
instead, we can just embed `FetchPartition` to make usage of the type much
simpler. Dropping this field basically makes this type much more appealing to
use.

I may be releasing v0.9.0 shortly, with plans to potentially split the kmsg
package into a dedicated module. More details on that if this happens.

### Breaking change

- [`ffc94ea`](https://github.com/twmb/franz-go/commit/ffc94ea) **minor breaking change**: embed `FetchPartition` in `FetchTopicPartition` rather than have a named `Partition` field

### Features

- [`1bb70a5`](https://github.com/twmb/franz-go/commit/1bb70a5) kprom: clarify seed ids, add two new metrics
- [`916fc10`](https://github.com/twmb/franz-go/commit/916fc10) client: add four new hooks to provide buffer/unbuffer interceptors
- [`1e74109`](https://github.com/twmb/franz-go/commit/1e74109) hooks: add HookNewClient
- [`3256518`](https://github.com/twmb/franz-go/commit/3256518) client: add Buffered{Produce,Fetch}Records methods
- [`ebf2f07`](https://github.com/twmb/franz-go/commit/ebf2f07) support KIP-516 for Fetch (topic IDs in fetch requests)
- [`e5e37fc`](https://github.com/twmb/franz-go/commit/e5e37fc) and [`3a3cc06`](https://github.com/twmb/franz-go/commit/3a3cc06) support KIP-709: batched OffsetFetchRequest in a forward-compatible way (and minor group sharded request redux)
- [`eaf9ebe`](https://github.com/twmb/franz-go/commit/eaf9ebe) kprom: use Registerer and Gatherer interface instead of concrete registry type.
- [`10e3f44`](https://github.com/twmb/franz-go/commit/10e3f44) and [`4fdc7e0`](https://github.com/twmb/franz-go/commit/4fdc7e0) support KIP-699 (batch FindCoordinator requests)

### Minor other changes

- [`d9b4fbe`](https://github.com/twmb/franz-go/commit/d9b4fbe) consumer group: add "group" to all log messages
- [`f70db15`](https://github.com/twmb/franz-go/commit/f70db15) broker: use ErrClientClosed more properly for internal requests that use the client context
- [`8f1e732`](https://github.com/twmb/franz-go/commit/8f1e732) producer: use ErrClientClosed properly if waiting to produce when the client is closed
- [`ee918d9`](https://github.com/twmb/franz-go/commit/ee918d9) sink: drop the drain loop sleep from 5ms to 50microsec
- [`e48c03c`](https://github.com/twmb/franz-go/commit/e48c03c) client: log "seed #" rather than a large negative for seed brokers

v0.8.6
===


- [`7ec0a45`](https://github.com/twmb/franz-go/commit/7ec0a45) **bugfix** consumer group: bugfix regex consuming (from v0.8.0+)
- [`91fe77b`](https://github.com/twmb/franz-go/commit/91fe77b) bench: faster record value generation, add -static-record flag
- [`f95859e`](https://github.com/twmb/franz-go/commit/f95859e) generated code: always return typed value if present
- [`db5fca5`](https://github.com/twmb/franz-go/commit/db5fca5) support KIP-734
- [`ae7182b`](https://github.com/twmb/franz-go/commit/ae7182b) group balancing: address KAFKA-12898

This small tag fixes a bug in group consuming via regex, adds a new
`-static-record` flag to the benchmark example, and has a few other very minor
changes (see commits).

Also, thank you @Neal for fixing some typos and eliminating a useless goroutine
in the benchmarks!

v0.8.5
===

- [`2732fb8`](https://github.com/twmb/franz-go/commit/2732fb8) **bugfix** consumer: bugfix consuming of specific partitions if no topics are specified

If `ConsumePartitions` was used without `ConsumeTopics`, the consumer would not
start. This was missed in the conversion of starting the consumer after
initializing a client to initializing the consumer _with_ the client.

v0.8.4
===

- [`e0346a2`](https://github.com/twmb/franz-go/commit/e0346a2) **very minor bugfix** consumer group: set defaultCommitCallback
- [`5047b31`](https://github.com/twmb/franz-go/commit/5047b31) and [`05a6b8a`](https://github.com/twmb/franz-go/commit/05a6b8a) bench: add -linger, -disable-idempotency, -log-level, -compression, -batch-max-bytes options

This is a small release containing a very minor bugfix and more flags on the
benchmark program. The bugfix basically re-adds a missing commit callback if
you are not specifying your own, and the default commit callback just logs
errors. So in short, this release ensures you see errors if there are any and
you are using the default commit callback.

v0.8.3
===

- [`053911b`](https://github.com/twmb/franz-go/commit/053911b) plugins: fix module declarations

This is a patch release on the prior commit to fix the path declarations in the
new plugin modules.

v0.8.2
===

- [`65a0ed1`](https://github.com/twmb/franz-go/commit/65a0ed1) add pluggable kgmetrics, kprom, kzap packages & examples

This release immediately follows the prior release so that the plugin packages
can refer to v0.8.1, which contains the (hopefully) final API breakages. This
allows Go's version resolution to ensure users of these plugins use the latest
franz-go package.

These new plugins should make it very easy to integrate prometheus or
go-metrics or zap with kgo.

v0.8.1
===

This release contains a few features, one minor bugfix, two minor breaking
API changes, and a few minor behavior changes.

This release will immediately be followed with a v0.8.2 release that contains
new examples using our new hooks added in this release.

### Breakage

One of the breaking API changes is followup from the prior release: all
broker-specific hooks were meant to be renamed to `OnBrokerXyz`, but I missed
`OnThrottle`. This is a rather niche hook so ideally this does not break
many/any users.

The other breaking API change is to make an API consistent: we have `Produce`
and `ProduceSync`, and now we have `CommitRecords` and
`CommitUncommittedOffsets` in addition to the original `CommitOffsets`, so
`BlockingCommitOffsets` should be renamed to `CommitOffsetsSync`. This has the
added benefit that now all committing functions will be next to each other in
the documentation. One benefit with breaking this API is that it may help
notify users of the much more easily used `CommitUncommittedOffsets`.

### Features

- Two new partitioners have been added, one of which allows you to set the
  `Partition` field on a record before producing and have the client use that
field for partitioning.

- Two new hooks have been added that allow tracking some metrics per-batch
  while producing and consuming. More per-batch metrics can be added to the
hooks later if necessary, because the hooks take a struct that can be expanded
upon.

- The client now returns an injected `ErrClientClosed` fetch partition when
  polling if the client has been closed, and `Fetches` now contains a helper
`IsClientClosed` function. This can be used to break out of a poll loop on
shutdown.

### Behavior changes

- The client will no longer add empty fetches to be polled. If fetch sessions
  are disabled, or in certain other cases, Kafka replies to a fetch requests
with the requested topics and partitions, but no records. The client would
process these partitions and add them to be polled. Now, the client will avoid
adding empty fetches (unless they contain an error), meaning polling should
always have fetches that contain either records or an error.

- When using sharded requests, the client no longer issues split pieces of the
  requests to partitions that are currently erroring. Previously, if a request
needed to go to the partition leader, but the leader was offline, the client
would choose a random broker to send the request to. The request was expected
to fail, but the failure error would be retriable, at which point we would
reload metadata and hope the initial partition leader error would be resolved.
We now just avoid this try-and-fail-and-hope loop, instead erroring the split
piece immediately.

### Examples

This contains one more example, [`examples/group_consuming`](./examples/group_consuming),
which demonstrates how to consume as a group and commit in three different ways,
and describes the downsides of autocommitting.

### Changes

- [`fa1fd35`](https://github.com/twmb/franz-go/commit/fa1fd35) **feature** consuming: add HookFetchBatchRead
- [`9810427`](https://github.com/twmb/franz-go/commit/9810427) **feature** producing: add HookProduceBatchWritten
- [`20e5912`](https://github.com/twmb/franz-go/commit/20e5912) **breaking api** hook: rename OnThrottle => OnBrokerThrottle
- [`a1d7506`](https://github.com/twmb/franz-go/commit/a1d7506) examples: add group consumer example, with three different commit styles
- [`058f692`](https://github.com/twmb/franz-go/commit/058f692) behavior change, consuming: only add fetch if it has records or errors
- [`d9649df`](https://github.com/twmb/franz-go/commit/d9649df) **feature** fetches: add IsClientClosed helper
- [`bc0add3`](https://github.com/twmb/franz-go/commit/bc0add3) consumer: inject ErrClientClosing when polling if the client is closed
- [`f50b320`](https://github.com/twmb/franz-go/commit/f50b320) client: make public ErrClientClosed
- [`8b7b43e`](https://github.com/twmb/franz-go/commit/8b7b43e) behavior change, client: avoid issuing requests to shards that we know are erroring
- [`96cb1c2`](https://github.com/twmb/franz-go/commit/96cb1c2) **bugfix** fix ACLResourcePatternType: add ANY
- [`8cf3e5a`](https://github.com/twmb/franz-go/commit/8cf3e5a) **breaking api** rename BlockingCommitOffsets to CommitOffsetsSync
- [`2092b4c`](https://github.com/twmb/franz-go/commit/2092b4c) and [`922f4b8`](https://github.com/twmb/franz-go/commit/922f4b8) **feature** add CommitRecords and CommitUncommittedOffsets
- [`6808a55`](https://github.com/twmb/franz-go/commit/6808a55) **feature** add BasicConsistentPartitioner / ManualPartitioner

v0.8.0
===

This is a **major breaking release**. This release is intended to be a release
candidate for a v1.0.0 tag; thus, I wanted to nail down all API breakage now to
help prevent future breakage and to ensure a path towards 1.0 stabilization. I
do not expect major API changes after this tag, and I intend to release v1.0.0
within a month of this tag.

## Why the breakage?

It never felt "right" that to consume, you needed to first create a client, and
then assign something to consume. One large reason that `AssignXyz` existed was
so that you could reassign what was being consumed at runtime. In fact, you
could consume from a group, then leave and switch to direct partition
consuming, and then switch back to consuming from a different group. This
flexibility was unnecessary, and assigning after the client was initialized was
awkward. Worse, allowing these reassignments necessitated extreme care to not
have race conditions or deadlocks. This was the source of many bugs in the
past, and while they have since been ironed out, we may as well just remove
them as a possibility while on the path towards v1.0.

Because we have this one major breakage already, I decided it was a good time
to clean up the other not-so-great aspects of the code.

- All current hooks have been renamed, because `OnE2E` being dedicated to
  brokers does not leave room for a different `E2E` hook in the future that is
not specific to brokers. Instead, if we namespace the current broker hooks, we
can namespace future hooks as well, and nothing will look weird. More
beneficially, in user code, the hook will be more self describing.

- One now redundant option has been removed (`BrokerConnDeadRetries`), and one
  useless option has been removed (`DisableClientID`).

Moving consumer options to the client required changing the names of some
consumer options, merging some duplicated group / direct options, adding
`*Client` as an argument to some callbacks, and cleaning up some consume APIs.

From there on, the breakages get more minor: `AutoTopicCreation` now is
`AllowAutoTopicCreation`, and `ProduceTopic` is now `DefaultPRoduceTopic`.

## Upgrade considerations

Due to the number of breaking changes, upgrading may _look_ more difficult than
it actually is. I've updated every example in this repo and all code usage
in my corresponding [`kcl`](github.com/twmb/kcl) repo, these updates were
completed relatively quickly.

## tl;dr of upgrading

The simpler fixes:

- Change `AutoTopicCreate` to `AllowAutoTopicCreate`
- Change `ProduceTopic` to `DefaultProduceTopic`
- Remove `BrokerConnDeadRetries` and `DisableClientID` if you used them (unlikely)
- Add `Broker` in any hook (`OnConnect` => `OnBrokerConnect`, etc)

If directly consuming, perform the following changes to options and move the options to `NewClient`:

- Drop the offset argument from `ConsumeTopics`
- Move `ConsumePartitions`
- Change `ConsumeTopicsRegex` to `ConsumeRegex`
- Delete `AssignPartitions`

If group consuming, perform the following changes to options and move the options to `NewClient`:

- Add `ConsumerGroup` with the group argument you used in `AssignGroup`
- Change `GroupTopics` to `ConsumeTopics`
- Add a `*Client` argument to any of `OnAssigned`, `OnRevoked`, `OnLost`, and `CommitCallback`

If using a group transaction session, perform the above group changes, and use `NewGroupTransactSession`, rather than `NewClient`.

## Changes

- [`6a048db`](https://github.com/twmb/franz-go/commit/6a048db) **breaking API** hooks: namespace all hooks with Broker
- [`8498383`](https://github.com/twmb/franz-go/commit/8498383) **breaking API** client: large breaking change for consuming APIs
- [`45004f8`](https://github.com/twmb/franz-go/commit/45004f8) **breaking API** config: rename ProduceTopic to DefaultProduceTopic, doc changes
- [`aa849a1`](https://github.com/twmb/franz-go/commit/aa849a1) **breaking API** options: prefix AutoTopicCreation with Allow
- [`be6adf5`](https://github.com/twmb/franz-go/commit/be6adf5) **breaking API** client: remove DisableClientID option
- [`68b1a04`](https://github.com/twmb/franz-go/commit/68b1a04) **breaking API** client: remove BrokerConnDeadRetries option; drop retries to 20
- [`88e131d`](https://github.com/twmb/franz-go/commit/88e131d) **bugfix** kerberos: fix off-by-one in asn1LengthBytes (but it appears this is still not fully working)
- [`20e0f66`](https://github.com/twmb/franz-go/commit/20e0f66) **feature** Fetches: add EachError, clarifying documentation
- [`085ad30`](https://github.com/twmb/franz-go/commit/085ad30) metadata: limit retries, bump produce load errors on failure
- [`b26489f`](https://github.com/twmb/franz-go/commit/b26489f) config: change default non-produce retries from unlimited to 30 (later commit just above changes down to 20)

v0.7.9
===

- [`5231902`](https://github.com/twmb/franz-go/commit/5231902) **bugfix** patch on prior commit

If I could yank tags, I would.

v0.7.8
===

- [`b7cb533`](https://github.com/twmb/franz-go/commit/b7cb533) **bugfix** allow any `*os.SyscallError` to be retriable

_This_ should be the last v0.7 release. This is a small bugfix to allow much
more retrying of failing requests, particularly around failed dials, which is
much more resilient to restarting brokers.

v0.7.7
===

- [`afa1209`](https://github.com/twmb/franz-go/commit/afa1209) txn: detect a fatal txnal client when beginning transactions
- [`5576dce`](https://github.com/twmb/franz-go/commit/5576dce) benchmarks: add comparisons to confluent-kafka-go & sarama
- [`d848174`](https://github.com/twmb/franz-go/commit/d848174) examples: add benchmarking example
- [`fec2a18`](https://github.com/twmb/franz-go/commit/fec2a18) client: fix request buffer pool, add promisedNumberedRecord pool
- [`a0d712e`](https://github.com/twmb/franz-go/commit/a0d712e) transactions: small wording changes in docs
- [`bad47ba`](https://github.com/twmb/franz-go/commit/bad47ba) and [`a9691bd`](https://github.com/twmb/franz-go/commit/a9691bd) **feature** hooks: add HookBrokerE2E

This is a small release with one useful new hook, a few minor updates /
internal fixes, and no bug fixes.

This now properly pools request buffers, which will reduce garbage when
producing, and re-adds pooling slices that records are appended to before
flushing. This latter pool is less important, but can help.

This now adds one more chance to recover a transactional client, which also
gives the user a chance to things are fatally failed when beginning
transactions.

Finally, this adds a benchmarking example and comparisons to sarama /
confluent-kafka-go. To say the least, the numbers are favorable.

This is likely the last release of the v0.7 series, the next change will be a
few breaking API changes that should hopefully simplify initializing a
consumer.

v0.7.6
===

This is a small release that adds defaults for any `kmsg.Request` that has a
`TimeoutMillis` field that previously did not have a default.

This also changes how the `TimeoutMillis` field is specified for generating,
and now all documentation around it is consistent.

Lastly, any field that has a default now has that default documented.

v0.7.5
===

This commit adds support for session tokens and user agents in `AWS_MSK_IAM`,
as well as adds an [example](./examples/sasl/aws_msk_iam) for how to use
`AWS_MSK_IAM`.

v0.7.4
===

- [`467e739`](https://github.com/twmb/franz-go/commit/467e739) FirstErrPromise: change semantics

This is a small release to change the semantics of `FirstErrPromise`: now, it
uses a `sync.WaitGroup` on every `Promise` and will wait until all records have
been published / aborted before returning from `Err`. This also adds an
`AbortingFirstErrPromise` that automatically aborts all records if any promise
finishes with an error.

v0.7.3
===

- [`30c4ba3`](https://github.com/twmb/franz-go/commit/30c4ba3) **feature** sasl: add support for `AWS_MSK_IAM`

This is a small release dedicated to adding support for `AWS_MSK_IAM` sasl.

v0.7.2
===

- [`522c9e2`](https://github.com/twmb/franz-go/commit/522c9e2) **bugfix** consumer group: use `JoinGroupResponse.Protocol`, not `SyncGroupResponse.Protocol`

This is a small bugfix release; the `Protocol` field in `SyncGroupResponse` was
added in 2.5.0, and my integration tests did not catch this because I usually
test against the latest releases. All `JoinGroupResponse` versions have the
protocol that was chosen, so we use that field.

v0.7.1
===

- [`98f74d1`](https://github.com/twmb/franz-go/commit/98f74d1) README: note that we actually do support KIP-533
- [`528f007`](https://github.com/twmb/franz-go/commit/528f007) and [`ef9a16a`](https://github.com/twmb/franz-go/commit/ef9a16a) and [`d0cc729`](https://github.com/twmb/franz-go/commit/d0cc729) **bugfix** client: avoid caching invalid coordinators; allow retries

This is a small bugfix release: previously, if FindCoordinator returned a node
ID of -1, we would permanently cache that. -1 is returned when the load has an
error, which we were not checking, but even still, we should not cache the
response if we do not know of the broker.

Now, we will only cache successful loads.

(Also, I noticed that I _do_ have an existing configuration knob for the retry
timeout, so that is now "Supported" in the KIP section, making all KIPs
supported).

v0.7.0
===

This is a big release, and it warrants finally switching to 0.7.0. There are a
few small breaking changes in this release, a lot of new features, a good few
bug fixes, and some other general changes. The documentation has been
overhauled, and there now exists an example of hooking in to prometheus metrics
as well as an example for EOS.

Most of the new features are quality of life improvements; I recommend taking a
look at them to see where they might simplify your code.

## Upgrade considerations

Besides the two breaking changes just below, one bug fix may affect how you if
you are using a group consumer with autocommitting. If autocommitting, the
consumer should always have been issuing a blocking commit when leaving the
group to commit the final consumed records. A bug existed such that this commit
would never actually be issued. That bug has been fixed, so now, if you rely on
autocommitting, the client closing may take a bit longer due to the blocking
commit.

## Breaking changes

There are two breaking changes in this release, one of which likely will go
unnoticed. First, to allow all hooks to display next to each other in godoc,
the interfaces have had their trailing "Hook" moved to a leading "Hook".
Second and definitely noticeable, record producing no longer returns an error.
The original error return was not really that intuitive and could lead to bugs
if not understood entirely, so it is much simpler to just always call the
promise.

- [`215f76f`](https://github.com/twmb/franz-go/commit/215f76f) small breaking API: prefix hook interfaces with "Hook"
- [`c045366`](https://github.com/twmb/franz-go/commit/c045366) producer: drop error return from Produce (breaking API change)

## Features

- [`c83d5ba`](https://github.com/twmb/franz-go/commit/c83d5ba) generate: support parsing and encoding unknown tags
- [`6a9eb0b`](https://github.com/twmb/franz-go/commit/6a9eb0b) kmsg: add Tags opaque type; ReadTags helper
- [`9de3959`](https://github.com/twmb/franz-go/commit/9de3959) add support for KIP-568 (force rebalance)
- [`d38ac84`](https://github.com/twmb/franz-go/commit/d38ac84) add HookGroupManageError
- [`0bfa547`](https://github.com/twmb/franz-go/commit/0bfa547) consumer group: add CommitCallback option
- [`231d0e4`](https://github.com/twmb/franz-go/commit/231d0e4) fetches: add EachRecord
- [`aea185e`](https://github.com/twmb/franz-go/commit/aea185e) add FirstErrPromise
- [`780d168`](https://github.com/twmb/franz-go/commit/780d168) Record: add helper constructors; allocation avoiders w/ unsafe
- [`55be413`](https://github.com/twmb/franz-go/commit/55be413) producer feature: allow a default Topic to produce to
- [`e05002b`](https://github.com/twmb/franz-go/commit/e05002b) and [`1b69836`](https://github.com/twmb/franz-go/commit/1b69836) consumer group: export APIs allow custom balancers
- [`6db1c39`](https://github.com/twmb/franz-go/commit/6db1c39) Fetches: add EachTopic helper
- [`b983d63`](https://github.com/twmb/franz-go/commit/b983d6), [`7c9f591`](https://github.com/twmb/franz-go/commit/7c9f59), [`3ad8fc7`](https://github.com/twmb/franz-go/commit/3ad8fc), and [`3ad8fc7`](https://github.com/twmb/franz-go/commit/3ad8fc7) producer: add ProduceSync

## Bug fixes

- [`45cb9df`](https://github.com/twmb/franz-go/commit/45cb9df) consumer: fix SetOffsets bug
- [`46cfcb7`](https://github.com/twmb/franz-go/commit/46cfcb7) group: fix blocking commit on leave; potential deadlock
- [`1aaa1ef`](https://github.com/twmb/franz-go/commit/1aaa1ef) consumer: fix data race in handleListOrEpochResults
- [`d1341ae`](https://github.com/twmb/franz-go/commit/d1341ae) sticky: fix extreme edge case for complex balancing
- [`9ada82d`](https://github.com/twmb/franz-go/commit/9ada82d) sink: create producerID *BEFORE* produce request (partial revert of dc44d10b)
- [`5475f6b`](https://github.com/twmb/franz-go/commit/5475f6b) sink: bugfix firstTimestamp
- [`2c473c4`](https://github.com/twmb/franz-go/commit/2c473c4) client: add OffsetDeleteRequest to handleCoordinatorReq
- [`bf5b74c`](https://github.com/twmb/franz-go/commit/bf5b74c) and 3ad8fc7 broker: avoid reaping produce cxn on no reads when acks == 0
- [`43a0009`](https://github.com/twmb/franz-go/commit/43a0009) sink w/ no acks: debug log needs to be before finishBatch
- [`8cf9eb9`](https://github.com/twmb/franz-go/commit/8cf9eb9) sink: bugfix panic on acks=0

## General

- [`939cba2`](https://github.com/twmb/franz-go/commit/939cba2) txns: make even safer (& drop default txn timeout to 40s)
- [`faaecd2`](https://github.com/twmb/franz-go/commit/faaecd2) implement KIP-735 (bump session timeout to 45s)
- [`3a95ec8`](https://github.com/twmb/franz-go/commit/3a95ec8) GroupTxnSession.End: document no error is worth retrying
- [`c5a47ea`](https://github.com/twmb/franz-go/commit/c5a47ea) GroupTransactSession.End: retry with abort on OperationNotAttempted
- [`6398677`](https://github.com/twmb/franz-go/commit/6398677) EndTxn: avoid creating a producer ID if nothing was produced
- [`c7c08fb`](https://github.com/twmb/franz-go/commit/c7c08fb) txnal producer: work around KAFKA-12671, take 2
- [`9585e1d`](https://github.com/twmb/franz-go/commit/9585e1d) FetchesRecordIter: avoid mutating input fetches

v0.6.14
===

- [`dc44d10`](https://github.com/twmb/franz-go/commit/dc44d10) sink: call producerID after creating the produce request
- [`ce113d5`](https://github.com/twmb/franz-go/commit/ce113d5) **bugfix** producer: fix potential lingering recBuf issue
- [`19d57dc`](https://github.com/twmb/franz-go/commit/19d57dc) **bugfix** metadata: do not nil cursors/records pointers ever
- [`e324b56`](https://github.com/twmb/franz-go/commit/e324b56) producing: evaluate whether a batch should fail before and after

This is a small bugfix release for v0.6.13, which would panic if a user was
producing to and consuming from the same topic within a single client.

At the same time, there was a highly-unlikely-to-be-experienced bug where
orphaned recBuf pointers could linger in a sink through a certain sequence
of events (see the producer bugfix commit for more details).

This also now avoids initializing a producer ID when consuming only. For code
terseness during the producer redux, I moved the createReq portion in sink to
below the producerID call. We actually call createReq when there are no records
ever produced: a metadata update adding a recBuf to a sink triggers a drain,
which then evaluates the recBuf and sees there is nothing to produce. This
triggered drain was initializing a producer ID unnecessesarily. We now create
the request and see if there is anything to flush before initializing the
producer ID, and we now document the reason for having producerID after
createReq so that I do not switch the order in the future again.

Lastly, as a feature enhancement, this unifies the logic that fails buffered
records before producing or after producing. The context can now be used to
cancel records after producing, and record timeouts / retries can be evaluated
before producing. The order of evaluation is first the context, then the record
timeout, and lastly the number of tries.

v0.6.13
===

- [`6ae76d0`](https://github.com/twmb/franz-go/commit/6ae76d0): **feature** producer: allow using the passed in context to cancel records
- [`2b9b8ca`](https://github.com/twmb/franz-go/commit/2b9b8ca): **bugfix** sink: reset needSeqReset
- [`314de8e`](https://github.com/twmb/franz-go/commit/314de8e): **feature** producer: allow records to fail when idempotency is enabled
- [`bf956f4`](https://github.com/twmb/franz-go/commit/bf956f4): **feature** producer: delete knowledge of a topic if we never load it
- [`83ecd8a`](https://github.com/twmb/franz-go/commit/83ecd8a): **bugfix** producer unknown wait: retry on retriable errors
- [`a3c2a5c`](https://github.com/twmb/franz-go/commit/a3c2a5c): consumer: backoff when list offsets or load epoch has any err (avoid spinloop)
- [`9c27589`](https://github.com/twmb/franz-go/commit/9c27589): zstd compression: add more options (switch to minimal memory usage)
- [`0554ad5`](https://github.com/twmb/franz-go/commit/0554ad5): producer: retry logic changes

This release focuses on restructing some code to allow deleting topics from the
client and, most importantly, to allow records to fail even with idempotency.

For deleting records, this is a minimal gain that will really only benefit a
user reassigning consuming topics, or for producers to a topic that does not
exist. Still, nifty, and for an EOS user, produce and consume topics will no
longer be next to each other (which may make scanning partitions to produce or
consume quicker).

For allowing records to have limited retries / be canceled with a context, this
is an important feature that gives users more control of aborting the work they
tried. As noted in the config docs, we cannot always safely abort records, but
for the most part, we can. The only real unsafe time is when we have written a
produce request but have not received a response. If we ever get a response for
the first batch in a record buffer (or if we do not write the batch ever), then
we can safely fail the records. We do not need to worry about the other
concurrent requests because if the first batch errors, the others will error
and we know we can fail records based off the first's status, and if the first
batch does not error, we will just finish the records (and not error them).

v0.6.12
===

This is a small release corresponding with Kafka's 2.8.0 release.
A few small things were fixed when trying out `kcl misc probe-versions`
with the new code, and with that, we are able to determine exactly
where to cut 2.8.0 from tip in the kversion package.

Lastly, this also converts topic IDs that are introduced in this release
from `[2]uint64` to `[16]byte`. A raw blob of bytes is easier to reason
about and affords us avoiding worrying about endianness.

v0.6.11
===
- [`46138f7`](https://github.com/twmb/franz-go/commit/46138f7): **feature** client: add ConnIdleTimeout option && connection reaping (further fix to this in later commit)
- [`26c1ea2`](https://github.com/twmb/franz-go/commit/26c1ea2): generate: return "Unknown" for unknown NameForKey
- [`557e15f`](https://github.com/twmb/franz-go/commit/557e15f): **module change** update module deps; bump lz4 to v4. This dep update will require go-getting the new v4 module.
- [`10b743e`](https://github.com/twmb/franz-go/commit/10b743e): producer: work around KAFKA-12671
- [`89bee85`](https://github.com/twmb/franz-go/commit/89bee85): **bugfix** consumer: fix potential slowReloads problem
- [`111f922`](https://github.com/twmb/franz-go/commit/111f922): consumer: avoid bubbling up retriable broker errors
- [`fea3195`](https://github.com/twmb/franz-go/commit/fea3195): **bugfix** client: fix fetchBrokerMetadata
- [`6b64728`](https://github.com/twmb/franz-go/commit/6b64728): **breaking API**: error redux -- this makes private many named errors; realistically this is a minor breaking change
- [`b2a0578`](https://github.com/twmb/franz-go/commit/b2a0578): sink: allow concurrent produces (this was lost in a prior release; we now again allow 5 concurrent produce requests per broker!)
- [`ebc8ee2`](https://github.com/twmb/franz-go/commit/ebc8ee2): **bugfix / improvements** producer: guts overhaul, fixing sequence number issues, and allowing aborting records when aborting transactions
- [`39caca6`](https://github.com/twmb/franz-go/commit/39caca6): Poll{Records,Fetches}: quit if client is closed
- [`56b8308`](https://github.com/twmb/franz-go/commit/56b8308): **bugfix** client: fix loadCoordinators
- [`d4fe91d`](https://github.com/twmb/franz-go/commit/d4fe91d): **bugfix** source: more properly ignore truncated partitions
- [`71c6109`](https://github.com/twmb/franz-go/commit/71c6109): **bugfix** consumer group: map needs to be one block lower
- [`009e1ba`](https://github.com/twmb/franz-go/commit/009e1ba): consumer: retry reloading offsets on non-retriable errors
- [`6318b15`](https://github.com/twmb/franz-go/commit/6318b15): **bugfix** group consumer: revoke all on LeaveGroup, properly blocking commit
- [`b876c09`](https://github.com/twmb/franz-go/commit/b876c09): **bugfix** kversion: KIP-392 landed in Kafka 2.3, not 2.2
- [`1c5af12`](https://github.com/twmb/franz-go/commit/1c5af12): kversion: support guessing with skipped keys, guessing raft

This is an important release with multiple bug fixes. All important commits
are noted above; less important ones are elided.

The producer code has been overhauled to more accurately manage sequence
numbers. Particularly, we cannot just reset sequence numbers whenever. A
sequence number reset is a really big deal, and so we should avoid it at all
costs unless absolutely necessary.

By more accurately tracking sequence numbers, we are able to abort buffered
records when aborting a transact session. The simple approach we take here is
to reset the producer ID after the abort is completed.

The most major consumer bug was one that incorrectly tracked uncommitted
offsets when multiple topics were consumed in a single group member. The other
bugs would be infrequently hit through some more niche edge cases / timing
issues.

The lz4 module was updated, which will require a go mod edit. This was done
because major-version-bumping is pretty undiscoverable with mod updates, and it
turns out I had always used v1 instead of v4.

Sometime in the past, a broker/producer bugfix accidentally lost the ability to
send 5 concurrent produce request at once. That ability is readded in this
release, but this can occasionally trigger a subtle problem within Kafka while
transactionally producing. For more details, see [KAFKA-12671](https://issues.apache.org/jira/browse/KAFKA-12671).
The client attempts to work around this problem by sleeping for 1s if it detects
the problem has a potential of happening when aborting buffered records. Realistically,
this problem should not happen, but the one way to easily detect it is to look
for a stuck LastStableOffset.

A `ConnIdleTimeout` option was added that allows for reaping idle connections.
The logic takes a relatively easy approach, meaning idle connections may take
up to 2x the idle timeout to be noticed, but that should not be problematic.
This will help prevent the problem of Kafka closing connections and the client
only noticing when it goes to eventually write on the connection again. This
will also help avoid sockets in the kernel being stuck in `CLOSE_WAIT`.

Finally, after being inspired from [KAFKA-12675](https://issues.apache.org/jira/browse/KAFKA-12671),
I have significantly optimized the sticky assignor, as well as added other
improvements for the cooperative adjusting of group balancer plans.

v0.6.10
===

- [`04f8e12`](https://github.com/twmb/franz-go/commit/04f8e12): update debug logging (broker and consumer group)
- [`62e2e24`](https://github.com/twmb/franz-go/commit/62e2e24): add definitions & code for control record keys and values
- [`e168855`](https://github.com/twmb/franz-go/commit/e168855): **bugfix** sticky: fix and drastically simplify isComplex detection
- [`bd5d5ad`](https://github.com/twmb/franz-go/commit/bd5d5ad): **bugfix** consumer: properly shut down manageFetchConcurrency
- [`a670bc7`](https://github.com/twmb/franz-go/commit/a670bc7): group balancer: debug => info logging; handle join better
- [`d74bbc3`](https://github.com/twmb/franz-go/commit/d74bbc3): **bugfix** group: avoid concurrently setting err; use better log message
- [`ca32e19`](https://github.com/twmb/franz-go/commit/ca32e19): **feature** consumer: add PollRecords to poll a limited number of records
- [`2ce2def`](https://github.com/twmb/franz-go/commit/2ce2def): txn: change what we set offsets to on revoke / error
- [`582335d`](https://github.com/twmb/franz-go/commit/582335d): txn: add PollFetches and Produce to GroupTransactSession
- [`c8cd120`](https://github.com/twmb/franz-go/commit/c8cd120): txn: document that canceling contexts is not recommended
- [`bef2311`](https://github.com/twmb/franz-go/commit/bef2311): enforce KIP-98 idempotency config rules: acks=all, retries > 0
- [`2dc7d3f`](https://github.com/twmb/franz-go/commit/2dc7d3f): **bugfix** client.EndTransaction: if offsets were added to transaction, ensure we commit
- [`938651e`](https://github.com/twmb/franz-go/commit/938651e): **bugfix** consumer & consumer group: small redux, bug fixes
- [`b531098`](https://github.com/twmb/franz-go/commit/b531098): **feature** add support for ListTransactions, minor DescribeTransactions change
- [`3e6bcc3`](https://github.com/twmb/franz-go/commit/3e6bcc3): **feature** client: add option to disable idempotent writes
- [`5f50891`](https://github.com/twmb/franz-go/commit/5f50891): **feature**: kgo.Fetches: add EachPartition callback iterator
- [`05de8e6`](https://github.com/twmb/franz-go/commit/05de8e6): **bugfix** use proper keyvals for join group error
- [`8178f2c`](https://github.com/twmb/franz-go/commit/8178f2c): group assigning: add debug logs for balancing plans
- [`e038916`](https://github.com/twmb/franz-go/commit/e038916): **bugfix** range balancer: use correct members for div/rem
- [`e806126`](https://github.com/twmb/franz-go/commit/e806126): **bugfix** (minor) broker: shift sizeBuf 1 left by 8 for guessing tls version
- [`cd5e7fe`](https://github.com/twmb/franz-go/commit/cd5e7fe): client: rewrite `*kmsg.ProduceRequest`'s Acks, TimeoutMillis
- [`8fab998`](https://github.com/twmb/franz-go/commit/8fab998): kgo: use a discard goroutine when produce acks is 0

This is an important release that fixes various bugs in the consumer code and
adds a few nice-to-have features. Particularly, this includes a minor redux of
the consumer group code that audited for correctness, and includes a few
transactional consumer fixes.

There is a known problem in sequence numbers in the producer code that will be
hit if any consumer resets its sequence numbers. This happens if a consumer
hits a produce timeout or hits the retry limit, and may happen if
`AbortBufferedRecords` is called. A fix for this is being worked on now and
will be in the next release.

Most commits since v0.6.9 are noted above, minus a few that are left out for
being very minor documentation updates or something similar.

v0.6.9
===

- [`0ca274c`](https://github.com/twmb/franz-go/commit/0ca274c): **bugfix** consumer group: fix race where we did not wait for a revoke to finish if the group closed
- [`0cdc2b6`](https://github.com/twmb/franz-go/commit/0cdc2b6): **bugfix** consumer: process FetchResponse's RecordBatches correctly (versions after v0.11.0 can use message sets)
- [`bcb330b`](https://github.com/twmb/franz-go/commit/bcb330b): kerr: add TypedErrorForCode and four new errors
- [`31a9bbc`](https://github.com/twmb/franz-go/commit/31a9bbc): **bugfix** (that is not hit by general consumers): FetchResponse's DivergingEpoch.EndOffset is an int64, not int32

This release fixes some problems with consuming. As it turns out, message sets,
which are unused after v0.11.0, are indeed returned with higher FetchResponse
versions. Thus, we cannot rely on the FetchResponse version alone to decide
whether to decode as a message set or a record batch, instead we must check the
magic field.

The `DivergingEpoch.EndOffset` bug likely would not be noticed by general
consumers, as this is a field set by leaders to followers and is not useful to
nor set for clients. As well, it's only used for the not-yet-finalized raft work.

I noticed a race while looping through integration tests in `-race` mode, this
was an easy fix. Practically, the race can only be encountered when leaving a
group if your revoke is slow and the timing races exactly with the manage loop
also revoking. This would mean your onRevoke could be called twice. Now, that
will not happen. This bug itself is basically a reminder to go through and
audit the consumer group code, one of the last chunks of code I intend to
potentially redux internally.

v0.6.8
===

- [`8a42602`](https://github.com/twmb/franz-go/commit/8a42602): **bugfix** kversion: avoid panic on a key with value of max int16 (32767)

This is a small bugfix release; Confluent replies with odd keys sometimes,
turns out this time it was max int16. That showed that the parentheses was in
the wrong place for SetMaxKeyVersions.

This also has one more commit that change the String output a little bit to use
"Unknown" instead of an empty string on unknown keys.

v0.6.7
===

- [`2bea568`](https://github.com/twmb/franz-go/commit/2bea568): **bugfix** producer: fix producing with NoAck
- [`d1ecc7b`](https://github.com/twmb/franz-go/commit/d1ecc7b): On invalidly broker large reads, guess if the problem is a tls alert on a plaintext connection

This is a small bugfix release to fix producing with no acks. As it turns out,
Kafka truly sends _no_ response when producing with no acks. Previously, we
would produce, then hang until the read timeout, and then think the entire
request failed. We would retry in perpetuity until the request retry limit hit,
and then we would fail all records in a batch.

This fixes that by immediately finishing all promises for produce requests if
we wrote successfully.

v0.6.6
===

- [`21ddc56`](https://github.com/twmb/franz-go/commit/21ddc56): kgo: sort `RequestSharded` by broker metadata before returning
- [`4979b52`](https://github.com/twmb/franz-go/commit/4979b52): **bugfix** consumer: ensure we advance when consuming compacted topics

This is a small bugfix release to fix a stuck-consumer bug on compacted topics.

v0.6.5
===

- [`d5b9365`](https://github.com/twmb/franz-go/commit/d5b9365): configuration: clamp max partition bytes to max bytes to work around faulty providers
- [`c7dbafb`](https://github.com/twmb/franz-go/commit/c7dbafb): **bugfix** consumer: kill the session if the response is less than version 7
- [`f57fc76`](https://github.com/twmb/franz-go/commit/f57fc76): **bugfix** producer: handle ErrBrokerTooOld in doInitProducerID, allowing the producer to work for 0.10.0 through 0.11.0

This is a small release to ensure that we can make progress when producing to
Kafka clusters v0.10.0 through v0.11.0 (as well as that we can consume from
them if the consume has a partition error).

v0.6.4
===

- [`802bf74`](https://github.com/twmb/franz-go/commit/802bf74): **bugfix** kgo: fix three races
- [`1e5c11d`](https://github.com/twmb/franz-go/commit/1e5c11d): kgo: Favor non-seeds when selecting brokers for requests that go to a random broker
- [`4509d41`](https://github.com/twmb/franz-go/commit/4509d41): kgo: Add `AllowedConcurrentFetches` option to allow bounding the maximum possible memory consumed by the client
- [pr #22](https://github.com/twmb/franz-go/pull/22): Add transactional producer / consumer example (thanks @dcrodman!)
- [`6a041a8`](https://github.com/twmb/franz-go/commit/6a041a8): Add explicit Client.LeaveGroup method
- [`fe7d976`](https://github.com/twmb/franz-go/commit/fe7d976): KIP-500 / KIP-631: add support for admin-level request DecommissionBroker
- [`ab66776`](https://github.com/twmb/franz-go/commit/ab66776): KIP-500 / KIP-631: add support for broker-only requests BrokerRegistration and BrokerHeartbeat
- [`31b1df7`](https://github.com/twmb/franz-go/commit/31b1df7): KIP-664: add support for DescribeProducers
- [`368bb21`](https://github.com/twmb/franz-go/commit/368bb21): **breaking kmsg protocol changes**: add support for KIP-516 CreateTopics and DeleteTopics bumps
- [`b50282e`](https://github.com/twmb/franz-go/commit/b50282e): kerr: ReplicaNotAvailable is retriable; add UnknownTopicID
- [`c05572d`](https://github.com/twmb/franz-go/commit/c05572d): kmsg: change (Request|Response)ForKey to use proper NewPtr functions
- [`360a4dc`](https://github.com/twmb/franz-go/commit/360a4dc): client: actually use ConnTimeoutOverhead properly; bump base to 20s
- [`0ff08da`](https://github.com/twmb/franz-go/commit/0ff08da): Allow list offsets v0 to work (for Kafka v0.10.0 and before)
- [`59c935c` through `c7caea1`](https://github.com/twmb/franz-go/compare/59c935c..c7caea1): fix fetch session bugs
- [pr #4](https://github.com/twmb/franz-go/pull/4): Redesign readme (thanks @weeco!)

Of note, this fixes three races, fixes fetch session bugs and has small
breaking protocol changes in kmsg.

For the three races, two of them are obvious in hindsight, but one makes no
sense and may not belong in franz-go itself. The fix for the third is to remove
a line that we do not need to do anyway.

For fetch sessions, sessions were not reset properly in the face of context
cancelations or across consumer topic reassignments. It was possible for fetch
sessions to get out of sync and never recover. This is fixed by resetting fetch
sessions in all appropriate places.

For kmsg protocol changes, DeleteTopics changed Topics from a list of strings
(topics to delete) to a list of structs where you could delete by topic or by
topic uuid. The Java code coincidentally historically used TopicNames, so it
did not need to break the protocol to add this new struct under the Topics
name. For lack of confusion, we change Topics to TopicNames and introduce the
struct under Topics.

v0.6.3
===

- [`db09137`](https://github.com/twmb/franz-go/commit/db09137): Add support for new DescribeCluster API (KIP-700); followup commit (`ab5bdd3`) deprecates `Include<*>AuthorizedOperations` in the metadata request.
- [`3866e0c`](https://github.com/twmb/franz-go/commit/3866e0c): Set Record.Attrs properly before calling Produce callback hook.
- [`05346db`](https://github.com/twmb/franz-go/commit/05346db) and [`3d3787e`](https://github.com/twmb/franz-go/commit/3d3787e): kgo: breaking change for kversions breaking change two commits ago (see two lines below)
- [`b921c14`](https://github.com/twmb/franz-go/commit/b921c14): doc fix: v0.10.0 changed produce from MessageSet v0 to MessageSet v1
- [`33a8b26`](https://github.com/twmb/franz-go/commit/33a8b26): Breaking pointerification change to kversions.Versions; introduce VersionGuess

Two small breaking changes related to kversions.Versions in this; switching to
pointers now makes the struct future compatible; if it grows, we avoid the
concern of passing around a large struct on the stack.

Contains one small fix related to KIP-360 w.r.t. the idempotent producer: when
we encounter OutOfOrderSequenceNumber, we need to reset _all_ partition's
sequence numbers, not just the failing one. This is now done by resetting
everything the next time the producer ID is loaded. The related Kafka ticket is
KAFKA-12152.


v0.6.2
===

- [`9761889`](https://github.com/twmb/franz-go/commit/9761889): bump dependencies
- [`d07538d`](https://github.com/twmb/franz-go/commit/d07538d) Allow clients to cancel requests with the passed context; fix potential hanging requests
- [`303186a`](https://github.com/twmb/franz-go/commit/303186a) Add BrokerThrottleHook (thanks @akesle)
- [`63d3a60`](https://github.com/twmb/franz-go/commit/63d3a60) and previous commits: Use new & better kmsg.ThrottleResponse; this removes the usage of reflect to check for a ThrottleMillis field
- [`b68f112`](https://github.com/twmb/franz-go/commit/b68f112) kerr: add description to error string
- [`7cef63c`](https://github.com/twmb/franz-go/commit/7cef63c) config: strengthen validation
- [`faac3cc`](https://github.com/twmb/franz-go/commit/faac3cc) producing: fail large messages more correctly (bugfix)
- [`bc1f2d1`](https://github.com/twmb/franz-go/commit/bc1f2d1) and [`2493ae7`](https://github.com/twmb/franz-go/commit/2493ae7): kversion: switchup Versions API, finalize 2.7.0
- [`991e7f3`](https://github.com/twmb/franz-go/commit/991e7f3): Add support for KIP-630's new FetchSnapshot API
- [`de22e10`](https://github.com/twmb/franz-go/commit/de22e10): and [`9805d36`](https://github.com/twmb/franz-go/commit/9805d36) and [`c1d9ff4`](https://github.com/twmb/franz-go/commit/c1d9ff4): Add Uuid support and some definitions for KIP-516 (topic IDs)
- [`539b06c`](https://github.com/twmb/franz-go/commit/539b06c): Completely rewrite the consumer.go & source.go logic and fix sink & source broker pointer issues
- [`385cecb`](https://github.com/twmb/franz-go/commit/385cecb): Adds a warning hint if the connection dies on the first read without sasl
- [`71cda7b`](https://github.com/twmb/franz-go/commit/71cda7b): Bump another batch of requests to be flexible (per KAFKA-10729 / 85f94d50271)
- [`9d1238b8e`](https://github.com/twmb/franz-go/commit/9d1238b8ee28ad032e0bc9aa4891e0fbcdd27a63) and [`8a75a8091`](https://github.com/twmb/franz-go/commit/8a75a80914500a7f9c9e0edc4e5aefde327adf45): Fix misleading doc comments on DescribeGroupsRequest and FindCoordinatorResponse
- [`7f30228b8`](https://github.com/twmb/franz-go/commit/7f30228b87d4f883ba5c344048201dfb4d90336e): Adds FetchMinBytes option
- [`56ab90c72`](https://github.com/twmb/franz-go/commit/56ab90c7273be5ccc40dc38ff669b36995c505ce): Support EnvelopeRequest & DefaultPrincipalBuilder for KIP-590
- [`37d6868fc`](https://github.com/twmb/franz-go/commit/37d6868fc474813a6fb1e814b1a7fd87cc34d8ee): Add producer ID and epoch to the Record struct
- [`9467a951d`](https://github.com/twmb/franz-go/commit/9467a951d71e511c28180239336093225082896c): Add explicit sharded requests and retriable broker specific requests (followup commit settles API naming; later commits fix small problems in initial implementation)
- [`b94b15549`](https://github.com/twmb/franz-go/commit/b94b155497b4216808236aa393f06e7dccd772ed): Add kmsg constants for enums
- [`7ac5aaff8`](https://github.com/twmb/franz-go/commit/7ac5aaff8cea693e6a57533c8e18cca75c17ecc0): Add a MinVersions option
- [`3b06d558b`](https://github.com/twmb/franz-go/commit/3b06d558ba7453a191f434ce5654853322742859): Add a max broker read bytes option
- [`4c5cf8422`](https://github.com/twmb/franz-go/commit/4c5cf84223401eb1f5f6a78671dc04ff51407bd3): Account for throttling in responses

The primary commit of this release is `539b06c`, which completely rewrites the
consumer.go and source.go files to be more easily understandable. This commit
also fixes a bug related to the `sink` and `source` broker pointers; if the
broker changed but kept the same node ID, the `sink` and `source` would not
have their old pointers (to the old broker) updated.

There were some small breaking API changes in the `kmsg` package, most notably,
the metadata request changed one field from a `string` to a `*string` due to
Kafka switching the field from non-nullable to nullable. As specified in the
`kmsg` docs, API compatibility is not guaranteed due to Kafka being able to
switch things like this (and I favor small breakages like this rather than
wrapping every type into a struct with a `null` field).

The `kversion` package has had its `Versions` type completely changed. The new
format should be future compatible for new changes. The current change was
required due to Kafka's `ApiVersions` having a gap in keys that it responds
with in 2.7.0.

Config validation has been strengthened quite a bit.

Includes a few other minor, not too noteworthy changes.

v0.6.1
===

- [`0ddc468b2`](https://github.com/twmb/franz-go/commit/0ddc468b2ceca4fc0e42206c397f6d7ccd74bc4b): Workaround for two minor EventHubs issues.

This is a small release to patch around two minor EventHubs protocol serializing issues.

v0.6.0
===

- [`0024a93a4`](https://github.com/twmb/franz-go/commit/0024a93a4d386318869fe029260f3e23a602c03e): Fix bug introduced in pkg/kgo/source.go
- [`ee58bc706`](https://github.com/twmb/franz-go/commit/ee58bc7065e50a40207a730afc6dfc14b47b1da1): Avoid noise when closing the client if we were not consuming
- [`6e0b042e3`](https://github.com/twmb/franz-go/commit/6e0b042e3ad2281b82030375257f0c200bc511c3): Pin the client by default to the latest stable API versions

This release also changes the name to franz-go. After sitting on frang for a
week, ultimately I was missing the fact that the name related to Kafka. `franz`
is an easy enough mental map, and then this brings back the `-go` suffix.

v0.5.0
===

- [`9dc80fd29`](https://github.com/twmb/franz-go/commit/9dc80fd2978e8fb792f669ec6595cc5dadfc9fa5): Add hooks, which enable metrics to be layered in.
- [`a68d13bcb`](https://github.com/twmb/franz-go/commit/a68d13bcb4931870c9ca57abc085aa937b47373a): Set SoftwareName & SoftwareVersion on ApiVersions request if missing
- [`5fb0a3831`](https://github.com/twmb/franz-go/commit/5fb0a3831f56e444497131674b582113344704db): Fix SASL Authenticate
- [`85243c5c5`](https://github.com/twmb/franz-go/commit/85243c5c5aee1e23f6d360efdc82480fd83fcfc3): Breaking API change for enums in ACL & config managing request / response types
- [`4bc453bc2`](https://github.com/twmb/franz-go/commit/4bc453bc237bd534947cebdc5bc72f8bd7e855d9): Add NewPtr functions for generated structs in kmsg
- [`dcaf32daa`](https://github.com/twmb/franz-go/commit/dcaf32daa16fdc20e1c9cf91847b74342d13e36f): Generate RequestWith function for all requests in kmsg
- [`c27b41c3e`](https://github.com/twmb/franz-go/commit/c27b41c3e85df89651d9634cd024a2114f273cbd): Add Requestor interface to kmsg package
- [`2262f3a8f`](https://github.com/twmb/franz-go/commit/2262f3a8f90db2f459203b8ae665b089c19901d2): Add New functions for generated structs in kmsg
- [`7fc3a701e`](https://github.com/twmb/franz-go/commit/7fc3a701ec2155417cc02d2535510fcb5c9cdb6d): Add UpdateFeatures for 2.7.0 (KIP-584)
- [`f09690186`](https://github.com/twmb/franz-go/commit/f09690186ce2697f78626ba06e7ccfb586f1c7ee): Add shortcuts to get sasl.Mechanisms from sasl structs
- [PR #12](https://github.com/twmb/franz-go/pull/12): Add clarifying wording on NewClient (thanks @weeco!)
- [`9687df7ad`](https://github.com/twmb/franz-go/commit/9687df7ad71c552c61eea86376ebf3a9f6bff09e): Breaking API change for kgo.Dialer
- [`aca99bcf1`](https://github.com/twmb/franz-go/commit/aca99bcf19e741850378adbfe64c62b009340d7d): New APIs for KIP-497 (alter isr)
- [`a5f66899f`](https://github.com/twmb/franz-go/commit/a5f66899f6b492de37d689566d34869f50744717): New APIs for KIP-595 (raft)
- [`cd499f4ea`](https://github.com/twmb/franz-go/commit/cd499f4eaceaa0bd73452b5737c7713fe2b60ca9): Fetch request changes for KIP-595 (raft)
- [`7fd87da1c`](https://github.com/twmb/franz-go/commit/7fd87da1c8562943b095a54b5bc6258e2d2bdc6c): Support defaults in generated code (and a few commits after adding defaults)
- [`74c7f57e8`](https://github.com/twmb/franz-go/commit/74c7f57e81dd45033e9eec3bacbc4dca75fef83d): Support for KIP-584 (ApiVersions changes for min/max features)

Multiple generator changes were made to better handle tags, add support for
enums, and parse many files in a `definitions` directory. Minor changes were
made in kgo to use the new `RequestWith` function. More debug & warn log lines
were added around opening a connection and initializing sasl.

This release also changed the name of the repo from "kafka-go" to "frang" to
avoid potential trademark issues down the road.

v0.4.9
======

Typo fix for the prior release (in 2.7.0 APIs).

v0.4.8
======

- [`2e235164d`](https://github.com/twmb/franz-go/commit/2e235164daca64cafc715642582b13424e494e5c): Further prepare for KIP-588 (recover on txn timed out)
- [`60de0dafb`](https://github.com/twmb/franz-go/commit/60de0dafbeb7bf72996ad5f24690820962d2f584): Further audit for KIP-360 correctness (recoverable producer id)
- [`2eb27c653`](https://github.com/twmb/franz-go/commit/2eb27c653a4cdb815bf366894a2b87a3555ee50b): client.Request: shard DeleteRecords, {AlterReplica,Describe}LogDirs
- [`ebadfdae1`](https://github.com/twmb/franz-go/commit/ebadfdae1d975a5a937a1f1d67fd909b728c7386): Add support for KIP-554 (SCRAM APIs)
- [`4ea4c4297`](https://github.com/twmb/franz-go/commit/4ea4c4297e0402fcf37ef913f4a161203ff83dd4): Un-admin a lot of requests and fix OffsetDelete to be a group request

v0.4.7
======

- [`598261505`](https://github.com/twmb/franz-go/commit/598261505033d0255c37dc06b9b6c1112818a1be): Breaking API change in kmsg to add validating & erroring when reading records/messages

v0.4.6
======

- Introduces the CHANGELOG file
- [`ff1dc467e`](https://github.com/twmb/franz-go/commit/ff1dc467e32b6be41656c1f3bc57cb4d45e32a0c): Breaking API change in kmsg to support future non-breaking changes
- [`4eb594044`](https://github.com/twmb/franz-go/commit/4eb594044cfc611b75352530f7122c596b15764c): Correctly copies over Version/ThrottleMillis when merging ListGroups
