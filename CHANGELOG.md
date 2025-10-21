v1.20.1
===

This small patchfix release fixes a longstanding bug in `RequestCachedMetadata`,
which became a problem now that kadm is using it by default: if no metadata was
cached and you requested all topics, no metadata request would be issued and
you'd get no valid response. Thank you [@countableSet](https://github.com/countableSet)
for the find and fix. **All users of kadm v1.12 should bump their franz-go dep**.

This also adds the two new 1.20 config options to `OptValues`, and a big doc
comment hinting to add new config opts going forward.

- [`1087d3c7`](https://github.com/twmb/franz-go/commit/1087d3c7) kgo: add new opts to OptValues && big doc to do so going forward...
- [`cad283f0`](https://github.com/twmb/franz-go/commit/cad283f0) **bugfix** kgo: fix for empty fetch mapped metadata (#1143)

v1.20.0
===

This is a comparatively small minor release that adds support for Kafka 4.1,
adds three new APIs, fixes four bugs (read below to gauge importance), has a
few improvements, and **switches the client from a default of 0ms linger to a
default of 10ms linger**.

Also of note: a new `srfake` package has been created so you can run a fake
Schema Registry server in your CI tests (thank you [@weeco](https://github.com/weeco)).
This complements the existing `kfake` package that allows you to run a fake
in-memory Kafka "cluster" for unit testing. If you did not know of either of these,
check them out! `kfake` supports many Kafka features, but transactions are still WIP.
All franz-go tests except transaction based tests pass against a kfake "cluster",
so odds are, it'll work for you.

There are a few external contributors this release to features, docs, bugs, and
internal improvements. If I do not call you out below directly, please know I'm
thankful for your contributions!

## Behavior changes

* This library now lingers by default for 10ms. You can switch back to 0ms
  lingering by adding `kgo.ProducerLinger(0)` to your options
  when initializing the client. The original theory for 0ms linger was more of
  a theory, and years of practice has shown that even a tiny linger can be
  beneficial to the throughput and batching of clients.
  See [#1072](https://github.com/twmb/franz-go/issues/1072) for more details.

## Bug fixes

* Metadata refreshes could panic if a very specific flow of events happened,
  specifically only on a cluster that is transitioning from not using topic IDs
  to using topic IDs, and only if the transition is not implemented 100% correctly.
  This bug has existed for years and was only encountered during the recent addition
  of topic IDs to Redpanda. See [`645f1126`](https://github.com/twmb/franz-go/commit/645f1126) for more details.

* The loop that determines whether more batches exist to be produced had its
  conditional backwards. This was hidden forever due to other minor logic flaws
  that caused the "do more batches exist?" check to occur more than it should
  have, so the bug caused no problems. The "do more batches exist?" checks have
  been improved and the conditional has been fixed.

* The internal linger timers fired way more than they needed to, causing
  batches to be cut WAY more frequently than they needed to when using
  lingering. The logic here has been fixed, so lingering should actually run
  its full time now and batches should be bigger.

* Azure resets connections when speaking ApiVersions v4. v1.19 of this library
  detected this resetting and after 3 attempts, downgrades to ApiVersions v3.
  However, the connection reset error is different when running on Windows.
  The code has been improved to detect the proper syscall when this library
  is running on Windows. Thanks [@axw](https://github.com/axw)!

## Improvements

* Previously, producing was limited to 5 inflight requests per broker even if
  you disabled idempotence and bumped the number. The inflight limit is now
  unbounded. Thanks [@pracucci](https://github.com/pracucci)!

## Features

* `OnPartitionsCallbackBlocked` now exists so that, if you are using
  `BlockRebalanceOnPoll`, you can be notified that a rebalance is desired. If
  your record processing function is slow, this allows you to interrupt your
  batch processing (if possible), wrap up committing, and allow a rebalance
  _before_ your client is kicked from the group.

* `ConsumeExcludeTopics`, if you are using regex consuming, allows you to have
  a higher-priority set of regular expressions to exclude topics from being
  consumed. This is useful if you want to consume everything _except_ a set of
  topics (for example, if you are replicating topics from one cluster to
  another). Thanks [@mmatczuk](https://github.com/mmatczuk)!

* `Fetches.RecordsAll` now exists to return a Go iterator for use in range loops.
  Thanks [@narqo](https://github.com/narqo)!
 
## Relevant commits

- [`1844d216`](https://github.com/twmb/franz-go/commit/1844d216) **feature** kgo: add OnPartitionsCallbackBlocked
- [`157580fd`](https://github.com/twmb/franz-go/commit/157580fd) kgo.RequestSharded: support ConsumerGroupDescribe, ShareGroupDescribe
- [`f176953e`](https://github.com/twmb/franz-go/commit/f176953e) **behavior change** kgo lingering: default to 10ms
- [`679f7c3d`](https://github.com/twmb/franz-go/commit/679f7c3d) kgo: add support for produce v13
- [`f7f61420`](https://github.com/twmb/franz-go/commit/f7f61420) generate: new definitions for share requests
- [`32997347`](https://github.com/twmb/franz-go/commit/32997347) generate: new non-share protocols for kafka 4.1
- [`be947c20`](https://github.com/twmb/franz-go/commit/be947c20) bench: add -batch-recs, -psync, -pgoros
- [`0b1dbf0c`](https://github.com/twmb/franz-go/commit/0b1dbf0c) **bugfix** kgo: multiple linger fixes
- [`2ea3251d`](https://github.com/twmb/franz-go/commit/2ea3251d) **bugfix** sink: fix old bug determining whether more batches should be produced
- [`195bed84`](https://github.com/twmb/franz-go/commit/195bed84) **feature** kgo: add ConsumeExcludeTopics
- [`645f9d4b`](https://github.com/twmb/franz-go/commit/645f9d4b) **bugfix** Check errno for (WSA)ECONNRESET
- [`645f1126`](https://github.com/twmb/franz-go/commit/645f1126) **bugfix** kgo: fix panic in metadata updates from inconsistent broker state
- [`612f26b6`](https://github.com/twmb/franz-go/commit/612f26b6) **feature** kgo: add Fetches.RecordsAll to return a Go native iterator
- [`ce2bcd18`](https://github.com/twmb/franz-go/commit/ce2bcd18) **improvement** kgo: use unlimited ring buffers in the Produce path, allowing >5 inflight requests

v1.19.5
===

Fixes a bug introduced in 1.19.3 that caused batched FindCoordinator requests
to no longer work against older brokers (Kafka brokers before 2.4, or all
Redpanda versions brokers).

All credit to [@douglasbouttell](https://github.com/douglasbouttell) for exactly diagnosing the bug.

- [`06272c66`](https://github.com/twmb/franz-go/commit/06272c66) bugfix kgo: bugfix batched FindCoordinator requests against older brokers

v1.19.4
===

Fixes one bug introduced from the prior release (an obvious data race in
retrospect), and one data race introduced in 1.19.0. I've looped the tests more
in this release and am not seeing further races. I don't mean to downplay the
severity here, but these are races on pointer-sized variables where reading the
before or after state is of little difference. One of the read/write races is
on a context.Context, so there are actually two pointer sized reads & writes --
but reading the (effectively) type vtable for the new context and then the data
pointer for the old context doesn't really break things here. Anyway, you
should upgrade.

This also adds a workaround for Azure EventHubs, which does not handle
ApiVersions correctly when the broker does not recognize the version we are
sending. The broker _should_ reply with an `UNSUPPORTED_VERSION` error and
reply with the version the broker _can_ handle. Instead, Azure is resetting the
connection. To workaround, we detect a cxn reset twice and then downgrade the
request we send client side to 0.

- [`7910f6b6`](https://github.com/twmb/franz-go/commit/7910f6b6) kgo: retry `connection reset by peer` from ApiVersions to work around EventHubs
- [`d310cabd`](https://github.com/twmb/franz-go/commit/d310cabd) kgo: fix data read/write race on ctx variable
- [`7a5ddcec`](https://github.com/twmb/franz-go/commit/7a5ddcec) kgo bugfix: guard sink batch field access more

v1.19.3
===

This release fully fixes (and has a positive field report) the KIP-890 problem
that was meant to be fixed in v1.19.2. See the commit description for more
details.

- [`a13f633b`](https://github.com/twmb/franz-go/commit/a13f633b) kgo: remove pinReq wrapping request


v1.19.2
===

This release fixes two bugs, a data race and a misunderstanding in some of the
implementation of KIP-890.

The data race has existed for years and has only been caught once. It could
only be encountered in a specific section of decoding a fetch response WHILE a
metadata response was concurrently being handled, and the metadata response
indicated a partition changed leaders. The race was benign; it was a read race,
and the decoded response is always discarded _because_ a metadata change
happened. Regardless, metadata handling and fetch response decoding are no
longer concurrent.

For KIP-890, some things were not called out all to clearly (imo) in the KIP.
If your 4.0 cluster had not yet enabled the transaction.version feature v2+,
then transactions would not work in this client. As it turns out, Kafka 4
finally started using a v2.6 introduced "features" field in a way that is
important to clients. In short: I opted into KIP-890 behavior based on if a
broker could handle requests (produce v12+, end txn v5+, etc). I also needed to
check if "transaction.version" was v2+. Features handling is now supported in
the client, and this single client-relevant feature is now implemented.

See the commits for more details.

- [`dda08fd9`](https://github.com/twmb/franz-go/commit/dda08fd9) kgo: fix KIP-890 handling of the transaction.version feature
- [`8a364819`](https://github.com/twmb/franz-go/commit/8a364819) kgo: fix data race in fetch response handling

v1.19.1
===

This release fixes a very old bug that finally started being possible to hit in
v1.19.0. The v1.19.0 release does not work for Kafka versions pre-4.0. This
release fixes that (by fixing the bug that has existed since Kafka 2.4) and
adds a GH action to test against Kafka 3.8 to help prevent regressions against
older brokers as this library marches forward.

- [`50aa74f1`](https://github.com/twmb/franz-go/commit/50aa74f1) kgo bugfix: ApiVersions replies only with key 18, not all keys

v1.19.0
===

This is the largest release of franz-go yet. The last patch release was Jan 20, '25.
The last minor release was Oct 14, '24.

A big reason for delays the past few month+ has been from spin looping tests
and investigating any issue that popped up. Another big delay is that Kafka has
a full company adding features -- some questionable -- and I'm one person that
spent a significant amount of time catching this library up with the latest
Kafka release. Lastly, Kafka released Kafka v3.9 three weeks after my last
major release, and simultaneously, a few requests came in for new features in
this library that required a lot of time.  I wanted a bit of a break and only
resumed development more seriously in late Feb. This release is likely >100hrs
of work over the last ~4mo, from understanding new features and implementing
them, reviewing PRs, and debugging rare test failures.

The next Kafka release is poised to implement more large features (share
groups), which unfortunately will mean even more heads down time trying to bolt
in yet another feature to an already large library. I hope that Confluent
chills with introducing massive client-impacting changes; they've introduced
more in the past year than has been introduced from 2019-2023.

## Bug fixes / changes / deprecations

* The BasicLogger will no longer panic if only a single key (no val) is used. Thanks [@vicluq](https://github.com/vicluq)!

* An internal coding error around managing fetch concurrency was fixed. Thanks [@iimos](https://github.com/iimos)!

* Some off by ones with retries were fixed (tldr: we retried one fewer times than configured)

* `AllowAutoTopicCreation` and `ConsumeRegex` can now be used together.
  Previously, topics would not be created if you were producing and consuming
  from the same client AND if you used the `ConsumeRegex` option.

* A data race in the consumer code path has been fixed. The race is hard to
  encounter (which is why it never came up even in my weeks of spin-looping
  tests with `-race`). See [PR #984](https://github.com/twmb/franz-go/pull/984)
  for more details.

* `EndBeginTxnUnsafe` is deprecated and unused. `EndAndBeginTransaction` now
  flushes, and you cannot produce while the function happens (the function will
  just be stuck flushing). As of KIP-890, the behavior that the library relied on
  is now completely unsupported. Trying to produce while ending & beginning a
  transaction very occasionally leads to duplicate messages. The function now is
  just a shortcut for flush, end, begin.

* The kversion package guts have been entirely reimplemented; version guessing
  should be more reliable.

* `OnBrokerConnect` now encompasses the entire SASL flow (if using SASL) rather
  than just connection dialing. This allows you more visibility into successful
  or broken connections, as well as visibility into how long it actually takes
  to initialize a connection. The `dialDur` arg has been renamed to `initDur`.
  You may see the duration increase in your metrics. enough If feedback comes
  in that this is confusing or unacceptable, I may issue a patch to revert
  the change and instead introduce a separate hook in the next minor release.
  I do not aim to create another minor release for a while.

## Features / improvements

* This release adds support for user-configurable memory pooling to a few select
  locations. See any "Pool" suffixed interface type in the documentation. You can
  use this to add bucketed pooling (or whatever strategy you choose) to cut down
  on memory waste in a few areas. As well, a few allocations that were previously
  many-tiny allocs have been converted to slab allocations (slice backed). Lastly,
  if you opt into `kgo.Record` pooling, the `Record` type has a new `Recycle`
  method to send it and all other pooled slices back to their pools.

* You can now completely override how compression or decompression is done via
  the new `WithCompressor` and `WithDecompressor` options. This allows you to
  use libraries or options that franz-go does not automatically support, perhaps
  opting for higher performance libraries or options or using memory more memory
  pooling behind the scenes.

* `ConsumeResetOffset` has been split into two options, `ConsumeResetOffset` and
  `ConsumeStartOffset`. The documentation has been cleaned up. I personally always
  found it confusing to use the reset offset for both what to start consuming from
  and what to reset to when the client sees an offset out of range error. The start
  offset defaults to the reset offset (and vice versa) if you only set one.

* For users that produce infrequently but want the latency to be low when producing,
  the client now has a `EnsureProduceConnectionIsOpen` method. You can call this
  before producing to force connections to be open.

* The client now has a `RequestCachedMetadata` function, which can be used to
  request metadata _only if_ the information you're requesting is not cached,
  or is cached but is too stale. This can be very useful for admin packages that
  need metadata to do anything else -- rather than requesting metadata for every
  single admin operation, you can have metadata requested once and use that
  repeatedly. Notably, I'll be switching `kadm` to using this function.

* KIP-714 support: the client now internally aggregates a small set of metrics
  and sends them to the broker by default. This client implements all required
  metrics and a subset of recommended metrics (the ones that make more sense).
  To opt out of metrics collection & sending to the broker by default, you
  can use the new `DisableClienMetrics` option. You can also provide your own
  metrics to send to the broker via the new `UserMetricsFn` option. The client
  does not attempt to sanitize any user provided metric names; be sure you provide
  the names in the correct format (see docs).

* KIP-848 support: this exists but is hidden. You must explicitly opt in by using
  the new WithContext option, and the context must have a special string key,
  `opt_in_kafka_next_gen_balancer_beta`. I noticed while testing that if you
  repeat `ConsumerGroupHeartbeat` requests (i.e. what can happen when clients
  are on unreliable networks), group members repeatedly get fenced. This is
  recoverable, but it happens way _way_ more than it should and I don't believe
  the broker implementation to be great at the moment. Confluent historically
  ignores any bug reports I create on the KAFKA issue tracker, but if you
  would like to follow along or perhaps nudge to help get a reply, please
  chime in on [KAFKA-19222][19222], [KAFKA-19233][19233], and [KAFKA-19235][19235].

* A few other more niche APIs have been added. See the full breadth of new APIs
  below and check pkg.go.dev for docs for any API you're curious about.

[19222]: https://issues.apache.org/jira/browse/KAFKA-19222
[19233]: https://issues.apache.org/jira/browse/KAFKA-19233
[19235]: https://issues.apache.org/jira/browse/KAFKA-19235

## API additions

This section contains all net-new APIs in this release. See the documentation
on pkg.go.dev.

```go
const (
        CodecNone CompressionCodecType = iota
        CodecGzip
        CodecSnappy
        CodecLz4
        CodecZstd
        CodecError = -1
)
const CompressDisableZstd CompressFlag = 1 + iota
const (
    MetricTypeSum = 1 + iota
    MetricTypeGauge
)

type CompressFlag uint16
type CompressionCodecType int8
type Compressor interface {
    Compress(dst *bytes.Buffer, src []byte, flags ...CompressFlag) ([]byte, CompressionCodecType)
}
type Decompressor interface {
    Decompress(src []byte, codecType CompressionCodecType) ([]byte, error)
}
type Metric struct {
        Name string
        Type MetricType
        ValueInt int64
        ValueFloat float64
        Attrs map[string]any
}
type MetricType uint8
type Pool any
type PoolDecompressBytes interface {
        GetDecompressBytes(compressed []byte, codec CompressionCodecType) []byte
        PutDecompressBytes([]byte)
}
type PoolKRecords interface {
        GetKRecords(n int) []kmsg.Record
        PutKRecords([]kmsg.Record)
}
type PoolRecords interface {
        GetRecords(n int) []Record
        PutRecords([]Record)
}
type ProcessFetchPartitionOpts struct {
        KeepControlRecords bool
        DisableCRCValidation bool
        Offset int64
        IsolationLevel IsolationLevel
        Topic string
        Partition int32
        Pools []Pool
}

func DefaultCompressor(...CompressionCodec) (Compressor, error)
func DefaultDecompressor(...Pool) Decompressor
func IsRetryableBrokerErr(error) bool
func ProcessFetchPartition(ProcessFetchPartitionOpts, *kmsg.FetchResponseTopicPartition, Decompressor, func(FetchBatchMetrics)) (FetchPartition, int64)

func DisableClientMetrics() Opt
func OnRebootstrapRequired(func() ([]string, error)) Opt
func UserMetricsFn(fn func() iter.Seq[Metric]) Opt
func WithContext(ctx context.Context) Opt
func WithPools(pools ...Pool) Opt

func ConsumeStartOffset(Offset) ConsumerOpt
func DisableFetchCRCValidation() ConsumerOpt
func RecheckPreferredReplicaInterval(time.Duration) ConsumerOpt
func WithDecompressor(decompressor Decompressor) ConsumerOpt

func DefaultProduceTopicAlways() ProducerOpt
func WithCompressor(Compressor) ProducerOpt

func (*Client) Context() context.Context
func (*Client) EnsureProduceConnectionIsOpen(context.Context, ...int32) error
func (*Client) RequestCachedMetadata(context.Context, *kmsg.MetadataRequest, time.Duration) (*kmsg.MetadataResponse, error)

func (*Record) Recycle()
```

## Relevant commits

This is a small selection of what I think are the most pertinent commits in
this release. This release is very large, though. Many commits and PRs have
been left out that introduce or change smaller things.

- [`07e57d3e`](https://github.com/twmb/franz-go/commit/07e57d3e) kgo: remove all EndAndBeginTransaction internal "optimizations"
- [`a54ffa96`](https://github.com/twmb/franz-go/commit/a54ffa96) kgo: add ConsumeStartOffset, expand offset docs, update readme KIPs
- [`PR #988`](https://github.com/twmb/franz-go/pull/988)#988 kgo: add support for KIP-714 (client metrics)
- [`7a17a03c`](https://github.com/twmb/franz-go/commit/7a17a03c) kgo: fix data race in consumer code path
- [`ae96af1d`](https://github.com/twmb/franz-go/commit/ae96af1d) kgo: expose IsRetryableBrokerErr
- [`1eb82fee`](https://github.com/twmb/franz-go/commit/1eb82fee) kgo: add EnsureProduceConnectionIsOpen
- [`fc778ba8`](https://github.com/twmb/franz-go/commit/fc778ba8) kgo: fix AllowAutoTopicCreation && ConsumeRegex when used together
- [`ae7eea7c`](https://github.com/twmb/franz-go/commit/ae7eea7c) kgo: add DisableFetchCRCValidation option
- [`6af90823`](https://github.com/twmb/franz-go/commit/6af90823) kgo: add the ability to pool memory in a few places while consuming
- [`8c7a36db`](https://github.com/twmb/franz-go/commit/8c7a36db) kgo: export utilities for decompressing and parsing partition fetch responses
- [`33400303`](https://github.com/twmb/franz-go/commit/33400303) kgo: do a slab allocation for Record's when processing a batch
- [`39c2157a`](https://github.com/twmb/franz-go/commit/39c2157a) kgo: add WithCompressor and WithDecompressor options
- [`9252a6b6`](https://github.com/twmb/franz-go/commit/9252a6b6) kgo: export Compressor and Decompressor
- [`be15c285`](https://github.com/twmb/franz-go/commit/be15c285) kgo: add Client.RequestCachedMetadata
- [`fc040bc0`](https://github.com/twmb/franz-go/commit/fc040bc0) kgo: add OnRebootstrapRequired
- [`c8aec00a`](https://github.com/twmb/franz-go/commit/c8aec00a) kversion: document changes through 4.0
- [`718c5606`](https://github.com/twmb/franz-go/commit/718c5606) kgo: remove all code handling EndBeginTxnUnsafe, make it a no-op
- [`5494c59e`](https://github.com/twmb/franz-go/commit/5494c59e) kversions: entirely reimplement internals
- [`9d266fcd`](https://github.com/twmb/franz-go/commit/9d266fcd) kgo: allow outstanding produce requests to be context canceled if the user disables idempotency
- [`c60bf4c2`](https://github.com/twmb/franz-go/commit/c60bf4c2) kgo: add DefaultProduceTopicAlways ProducerOpt
- [`50cfe060`](https://github.com/twmb/franz-go/commit/50cfe060) kgo: fix off-by-one with retries accounting
- [`e9ba83a6`](https://github.com/twmb/franz-go/commit/e9ba83a6), [`05099ba0`](https://github.com/twmb/franz-go/commit/05099ba0) kgo: add WithContext, Client.Context()
- [`ddb0c0c3`](https://github.com/twmb/franz-go/commit/ddb0c0c3) kgo: fix cancellation of a fetch in manageFetchConcurrency
- [`83843a53`](https://github.com/twmb/franz-go/commit/83843a53) kgo: fixed panic when keyvals len equals 1

v1.18.1
===

This patch release contains a myriad of fixes for _relatively_ minor bugs, a
few improvements, and updates all dependencies. Both `pkg/kadm` and `pkg/sr`
are also being released as minors in tandem with a few quality of life APIs.

## Bug fixes

* Previously, if records were successfully produced but returned with an
  invalid offset (-1), the client would erroneously return bogus offsets
  to the end users. This has been fixed to return -1. (Note this was never
  encountered in the wild).

* Pausing topics & partitions while using `PollRecords` previously could result
  in incorrect accounting in `BufferedFetchRecords` and `BufferedFetchBytes`,
  permanently causing the numbers returned to be larger than reality. That is,
  it is possible the functions would return non-zero numbers even though nothing
  was buffered.

* When consuming from a follower (i.e. you were using the `Rack` option and your
  cluster is configured with follower fetching), if the follower you consumed from
  had a _higher_ log start offset than the leader, and if you were trying to consume
  from an early offset that exists on the leader but not on the follower, the client
  would enter a permanent spinloop trying to list offsets against the follower.
  This is due to KIP-320 case 3, which mentions that clients should send a ListOffsets 
  to the follower -- _this is not the case_, Kafka actually returns NotLeaderOrFollower
  when sending that request to the follower. Now the client clears the preferred replica
  and sends the next fetch request to the leader, at which point the leader can either
  serve the request or redirect back to a different preferred replica.

## Improvements

* When finishing batches, if any records were blocked in Produce due to
  the client hitting the maximum number of buffered records, the client would broadcast
  to all waiters that a message was finished for _every_ message finished until there were
  no other goroutines waiting to try to produce. When lingering
  is enabled, linger occurs _except_ when the client has reached the maximum number of
  buffered records. Once the client is as max buffered records, the client tries to flush until more records can be buffered.
  If you have a few concurrent producers, they will all hang _trying_ to buffer. As soon
  as one is signaled, it will grab the free spot, enter into the client as buffered,
  and then see the client is now again at max buffered and immediately create a batch
  rather than lingering. Thus, signalling one at a time would cause many small single-record
  batches to be created and each cause a round trip to the cluster. This would result in slow performance.
  Now, by finishing a batch at a time, the client opens many slots at a time for any producers _waiting_,
  and ideally they can fit into being buffered without hitting max buffered and clearing any linger state.
  Note that single-message batches can still cause the original behavior, but there is not
  much more that can be done.

* Decompression errors encountered while consuming are now returned to the end user, rather
  than being stripped internally. Previously, stripping the error internally would result in
  the client spinlooping: it could never make forward progress and nothing ever signaled the
  end user that something was going wrong.

## Relevant commits

* [`13584b5`](https://github.com/twmb/franz-go/commit/13584b5) **feature** kadm: always request authorized operations
* [`847095b`](https://github.com/twmb/franz-go/commit/847095b) **bugfix** kgo: redirect back to the leader on KIP-392 case 3 failure
* [`d6d3015`](https://github.com/twmb/franz-go/commit/d6d3015) **feature** pkg/sr: add PreReq option (and others by [@mihaitodor](https://github.com/mihaitodor), thank you!)
* [`1473778`](https://github.com/twmb/franz-go/commit/1473778) **improvement** kgo: return decompression errors while consuming
* [`3e9beae`](https://github.com/twmb/franz-go/commit/3e9beae) **bugfix** kgo: fix accounting when topics/partitions are {,un}paused for PollRecords
* [`ead18d3`](https://github.com/twmb/franz-go/commit/ead18d3) **improvement** kgo: broadcast batch finishes in one big blast
* [`aa1c73c`](https://github.com/twmb/franz-go/commit/aa1c73c) **feature** kadm: add func to decode AuthorizedOperations (thanks [@weeco](https://github.com/weeco)!)
* [`f66d495`](https://github.com/twmb/franz-go/commit/f66d495) kfake: do not listen until the cluster is fully set up
* [`2eed36e`](https://github.com/twmb/franz-go/commit/2eed36e) **bugfix** pkg/kgo: fix handling of invalid base offsets (thanks [@rodaine](https://github.com/rodaine)!)

v1.18.0
===

This release adds support for Kafka 3.7, adds a few community requested APIs,
some internal improvements, and fixes two bugs. One of the bugfixes is for a
deadlock; it is recommended to bump to this release to ensure you do not run
into the deadlock. The features in this release are relatively small.

This adds protocol support for [KIP-890][KIP-890] and [KIP-994][KIP-994], and
adds further protocol support for [KIP-848][KIP-848]. If you are using
transactions, you may see a new `kerr.TransactionAbortable` error, which
signals that your ongoing transaction should be aborted and will not be
successful if you try to commit it.

[KIP-890]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
[KIP-994]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-994%3A+Minor+Enhancements+to+ListTransactions+and+DescribeTransactions+APIs

Lastly, there have been a few improvements to `pkg/sr` that are not mentioned
in these changelog notes.

## Bug fixes

* If you canceled the context used while producing while your client was
  at the maximum buffered records or bytes, it was possible to experience
  deadlocks. This has been fixed. See #832 for more details.

* Previously, if using `GetConsumeTopics` while regex consuming, the function
  would return all topics ever discovered. It now returns only the topics that
  are being consumed.

## Improvements

* The client now internaly ignores OutOfOrderSequenceNumber errors that are
  encountered when consuming _if possible_. If a producer produces very infrequently,
  it is possible the broker forgets the producer by the next time the producer
  produces. In this case, the producer receives an OutOfOrderSequenceNumber error.
  The client now internally resets properly so that you do not see the error.

## Features

* `AllowRebalance` and `CloseAllowingRebalance` have been added to `GroupTransactSession`.
* The `FetchTopic` type now has includes the topic's `TopicID`.
* The `ErrGroupSession` internal error field is now public, allowing you to test how you handle the internal error.
* You may now receive a `kerr.TransactionAbortable` error from many functions while using transactions.

## Relevant commits

* [`0fd1959d`](https://github.com/twmb/franz-go/commit/0fd1959d) kgo: support Kafka 3.8's kip-890 modifications
* [`68163c55`](https://github.com/twmb/franz-go/commit/68163c55) **bugfix** kgo: do not add all topics to internal tps map when regex consuming
* [`3548d1f7`](https://github.com/twmb/franz-go/commit/3548d1f7) **improvement** kgo: ignore OOOSN where possible
* [`6a759401`](https://github.com/twmb/franz-go/commit/6a759401) **bugfix** kgo: fix potential deadlock when reaching max buffered (records|bytes)
* [`4bfb0c68`](https://github.com/twmb/franz-go/commit/4bfb0c68) **feature** kgo: add TopicID to the FetchTopic type
* [`06a9c47d`](https://github.com/twmb/franz-go/commit/06a9c47d) **feature** kgo: export the wrapped error from ErrGroupSession
* [`4affe8ef`](https://github.com/twmb/franz-go/commit/4affe8ef) **feature** kgo: add AllowRebalance and CloseAllowingRebalance to GroupTransactSession

v1.17.1
===

This patch release fixes four bugs (two are fixed in one commit), contains two
internal improvements, and adds two other minor changes.

## Bug fixes

* If you were using the `MaxBufferedBytes` option and ever hit the max, odds are
  likely that you would experience a deadlock eventually. That has been fixed.

* If you ever produced a record with no topic field and without using `DefaultProduceTopic`,
  or if you produced a transactional record while not in a transaction, AND if the client
  was at the maximum buffered records, odds are you would eventually deadlock.
  This has been fixed.

* It was previously not possible to set lz4 compression levels.

* There was a data race on a boolean field if a produce request was being
  written at the same time a metadata update happened, _and_ if the metadata
  update has an error on the topic or partition that is actively being written.
  Note that the race was unlikely and if you experienced it, you would have noticed
  an OutOfOrderSequenceNumber error. See [this comment](https://github.com/twmb/franz-go/pull/781#issuecomment-2248563542)
  for more details.

## Improvements

* Canceling the context you pass to `Produce` now propagates in two more areas:
  the initial `InitProducerID` request that occurs the first time you produce,
  and if the client is internally backing off due to a produce request failure.
  Note that there is no guarantee on _which_ context is used for cancelation if
  you produce many records, and the client does _not_ allow canceling if it is
  currently unsafe to do so. However, this does mean that if your cluster is
  somewhat down such that `InitProducerID` is failing on your new client, you
  can now actually cause the `Produce` to quit. See [this comment](https://github.com/twmb/franz-go/issues/769#issuecomment-2235707417)
  for what it means for a record to be "safe" to fail.

* The client now ignores aborted records while consuming only if you have
  configured `FetchIsolationLevel(ReadCommitted())`. Previously, the client relied
  entirely on the `FetchResponse` `AbortedTransactions` field, but it's possible
  that brokers could send aborted transactions even when not using read committed.
  Specifically, this was a behavior difference in Redpanda, and the KIP that introduced
  transactions and all relevant documents do not mention what the broker behavior
  actually should be here. Redpanda itself was also changed to not send aborted
  transactions when using read committed, but we may as well improve franz-go as well.

* Decompression now better reuses buffers under the hood, reducing allocations.

* Brokers that return preferred replicas to fetch from now causes an info level
  log in the client.

## Relevant commits

* [`305d8dc`](https://github.com/twmb/franz-go/commit/305d8dc) kgo: allow record ctx cancelation to propagate a bit more
* [`24fbb0f`](https://github.com/twmb/franz-go/commit/24fbb0f) **bugfix** kgo: fix deadlock in Produce when using MaxBufferedBytes
* [`1827add`](https://github.com/twmb/franz-go/commit/1827add) **bugfix** kgo sink: fix read/write race for recBatch.canFailFromLoadErrs
* [`d7ea2c3`](https://github.com/twmb/franz-go/commit/d7ea2c3) **bugfix** fix setting lz4 compression levels (thanks [@asg0451](https://github.com/asg0451)!)
* [`5809dec`](https://github.com/twmb/franz-go/commit/5809dec) optimise: use byteBuffer pool in decompression (thanks [@kalbhor](https://github.com/kalbhor)!)
* [`cda897d`](https://github.com/twmb/franz-go/commit/cda897d) kgo: add log for preferred replicas
* [`e62b402`](https://github.com/twmb/franz-go/commit/e62b402) **improvement** kgo sink: do not back off on certain edge case
* [`9e32bf9`](https://github.com/twmb/franz-go/commit/9e32bf9) kgo: ignore aborted txns if using `READ_UNCOMMITTED`

v1.17.0
===

This long-coming release, four months after v1.16.0, adds support for Kafka 3.7
and adds a few community added or requested APIs. There will be a kadm release
shortly following this one, and maybe a plugin release.

This adds full support for [KIP-951][KIP-951], as well as protocol support for
[KIP-919][KIP-919] (which has no client facing features) and [KIP-848][KIP-848]
(protocol only, not the feature!). KIP-951 should make the client faster at
handling when the broker moves partition leadership to a different broker.

There are two fairly minor bug fixes in the kgo package in this release, both
described below. There is also one bugfix in the pkg/sr independent (and
currently) untagged module. Because pkg/sr is untagged, the bugfix was released
a long time ago, but the relevant commit is still mentioned below.

[KIP-951]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
[KIP-919]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-919%3A+Allow+AdminClient+to+Talk+Directly+with+the+KRaft+Controller+Quorum+and+add+Controller+Registration
[KIP-848]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol

## Bug fixes

* Previously, upgrading a consumer group from non-cooperative to cooperative
  _while_ the group was running did not work. This is now fixed (by [@hamdanjaveed](https://github.com/hamdanjaveed), thank you!).

* Previously, if a cooperative consumer group member rebalanced _while_ fetching
  offsets for partitions, if those partitions were _not_ lost in the rebalance,
  the member would call OnPartitionsAssigned with those partitions _again_.
  Now, those partitions are passed to OnPartitionsAssigned only once (the first time).

## Improvements

* The client will now stop lingering if you hit max buffered records or bytes.
  Previously, if your linger was long enough, you could stall once you hit
either of the Max options; that is no longer the case.

* If you are issuing admin APIs on the same client you are using for consuming
  or producing, you may see fewer metadata requests being issued.

There are a few other even more minor improvements in the commit list if you
wish to go spelunking :).

## Features

* The `Offset` type now has a new method `AtCommitted()`, which causes the
  consumer to not fetch any partitions that do _not_ have a previous commit.
  This mirrors Kafka's `auto.offset.reset=none` option.

* KIP-951, linked above and the commit linked below, improves latency around
  partition leader transfers on brokers.

* `Client.GetConsumeTopics` allows you to query what topics the client is
  currently consuming. This may be useful if you are consuming via regex.

* `Client.MarkCommitOffsets` allows you to mark offsets to be committed in
   bulk, mirroring the non-mark API `CommitOffsets`.

## Relevant commits

#### franz-go

- [`a7caf20`](https://github.com/twmb/franz-go/commit/a7caf20) **feature** kgo.Offset: add AtCommitted()
- [`55dc7a0`](https://github.com/twmb/franz-go/commit/55dc7a0) **bugfix** kgo: re-add fetch-canceled partitions AFTER the user callback
- [`db24bbf`](https://github.com/twmb/franz-go/commit/db24bbf) **improvement** kgo: avoid / wakeup lingering if we hit max bytes or max records
- [`993544c`](https://github.com/twmb/franz-go/commit/993544c) **improvement** kgo: Optimistically cache mapped metadata when cluster metadata is periodically refreshed (thanks [@pracucci](https://github.com/pracucci)!)
- [`1ed02eb`](https://github.com/twmb/franz-go/commit/1ed02eb) **feature** kgo: add support for KIP-951
- [`2fbbda5`](https://github.com/twmb/franz-go/commit/2fbbda5) **bugfix** fix: clear lastAssigned when revoking eager consumer
- [`d9c1a41`](https://github.com/twmb/franz-go/commit/d9c1a41) pkg/kerr: add new errors
- [`54d3032`](https://github.com/twmb/franz-go/commit/54d3032) pkg/kversion: add 3.7
- [`892db71`](https://github.com/twmb/franz-go/commit/892db71) **pkg/sr bugfix** sr SubjectVersions calls pathSubjectVersion
- [`ed26ed0`](https://github.com/twmb/franz-go/commit/ed26ed0) **feature** kgo: adds Client.GetConsumeTopics (thanks [@UnaffiliatedCode](https://github.com/UnaffiliatedCode)!)
- [`929d564`](https://github.com/twmb/franz-go/commit/929d564) **feature** kgo: adds Client.MarkCommitOffsets (thanks [@sudo-sturbia](https://github.com/sudo-sturbia)!)

#### kfake

kfake as well has a few improvements worth calling out:

- [`18e2cc3`](https://github.com/twmb/franz-go/commit/18e2cc3) kfake: support committing to non-existing groups
- [`b05c3b9`](https://github.com/twmb/franz-go/commit/b05c3b9) kfake: support KIP-951, fix OffsetForLeaderEpoch
- [`5d8aa1c`](https://github.com/twmb/franz-go/commit/5d8aa1c) kfake: fix handling ListOffsets with requested timestamp

v1.16.1
===

This patch release fixes one bug and un-deprecates SaramaHasher.

SaramaHasher, while not identical to Sarama's partitioner, actually _is_
identical to some other partitioners in the Kafka client ecosystem. So, the old
function is now un-deprecated, but the documentation correctly points you to
SaramaCompatHasher and mentions why you may still want to use SaramaHasher.

For the bug: if you tried using CommitOffsetsSync during a group rebalance, and
you canceled your context while the group was still rebalancing, then
CommitOffsetsSync would enter a deadlock and never return. That has been fixed.

- [`cd65d77`](https://github.com/twmb/franz-go/commit/cd65d77) and [`99d6dfb`](https://github.com/twmb/franz-go/commit/99d6dfb) kgo: fix bug
- [`d40ac19`](https://github.com/twmb/franz-go/commit/d40ac19) kgo: un-deprecate SaramaHasher and add docs explaining why

v1.16.0
===

This release contains a few minor APIs and internal improvements and fixes two
minor bugs.

One new API that is introduced also fixes a bug. API-wise, the `SaramaHasher`
was actually _not_ a 1:1 compatible hasher. The logic was identical, but there
was a rounding error because Sarama uses int32 module arithmetic, whereas kgo
used int (which is likely int64) which caused a different hash result. A new
`SaramaCompatHasher` has been introduced and the old `SaramaHasher` has been
deprecated.

The other bugfix is that `OptValue` on the `kgo.Logger` option panicked if you
were not using a logger. That has been fixed.

The only other APIs that are introduced are in the `kversions` package; they
are minor, see the commit list below.

If you issue a sharded request and any of the responses has a retryable error
_in_ the response, this is no-longer returned as a top-level shard error. The
shard error is now nil, and you can properly inspect the response fully.

Lastly (besides other internal minor improvements not worth mentioning),
metadata fetches can now inject fake fetches if the metadata response has topic
or partition load errors. This is unconditionally true for non-retryable
errors. If you use `KeepRetryableFetchErrors`, you can now _also_ see when
metadata fetching is showing unknown topic errors or other retryable errors.

- [`a2340eb`](https://github.com/twmb/franz-go/commit/a2340eb) **improvement** pkg/kgo: inject fake fetches on metadata load errors
- [`d07efd9`](https://github.com/twmb/franz-go/commit/d07efd9) **feature** kversion: add `VersionStrings`, `FromString`, `V3_6_0`
- [`8d30de0`](https://github.com/twmb/franz-go/commit/8d30de0) **bugfix** pkg/kgo: fix OptValue with no logger set
- [`012cd7c`](https://github.com/twmb/franz-go/commit/012cd7c) **improvement** kgo: do not return response ErrorCode's as shard errors
- [`1dc3d40`](https://github.com/twmb/franz-go/commit/1dc3d40) **bugfix**: actually have correct sarama compatible hasher (thanks [@C-Pro](https://github.com/C-Pro))

v1.15.4
===

This patch release fixes a difficult to encounter, but
fatal-for-group-consuming bug.

The sequence of events to trigger this bug:
* OffsetCommit is issued before Heartbeat
* The coordinator for the group needs to be loaded (so, likely, a previous `NOT_COORDINATOR` error was received)
* OffsetCommit triggers the load
* a second OffsetCommit happens while the first is still running, canceling the first OffsetCommit's context

In this sequence of events, FindCoordinator will fail with `context.Canceled`
and, importantly, also return that error to Heartbeat. In the guts of the
client, a `context.Canceled` error _should_ only happen when a group is being
left, so this error is recognized as a group-is-leaving error and the group
management goroutine exits. Thus, the group is never rejoined.

This likely requires a system to be overloaded to begin with, because
FindCoordinator requests are usually very fast.

The fix is to use the client context when issuing FindCoordinator, rather than
the parent request. The parent request can still quit, but FindCoordinator
continues. No parent request can affect any other waiting request.

This patch also includes a dep bump for everything but klauspost/compress;
klauspost/compress changed go.mod to require go1.19, while this repo still
requires 1.18. v1.16 will change to require 1.19 and then this repo will bump
klauspost/compress.

There were multiple additions to the yet-unversioned kfake package, so that an
advanced "test" could be written to trigger the behavior for this patch and
then ensure it is fixed.  To see the test, please check the comment on PR
[650](https://github.com/twmb/franz-go/pull/650).

- [`7d050fc`](https://github.com/twmb/franz-go/commit/7d050fc) kgo: do not cancel FindCoordinator if the parent context cancels

v1.15.3
===

This patch release fixes one minor bug, reduces allocations on gzip and lz4
decompression, and contains a behavior improvement when OffsetOutOfRange is
received while consuming.

For the bugfix: previously, if the client was using a fetch session (as is the
default when consuming), and all partitions for a topic transfer to a different
broker, the client would not properly unregister the topic from the prior
broker's fetch session. This could result in more data being consumed and
discarded than necessary (although, it's possible the broker just reset the
fetch session anyway, I'm not entirely positive).

- [`fdf371c`](https://github.com/twmb/franz-go/commit/fdf371c) use bytes buffer instead of ReadAll (thanks [@kalbhor](https://github.com/kalbhor)!)
- [`e6ed69f`](https://github.com/twmb/franz-go/commit/e6ed69f) consuming: reset to nearest if we receive OOOR while fetching
- [`1b6a721`](https://github.com/twmb/franz-go/commit/1b6a721) **bugfix** kgo source: use the proper topic-to-id map when forgetting topics

v1.15.2
===

This patch release fixes two bugs and changes Mark functions to be no-ops when
not using AutoCommitMarks to avoid confusion. This also includes a minor commit
further improving the sticky balancer. See the commits for more details.

- [`72778cb`](https://github.com/twmb/franz-go/commit/72778cb) **behavior change** kgo: no-op mark functions when not using AutoCommitMarks
- [`e209bb6`](https://github.com/twmb/franz-go/commit/e209bb6) **bugfix** kgo: pin AddPartitionsToTxn to v3 when using one transaction
- [`36b4437`](https://github.com/twmb/franz-go/commit/36b4437) sticky: further improvements
- [`af5bc1f`](https://github.com/twmb/franz-go/commit/af5bc1f) **bugfix** kgo: be sure to use topics when other topics are paused

v1.15.1
===

This patch release contains a bunch of internal improvements to kgo and
includes a bugfix for a very hard to encounter logic race. Each improvement
is a bit focused on a specific use case, so I recommend reading any relevant-to-you
commit message below.

As well, the kversion package now detects Kafka 3.6, and the kgo package now
handles AddPartitionsToTxn v4 (however, you will probably not be issuing this
request).

Lastly, this release is paired with a minor kadm release, which adds the
ErrMessage field CreateTopicsResponse and DeleteTopicsResponse, and,
importantly, fixes a data race in the ApiVersions request.

#### franz-go

- [`2a3b6bd`](https://github.com/twmb/franz-go/commit/2a3b6bd) **improvement** kversion: detect 3.6
- [`fe5a660`](https://github.com/twmb/franz-go/commit/fe5a660) **improvement** kgo: add sharding for AddPartitionsToTxn for KIP-890
- [`b2ccc2f`](https://github.com/twmb/franz-go/commit/b2ccc2f) **improvement** kgo: reintroduce random broker iteration
- [`54a7418`](https://github.com/twmb/franz-go/commit/54a7418) **improvement** kgo: allow PreTxnCommitFnContext to modify empty offsets
- [`c013050`](https://github.com/twmb/franz-go/commit/c013050) **bugfix** kgo: avoid rare panic
- [`0ecb52b`](https://github.com/twmb/franz-go/commit/0ecb52b) **improvement** kgo: do not rotate the consumer session when pausing topics/partitions
- [`1429d47`](https://github.com/twmb/franz-go/commit/1429d47) **improvement** sticky balancer: try for better topic distribution among members

#### kadm

- [`1955938`](https://github.com/twmb/franz-go/commit/1955938) **bugfix** kadm: do not reuse ApiVersions in many concurrent requests
- [`66974e8`](https://github.com/twmb/franz-go/commit/66974e8) **feature** kadm: include ErrMessage in topic response

v1.15.0
===

This release comes 74 days (just over two months) since the last minor release.
This mostly contains new features, with one relatively minor but important bug
addressed (and one _very_ minor bug fixed).

## Bug fixes

* Fetch sessions now properly send forgotten topics if we forget the entire
  topic (not just individual partitions while other partitions are still
  consumed on the broker). For long-running clients where partitions move
  around the cluster a bunch over time, this ensures we are not sending requests
  with null topics / null topic IDs. See [#535](https://github.com/twmb/franz-go/issues/535)
   for more details.
* If the client talks to an http endpoint, we now properly detect the bytes
  'HTTP' and send a more relevant error message. This previously existed, but
  used the wrong int32 internally so 'HTTP' was not properly detected (thanks
  [@alistairking](https://github.com/alistairking)!).

## Features

* `RecordReader` now supports %v{json} to parse json inputs
* `RecordReader` now supports %v{}, an empty no-op formatting directive
* Adds `PurgeTopicsFromProducing` and `PurgeTopicsFromConsuming` to purge _either_
  the producing or consuming half of the client, if you are producing to and
  consuming from the same topics in the same client.
* The new `ConsiderMissingTopicDeletedAfter` option allows finer grained
  control for how long a regexp-discovered topic can be missing on the cluster
  before the topic is considered deleted (and then internally purged in the
  client)
* Adds `NewErrFetch` to create a single-error `Fetches`, for use in end-user
  tests / stubs.
* The new `MaxBufferedBytes` option can control how many bytes are buffered
  when producing, an option similar to `MaxBufferedRecords`.
* Adds `BufferedFetchBytes` and `BufferedProduceBytes` to return the total
  bytes in records buffered (note this counts only keys, values, and headers).
* Adds `PreTxnCommitFnContext` to allow custom `Metadata` annotation for
  transactional commits.
* Adds `LeaveGroupContext` to control how long leaving a group can take, as
  well as to return any leave group error.

## Relevant commits

#### franz-go

- [`4dcfb06`](https://github.com/twmb/franz-go/commit/4dcfb06) **feature** kgo: add LeaveGroupContext
- [`910e91d`](https://github.com/twmb/franz-go/commit/910e91d) and [`60b601a`](https://github.com/twmb/franz-go/commit/60b601a) **feature** kgo: add PreTxnCommitFnContext
- [`c80d6f4`](https://github.com/twmb/franz-go/commit/c80d6f4) **feature** kgo: add Buffered{Fetch,Produce}Bytes
- [`304559f`](https://github.com/twmb/franz-go/commit/304559f) **feature** kgo: support MaxBufferedBytes
- [`310a5da`](https://github.com/twmb/franz-go/commit/310a5da) **feature** kgo: add NewErrFetch
- [`504a9d7`](https://github.com/twmb/franz-go/commit/504a9d7) **feature** kgo: expose ConsiderMissingTopicDeletedAfter
- [`253e1a9`](https://github.com/twmb/franz-go/commit/253e1a9) **feature** kgo: add PurgeTopicsFrom{Producing,Consuming}
- [`055e2d8`](https://github.com/twmb/franz-go/commit/055e2d8) **improvement** kgo record formatter: accept %v{} to be a no-op (plain read/format)
- [`37edfb9`](https://github.com/twmb/franz-go/commit/37edfb9) **improvement** kgo.RecordReader: support %v{json} to read json values
- [`8a9a459`](https://github.com/twmb/franz-go/commit/8a9a459) **bugfix** kgo: track topic IDs in the fetch session
- [`9d25d3a`](https://github.com/twmb/franz-go/commit/9d25d3a) **bugfix** kgo: fix typo in parseReadSize to properly detect and warn about talking to HTTP endpoints (thanks [@alistairking](https://github.com/alistairking)!)

#### kadm

This release comes with a corresponding kadm release that contains a few
behavior changes and improvements.

- [`bfd07b2`](https://github.com/twmb/franz-go/commit/bfd07b2) kadm: fix occasionally empty topic/partitions in Lag
- [`00ac608`](https://github.com/twmb/franz-go/commit/00ac608) kadm: change FetchOffsetsForTopics to only return requested topics by default

v1.14.4
===

This small patch fixes kversion.VersionGuess to properly guess versions against
zookeeper broker versions v2.7 through 3.4. See the commit for details.

- [`5978156`](https://github.com/twmb/franz-go/commit/5978156) **bugfix** kversion: fix version detection for Kafka v2.7 through 3.4

v1.14.3
===

This patch fixes regex consuming a deleted topic causing an unending internal
loop of metadata reloading (trying to discover where the topic went).

- [`627d39a`](https://github.com/twmb/franz-go/commit/627d39a) **bugfix** kgo: fix / improve handling deleted topics while regex consuming

v1.14.2
===

This patch fixes an internal logic race that can be easily encountered when
specifying exact offsets to consume from. If you encountered this bug, your
consumer could just stop consuming for an indeterminite amount of time. This
bug has existed for a _long_ time and relies on both the client being slow and
the broker being fast to hit.

- [`1f696ca`](https://github.com/twmb/franz-go/commit/1f696ca) **bugfix** kgo: avoid a consumer logic race where the consumer stops consuming

v1.14.1
===

This patch release, quick on the heels of v1.14.0, fixes a race condition
introduced in v1.14 in the `PauseFetchTopics` and `PauseFetchPartitions`
functions, a second race condition that can occur when purging a topic, and
fully addresses [#493][https://github.com/twmb/franz-go/issues/493] which was
not completely addressed in v1.14.0.

- [`8c785fa`](https://github.com/twmb/franz-go/commit/8c785fa) **bugfix** kgo: fix race between client closing and purging
- [`dc5283e`](https://github.com/twmb/franz-go/commit/dc5283e) kgo: re-fix #493, supporting other buggy clients, and add a test
- [`32ac27f`](https://github.com/twmb/franz-go/commit/32ac27f) **bugfix** kgo: ensure assignPartitions is locked when pausing topics/partitions

v1.14.0
===

This release contains a few new APIs, one behavior change, and one minor bugfix.

## Bug fixes

Previously, `HookBrokerRead` and `HookBrokerE2E` could not be used at the same
time. This has been fixed.

## Behavior changes

`PauseFetch{Topics,Partitions}` now causes the client to drop all buffered
fetches and kill all in-flight fetch requests. Importantly, this also means
that once you pause, it is no longer possible for what you paused to be
returned while polling. Previously, the client made no attempt to clear
internal buffers / in flight requests, meaning you could receive paused data
for a while.

Seed brokers now show up in logs as `seed_###` rather than `seed ###` (an
underscore has been added).

## Features

* `kgo.Offset` now has an `EpochOffset` getter function that allows access
  to the actual epoch and offset that are inside the opaque `Offset` type.
* `AddConsumePartitions` allows adding individual partitions to consume, and
  the new counterpart `RemoveConsumePartitions` allows removing individual
  partitions from being consumed. Removing is different from purging, please
  see the docs.
* `KeepRetryableFetchErrors` bubbles up retryable errors to the end user that
  are encountered while fetching. By default, these errors are stripped.
* kversion now supports Kafka 3.5
* kversion now supports version guessing against KRaft by default
* `kgo.DialTLS` now exists to even more easily opt into TLS.
* `kgo.Client.Opts` now exists to return the original options that were used
  to configure the client, making initializing new clients easier.
* `kgo.NodeName` returns a string form of a broker node name. Internally, seed
  brokers use math.MinInt32 for node IDs, which shows up as massively negative
  numbers in logs sometimes. `NodeName` can help convert that to `seed_<#>`.

## Relevant commits

- [`c3b083b`](https://github.com/twmb/franz-go/commit/c3b083b) **improvement** kgo: do not returned paused topics/partitions after pausing
- [`e224e90`](https://github.com/twmb/franz-go/commit/e224e90) **bugfix** kgo: allow HookBrokerRead and HookBrokerE2E to both be called
- [`875761a`](https://github.com/twmb/franz-go/commit/875761a) **feature** kgo Offset: add EpochOffset getter field
- [`c5d0fc5`](https://github.com/twmb/franz-go/commit/c5d0fc5) kgo: add a debug log for stripping retryable errors from fetches
- [`b45d663`](https://github.com/twmb/franz-go/commit/b45d663) kgo: add more context to opportunistic metadata loads while fetching
- [`9dae366`](https://github.com/twmb/franz-go/commit/9dae366) kgo: allow retries on dial timeouts
- [`00e4e76`](https://github.com/twmb/franz-go/commit/00e4e76) kgo: tolerate buggy v1 group member metadata
- [`34c8b3d`](https://github.com/twmb/franz-go/commit/34c8b3d) **feature** kgo: add AddConsumePartitions and RemoveConsumePartitions
- [`b5cafba`](https://github.com/twmb/franz-go/commit/b5cafba) sasl: validate non-empty user/pass/token
- [`76d2e71`](https://github.com/twmb/franz-go/commit/76d2e71) **feature** kgo: add KeepRetryableFetchErrors
- [`0df3ec0`](https://github.com/twmb/franz-go/commit/0df3ec0) kgo: fix new niche CI problem against Kafka 3.5
- [`8ff1d0d`](https://github.com/twmb/franz-go/commit/8ff1d0d) **feature** pkg/kversion: attempt to guess KRaft by default as well
- [`e92f5d9`](https://github.com/twmb/franz-go/commit/e92f5d9) **feature** pkg/kversion: detect v3.5
- [`f1b923e`](https://github.com/twmb/franz-go/commit/f1b923e) **feature** kgo: add DialTLS option
- [`9667967`](https://github.com/twmb/franz-go/commit/9667967) **feature** kgo.Client: add Opts to return the original opts, to allow seeding new clients
- [`8e14928`](https://github.com/twmb/franz-go/commit/8e14928) **feature** kgo: add NodeName for easy user formatting of internal seed node IDs

v1.13.6
===

This minor patch release allows an injected fake fetch to be returned if that
is the only fetch ready to be returned. A fake fetch is a fetch that actually
does not contain any data, but instead contains an error (and may contain an
empty topic with a single 0 partition). Previously, a fake fetch could only be
returned if actual data was also ready to be returned. Now, a fake fetch can
always be returned. Fake fetches are most commonly experienced if you are
unable to join a group -- the client injects a fake fetch to notify you, the
client, that an error occurred. This patch actually allows you to consume that
error.

- [`5c87ce0`](https://github.com/twmb/franz-go/commit/5c87ce0) consumer: return from Poll if the only error is an injected fake

v1.13.5
===

This tiny patch release relaxes SASL/PLAIN to ignore any server response. This
patch is only useful if you are using this client against Tencent (at least as
we known at this moment). A normal broker does not reply to a successful PLAIN
auth with any data, but Tencent apparently does. Sarama and the Kafka client
itself both seem to ignore extra data once auth is successful, and if a broker
actually _rejected_ the auth then the broker would close the connection, so
ignoring this data seems fine.

- [`3addecc`](https://github.com/twmb/franz-go/commit/3addecc) sasl plain: ignore any challenge data

v1.13.4
===

This bugfix release fixes a race condition when calling client.Close.
This bug was introduced in commit [`e45cd72`](https://github.com/twmb/franz-go/commit/e45cd72) in
release v1.13.0. If a metadata request discovers a new broker as the client is
closing, there is a tiny window where the metadata goroutine can update a map
that the Close function is also concurrently accessing. The commit in this
release fixes that by guarding the Close access with a mutex.

- [`ee3d7c1`](https://github.com/twmb/franz-go/commit/ee3d7c1) **bugfix** kgo: fix race condition in close (thanks [@matino](https://github.com/matino)

v1.13.3
===

This minor patch release adds a few internal improvements and fixes no bugs.

Release 1.13.2 patched commits from occuring between join and sync; this patch
extends that to blocking LeaveGroup or Close if an active join and sync is
occurring. This is necessary because revoke callbacks are called while leaving
a group, and usually, you commit in a revoke callback.

A few extra logs are added, some areas of code are better, and a
context.Canceled error that could occasionally be returned in fetches when an
internal consumer session changed is no longer returned.

This release went through three+ straight days of looped integration testing on
two laptops to track down extremely rare test failures; those have been tracked
down.

Lastly, if you use a regex consumer and delete topics, the client now
internally purges and stops fetching those topics. Previously, any topic
discovered while regex consuming was permanently consumed until you manually
called `PurgeTopicsFromClient`.

- [`bb66f24`](https://github.com/twmb/franz-go/commit/bb66f24) kgo: purge missing-from-meta topics while regex consuming
- [`f72fdaf`](https://github.com/twmb/franz-go/commit/f72fdaf) kgo: always retry on NotLeader for sharded requests
- [`682d1f8`](https://github.com/twmb/franz-go/commit/682d1f8) kgo: add info log when the client is throttled
- [`88fa883`](https://github.com/twmb/franz-go/commit/88fa883) kgo: avoid counting pinReq version failures against retries
- [`de53fda`](https://github.com/twmb/franz-go/commit/de53fda) kgo: add a bit more context to sharded logs, avoid info log on Close
- [`7338bcf`](https://github.com/twmb/franz-go/commit/7338bcf) kgo: avoiding context.Canceled fetch from List/Epoch, improve testing&logs
- [`055b349`](https://github.com/twmb/franz-go/commit/055b349) consumer: do not use the partition epoch when assigning offsets
- [`d833f61`](https://github.com/twmb/franz-go/commit/d833f61) group consuming: block LeaveGroup between join&sync

v1.13.2
===

This patch improves the behavior of committing offsets while a rebalance is
happening.

In Kafka, if a commit arrives on the broker between JoinGroup and SyncGroup,
the commit is rejected with `REBALANCE_IN_PROGRESS`. If a commit is started
before JoinGroup but arrives to the broker after SyncGroup, the commit is
rejected with `INVALID_GENERATION`.

This patch changes how committing offsets interacts with join&sync: a client
that is joining the group will block OffsetCommit (preventing the first error),
and issuing an OffsetCommit will block the client from entering JoinGroup
(preventing the second error).

Previously, you could occasionally encounter these errors with the
cooperative-sticky group balancer because the standard behavior for eager
balancers is to commit in OnPartitionsRevoked before all partitions are dropped
at the end of a group session. You now should no longer encounter these errors
during a commit at all with any balancer unless something is going quite wrong
(massive client timeouts or broker hangs).

This does mean that OffsetCommit may take longer if you happen to commit in
while a client is joining and the join&sync takes a long time.

- [`ee70930`](https://github.com/twmb/franz-go/commit/ee70930) kgo groups: block join&sync while a commit is inflight

v1.13.1
===

This patch release fixes a bug where a producer could enter a deadlock if a
topic is deleted and recreated very quickly while producing.

- [`769e02f`](https://github.com/twmb/franz-go/commit/769e02f) producer: avoid deadlock when when quickly recreating a topic

v1.13.0
===

This release contains a few new APIs, two rare bug fixes, updates to plugins,
and changes the library to now require 1.18.

## Go version

This library has supported Go 1.15 since the beginning. There have been many
useful features that this library has not been able to use because of continued
backcompat for 1.15. There is really no reason to support such an old version
of Go, and Go itself does not support releases prior to 1.18 -- and 1.18 is
currently only supported for security backports. Switching to 1.18 allows this
library to remove a few 1.15 / 1.16 backcompat files, and allows switching this
library from `interface{}` to `any`.

## Behavior changes

If group consuming fails with an error that looks non-retryable, the error is
now injected into polling as a fake errored fetch. Multiple people have ran
into problems where their group consumers were failing due to ACLs or due to
network issues, and it is hard to detect these failures: you either have to pay
close attention to logs, or you have to hook into HookGroupManageError. Now,
the error is injected into polling.

## Bug fixes

This release contains two bug fixes, one of which is very rare to encounter, and
one of which is very easy to encounter but requires configuring the client in
a way that (nearly) nobody does.

Rare: If you were using EndAndBeginTransaction, there was an internal race that
could result in a deadlock.

Rare configuration: If you configured balancers manually, and you configured
CooperativeSticky with any other eager balancer, then the client would
internally _sometimes_ think it was eager consuming, and _sometimes_ think it
was cooperative consuming. This would result in stuck partitions while
consuming.

## Features

* HookClientClosed: A new hook that allows a callback when the client is closed.
* HookProduceRecordPartitioned: A new hook that is called when a record's partition is chosen.
* Client.ConfigValue: Returns the value for any configuration option.
* Client.ConfigValues: Returns the values for any configuration option (for multiple value options, or for strings that are internally string pointers).
* kadm: a few minor API improvements.
* plugin/klogr: A new plugin that satisfies the go-logr/logr interfaces.
* pkg/kfake: A new experimental package that will be added to over time, this mocks brokers and can be used in unit testing (only basic producing & consuming supported so far).

## Relevant commits

- [`1b229ce`](https://github.com/twmb/franz-go/commit/1b229ce) kgo: bugfix transaction ending & beginning
- [`461d2ef`](https://github.com/twmb/franz-go/commit/461d2ef) kgo: add HookClientClosed and HookProduceRecordPartitioned
- [`3a7f35e`](https://github.com/twmb/franz-go/commit/3a7f35e) kgo.Client: add UpdateFetchMaxBytes
- [`b0fa1a0`](https://github.com/twmb/franz-go/commit/b0fa1a0) kgo.Client: add ConfigValue and ConfigValues
- [`b487a15`](https://github.com/twmb/franz-go/commit/b487a15) kgo: inject the group lost error into polling as `*ErrGroupLost`
- [`bc638b0`](https://github.com/twmb/franz-go/commit/bc638b0) plugin/kotel.Tracer: add KeyFormatter, only accept utf8 keys
- [`a568b21`](https://github.com/twmb/franz-go/commit/a568b21) bugfix kgo: do not default to eager if there is any eager balancer
- [`bf20ac0`](https://github.com/twmb/franz-go/commit/bf20ac0) plugin/kzap: support LogLevelNone
- [`a9369be`](https://github.com/twmb/franz-go/commit/a9369be) kadm: add state config altering using the older AlterConfigs request
- [`f5ddf71`](https://github.com/twmb/franz-go/commit/f5ddf71) kadm: add NumPartitions,ReplicationFactor,Configs to CreateTopicResponse
- [`e45cd72`](https://github.com/twmb/franz-go/commit/e45cd72) consuming: close fetch sessions when closing the client
- [`d8230ca`](https://github.com/twmb/franz-go/commit/d8230ca) plugin/klogr: support for go-logr
- [`0f42e43`](https://github.com/twmb/franz-go/commit/0f42e43) check hooks and flatten slices of hooks

v1.12.1
===

This patch adds back the pre-v1.11.7 behavior of returning `UNKNOWN_TOPIC_ID`
when consuming. Patch v1.11.7 explicitly stripped this error because it was
found during testing that Kafka 3.4 now returns the error occasionally when
immediately consuming from a new topic (i.e. in tests), and the error is
explicitly marked as retryable within the Kafka source code. However, with
topic IDs, there is no way for a client to recover if the topic has been
deleted and recreated. By returning the error, end users can notified that
their client is in a fatal state and they should restart. In the future, I will
add an option to consume recreated topics.

- [`efe1cdb`](https://github.com/twmb/franz-go/commit/efe1cdb) consumer: return UnknownTopicID even though it is retryable

v1.12.0
===

This release is tested against Kafka 3.4, and kversion can now properly version
guess 3.4. There are two external kgo features in this release and there is one
internal feature.

Externally,

* `Client.CommitMarkedOffsets` makes committing marked offsets a bit easier
* `Client.UpdateSeedBrokers` allows for updating seed brokers, which can be useful on extremely long lived clients.

Internally,

* KIP-792 improves fencing of zombie group consumers using cooperative rebalancing

- [`c5f86ea`](https://github.com/twmb/franz-go/commit/c5f86ea) **feature** kgo.Client: add `UpdateSeedBrokers(...string) error`
- [`3e45339`](https://github.com/twmb/franz-go/commit/3e45339) **internal improvement** group balancer: support KIP-792
- [`fe727f8`](https://github.com/twmb/franz-go/commit/fe727f8) **feature** kversion: cut Kafka 3.4
- [`6751589`](https://github.com/twmb/franz-go/commit/6751589) **feature** pkg/kgo: add `Client.CommitMarkedOffsets` (thanks [@celrenheit](https://github.com/celrenheit)!)

v1.11.7
===

The integration test introduced in v1.11.6 was frequently failing in CI. After
some investigation, the panic is caused from `PurgeTopicsFromClient` _while_ a
load of offsets was happening (i.e. consuming was being initialized). That
race is specifically easily triggered in the integration test.

This patch also relaxes the error stripping while consuming. While consuming,
retryable errors are stripped. The errors that were stripped was a small
specific set that was originally taken from Sarama (nearly 4yr ago). Kafka 3.4
returns a new retryable error frequently if consuming against a topic
immediately after the topic is created. This patch changes the error set to
just strip all retryable errors, since the kerr package already indicates which
errors are retryable. This is more compatible against the broker returning
errors that are retryable.

This is the last commit in the v1.11 series -- immediately after tagging this,
I will be working on v1.12 which will officially test against Kafka 3.4.

- [`17567b0..78a12c3`](https://github.com/twmb/franz-go/compare/17567b0..78a12c3) **bugfix** consumer: fix potential panic when calling PurgeTopicsFromClient
- [`17567b0`](https://github.com/twmb/franz-go/commit/17567b0) **improvement** kgo: always strip retryable errors when consuming

v1.11.6
===

v1.11.5 introduced a regression when:

* Using a direct consumer (not a consumer group)
* Using `ConsumePartitions`
* Not specifying _all_ partitions in the topic

With the above setup, 1.11.5 introduced a bug that would consume all partitions
in the topic, i.e. all partitions in a topic that were not specified in
`ConsumePartitions`.

This patch restores the old behavior to only consume partitions that are
requested. This patch also makes it possible to purge topics or partitions from
the direct consumer, and makes it possible to re-add a topic. Lastly, three
integration tests were added to ensure that the problems fixed in 1.11.5 and in
this patch do not crop up again.

* [`edd0985`](https://github.com/twmb/franz-go/commit/edd0985) **bugfix** kgo: patch ConsumePartitions regression from 1.11.5

v1.11.5
===

v1.11.4 is retracted and because it actually requires a v2 release. When a
breaking kmsg release must happen, v2 of franz-go will happen and I will make a
change thorough enough such that major kmsg changes will not require a major
franz-go change again.

When this repo was initially written, the Kafka documentation for the wire
serialization of a Kafka record was wrong and different from how things were
actually implemented in the Java source. I used the documentation for my own
implementation. The documentation was [later fixed](https://github.com/apache/kafka/commit/94ccd4d).

What this means is that in this repo, `kmsg.Record.TimestampDelta` should be
changed from an int32 to int64. Rather than release a major version of kmsg, I
have added a `TimestampDelta64` field.

The old 32 bit timestamp delta meant that this package could only represent
records that had up to 24 days of a delta within a batch. Apparently, this may
happen in compacted topics.

More minor: previously, `AddConsumeTopics` did not work on direct consumers nor
on a client that consumed nothing to begin with. These shortcomings have been
addressed.

* [`12e3c11`](https://github.com/twmb/franz-go/commit/12e3c11) **bugfix** franz-go: support 64 bit timestamp deltas
* [`f613fb8`](https://github.com/twmb/franz-go/commit/f613fb8) **bugfix** pkg/kgo: patch AddConsumeTopics

v1.11.3
===

This patch release fixes a panic that can occur when fetching offsets in the
following scenario:

* You are fetching offsets for a group that the client has not yet loaded
  internally
* The internal load's FindCoordinator request fails OR the group cannot be
  loaded

FindCoordinator usually does not fail outright because the request internally
retries. As well, the group load is usually successful. Group loading only
fails if you are unauthorized to describe the group or if the group coordinator
is not loaded.

The most common case to encounter this error is when you issue a group request
against a new cluster. The first time a group request is seen, the group
coordinator loads. While loading, group requests are failed with
`COORDINATOR_LOAD_IN_PROGRESS` or some other similar error.

* [`5289ef6`](https://github.com/twmb/franz-go/commit/5289ef6) **bugfix** kgo.Client: avoid panic in OffsetFetchRequest when coordinator is not loaded

v1.11.2
===

This patch release fixes `HookFetchRecordUnbuffered` never being called if a
hook also implemented `HookFetchRecordBuffered`. No existing plugin currently
implements these hooks (though one will soon), so this patch is only relevant
to you if you manually have added these hooks.

* [`2a37df9`](https://github.com/twmb/franz-go/commit/2a37df9) **bugfix** kgo: patch HookFetchRecordUnbuffered


v1.11.1
===

This patch release fixes a bug in `ConsumePreferringLagFn`. The code could
panic if you:

* Consumed from two+ topics
* Two of the topics have a different amount of partitions
* The single-partition topic has some lag, the topic with more partitions has
  one partition with no lag, and another partition with _more_ lag than the
  single-partition topic

In this case, the code previously would create a non-existent partition to
consume from for the single-partition topic and this would immediately result
in a panic when the fetch request was built.

See the commit for more details.

* [`38f2ec6`](https://github.com/twmb/franz-go/commit/38f2ec6) **bugfix** pkg/kgo: bugfix ConsumePreferringLagFn

v1.11.0
===

This is a small release containing two minor features and a few behavior
improvements. The `MarkedOffsets` function allows for manually committing
marked offsets if you override `OnPartitionsRevoked`, and
`UnsafeAbortBufferedRecords` allows for forcefully dropping anything being
produced (albeit with documented caveats and downsides)

The client now guards against a broker that advertises FetchRequest v13+ (which
_only_ uses TopicIDs) but does not actually return / use TopicIDs. If you have
an old IBP configured, the broker will not use TopicIDs even if the broker
indicates it should. The client will now pin fetching to a max version of 12 if
a topic has no TopicID.

The client now sets a record's `Topic` field earlier to `DefaultProduceTopic`,
which allows the `Topic` field to be known present (or known non-present) in
the `OnRecordBuffered` hook.

Lastly, we now universally use Go 1.19 atomic types if compiled with 1.19+. Go
uses compiler intrinsics to ensure proper int64 / uint64 alignment within
structs for the atomic types; Go does not ensure plain int64 / uint64 are
properly aligned. A lot of work previously went into ensuring alignment and
having a GitHub workflow that ran `go vet` on qemu armv7 emulation, but
apparently that was not comprehensive enough. Now, if you use a 32 bit arch, it
is recommended to just compile with 1.19+.

* [`d1b6897`](https://github.com/twmb/franz-go/commit/d1b6897) **feature** kgo: add UnsafeAbortBufferedRecords
* [`d0c42ad`](https://github.com/twmb/franz-go/commit/d0c42ad) **improvement** kgo source: do not use fetch topic IDs if the broker returns no ID
* [`cc3a355`](https://github.com/twmb/franz-go/commit/cc3a355) and [`a2c4bad`](https://github.com/twmb/franz-go/commit/a2c4bad) **improvement** kgo: universally switch to 1.19's atomics if on Go 1.19+
* [`66e626f`](https://github.com/twmb/franz-go/commit/66e626f) producer: set Record.Topic earlier
* [`3186e61`](https://github.com/twmb/franz-go/commit/3186e61) **feature** kgo: add MarkedOffsets function

v1.10.4
===

This patch release fixes two bugs introduced with v1.10.0. These bugs are not
encountered in when using the client to simply consume or produce. Only admin
usages of the client may encounter the bugs this patch is fixing.

v1.10.0 introduced support for batch offset fetching or coordinator finding.
These changes introduced a bug where empty coordinator keys (i.e., group names
or transactional IDs) would be stripped from requests, and then a field in a
nil pointer could be accessed and panic the program. These changes also
introduced a bug that did not properly mirror one field for batched
`FindCoordinator` requests.

- [`ca67da4`](https://github.com/twmb/franz-go/commit/ca67da4) **bugfix** kgo: fix batch coordinator fetching
- [`c6f7f9a`](https://github.com/twmb/franz-go/commit/c6f7f9a) **bugfix** kgo: allow empty groups when finding coordinator / fetching offsets

v1.10.3
===

This small patch release is another attempted fix at [#239](https://github.com/twmb/franz-go/issues/239).
It is only possible to encounter this bug if a broker completely dies and never
comes back, and you do not replace the broker (i.e., broker 3 dies and it is
just gone forever).

Previously, kgo would cache the broker controller until `NOT_CONTROLLER` is
seen. We now clear it a bit more widely, but this is just extra defensive
behavior: the controller is updated on every metadata request.

Worse however, kgo previously cached group or transactional-id coordinators
until `COORDINATOR_NOT_AVAILABLE`, `COORDINATOR_LOAD_IN_PROGRESS`, or
`NOT_CONTROLLER` were seen. If the coordinator outright died and never comes
back and is never replaced, all coordinator requests to that specific
coordinator would fail.

Now, if we fail to dial the coordinator or controller 3x in a row, we delete
the coordinator or controller to force a reload on the next retry. We only do
this for dial errors because any other error means we actually contacted the
broker and it exists.

Lastly, we change the default max produce record batch bytes from 1,000,000 to
1,000,012, to exactly mirror Kafka's max.message.bytes.

- [`e2e80bf`](https://github.com/twmb/franz-go/commit/e2e80bf) kgo: clear controller/coordinator caches on failed dials

v1.10.2
===

This patch release contains one very minor bug fix, tightens a failure
scenario, adds two missing errors to kerr, fixes a build constraint, and has a
few internal style fixes from [@PleasingFungus][1.10.2:pf] (thanks!).

[1.10.2:pf]: https://github.com/PleasingFungus

The bug was introduced in v1.9.0 through a patch that fixed a potential spin
loop. In fixing the spin loop, I inadvertently caused consumer fetch sessions
to reset when there is no more data to consume. In your application, this would
show up as more frequent metadata updates and up to 100ms of extra latency when
there is new data to consume.

The tightened failure scenario allows records to be failed in more cases.
Previously, as soon as a record was added to a produce request internally, the
record could not be failed until a produce response is received. This behavior
exists for duplicate prevention. However, there is a period of time between a
produce request being created and actually being written, and if an
`AddPartitionsToTxn` request takes a long time and then fails, the produce
request would never be written and the records could never be failed. The
tightened failure scenario allows records to be failed all the way up until
they are actually serialized and written.

- [`d620765`](https://github.com/twmb/franz-go/commit/d620765) sink: tighten canFailFromLoadErrs
- [`6c0abd1`](https://github.com/twmb/franz-go/commit/6c0abd1) **minor bugfix** source: avoid backoff / session reset when there is no consumed data
- [`6ce8bdf`](https://github.com/twmb/franz-go/commit/6ce8bdf) kerr: add two missing errors to ErrorForCode
- [PR #264](https://github.com/twmb/franz-go/pull/264) fix `isNetClosedErr` build constraints: franz-go was using the `strings.Contains` version, which was meant for Go 1.15 only (thanks [@PleasingFungus][1.10.2:pf]!)

v1.10.1
===

This patch release contains minor kgo internal improvements, and enables a new
API for kadm. This patch contains no bug fixes. This patch should help recover
faster if a transaction coordinator goes down while producing.

This is released in tandem with kadm v1.6.0, which contains a small kadm bugfix
for `DeleteRecords`, `OffsetForLeaderEpoch`, and `DescribeProducers`, and
(hopefully) finishes support for all current admin APIs.

- [`56fcfb4`](https://github.com/twmb/franz-go/commit/56fcfb4) sink: log all aspects of wanting to / failing records
- [`9ee5efa`](https://github.com/twmb/franz-go/commit/9ee5efa) sink: update metadata when AddPartitionsToTxn fails repeatedly
- [`bc6810d`](https://github.com/twmb/franz-go/commit/bc6810d) broker: hide retryable errors *once*
- [`83f0dbe`](https://github.com/twmb/franz-go/commit/83f0dbe) kgo: add support for sharding WriteTxnMarkers

v1.10.0
===

This is a small release that contains one bug fix, one new feature, and
improvements in log lines, and improvements to work around AWS MSK being a bit
odd with SASL reauthentication.

Previously, the client's sticky partitioners actually did not preserve
stickiness because the client did not attach previous-partitions when rejoining
the group. That is now fixed.

The new feature, `ConsumePreferringLagFn`, allows you to have some advanced
reordering of how to consume. The recommended way of using this option is
`kgo.ConsumePreferringLagFn(kgo.PreferLagAt(50))`, which allows you to favor
laggy partitions if the client is more than 50 records behind in the topic.

The kadm module is also released with v1.4.0, which contains new APIs to find
coordinators for groups or transactional IDs, and an API to fetch API versions
for all brokers in the cluster.

- [`a995b1b`](https://github.com/twmb/franz-go/commit/a995b1b) kgo broker: retry sasl auth failures during reauthentication
- [`8ab8074`](https://github.com/twmb/franz-go/commit/8ab8074) kgo connection: always allow one request after SASL
- [`dcfcacb`](https://github.com/twmb/franz-go/commit/dcfcacb) **bugfix** `{Cooperative,Sticky}Balancer`: bug fix lack of stickiness
- [`76430a8`](https://github.com/twmb/franz-go/commit/76430a8) **feature** kgo: add `ConsumePreferringLagFn` to consume preferring laggy partitions
- [`9ac6c97`](https://github.com/twmb/franz-go/commit/9ac6c97) **improvement** kgo: support forward & backward batch requests for FindCoordinator, OffsetFetch


v1.9.1
===

This is a small patch release to work around two behavior problems, one with
AWS and one with Redpanda. This is not an important bug release if you are
using this library against Kafka itself.

For AWS, AWS is unexpectedly expiring certain permissions before the SASL
lifetime is up. This manifests as `GROUP_AUTHORIZATION_ERROR` while consuming.
Previously, we the client would mark connections to reauthenticate when the
connection was within 3s of SASL expiry. We now are more pessimistic and
reauthenticate within 95% to 98% of the lifetime, with a 2s minimum. This is
similar to the Java client, which has always used 85 to 95% of the SASL
lifetime and has no minimum.

For Redpanda, Redpanda's transaction support is nearly complete (v22.3 release
imminent), but Redpanda can return `UNKNOWN_SERVER_ERROR` a bit more than Kafka
does. These errors are being ironed out, but there is no harm in the client to
pre-emptively handling these as retryable.

- [`3ecaff2`](https://github.com/twmb/franz-go/commit/3ecaff2) kgo txn: handle `UNKNOWN_SERVER_ERROR` more widely
- [`eb6e3b5`](https://github.com/twmb/franz-go/commit/eb6e3b5) kgo sasl reauth: be more pessimistic

v1.9.0
===

This release contains one important bugfix (sequence number int32 overflow) for
long-lived producers, one minor bugfix that allows this client to work on 32
bit systems, and a few other small improvements.

This project now has integration tests ran on every PR (and it is now forbidden
to push directly to master). These integration tests run against Kraft (Kafka +
Raft), which itself seems to not be 100% polished. A good amount of
investigation went into hardening the client internals to not fail when Kraft
is going sideways.

This release also improves behavior when a consumer group leader using an
instance ID restarts _and_ changes the topics it wants to consume from. See the
KIP-814 commit for more details.

It is now easier to setup a TLS dialer with a custom dial timeout, it is easier
to detect if requests are failing due to missing SASL, and it is now possible
to print attributes with `RecordFormatter`.

Lastly, the corresponding kadm v1.3.0 release adds new LeaveGroup admin APIs.

#### franz-go

- [`b18341d`](https://github.com/twmb/franz-go/commit/b18341d) kgo: work around KIP-814 limitations
- [`6cac810`](https://github.com/twmb/franz-go/commit/6cac810) kversions: bump Stable from 3.0 to 3.3
- [PR #227](https://github.com/twmb/franz-go/pull/227) **bugfix** further sequence number overflows fix (thanks [@ladislavmacoun](https://github.com/ladislavmacoun)!)
- [PR #223](https://github.com/twmb/franz-go/pull/223) add GitHub actions integration test (thanks [@mihaitodor](https://github.com/mihaitodor)!) and [PR #224](https://github.com/twmb/franz-go/pull/224) fixup kgo guts to work around new kraft failures
- [`203a837`](https://github.com/twmb/franz-go/commit/203a837) franz-go: fix 32 bit alignment, fix a few lints
- [`719c6f4`](https://github.com/twmb/franz-go/commit/719c6f4) kgo: avoid overflow on 32 bit systems
- [`db5c159`](https://github.com/twmb/franz-go/commit/db5c159) **feature** kgo: add DialTimeout function, complementing DialTLSConfig
- [`b4aebf4`](https://github.com/twmb/franz-go/commit/b4aebf4) kgo: add ErrFirstReadEOF, which unwraps to io.EOF
- [`bbac68b`](https://github.com/twmb/franz-go/commit/bbac68b) RecordFormatter: support %a; formatter&reader: support 'bool'

#### kadm

- [`d3ee144`](https://github.com/twmb/franz-go/commit/d3ee144) kadm: add LeaveGroup api
- [`7b8d404`](https://github.com/twmb/franz-go/commit/7b8d404) kadm: ListOffsetsAfterMill(future) should return end offsets

v1.8.0
===

This feature release adds one new API in kgo, one new field on `kgo.Record`,
and stabilizes `kversion.V3_3_0()`. There is also one minor bug fix:
`Record.TimestampType()` previously did not correctly return 1 to indicate
broker generated timestamp types.

This release improves the behavior of the client if the entire cluster becomes
unreachable. Previously, the client may not have been able to reload metadata
to discover new endpoints; now, the client periodically tries a seed broker to
issue randomly-routable requests to. This release also makes a few more
internal errors retryable, which can help reduce some request failures.

A few docs have been improved. Notably, the UniformBytesPartitioner contains a
new line describing that it may have poor interaction with lingering. See the
doc comment for more details as well as the linked Kafka issue. This library
does not use lingering by default.

- [`f35ef66`](https://github.com/twmb/franz-go/commit/f35ef66) make `errUnknown{Controller,Coordinator}` retryable, improve error wording
- [`750bf54`](https://github.com/twmb/franz-go/commit/750bf54) kversion: stabilize v3.3
- [`3e02574`](https://github.com/twmb/franz-go/commit/3e02574) kgo: occasionally use seed brokers when choosing "random" brokers
- [`3ebd775`](https://github.com/twmb/franz-go/commit/3ebd775) UniformBytesPartitioner: note sub-optimal batches with lingering
- [PR #206](https://github.com/twmb/franz-go/pull/206) **bugfix** Record.TimestampType function to correctly return 1 for broker-set timestamps (thanks [@JacobSMoller](https://github.com/JacobSMoller))
- [PR #201](https://github.com/twmb/franz-go/pull/201) **feature** add Context field to Record to enable more end-user instrumentation (thanks [@yianni](https://github.com/yianni))
- [PR #197](https://github.com/twmb/franz-go/pull/197) **feature** add ValidateOpts to validate options before client initialization (thanks [@dwagin](https://github.com/dwagin))

v1.7.1
===

This release fixes two bugs, one that is obscure and unlikely to be ran into in
most use cases, and one that is not as obsure and may be ran into in a racy
scenario. It is recommended to upgrade.

[`3191842`][171a] fixes a bug that could eventually lead to a completely
stalled consumer. The problem can happen whenever an internal "consumer
session" is stopped and restarted -- which happens on most rebalances, and
happens whenever a partition moves from one broker to another. This logic race
required no active fetches to be in flight nor buffered, and required a fetch
to _just about_ be issued.

[`0ca6478`][171b] fixes a complicated bug that could result in a panic. It
requires the following:

* using a single client to produce and consume to the
* consuming from that topic first
* producing to that topic after the first consume
* the metadata load that is triggered from the produce fails with partition errors
* the metadata load retry moves a previously-errored partition from one broker to another

Any deviation from this sequence of events would not result in a panic. If the
final step did not move the partition between brokers, the client would still
be internally problematic, but there would be no visible problem (the partition
would be produced to two brokers, the produce to the wrong broker would fail
while the correct broker would succeed),

[171a]: https://github.com/twmb/franz-go/commit/3191842a81033342e8d37a529bd0a1b3d190fd9f
[171b]: https://github.com/twmb/franz-go/commit/0ca6478600c632deed4c7d65c13c3459d19071bd

## Relevant commits

- [`0ca6478`](https://github.com/twmb/franz-go/commit/0ca6478) kgo: avoid pointer reuse in metadata across producers & consumers
- [`3191842`](https://github.com/twmb/franz-go/commit/3191842) consumer: bugfix fetch concurrency loop
- [`5f24fae`](https://github.com/twmb/franz-go/commit/5f24fae) kgo: fix an incorrect log line, add another log line

v1.7.0
===

This feature release adds a few new APIs to expose more information in the
client or make some things simpler. There are also two minor bug fixes:
manually committing a topic with no partitions would previously result in an
error, and we previously did not properly load the partition leader epochs for
a brief period of time when the client started which prevented truncation
detection. See the commits below for more details.

- [`3a229d9`](https://github.com/twmb/franz-go/commit/3a229d9) kgo.Fetches: add Err0
- [`5dd3321`](https://github.com/twmb/franz-go/commit/5dd3321) kgo.EpochOffset: export Less, add docs
- [`2b38ec5`](https://github.com/twmb/franz-go/commit/2b38ec5) kgo: add CloseAllowingRebalance
- [`ac2f97b`](https://github.com/twmb/franz-go/commit/ac2f97b) kgo.Client: add GroupMetadata, ProducerID, PartitionLeader functions
- [`9497cf3`](https://github.com/twmb/franz-go/commit/9497cf3) **minor bugfix** kgo client.go: bugfix -1 leader epoch for a few minutes
- [`11e3277`](https://github.com/twmb/franz-go/commit/11e3277) **bugfix** consumer groups: do not commit empty topics
- [`52126de`](https://github.com/twmb/franz-go/commit/52126de) kgo: add optional interface to close sasls on client close
- [`#193`](https://github.com/twmb/franz-go/pull/193) **minor logging bugfix** Use correct error in debug log for request failure (thanks [@keitwb](https://github.com/keitwb))
- [`#191`](https://github.com/twmb/franz-go/pull/191) Add Fetches.NumRecords(), Fetches.Empty() (thanks [@un000](https://github.com/un000))
- [`669a761`](https://github.com/twmb/franz-go/commit/669a761) kerr: add two new errors

Also recently and newly released:

* [pkg/kmsg v1.2.0](https://github.com/twmb/franz-go/releases/tag/pkg%2Fkmsg%2Fv1.2.0), adding protocol support for new incoming KIPs.
* [pkg/kadm v1.2.1](https://github.com/twmb/franz-go/releases/tag/pkg%2Fkadm%2Fv1.2.1), removing sorting from metadata response replica fields (v1.2 added new helpers)
* [pkg/sasl/kerberos v1.1.0](https://github.com/twmb/franz-go/releases/tag/pkg%2Fsasl%2Fkerberos%2Fv1.1.0), making it easier to persist and shutdown the Kerberos client

v1.6.0
===

This release contains two new APIs, a minor bugfix, and other notable
improvements and stabilizations.

The [kadm][160kadmdoc] package is now stabilized as a separate,
[v1.0 module][160kadmtag]. This module will remain separate from the top level
franz-go so that it can have independent minor releases, and has more freedom
to have major version bumps if necessary. I took stabilization as an
opportunity to break & fix some infrequently used APIs; this can be seen in the
commit linked below.

I have also created a new package for the schema registry, [sr][160srdoc]. This
will likely live as a separate module for similar reasons to kmsg and kadm: to
have more freedom to make major-version-changing breaking changes if Confluent
decides to change some of their HTTP API. I expect to stabilize sr once I know
of at least one use case that helps double check the API is alright.

[160kadmdoc]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm
[160kadmtag]: https://github.com/twmb/franz-go/releases/tag/pkg%2Fkadm%2Fv1.0.0
[160srdoc]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/sr

## Bugfix

Previously, if you consumed a topic that had record timestamps generated by the
broker per `LogAppendTime`, this client would not set the timestamp properly
when consumed. We now set the timestamp properly.

## Improvements

The client now allows the user to set a timestamp when producing: if a record's
timestamp is non-zero, the client will not override it.

The default partitioner has been changed to the new `UniformBytesPartitioner`.
This is an improvement on the previous `StickyKeyPartitioner` in a few ways and
can be read about in [KIP-794][KIP-794]. The implementation in this client is
slightly different from KIP-794 in ways that are documented on the
`UniformBytesPartitioner` itself. This new partitioner can result in more
balanced load over a long period of producing to your brokers. Note: the new
default partitioner partitions keys the _same_ as the old default partitioner,
so this is not a behavior change.

## Features

* `UniformBytesPartitioner` exists, which can be used for more balanced
  producing over long time periods. The default batch size switch is 64KiB, and
you may want to tune this up or down if you produce in high or low throughput.

* `kversion.V3_2_0()` now officially exists, and kversion correctly detects
  v3.2 brokers.

* kmsg has a new `UnsafeReadFrom` interface to help reduce garbage in some
  advanced scenarios (requires kmsg@v1.1.0).

[KIP-794]: https://wiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner

## Relevant commits

- [`b279658`](https://github.com/twmb/franz-go/commit/b279658) kadm: break APIs for 1.0
- [`a23a076`](https://github.com/twmb/franz-go/commit/a23a076) **bugfix** source: properly set Timestamp for LogAppendTime records
- [`653010d`](https://github.com/twmb/franz-go/commit/653010d) **improvement** producer: allow the user to set Timestamp
- [`d53c0fe`](https://github.com/twmb/franz-go/commit/d53c0fe) **feature** kmsg: add new UnsafeReadFrom interface
- [`5536ec1`](https://github.com/twmb/franz-go/commit/5536ec1) **feature** kversion: add `V3_2_0`
- [`0f65bb1`](https://github.com/twmb/franz-go/commit/0f65bb1) **feature** kgo: expose UniformBytesPartitioner
- [`82af4a1`](https://github.com/twmb/franz-go/commit/82af4a1) and followup commits: add package sr, providing a client for the schema registry

v1.5.3
===

This bugfix release fixes a problem I noticed when reading Kafka source code
last week. KIP-98 never specified what a sequence number wrapping meant when
producing with idempotency enabled / with transactions, so I implemented this
to wrap to negative. As it turns out, there is validation in the Kafka code
that the sequence number must wrap to 0.

Thus, using this client, if an application produced `2**31` records to a single
partition with idempotent production or with transactions without restarting,
this client would enter a fatal state.

I'm not sure how common this use case is considering this bug has not been
reported in 2 years, but this patch release is to ensure people do not
encounter this bug after this point.

I've tested this by producing >2.147bil records to a single-partition Kafka topic
before this patch, which eventually results in `INVALID_RECORD` errors, and
then producing >2.2 records to a single-partition Kafka topic after this
patch. The patch allows the client to produce indefinitely.

This release also includes a few commits that eliminate two external
dependencies, and contains a patch in kadm to avoid a panic on nil input to a
function.

- [`4e2fa3f`](https://github.com/twmb/franz-go/commit/4e2fa3f) sticky: vendor go-rbtree, drop external dep
- [`6c0756d`](https://github.com/twmb/franz-go/commit/6c0756d) drop go-cmp dep
- [`27880b4`](https://github.com/twmb/franz-go/commit/27880b4) **bugfix** sink: sequence number wrapping for EOS
- [`afc9017`](https://github.com/twmb/franz-go/commit/afc9017) kadm.CalculateGroupLag: avoid panic if input maps are nil

v1.5.2
===

This tiny release contains another fix for `RecordReader`, specifically for
regex parsing, and changes the behavior of `MarkCommitRecords` to its original
behavior.

* For `RecordReader`, the regex type was not properly registered everywhere
  internally. This resulted in `%v{re#...#}\n` trying to parse a newline as
part of the regex, rather than as a delimiter. This is fixed and a test case
added. Note that this feature is niche _and_ new, so this is a very minor bug.

* For `MarkCommitRecords`, v1.3.3 changed the behavior to allow rewinds. This
  is difficult to reason about and can result in bugs. More likely, you just
want to periodically mark to move forward; the behavior now is more in line
with what people expect.

- [`ff5a3ed`](https://github.com/twmb/franz-go/commit/ff5a3ed) MarkCommitRecords: forbid rewinds
- [`41284b3`](https://github.com/twmb/franz-go/commit/41284b3) RecordReader: fix regex reading even more

v1.5.1
===

This release fixes a minor bug in `RecordReader`, and has a behavior change for
polling records (and a corresponding tiny new helper API for this).

For the bugfix, `RecordReader` did not always properly return
`io.ErrUnexpectedEOF`. We now return it more properly and add tests for the
missing edge cases.

For the behavior change, we now inject an error into `PollFetches` and
`PollRecords` if the user context is canceled. Previously, we would just quit
the poll and return. This change introduces a new corresponding function,
`Fetches.Err() error`. The thought here is that now we have more injected
errors, so `Fetches.Err` can make it easier to check various errors.

- [`cbc8962`](https://github.com/twmb/franz-go/commit/cbc8962) **behavior change** Poll{Records,Fetches}: inject an error for canceled context
- [#163](https://github.com/twmb/franz-go/pull/163) docs: updates StickyBalancer godoc (thanks [@Streppel](https://github.com/Streppel))
- [`2018d20`](https://github.com/twmb/franz-go/commit/2018d20) **bugfix** RecordReader: properly return `io.ErrUnexpectedEOF`
- [#161](https://github.com/twmb/franz-go/pull/161) examples: avoid duplicated module name (thanks [@robsonpeixoto](https://github.com/robsonpeixoto))

v1.5.0
===

This release adds a few new APIs, has a few small behavior changes, and has one
"breaking" change.

## Breaking changes

The `kerberos` package is now a dedicated separate module. Rather than
requiring a major version bump, since this fix is entirely at the module level
for an almost entirely unused package, I figured it is _okayish_ to technically
break compatibility for the few usages of this package, when the fix can be
done entirely when `go get`ing.

The [gokrb5](https://github.com/jcmturner/gokrb5) library, basically the only
library in the Go ecosystem that implements Kerberos, has a slightly [broken
license](https://github.com/jcmturner/gokrb5/issues/461). Organizations that
are sensitive to this were required to not use franz-go even if they did not
use Kerberos because franz-go pulls in a dependency on gokrb5.

Now, with `kerberos` being a distinct and separate module, depending on
franz-go only will _not_ cause an indirect dependency on gokrb5.

If your upgrade is broken by this change, run:

```go
go get github.com/twmb/franz-go/pkg/sasl/kerberos@v1.0.0
go get github.com/twmb/franz-go@v1.5.0
```

## Behavior changes

* `UnknownTopicRetries` now allows -1 to signal disabling the option (meaning
  unlimited retries, rather than no retries). This follows the convention of
other options where -1 disables limits.

## Improvements

* Waiting for unknown topics while producing now takes into account both the
  produce context and aborting. Previously, the record context was only taken
into account _after_ a topic was loaded. The same is true for aborting buffered
records: previously, abort would hang until a topic was loaded.

* New APIs are added to kmsg to deprecate the previous `Into` functions. The
  `Into` functions still exist and will not be removed until kadm is stabilized
(see [#141](https://github.com/twmb/franz-go/issues/141)).

## Features

* `ConsumeResetOffset` is now clearer, you can now use `NoResetOffset` with
  start _or_ end _or_ exact offsets, and there is now the very useful
`Offset.AfterMilli` function. Previously, `NoResetOffset` only allowed starting
consuming at the start and it was not obvious why. We keep the previous
default-to-start behavior, but we now allow modifying it. As well, `AfterMilli`
can be used to largely replace `AtEnd`. Odds are, you want to consume all
records after your program starts _even if_ new partitions are added to a
topic. Previously, if you added a partition to a topic, `AtEnd` would miss
records that were produced until the client refreshed metadata and discovered
the partition. Because of this, you were safer using `AtStart`, but this
unnecessarily forced you to consume everything on program start.

* Custom group balancers can now return errors, you can now intercept commits
  to attach metadata, and you can now intercept offset fetches to read
metadata.  Previously, none of this was possible. I considered metadata a bit
of a niche feature, but accessing it (as well as returning errors when
balancing) is required if you want to implement streams. New APIs now exist to
support the more advanced behavior: `PreCommitFnContext`, `OnOffsetsFetched`,
and `GroupMemberBalancerOrError`. As well, `BalancePlan.AsMemberIDMap` now
exists to provide access to a plan's underlying plan map. This did not exist
previously because I wanted to keep the type opaque for potential future
changes, but the odds of this are low and we can attempt forward compatibility
when the time arises.

* `RecordReader` now supports regular expressions for text values.

## Relevant commits

- [`a2cbbf8`](https://github.com/twmb/franz-go/commit/a2cbbf8) go.{mod,sum}: go get -u ./...; go mod tidy
- [`ce7a84f`](https://github.com/twmb/franz-go/commit/ce7a84f) kerberos: split into dedicated module, p1
- [`e8e5c82`](https://github.com/twmb/franz-go/commit/e8e5c82) and [`744a60e`](https://github.com/twmb/franz-go/commit/744a60e) kgo: improve ConsumeResetOffset, NoResetOffset, add Offset.AfterMilli
- [`78fff0f`](https://github.com/twmb/franz-go/commit/78fff0f) and [`e8e5117`](https://github.com/twmb/franz-go/commit/e8e5117) and [`b457742`](https://github.com/twmb/franz-go/commit/b457742): add GroupMemberBalancerOrError
- [`b5256c7`](https://github.com/twmb/franz-go/commit/b5256c7) kadm: fix long standing poor API (Into fns)
- [`8148c55`](https://github.com/twmb/franz-go/commit/8148c55) BalancePlan: add AsMemberIDMap
- [`113a2c0`](https://github.com/twmb/franz-go/commit/113a2c0) add OnOffsetsFetched function to allow inspecting commit metadata
- [`0a4f2ec`](https://github.com/twmb/franz-go/commit/0a4f2ec) and [`cba9e26`](https://github.com/twmb/franz-go/commit/cba9e26) kgo: add PreCommitFnContext, enabling pre-commit interceptors for metadata
- [`42e5b57`](https://github.com/twmb/franz-go/commit/42e5b57) producer: allow a canceled context & aborting to quit unknown wait
- [`96d647a`](https://github.com/twmb/franz-go/commit/96d647a) UnknownTopicRetries: allow -1 to disable the option
- [`001c6d3`](https://github.com/twmb/franz-go/commit/001c6d3) RecordReader: support regular expressions for text values

v1.4.2
===

This release fixes a potential incremental fetch session spin loop /
undesirable behavior. This was not reported, but can happen if you use many
clients against your cluster.

Previously, if a broker advertised that it supported consumer fetch sessions
but did not actually create any and returned "0" to signify no session was
created, the client would accept that 0 as a new fetch session. If the fetch
response returned no data and thus made no forward progress, the next fetch
request would include no partitions (believing a fetch session was created),
and the broker would again reply immediately with no data and no fetch session.
This would loop. Now, if the broker indicates no fetch session was created, we
immediately stop trying to create new fetch sessions and never try again.

In practice, fetch sessions are rejected if and replied to with 0 if a new one
cannot be created. The default fetch session cache in Kafka is 1,000. If you
have more than 1,000 active clients (where brokers count as clients against
other brokers), you are at risk of this bug.

This bug would manifest in clearly visible ways: higher cpu, no forward
progress while consuming. If you have not seen these, you have not experienced
the bug. However, it is recommended that all users upgrade to avoid it.

This has two followup fixes to [`83b0a32`][83b0a32], one which fixes behavior
that broke `EndBeginTxnSafe`, and one which mirrors some of the logic
supporting `EndBeginTxnUnsafe` into `EndTransaction` itself. This also fixes a
very rare data race that _realistically_ would result in a new connection being
killed immediately (since at the CPU, reads/writes of pointers is atomic).

- [`2faf459`](https://github.com/twmb/franz-go/commit/2faf459) **bugfix** broker: fix rare data race
- [`8f7c8cd`](https://github.com/twmb/franz-go/commit/8f7c8cd) **bugfix** EndBeginTxnUnsafe: partially back out of [`83b0a32`][83b0a32]
- [`85a680e`](https://github.com/twmb/franz-go/commit/85a680e) **bugfix** consuming: do not continually try to create fetch sessions
- [`2decd27`](https://github.com/twmb/franz-go/commit/2decd27) **bugfix** EndTransaction: mirror EndBeginTxnUnsafe logic

[83b0a32]: https://github.com/twmb/franz-go/commit/83b0a32

v1.4.1
===

This release pins kmsg to its newly stable version v1.0.0, fixing a compilation
issue for anybody doing `go get -u`.

The kmsg package was previously unversioned because Kafka sometimes changes the
protocol in such a way that breaks the API as chosen in kmsg (plain types, not
objects for everything). The most recent change in kmsg to match a recent type
rename in Kafka broke kgo because kgo depended on the old name. Again, this was
not pinned because franz-go did not depend on a specific version of kmsg. To
prevent this issue from happening again, we now pin to a stable kmsg version.

There are also two small bugfixes and a few improvements. Previously,
`ProducerFenced` was marked as retriable, which could result in the client
internally entering a fatal state that the user was unaware of. This should now
be bubbled up. As well, there were a few subtle issues with `EndBeginTxnUnsafe`
that have been addressed.

Notable commits & PRs:

- [`83b0a32`](https://github.com/twmb/franz-go/commit/83b0a32) **bugfix** kgo: EndAndBeginTransaction w/ EndBeginTxnUnsafe: fix three issues
- [`bd1d43d`](https://github.com/twmb/franz-go/commit/bd1d43d) sink: small AddPartitionsToTxn improvements
- [`65ca0bd`](https://github.com/twmb/franz-go/commit/65ca0bd) **bugfix** kerr: ProducerFenced is not retriable
- [PR #148](https://github.com/twmb/franz-go/pull/148) lower `FetchMaxPartitionBytes` to 1MiB to be in line with the Kafka default (thanks [@jcsp](https://github.com/jcsp))
- [`806cf53`](https://github.com/twmb/franz-go/commit/806cf53) **feature** kmsg: add TextMarshaler/TextUnmarshaler to enums
- [`49f678d`](https://github.com/twmb/franz-go/commit/49f678d) update deps, pulling in klauspost/compress v1.15.1 which makes zstd encoding & decoding stateless

v1.4.0
===

This release adds a lot of new features and changes a few internal behaviors.
The new features have been tested, but it is possible that a bug slipped by—if
you see one, please open an issue and the bug can be fixed promptly.

## Behavior changes

* **Promises are now serialized**. Previously, promises were called at the end
  of handling produce requests. As well, errors that caused records to fail
independent of producing could fail whenever. Now, all promises are called in
one loop. Benchmarking showed that concurrent promises did not really help,
even in cases where the promises could be concurrent. As well, my guess is that
most people serialize promises, resulting in more complicated logic punted to
the users. Now with serializing promises, user code can be simpler.

* The default `MetadataMinAge` has been lowered from 5s to 2.5s. Metadata
refreshes internally on retryable errors, 2.5s helps fail records for
non-existing topics quicker. Related, for sharded requests, we now cache topic
& partition metadata for the `MetadataMinAge`. This mostly benefits
`ListOffsets`, where usually a person may list both the start and end back to
back. We cannot cache indefinitely because a user may add partitions outside
this client, but 2.5s is still helpful especially for how infrequently sharded
requests are issued.

* Group leaders now track topics that the leader is not interested in
  consuming. Previously, if leader A consumed only topic foo and member B only
bar, then leader A would not notice if partitions were added to bar. Now, the
leader tracks bar. This behavior change only affects groups where the members
consume non-overlapping topics.

* Group joins & leaves now include a reason, as per KIP-800. This will be
  useful when Kafka 3.2 is released.

* Transactions no longer log `CONCURRENT_TRANSACTIONS` errors at the info
  level. This was a noisy log that meant nothing and was non-actionable. We
still track this at the debug level.

## Features

A few new APIs and options have been added. These will be described shortly
here, and the commits are linked below.

* `ConcurrentTransactionsBackoff`: a new option that allows configuring the
  backoff when starting a transaction runs into the `CONCURRENT_TRANSACTIONS`
error. Changing the backoff can decrease latency if Kafka is fast, but can
increase load on the cluster.

* `MaxProduceRequestsInflightPerBroker`: a new option that allows changing the
  max inflight produce requests per broker _if_ you disable idempotency.
Idempotency has an upper bound of 5 requests; by default, disabling idempotency
sets the max inflight to 1.

* `UnknownTopicRetries`: a new option that sets how many times a metadata load
  for a topic can return `UNKNOWN_TOPIC_OR_PARTITION` before all records
buffered for the topic are failed. As well, we now use this option more widely:
if a topic is loaded successfully and then later repeatedly experiences these
errors, records will be failed. Previously, this limit was internal and was
only applied before the topic was loaded successfully once.

* `NoResetOffset`: a new special offset that can be used with
  `ConsumeResetOffset` to trigger the client to enter a fatal state if
`OffsetOutOfRange` is encountered.

* `Client.PurgeTopicsFromClient`: a new API that allows for completely removing
a topic from the client. This can help if you regex consume and delete a topic,
or if you produce to random topics and then stop producing to some of them.

* `Client.AddConsumeTopics`: a new API that enables you to consume from topics
  that you did not initially configure. This enables you to add more topics to
consume from without restarting the client; this works both both direct
consumers and group consumers.

* `Client.TryProduce`: a new API that is a truly non-blocking produce. If the
  client has the maximum amount of records buffered, this function will
immediately fail a new promise with `ErrMaxBuffered`.

* `Client.ForceMetadataRefresh`: a new API that allows you to manually trigger
a metadata refresh. This can be useful if you added partitions to a topic and
want to trigger a metadata refresh to load those partitions sooner than the
default `MetadataMaxAge` refresh interval.

* `Client.EndAndBeginTransaction`: a new API that can be used to have higher
  throughput when producing transactionally. This API requires care; if you use
it, read the documentation for what it provides and any downsides.

* `BlockRebalancesOnPoll` and `Client.AllowRebalance`: a new option and
  corresponding required API that allows for easier reasoning about when
rebalances can happen. This option can be greatly beneficial to users for
simplifying code, but has a risk around taking so long that your group member
is booted from the group. Two examples were added using these options.

* `kversion.V3_1_0`: the kversion package now officially detects v3.1 and has
  an API for it.

## Relevant commits

- [PR #137](https://github.com/twmb/franz-go/pull/137) and [`c3fc8e0`](https://github.com/twmb/franz-go/commit/c3fc8e0): add two more goroutine per consumer examples (thanks [@JacobSMoller](https://github.com/JacobSMoller))
- [`cffbee7`](https://github.com/twmb/franz-go/commit/cffbee7) consumer: add BlockRebalancesOnPoll option, AllowRebalance (commit accidentally pluralized)
- [`39af436`](https://github.com/twmb/franz-go/commit/39af436) docs: add metrics-and-logging.md
- [`83dfa9d`](https://github.com/twmb/franz-go/commit/83dfa9d) client: add EndAndBeginTransaction
- [`d11066f`](https://github.com/twmb/franz-go/commit/d11066f) committing: internally retry on some errors when cooperative
- [`31f3f5f`](https://github.com/twmb/franz-go/commit/31f3f5f) producer: serialize promises
- [`e3ef142`](https://github.com/twmb/franz-go/commit/e3ef142) txn: move concurrent transactions log to debug level
- [`10ee8dd`](https://github.com/twmb/franz-go/commit/10ee8dd) group consuming: add reasons to JoinGroup, LeaveGroup per KIP-800
- [`0bfaf64`](https://github.com/twmb/franz-go/commit/0bfaf64) consumer group: track topics that the leader is not interested in
- [`e8495bb`](https://github.com/twmb/franz-go/commit/e8495bb) client: add ForceMetadataRefresh
- [`c763c9b`](https://github.com/twmb/franz-go/commit/c763c9b) consuming: add NoResetOffset
- [`4e0e1d7`](https://github.com/twmb/franz-go/commit/4e0e1d7) config: add UnknownTopicRetries option, use more widely
- [`7f58a97`](https://github.com/twmb/franz-go/commit/7f58a97) config: lower default MetadataMinAge to 2.5s
- [`e7bd28f`](https://github.com/twmb/franz-go/commit/e7bd28f) Client,GroupTransactSession: add TryProduce
- [`2a2cf66`](https://github.com/twmb/franz-go/commit/2a2cf66) consumer: add AddConsumeTopics
- [`d178e26`](https://github.com/twmb/franz-go/commit/d178e26) client: add PurgeTopicsFromClient
- [`336d2c9`](https://github.com/twmb/franz-go/commit/336d2c9) kgo: add ConcurrentTransactionsBackoff, MaxProduceRequestsInflightPerBroker
- [`fb04711`](https://github.com/twmb/franz-go/commit/fb04711) kversion: cut v3.1

v1.3.5
===

This patch release fixes a panic in `GroupTransactSession.End` and has three
behavior changes that are beneficial to users. The panic was introduced in
v1.3.0; if using a `GroupTransactSession`, it is recommended you upgrade to
this release.

The next release aims to be v1.4.0, this release is a small one to address a
few issues before the much larger and feature filled v1.4 release.

- [`010e8e1`](https://github.com/twmb/franz-go/commit/010e8e1) txn: fix panic in GroupTransactSession.End
- [`f9cd625`](https://github.com/twmb/franz-go/commit/f9cd625) consuming: handle exact offset consuming better
- [`2ab1978`](https://github.com/twmb/franz-go/commit/2ab1978) EndTransaction: return nil rather than an error if not in a transaction
- [`96bfe52`](https://github.com/twmb/franz-go/commit/96bfe52) broker: remove 5s minimum for sasl session lifetime

v1.3.4
===

This small patch release fixes a problem with
[`4f2e7fe3`](https://github.com/twmb/franz-go/commit/4f2e7fe3) which was meant
to address [#98](https://github.com/twmb/franz-go/issues/98). The fix was not
complete in that the fix would only trigger if a group member had partitions
added to it. We now rearrange the logic such that it occurs always. This bug
was found while making a change in the code in support of a new feature in the
v1.4 branch; this bug was not encountered in production.

This also bumps the franz-go/pkg/kmsg dependency so that `JoinGroup.Reason` is
properly tagged as v9+.

The next release will be v1.4, which is nearly ready to be merged into this
branch and tagged. Follow issue #135 for more details.

- [`02560c7`](https://github.com/twmb/franz-go/commit/02560c7) consumer group: bugfix fetch offsets spanning rebalance

v1.3.3
===

This patch release contains two minor bug fixes and a few small behavior
changes. The upcoming v1.4 release will contain more changes to lint the entire
codebase and will have a few new options as features to configure the client
internals. There are a few features in development yet that I would like to
complete before tagging the next minor release.

## Bug fixes

Repeated integration testing resulted in a rare data race, and one other bug
was found by linting. For the race, if a group was left _or_ heartbeating
stopped _before_ offset fetching finished, then there would be a concurrent
double write to an error variable: one write would try to write an error from
the request being cut (which would be `context.Canceled`), and the other write
would write the same error, but directly from `ctx.Err`. Since both of these
are the same type pointer and data pointer, it is unlikely this race would
result in anything if it was ever encountered, and encountering it would be
rare.

For the second bug, after this prior one, I wondered if any linter would have
caught this bug (the answer is no). However, in the process of heavily linting
the code base, a separate bug was found. This bug has **no impact**, but it is
good to fix. If you previously consumed and specified _exact_ offsets to
consume from, the internal `ListOffsets` request would use that offset as a
timestamp, rather than using -1 as I meant to internally. The whole point is
just to load the partition, so using a random number for a timestmap is just as
good as using -1, but we may as well use -1 to be proper.

## Behavior changes

Previously when producing, the buffer draining goroutine would sleep for 50ms
whenever it started. This was done to give high throughput producers a chance
to produce more records within the first batch, rather than the loop being so
fast that one record is in the first batch (and more in the others). This 50ms
sleep is a huge penalty to oneshot producers (producing one message at a time,
synchronously) and also was a slight penalty to high throughput producers
whenever the drain loop quit and needed to be resumed. We now eliminate this
sleep. This may result in more smaller batches, but definitely helps oneshot
producers. A linger can be used to avoid small batches, if needed.

Previously, due to MS Azure improperly replying to produce requests when acks
were zero, a discard goroutine was added to drain the produce connection if the
client was configured to produce with no acks. Logic has been added to quit
this goroutine if nothing is read on that goroutine for 3x the connection
timeout overhead, which is well enough time that a response should be received
if the broker is ever going to send one.

Previously, `MarkCommitRecords` would forbid rewinds and only allow advancing
offsets. The code now allows rewinds if you mark an early offset after you have
already marked a later offset. This brings the behavior in line with the
current `CommitOffsets`.

Previously, if the client encountered `CONCURRENT_TRANSACTIONS` during transactions,
it would sleep for 100ms and then retry the relevant request. This sleep has been
dropped to 20ms, which should help latency when transacting quickly. The v1.4
release will allow this number to be configured with a new option.

## Additions

KIP-784 and KIP-814 are now supported (unreleased yet in Kafka). Support for
KIP-814 required bumping the franz-go's kmsg dep. Internally, only KIP-814
affects client behavior, but since this is unrelased, it is not changing any
behavior.

## Relevant commits

- [`b39ca31`](https://github.com/twmb/franz-go/commit/b39ca31) fetchOffsets: fix data race
- [`4156e9f`](https://github.com/twmb/franz-go/commit/4156e9f) kgo: fix one bug found by linting
- [`72760bf..ad991d8`](https://github.com/twmb/franz-go/compare/72760bf..ad991d8) kmsg, kversion, kgo: support KIP-814 (SkipAssignment in JoinGroupResponse)
- [`db9017a`](https://github.com/twmb/franz-go/commit/db9017a) broker: allow the acks==0 producing discard goroutine to die
- [`eefb1f3`](https://github.com/twmb/franz-go/commit/eefb1f3) consuming: update docs & simplify; `MarkCommitRecords`: allow rewinds
- [`8808b94`](https://github.com/twmb/franz-go/commit/8808b94) sink: remove 50ms wait on new drain loops
- [`a13f918`](https://github.com/twmb/franz-go/commit/a13f918) kmsg & kversion: add support for KIP-784 (ErrorCode in DescribeLogDirs response)
- [PR #133](https://github.com/twmb/franz-go/pull/133) - lower concurrent transactions retry to 20ms; configurability will be in the next release (thanks [@eduard-netsajev](https://github.com/eduard-netsajev))

v1.3.2
===

This patch fixes a bug of unclear severity related to transactions. Credit goes
to [@eduard-netsajev](https://github.com/eduard-netsajev) for finding this long
standing problem.

In Kafka, if you try to start a transaction too soon after finishing the
previous one, Kafka may not actually have internally finished the prior
transaction yet and can return a `CONCURRENT_TRANSACTIONS` error. To work
around this, clients are expected to retry when they see this error (even
though it is marked as not retriable).

This client does that properly, but unfortunately did not bubble up any _non_
`CONCURRENT_TRANSACTIONS` errors.

From the code, it _appears_ as if in the worst case, this could have meant that
transactions invisibly looked like they were working and being used when they
actually were not. However, it's likely that other errors would be noticed
internally, and it's possible that if you encountered problems, the entire ETL
pipeline would stall anyway.

All told, it's not entirely clear what the ramifications for this bug are, and
it is recommended that if you use transactions, you should update immediately.

- [PR #131](https://github.com/twmb/franz-go/pull/131) - txns: don't ignore error in doWithConcurrentTransactions

v1.3.1
===

This small patch release fixes a leaked-goroutine problem after closing a
consuming client, and adds one config validation to hopefully reduce confusion.

For the bug, if a fetch was buffered when you were closing the client, an
internal goroutine would stay alive. In normal cases where you have one client
in your program and you close it on program shutdown, this leak is not really
important. If your program recreates consuming clients often and stays alive,
the leaked goroutine could eventually result in unexpected memory consumption.
Now, the client internally unbuffers all fetches at the end of `Close`, which
allows the previously-leaking goroutine to exit.

For the config validation, using `ConsumePartitions` and `ConsumeTopics` with
the same topic in both options would silently ignore the topic in
`ConsumePartitions`. Now, the client will return an error that using the same
topic in both options is invalid.

- [`ea11266`](https://github.com/twmb/franz-go/commit/ea11266) config: add duplicately specified topic validation
- [`bb581f4`](https://github.com/twmb/franz-go/commit/bb581f4) **bugfix** client: unbuffer fetches on Close to allow mangeFetchConcurrency to quit

v1.3.0
===

This release contains three new features, a few behavior changes, and one minor
bugfix.

For features, you can now adjust fetched offsets before they are used (thanks
@michaelwilner!), you can now "ping" your cluster to see if the client can
connect at all, and you can now use `SetOffsets` when consuming partitions
manually. As a somewhat of a feature-ish, producing no longer requires a
context, instead if a context is nil, `context.Background` is used (this was
added to allow more laziness when writing small unimportant files).

The transactional behavior change is important: the documentation changes are
worth reading, and it is worth using a 2.5+ cluster along with the
`RequireStableFetchOffsets` option if possible. The metadata leader epoch
rewinding behavior change allows the client to continue in the event of odd
cluster issues.

In kadm, we now return individual per-partition errors if partitions are not
included in OffsetCommit responses. The generated code now has a few more enums
(thanks [@weeco](https://github.com/weeco)!)

Lastly, as a small bugfix, `client.Close()` did not properly stop seed brokers.
A previous commit split seed brokers and non-seed brokers internally into two
fields but did not add broker shutdown on the now-split seed broker field.

- [`e0b520c`](https://github.com/twmb/franz-go/commit/e0b520c) **behavior change** kadm: set per-partition errors on missing offsets in CommitOffsets
- [`32425df`](https://github.com/twmb/franz-go/commit/32425df) **feature** client: add Ping method
- [`a059901`](https://github.com/twmb/franz-go/commit/a059901) **behavior change**  txns: sleep 200ms on commit, preventing rebalance / new commit
- [`12eaa1e`](https://github.com/twmb/franz-go/commit/12eaa1e) **behavior change** metadata: allow leader epoch rewinds after 5 tries
- [`029e655`](https://github.com/twmb/franz-go/commit/029e655) **feature-ish** Produce{,Sync}: default to context.Background if no ctx is provided
- [`eb2cec3`](https://github.com/twmb/franz-go/commit/eb2cec3) **bugfix** client: stop seed brokers on client.Close
- [`2eae20d`](https://github.com/twmb/franz-go/commit/2eae20d) **feature** consumer: allow SetOffsets for direct partition consuming
- [pr #120](https://github.com/twmb/franz-go/pull/120) **feature** Add groupopt to swizzle offset assignments before consumption (thanks [@michaelwilner](https://github.com/michaelwilner)!)

v1.2.6
===

This patch release contains a behavior change to immediate metadata triggers to
better support some loading error conditions and drops the default
MetadataMinAge from 10s to 5s. For more details, see commit
[`8325ba7`][8325ba7] or issue [114][issue114].

This release also contains a bugfix for using preferred read replicas.
Previously, if a replica was not the leader of any partition being consumed,
the client would not internally save knowledge of that replica and it could not
be consumed from. This would result in the client ping-ponging trying to fetch
from the leader and then erroring when the leader indicates the client should
consume from a replica the client does not know about. This is no longer the
case.

Lastly, this patch fixes the behavior of rotating through partitions while
consuming. Previously, the client rotated through partitions to evaluate for
consuming, but then saved those partitions to nested maps which had
non-deterministic (and, due to how Go ranges, mostly not-that-random) range
semantics. The client now always rotates through partitions and topics in the
order the partitions are evaluated. The old behavior could theoretically lead
to more starvation than necessary while consuming. The new behavior should have
no starvation, but really due to the randomness of the prior behavior, the new
semantics may be a wash. However, the new semantics are the original intended
semantics.

- [`bc391a3`](https://github.com/twmb/franz-go/commit/bc391a3) **improvement** source: rotate through topics/partitions as we fetch
- [`6bbdaa2`](https://github.com/twmb/franz-go/commit/6bbdaa2) **bugfix** client: create a sink & source for all partition replicas
- [`8325ba7`][8325ba7] **behavior change** metadata: minor changes, & wait a bit when looping for now triggers
- [`b8b7bd1`](https://github.com/twmb/franz-go/commit/b8b7bd1) **behavior change** RecordFormatter: always use UTC
- [`6ab9044`](https://github.com/twmb/franz-go/commit/6ab9044) kadm: return defined errors for On functions

[8325ba7]: https://github.com/twmb/franz-go/commit/8325ba7
[issue114]: https://github.com/twmb/franz-go/issues/114

v1.2.5
===

This small patch release fixes one non-impacting bug in SCRAM authentication,
and allows more errors to be abort&retryable rather than fatal while
transactionally committing.

For SCRAM, this client did not implement the client-final-reply completely
correctly: this client replied with just the `client nonce`, not the `client
nonce + server nonce`. Technically this was not to spec, but no broker today
enforces this final nonce correctly. However, the client has been fixed so that
if someday brokers do start enforcing the nonce correctly, this client will be
ready.

For transactional committing, we can handle a few extra errors while committing
without entering a fatal state. Previously, `ILLEGAL_GENERATION` was handled:
this meant that a rebalance began and completed before the client's commit went
through. In this case, we just aborted the transaction and continued
successfully. We can do this same thing for `REBALANCE_IN_PROGRESS`, which is
similar to the prior error, as well as for errors that result from client
request retry limits.

The integration tests now no longer depend on `kcl`, meaning you can simply `go
test` in the `kgo` package to test against your local brokers. Seeds can be
provided by specifying a `KGO_SEEDS` environment variable, otherwise the
default is 127.0.0.1:9092.

Lastly, a few bugs have been fixed in the not-yet-stable,
currently-separate-module kadm package. If you use that package, you may have
already pulled in these fixes.

- [`17dfae8`](https://github.com/twmb/franz-go/commit/17dfae8) go.{mod,sum}: update deps
- [`3b34db0`](https://github.com/twmb/franz-go/commit/3b34db0) txn test: remove increasing-from-0 strictness when producing
- [`d0a27f3`](https://github.com/twmb/franz-go/commit/d0a27f3) testing: remove dependency on kcl
- [`8edf934`](https://github.com/twmb/franz-go/commit/8edf934) txn: allow more commit errors to just trigger abort
- [`03c58cb`](https://github.com/twmb/franz-go/commit/03c58cb) scram: use c-nonce s-nonce, not just c-nonce, in client-reply-final
- [`8f34083`](https://github.com/twmb/franz-go/commit/8f34083) consumer group: avoid regex log if no topics were added/skipped

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
RequestTimeoutOverhead same as everything else. The default produce request
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
- [`d378b32`](https://github.com/twmb/franz-go/commit/d378b32) config: edit comment for FetchMaxWait (about Java setting) (thanks [@dwagin](https://github.com/dwagin)!)
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
- [`2d6c1a8`](https://github.com/twmb/franz-go/commit/2d6c1a8) **behavior change** client: lower RequestTimeoutOverhead and RetryTimeout defaults
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
- [`cedffb7`](https://github.com/twmb/franz-go/commit/cedffb7) **feature** plugins: Add kzerolog logging adapter (thanks [@fsaintjacques](https://github.com/fsaintjacques)!)
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

- [`1469495`](https://github.com/twmb/franz-go/commit/1469495) **bugfix** isRetriableBrokerErr: nil error is **not** retriable (thanks [@Neal](https://github.com/Neal)!)
- [`33635e2`](https://github.com/twmb/franz-go/commit/33635e2) **feature** consumer: add pausing / unpausing topics and partitions
- [`082db89`](https://github.com/twmb/franz-go/commit/082db89) **bugfix** add locking to g.uncommitted MarkCommitRecords (thanks [@vtolstov](https://github.com/vtolstov)!)
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
- [`8da4eaa`](https://github.com/twmb/franz-go/commit/8da4eaa) and [`37ecd21`](https://github.com/twmb/franz-go/commit/37ecd21) **feature** client: enable ipv6 support in seed brokers (thanks [@vtolstov](https://github.com/vtolstov) and [@SteadBytes](https://github.com/SteadBytes)!)
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
`AllowAutoTopicCreation`, and `ProduceTopic` is now `DefaultProduceTopic`.

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
- [pr #22](https://github.com/twmb/franz-go/pull/22): Add transactional producer / consumer example (thanks [@dcrodman](https://github.com/dcrodman)!)
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
- [pr #4](https://github.com/twmb/franz-go/pull/4): Redesign readme (thanks [@weeco](https://github.com/weeco)!)

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
- [`303186a`](https://github.com/twmb/franz-go/commit/303186a) Add BrokerThrottleHook (thanks [@akesle](https://github.com/akesle))
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
- [PR #12](https://github.com/twmb/franz-go/pull/12): Add clarifying wording on NewClient (thanks [@weeco](https://github.com/weeco)!)
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
