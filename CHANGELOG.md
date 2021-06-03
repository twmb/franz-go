v0.7.9
===

- [`5231902`](https://github.com/twmb/franz-go/commit/5231902) **bugfix** patch on prior commit

If I could yank tags, I would. Nice 10 minutes between them though! 🙃

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
