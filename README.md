franz-go - A complete Apache Kafka client written in Go
===

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)][godev]
![GitHub](https://img.shields.io/github/license/twmb/franz-go)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/twmb/franz-go)
[![Discord Chat](https://img.shields.io/badge/discord-online-brightgreen.svg)](https://discord.gg/K4R5c8zsMS)

[godev]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo

Franz-go is an all-encompassing Apache Kafka client fully written Go. This library aims to provide **every Kafka feature** from 
Apache Kafka v0.8.0 onward. It has support for transactions, regex topic consuming, the latest partitioning strategies,
data loss detection, closest replica fetching, and more. If a client KIP exists, this library aims to support it.

This library attempts to provide an intuitive API while interacting with Kafka the way Kafka expects (timeouts, etc.).

## Features

- Feature complete client (Kafka >= 0.8.0 through v2.8.0+)
- Full Exactly-Once-Semantics (EOS)
- Idempotent & transactional producers
- Simple (legacy) consumer
- Group consumers with eager (roundrobin, range, sticky) and cooperative (cooperative-sticky) balancers
- All compression types supported: gzip, snappy, lz4, zstd
- SSL/TLS provided through custom dialer options
- All SASL mechanisms supported (GSSAPI/Kerberos, PLAIN, SCRAM, and OAUTHBEARER)
- Low-level admin functionality supported through a simple `Request` function
- Utilizes modern & idiomatic Go (support for contexts, variadic configuration options, ...)
- Highly performant by avoiding channels and goroutines where not necessary
- Written in pure Go (no wrapper lib for a C library or other bindings)
- Ability to add detailed log messages or metrics using hooks

## Getting started

Here's a basic overview of producing and consuming:

```go
seeds := []string{"localhost:9092"}
cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
if err != nil {
	panic(err)
}
defer cl.Close()

ctx := context.Background()

// 1.) Producing a message
// All record production goes through Produce, and the callback can be used
// to allow for synchronous or asynchronous production.
var wg sync.WaitGroup
wg.Add(1)
record := &kgo.Record{Topic: "foo", Value: []byte("bar")}
cl.Produce(ctx, record, func(_ *Record, err error) {
	defer wg.Done()
	if err != nil {
		fmt.Printf("record had a produce error: %v\n", err)
	}

})
wg.Wait()

// Alternatively, ProduceSync exists to synchronously produce a batch of records.
if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
	fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
}

// 2.) Consuming messages from a topic
// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
cl.AssignGroup("my-group-identifier", kgo.GroupTopics("foo"))
for {
	fetches := cl.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retriable errors are
		// returned from polls so that users can notice and take action.
		panic(fmt.Sprint(errs))
	}

	// We can iterate through a record iterator...
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		fmt.Println(string(record.Value), "from an iterator!")
	}

	// or a callback function.
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, record := range p.Partition.Records {
			fmt.Println(string(record.Value), "from range inside a callback!")
		}

		// We can even use a second callback!
		p.EachRecord(func(record *Record) {
			fmt.Println(string(record.Value), "from a second callback!")
		})
	})
}
```

This only shows producing and consuming in the most basic sense, and does not
show the full list of options to customize how the client runs, nor does it
show transactional producing / consuming. Check out the [examples](./examples)
directory for more!

API reference documentation can be found on
[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)][godev].
Supplementary information can be found in the docs directory:

<pre>
<a href="./docs">docs</a>
├── <a href="./docs/admin-requests.md">admin requests</a> - an overview of how to issue admin requests
├── <a href="./docs/package-layout.md">package layout</a> - describes the packages in franz-go
├── <a href="./docs/producing-and-consuming.md">producing and consuming</a> - descriptions of producing & consuming & the guarantees
└── <a href="./docs/transactions.md">transactions</a> - a description of transactions and the safety even in a pre-KIP-447 world
</pre>

## Version Pinning

By default, the client issues an ApiVersions request on connect to brokers
and defaults to using the maximum supported version for requests that each
broker supports.

Kafka 0.10.0 introduced the ApiVersions request; if you are working
with brokers older than that, you must use the kversions package. Use the 
MaxVersions option for the client if you do so.

As well, it is recommended to set the MaxVersions to the version of your
broker cluster. Until [KIP-584][5] is implemented, it is possible that
if you do not pin a max version, this client will speak with some features
to one broker while not to another when you are in the middle of a broker
update roll.

[5]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features

## Metrics & logging

The franz-go client takes a neutral approach to metrics by providing hooks
that you can use to plug in your own metrics.

All connections, disconnections, reads, writes, and throttles can be hooked
into.  If there is an aspect of the library that you wish you could have
insight into, please open an issue and we can discuss adding another hook.

Hooks allow you to log in the event of specific errors, or to trace latencies,
count bytes, etc., all with your favorite monitoring systems.

In addition to hooks, logging can be plugged in with a general `Logger`
interface.  A basic logger is provided if you just want to write to a given
file in a simple format. All logs have a message and then key/value pairs of
supplementary information. It is recommended to always use a logger and to use
`LogLevelInfo`.

See [this example](./examples/hooks_and_logging/prometheus) for an example of
integrating with prometheus!

## Benchmarks

This client is quite fast; it is the fastest and most cpu and memory efficient
client in Go.

For 100 byte messages,

- This client is 4x faster at producing than confluent-kafka-go, and up to
  10x-20x faster (at the expense of more memory usage) at consuming.

- This client is 2.5x faster at producing than sarama, and 1.5x faster at
  consuming.

- This client is 2.4x faster at producing than segment's kafka-go, and so
  much faster at consuming that I'm not sure I wrote the consuming comparison
  correctly here.

To check benchmarks yourself, see the [bench](./examples/bench) example. This
example lets you produce or consume to a cluster and see the byte / record
rate. The [compare](./examples/bench/compare) subdirectory shows comparison
code.

## Supported KIPs

Theoretically, this library supports every (non-Java-specific) client facing
KIP. Any KIP that simply adds or modifies a protocol is supported by code
generation.

| KIP |  Kafka release | Status |
|-----|----------------|--------|
| [KIP-1](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1+-+Remove+support+of+request.required.acks) — Disallow acks > 1 | 0.8.3 | Supported & Enforced |
| [KIP-4](https://cwiki.apache.org/confluence/display/KAFKA/KIP-4+-+Command+line+and+centralized+administrative+operations) — Request protocol changes | 0.9.0 through 0.10.1 |  Supported |
| [KIP-8](https://cwiki.apache.org/confluence/display/KAFKA/KIP-8+-+Add+a+flush+method+to+the+producer+API) — Flush method on Producer | 0.8.3 | Supported |
| [KIP-12](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=51809888) — SASL & SSL | 0.9.0 | Supported |
| [KIP-13](https://cwiki.apache.org/confluence/display/KAFKA/KIP-13+-+Quotas) — Throttling (on broker) | 0.9.0 | Supported |
| [KIP-15](https://cwiki.apache.org/confluence/display/KAFKA/KIP-15+-+Add+a+close+method+with+a+timeout+in+the+producer) — Close with a timeout | 0.9.0 | Supported (via context) |
| [KIP-19](https://cwiki.apache.org/confluence/display/KAFKA/KIP-19+-+Add+a+request+timeout+to+NetworkClient) — Request timeouts | 0.9.0 | Supported |
| [KIP-22](https://cwiki.apache.org/confluence/display/KAFKA/KIP-22+-+Expose+a+Partitioner+interface+in+the+new+producer) — Custom partitioners | 0.9.0 | Supported |
| [KIP-31](https://cwiki.apache.org/confluence/display/KAFKA/KIP-31+-+Move+to+relative+offsets+in+compressed+message+sets) — Relative offsets in message sets | 0.10.0 | Supported |
| [KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message) — Timestamps in message set v1 | 0.10.0 | Supported |
| [KIP-35](https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version) — ApiVersion | 0.10.0 | Supported |
| [KIP-40](https://cwiki.apache.org/confluence/display/KAFKA/KIP-40%3A+ListGroups+and+DescribeGroup) — ListGroups and DescribeGroups | 0.9.0 | Supported |
| [KIP-41](https://cwiki.apache.org/confluence/display/KAFKA/KIP-41%3A+KafkaConsumer+Max+Records) — max.poll.records | 0.10.0 | Supported (via PollRecords) |
| [KIP-42](https://cwiki.apache.org/confluence/display/KAFKA/KIP-42%3A+Add+Producer+and+Consumer+Interceptors) — Producer & consumer interceptors | 0.10.0 | Partial support (hooks) |
| [KIP-43](https://cwiki.apache.org/confluence/display/KAFKA/KIP-43%3A+Kafka+SASL+enhancements) — SASL PLAIN & handshake | 0.10.0 | Supported |
| [KIP-48](https://cwiki.apache.org/confluence/display/KAFKA/KIP-48+Delegation+token+support+for+Kafka) — Delegation tokens | 1.1.0 | Supported |
| [KIP-54](https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy) — Sticky partitioning | 0.11.0 | Supported |
| [KIP-57](https://cwiki.apache.org/confluence/display/KAFKA/KIP-57+-+Interoperable+LZ4+Framing) — Fix lz4 | 0.10.0 | Supported |
| [KIP-62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread) — background heartbeats & improvements | 0.10.1 | Supported |
| [KIP-70](https://cwiki.apache.org/confluence/display/KAFKA/KIP-70%3A+Revise+Partition+Assignment+Semantics+on+New+Consumer%27s+Subscription+Change) — On{Assigned,Revoked} | 0.10.1 | Supported |
| [KIP-74](https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes) — Fetch response size limits | 0.10.1 | Supported |
| [KIP-78](https://cwiki.apache.org/confluence/display/KAFKA/KIP-78%3A+Cluster+Id) — ClusterID in Metadata | 0.10.1 | Supported |
| [KIP-79](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090) — List offsets for times | 0.10.1 | Supported |
| [KIP-81](https://cwiki.apache.org/confluence/display/KAFKA/KIP-81%3A+Bound+Fetch+memory+usage+in+the+consumer) — Bound fetch memory usage | WIP | Supported (through a combo of options) |
| [KIP-82](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers) — Record headers | 0.11.0 | Supported |
| [KIP-84](https://cwiki.apache.org/confluence/display/KAFKA/KIP-84%3A+Support+SASL+SCRAM+mechanisms) — SASL SCRAM | 0.10.2 | Supported |
| [KIP-86](https://cwiki.apache.org/confluence/display/KAFKA/KIP-86%3A+Configurable+SASL+callback+handlers) — SASL Callbacks | 0.10.2 | Supported (through callback fns) |
| [KIP-88](https://cwiki.apache.org/confluence/display/KAFKA/KIP-88%3A+OffsetFetch+Protocol+Update) — OffsetFetch for admins | 0.10.2 | Supported
| [KIP-91](https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer) — Intuitive producer timeouts | 2.1.0 | Supported (as a matter of opinion) |
| [KIP-97](https://cwiki.apache.org/confluence/display/KAFKA/KIP-97%3A+Improved+Kafka+Client+RPC+Compatibility+Policy) — Backwards compat for old brokers | 0.10.2 | Supported |
| [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) — EOS | 0.11.0 | Supported |
| [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation) — OffsetForLeaderEpoch v0 | 0.11.0 | Supported |
| [KIP-102](https://cwiki.apache.org/confluence/display/KAFKA/KIP-102+-+Add+close+with+timeout+for+consumers) — Consumer close timeouts | 0.10.2 | Supported (via context) |
| [KIP-107](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+deleteRecordsBefore%28%29+API+in+AdminClient) — DeleteRecords | 0.11.0 | Supported |
| [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy) — CreateTopic validate only field | 0.10.2 | Supported |
| [KIP-110](https://cwiki.apache.org/confluence/display/KAFKA/KIP-110%3A+Add+Codec+for+ZStandard+Compression) — zstd | 2.1.0 | Supported |
| [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD) — Broker request protocol changes | 1.0.0 | Supported |
| [KIP-113](https://cwiki.apache.org/confluence/display/KAFKA/KIP-113%3A+Support+replicas+movement+between+log+directories) — LogDir requests | 1.0.0 | Supported |
| [KIP-117](https://cwiki.apache.org/confluence/display/KAFKA/KIP-117%3A+Add+a+public+AdminClient+API+for+Kafka+admin+operations) — Admin client | 0.11.0 | Supported (via kmsg) |
| [KIP-124](https://cwiki.apache.org/confluence/display/KAFKA/KIP-124+-+Request+rate+quotas) — Request rate quotas | 0.11.0 | Supported |
| [KIP-126](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=68715855) — Ensure proper batch size after compression | 0.11.0 | Supported (avoided entirely) |
| [KIP-133](https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs) — Describe & Alter configs | 0.11.0 | Supported |
| [KIP-140](https://cwiki.apache.org/confluence/display/KAFKA/KIP-140%3A+Add+administrative+RPCs+for+adding%2C+deleting%2C+and+listing+ACLs) — ACLs | 0.11.0 | Supported |
| [KIP-144](https://cwiki.apache.org/confluence/display/KAFKA/KIP-144%3A+Exponential+backoff+for+broker+reconnect+attempts) — Broker reconnect backoff | 0.11.0 | Supported |
| [KIP-152](https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures) — More SASL; SASLAuthenticate | 1.0.0 | Supported |
| [KIP-183](https://cwiki.apache.org/confluence/display/KAFKA/KIP-183+-+Change+PreferredReplicaLeaderElectionCommand+to+use+AdminClient) — Elect preferred leaders | 2.2.0 | Supported |
| [KIP-185](https://cwiki.apache.org/confluence/display/KAFKA/KIP-185%3A+Make+exactly+once+in+order+delivery+per+partition+the+default+Producer+setting) — Idempotency is default | 1.0.0 | Supported |
| [KIP-192](https://cwiki.apache.org/confluence/display/KAFKA/KIP-192+%3A+Provide+cleaner+semantics+when+idempotence+is+enabled) — Cleaner idempotence semantics | 1.0.0 | Supported |
| [KIP-195](https://cwiki.apache.org/confluence/display/KAFKA/KIP-195%3A+AdminClient.createPartitions) — CreatePartitions | 1.0.0 | Supported |
| [KIP-204](https://cwiki.apache.org/confluence/display/KAFKA/KIP-204+%3A+Adding+records+deletion+operation+to+the+new+Admin+Client+API) — DeleteRecords via admin API | 1.1.0 | Supported |
| [KIP-207](https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change) — New error in ListOffsets | 2.2.0 | Supported |
| [KIP-219](https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+throttle+communication) — Client-side throttling | 2.0.0 | Supported |
| [KIP-222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-222+-+Add+Consumer+Group+operations+to+Admin+API) — Group operations via admin API | 2.0.0 | Supported |
| [KIP-226](https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration) — Describe configs v1 | 1.1.0 | Supported |
| [KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability) — Incremental fetch | 1.1.0 | Supported |
| [KIP-229](https://cwiki.apache.org/confluence/display/KAFKA/KIP-229%3A+DeleteGroups+API) — DeleteGroups | 1.1.0 | Supported |
| [KIP-249](https://cwiki.apache.org/confluence/display/KAFKA/KIP-249%3A+Add+Delegation+Token+Operations+to+KafkaAdminClient) — Delegation tokens in admin API | 2.0.0 | Supported |
| [KIP-255](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75968876) — SASL OAUTHBEARER | 2.0.0 | Supported |
| [KIP-266](https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior) — Fix indefinite consumer timeouts | 2.0.0 | Supported (via context) |
| [KIP-279](https://cwiki.apache.org/confluence/display/KAFKA/KIP-279%3A+Fix+log+divergence+between+leader+and+follower+after+fast+leader+fail+over) — OffsetForLeaderEpoch bump | 2.0.0 | Supported |
| [KIP-289](https://cwiki.apache.org/confluence/display/KAFKA/KIP-289%3A+Improve+the+default+group+id+behavior+in+KafkaConsumer) — Default group.id to null | 2.2.0 | Supported |
| [KIP-294](https://cwiki.apache.org/confluence/display/KAFKA/KIP-294+-+Enable+TLS+hostname+verification+by+default) — TLS verification | 2.0.0 | Supported (via dialer) |
| [KIP-302](https://cwiki.apache.org/confluence/display/KAFKA/KIP-302+-+Enable+Kafka+clients+to+use+all+DNS+resolved+IP+addresses) — Use multiple addrs for resolved hostnames | 2.1.0 | Supported (via dialer) |
| [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation) — Fetcher: detect log truncation | 2.1.0 | Supported |
| [KIP-322](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=87295558) — DeleteTopics disabled error code | 2.1.0 | Supported |
| [KIP-339](https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API) — IncrementalAlterConfigs | 2.3.0 | Supported |
| [KIP-341](https://cwiki.apache.org/confluence/display/KAFKA/KIP-341%3A+Update+Sticky+Assignor%27s+User+Data+Protocol) — Sticky group bugfix | ? | Supported |
| [KIP-342](https://cwiki.apache.org/confluence/display/KAFKA/KIP-342%3A+Add+support+for+Custom+SASL+extensions+in+OAuthBearer+authentication) — OAUTHBEARER extensions | 2.1.0 | Supported |
| [KIP-345](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances) — Static group membership | 2.4.0 | Supported |
| [KIP-357](https://cwiki.apache.org/confluence/display/KAFKA/KIP-357%3A++Add+support+to+list+ACLs+per+principal) — List ACLs per principal via admin API | 2.1.0 | Supported |
| [KIP-360](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820) — Safe epoch bumping for `UNKNOWN_PRODUCER_ID` | 2.5.0 | Supported |
| [KIP-361](https://cwiki.apache.org/confluence/display/KAFKA/KIP-361%3A+Add+Consumer+Configuration+to+Disable+Auto+Topic+Creation) — Allow disable auto topic creation | 2.3.0 | Supported |
| [KIP-368](https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate) — Periodically reauthenticate SASL | 2.2.0 | Supported |
| [KIP-369](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89070828) — An always round robin produce partitioner | 2.4.0 | Supported |
| [KIP-380](https://cwiki.apache.org/confluence/display/KAFKA/KIP-380%3A+Detect+outdated+control+requests+and+bounced+brokers+using+broker+generation) — Inter-broker protocol changes | 2.2.0 | Supported |
| [KIP-389](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89070828) — Group max size error | 2.2.0 | Supported |
| [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) — Closest replica fetching w/ rack | 2.2.0 | Supported |
| [KIP-394](https://cwiki.apache.org/confluence/display/KAFKA/KIP-394%3A+Require+member.id+for+initial+join+group+request) — Require member.id for initial join request |  2.2.0 | Supported |
| [KIP-396](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=97551484) — Commit offsets manually | 2.4.0 | Supported |
| [KIP-412](https://cwiki.apache.org/confluence/display/KAFKA/KIP-412%3A+Extend+Admin+API+to+support+dynamic+application+log+levels) — Dynamic log levels w/ IncrementalAlterConfigs | 2.4.0 | Supported |
| [KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol) — Incremental rebalance (see KAFKA-8179) | 2.4.0 | Supported |
| [KIP-430](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430+-+Return+Authorized+Operations+in+Describe+Responses) — Authorized ops in DescribeGroups | 2.3.0 | Supported |
| [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) — Producer scalability for EOS | 2.5.0 | Supported |
| [KIP-455](https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment) — Replica reassignment API | 2.4.0 | Supported |
| [KIP-460](https://cwiki.apache.org/confluence/display/KAFKA/KIP-460%3A+Admin+Leader+Election+RPC) — Leader election API | 2.4.0 | Supported |
| [KIP-464](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708722) — CreateTopic defaults | 2.4.0 | Supported |
| [KIP-467](https://cwiki.apache.org/confluence/display/KAFKA/KIP-467%3A+Augment+ProduceResponse+error+messaging+for+specific+culprit+records) — Per-record error codes when producing | 2.4.0 | Supported (and ignored) |
| [KIP-480](https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner) — Sticky partition producing | 2.4.0 | Supported |
| [KIP-482](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields) — Tagged fields (KAFKA-8885) | 2.4.0 | Supported |
| [KIP-496](https://cwiki.apache.org/confluence/display/KAFKA/KIP-496%3A+Administrative+API+to+delete+consumer+offsets) — OffsetDelete admin command | 2.4.0 | Supported |
| [KIP-497](https://cwiki.apache.org/confluence/display/KAFKA/KIP-497%3A+Add+inter-broker+API+to+alter+ISR) — New AlterISR API | 2.7.0 | Supported |
| [KIP-498](https://cwiki.apache.org/confluence/display/KAFKA/KIP-498%3A+Add+client-side+configuration+for+maximum+response+size+to+protect+against+OOM) — Max bound on reads | ? | Supported |
| [KIP-511](https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers) — Client name/version in ApiVersions request | 2.4.0 | Supported |
| [KIP-514](https://cwiki.apache.org/confluence/display/KAFKA/KIP-514%3A+Add+a+bounded+flush%28%29+API+to+Kafka+Producer) — Bounded Flush | 2.4.0 | Supported (via context) |
| [KIP-518](https://cwiki.apache.org/confluence/display/KAFKA/KIP-518%3A+Allow+listing+consumer+groups+per+state) — List groups by state | 2.6.0 | Supported |
| [KIP-519](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=128650952) — Configurable SSL "engine" | 2.6.0 | Supported (via dialer) |
| [KIP-525](https://cwiki.apache.org/confluence/display/KAFKA/KIP-525+-+Return+topic+metadata+and+configs+in+CreateTopics+response) — CreateTopics v5 returns configs | 2.4.0 | Supported |
| [KIP-526](https://cwiki.apache.org/confluence/display/KAFKA/KIP-526%3A+Reduce+Producer+Metadata+Lookups+for+Large+Number+of+Topics) — Reduce metadata lookups | 2.5.0 | Supported |
| [KIP-533](https://cwiki.apache.org/confluence/display/KAFKA/KIP-533%3A+Add+default+api+timeout+to+AdminClient) — Default API timeout (total time, not per request) | 2.5.0 | Supported (via RetryTimeout) |
| [KIP-546](https://cwiki.apache.org/confluence/display/KAFKA/KIP-546%3A+Add+Client+Quota+APIs+to+the+Admin+Client) — Client Quota APIs | 2.5.0 | Supported |
| [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API) — Broker side SCRAM APIs | 2.7.0 | Supported |
| [KIP-559](https://cwiki.apache.org/confluence/display/KAFKA/KIP-559%3A+Make+the+Kafka+Protocol+Friendlier+with+L7+Proxies) — Protocol info in sync/join | 2.5.0 | Supported |
| [KIP-568](https://cwiki.apache.org/confluence/display/KAFKA/KIP-568%3A+Explicit+rebalance+triggering+on+the+Consumer) — Explicit rebalance triggering on the consumer | 2.6.0 | Supported |
| [KIP-569](https://cwiki.apache.org/confluence/display/KAFKA/KIP-569%3A+DescribeConfigsResponse+-+Update+the+schema+to+include+additional+metadata+information+of+the+field) — Docs & type in DescribeConfigs | 2.6.0 | Supported |
| [KIP-570](https://cwiki.apache.org/confluence/display/KAFKA/KIP-570%3A+Add+leader+epoch+in+StopReplicaRequest) — Leader epoch in StopReplica | 2.6.0 | Supported |
| [KIP-580](https://cwiki.apache.org/confluence/display/KAFKA/KIP-580%3A+Exponential+Backoff+for+Kafka+Clients) — Exponential backoff | 2.6.0 | Supported |
| [KIP-584](https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features) — Versioning scheme for features | ? | Supported (nothing to do yet) |
| [KIP-588](https://cwiki.apache.org/confluence/display/KAFKA/KIP-588%3A+Allow+producers+to+recover+gracefully+from+transaction+timeouts) — Producer recovery from txn timeout | 2.7.0 | Supported |
| [KIP-590](https://cwiki.apache.org/confluence/display/KAFKA/KIP-590%3A+Redirect+Zookeeper+Mutation+Protocols+to+The+Controller) — Envelope (broker only) | 2.7.0 | Supported |
| [KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum) — New APIs for raft protocol | 2.7.0 | Supported |
| [KIP-599](https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations) — Throttling on create/delete topic/partition | 2.7.0 | Supported |
| [KIP-602](https://cwiki.apache.org/confluence/display/KAFKA/KIP-602%3A+Change+default+value+for+client.dns.lookup) — Use all resolved addrs by default | 2.6.0 | Supported (via dialer) |
| [KIP-651](https://cwiki.apache.org/confluence/display/KAFKA/KIP-651+-+Support+PEM+format+for+SSL+certificates+and+private+key) — Support PEM | 2.7.0 | Supported (via dialer) |
| [KIP-654](https://cwiki.apache.org/confluence/display/KAFKA/KIP-654%3A+Aborted+transaction+with+non-flushed+data+should+throw+a+non-fatal+exception) — Aborted txns with unflushed data is not fatal | 2.7.0 | Supported (default behavior) |
| [KIP-664](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions) — Describe producers / etc. | 2.8.0 (mostly) | Supported |
| [KIP-700](https://cwiki.apache.org/confluence/display/KAFKA/KIP-700%3A+Add+Describe+Cluster+API) — DescribeCluster | 2.8.0 | Supported |
| [KIP-730](https://cwiki.apache.org/confluence/display/KAFKA/KIP-730%3A+Producer+ID+generation+in+KRaft+mode) - AllocateProducerIDs | 3.0.0 | Supported |
| [KIP-735](https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout) — Bump default session timeout | ? | Supported |

Missing from above but included in librdkafka is:

- [KIP-85](https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients), which does not seem relevant for franz-go
- [KIP-92](https://cwiki.apache.org/confluence/display/KAFKA/KIP-92+-+Add+per+partition+lag+metrics+to+KafkaConsumer) for consumer lag metrics, which is better suited for an external system via the admin api
- [KIP-223](https://cwiki.apache.org/confluence/display/KAFKA/KIP-223+-+Add+per-topic+min+lead+and+per-partition+lead+metrics+to+KafkaConsumer) for more metrics
- [KIP-235](https://cwiki.apache.org/confluence/display/KAFKA/KIP-235%3A+Add+DNS+alias+support+for+secured+connection), which is confusing but may be implement via a custom dialer and custom kerberos?
- [KIP-359](https://cwiki.apache.org/confluence/display/KAFKA/KIP-359%3A+Verify+leader+epoch+in+produce+requests) to verify leader epoch when producing; this is easy to support but actually is not implemented in Kafka yet
- [KIP-421](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=100829515) for dynamic values in configs; librdkafka mentions it does not support it, and neither does franz-go for the same reason (we do not use a config file)
- [KIP-436](https://cwiki.apache.org/confluence/display/KAFKA/KIP-436%3A+Add+a+metric+indicating+start+time) is about yet another metric
- [KIP-517](https://cwiki.apache.org/confluence/display/KAFKA/KIP-517%3A+Add+consumer+metrics+to+observe+user+poll+behavior), more metrics
