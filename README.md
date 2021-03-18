franz-go - Apache Kafka client written in Go
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

- Feature complete client (up to Kafka v2.7.0+)
- Supported compression types: snappy, gzip, lz4 and zstd
- SSL/TLS Support
- Exactly once semantics / idempotent producing
- Transactions support
- All SASL mechanisms are supported (OAuthBearer, GSSAPI/Kerberos, SCRAM-SHA-256/512 and plain)
- Supported Kafka versions >=0.8
- Provides low level functionality (such as sending API requests) as well as high level functionality (e.g. consuming in groups)
- Utilizes modern & idiomatic Go (support for contexts, variadic configuration options, ...)
- Highly performant, see [Performance](./docs/performance.md) (benchmarks will be added)
- Written in pure Go (no wrapper lib for a C library or other bindings)
- Ability to add detailed log messages or metrics using hooks

## Getting started

Basic usage for producing and consuming Kafka messages looks like this:

```go
seeds := []string{"localhost:9092"}
client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
if err != nil {
    panic(err)
}
defer client.Close()

ctx := context.Background()

// 1.) Producing a message
// All record production goes through Produce, and the callback can be used
// to allow for syncronous or asyncronous production.
var wg sync.WaitGroup
wg.Add(1)
record := &kgo.Record{Topic: "foo", Value: []byte("bar")}
err := client.Produce(ctx, record, func(_ *Record, err error) {
        defer wg.Done()
        if err != nil {
                fmt.Printf("record had a produce error: %v\n", err)
        }

}
if err != nil {
        panic("we are unable to produce if the context is canceled, we have hit max buffered," +
                "or if we are transactional and not in a transaction")
}
wg.Wait()

// 2.) Consuming messages from a topic
// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
// client.AssignGroup("my-group-identifier", kgo.GroupTopics("foo"))
for {
        fetches := client.PollFetches(ctx)
        iter := fetches.RecordIter()
        for !iter.Done() {
            record := iter.Next()
            fmt.Println(string(record.Value))
        }
}
```

- Take a look at [more examples](./examples) 
- [Architecture documentation](./docs/architecture.md) describing all Franz-go package purposes
- Reference docs [https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
- Consuming in [Consumer Groups](./docs/consumer-groups.md)
- [Producing](./docs/producer.md)
- Sending [admin requests](./docs/admin-requests.md)

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

## Metrics

Using hooks you can attach to any events happening within franz-go. This allows you to use your favorite metric library
and collect the metrics you are interested in.

## Supported KIPs

Theoretically, this library supports every (non-Java-specific) client facing KIP.
Most are tested, some need testing. Any KIP that simply adds or modifies
a protocol is supported by code generation.

- [KIP-12](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=51809888) (sasl & ssl; 0.9.0)
- [KIP-13](https://cwiki.apache.org/confluence/display/KAFKA/KIP-13+-+Quotas) (throttling; supported but not obeyed)
- [KIP-31](https://cwiki.apache.org/confluence/display/KAFKA/KIP-31+-+Move+to+relative+offsets+in+compressed+message+sets) (relative offsets in message set; 0.10.0)
- [KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message) (timestamps in message set v1; 0.10.0)
- [KIP-35](https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version) (adds ApiVersion; 0.10.0)
- [KIP-36](https://cwiki.apache.org/confluence/display/KAFKA/KIP-36+Rack+aware+replica+assignment) (rack aware replica assignment; 0.10.0)
- [KIP-40](https://cwiki.apache.org/confluence/display/KAFKA/KIP-40%3A+ListGroups+and+DescribeGroup) (ListGroups and DescribeGroup v0; 0.9.0)
- [KIP-43](https://cwiki.apache.org/confluence/display/KAFKA/KIP-43%3A+Kafka+SASL+enhancements) (sasl enhancements & handshake; 0.10.0)
- [KIP-54](https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy) (sticky group assignment)
- [KIP-62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread) (join group rebalnce timeout, background thread heartbeats; v0.10.1)
- [KIP-74](https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes) (fetch response size limit; 0.10.1)
- [KIP-78](https://cwiki.apache.org/confluence/display/KAFKA/KIP-78%3A+Cluster+Id) (cluster id in metadata; 0.10.1)
- [KIP-79](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090) (list offset req/resp timestamp field; 0.10.1)
- [KIP-84](https://cwiki.apache.org/confluence/display/KAFKA/KIP-84%3A+Support+SASL+SCRAM+mechanisms) (sasl scram; 0.10.2)
- [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) (EOS; 0.11.0)
- [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation) (offset for leader epoch introduced; broker usage yet; 0.11.0)
- [KIP-107](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+deleteRecordsBefore%28%29+API+in+AdminClient) (delete records; 0.11.0)
- [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy) (validate create topic; 0.10.2)
- [KIP-110](https://cwiki.apache.org/confluence/display/KAFKA/KIP-110%3A+Add+Codec+for+ZStandard+Compression) (zstd; 2.1.0)
- [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD) (JBOD disk failure, protocol changes; 1.0.0)
- [KIP-113](https://cwiki.apache.org/confluence/display/KAFKA/KIP-113%3A+Support+replicas+movement+between+log+directories) (JBOD log dir movement, protocol additions; 1.0.0)
- [KIP-124](https://cwiki.apache.org/confluence/display/KAFKA/KIP-124+-+Request+rate+quotas) (request rate quotas; 0.11.0)
- [KIP-133](https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs) (describe & alter configs; 0.11.0)
- [KIP-152](https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures) (more sasl, introduce sasl authenticate; 1.0.0)
- [KIP-183](https://cwiki.apache.org/confluence/display/KAFKA/KIP-183+-+Change+PreferredReplicaLeaderElectionCommand+to+use+AdminClient) (elect preferred leaders; 2.2.0)
- [KIP-185](https://cwiki.apache.org/confluence/display/KAFKA/KIP-185%3A+Make+exactly+once+in+order+delivery+per+partition+the+default+Producer+setting) (idempotent is default; 1.0.0)
- [KIP-195](https://cwiki.apache.org/confluence/display/KAFKA/KIP-195%3A+AdminClient.createPartitions) (create partitions request; 1.0.0)
- [KIP-207](https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change) (new error in list offset request; 2.2.0)
- [KIP-219](https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+throttle+communication) (throttling happens after response; 2.0.0)
- [KIP-226](https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration) (describe configs v1; 1.1.0)
- [KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability) (incremental fetch requests; 1.1.0)
- [KIP-229](https://cwiki.apache.org/confluence/display/KAFKA/KIP-229%3A+DeleteGroups+API) (delete groups request; 1.1.0)
- [KIP-255](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75968876) (oauth via sasl/oauthbearer; 2.0.0)
- [KIP-279](https://cwiki.apache.org/confluence/display/KAFKA/KIP-279%3A+Fix+log+divergence+between+leader+and+follower+after+fast+leader+fail+over) (leader / follower failover; changed offsets for leader epoch; 2.0.0)
- [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation) (fetcher log truncation detection; 2.1.0)
- [KIP-322](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=87295558) (new error when delete topics is disabled; 2.1.0)
- [KIP-339](https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API) (incremental alter configs; 2.3.0)
- [KIP-341](https://cwiki.apache.org/confluence/display/KAFKA/KIP-341%3A+Update+Sticky+Assignor%27s+User+Data+Protocol) (sticky group bug fix)
- [KIP-342](https://cwiki.apache.org/confluence/display/KAFKA/KIP-342%3A+Add+support+for+Custom+SASL+extensions+in+OAuthBearer+authentication) (oauth extensions; 2.1.0)
- [KIP-345](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances) (static group membership, see KAFKA-8224)
- [KIP-360](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820) (safe epoch bumping for `UNKNOWN_PRODUCER_ID`; 2.5.0)
- [KIP-368](https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate) (periodically reauth sasl; 2.2.0)
- [KIP-369](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89070828) (always round robin produce partitioner; 2.4.0)
- [KIP-380](https://cwiki.apache.org/confluence/display/KAFKA/KIP-380%3A+Detect+outdated+control+requests+and+bounced+brokers+using+broker+generation) (inter-broker command changes; 2.2.0)
- [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) (fetch request from closest replica w/ rack; 2.2.0)
- [KIP-394](https://cwiki.apache.org/confluence/display/KAFKA/KIP-394%3A+Require+member.id+for+initial+join+group+request) (require member.id for initial join; 2.2.0)
- [KIP-412](https://cwiki.apache.org/confluence/display/KAFKA/KIP-412%3A+Extend+Admin+API+to+support+dynamic+application+log+levels) (dynamic log levels with incremental alter configs; 2.4.0)
- [KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol) (incremental rebalance, see KAFKA-8179; 2.4.0)
- [KIP-430](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430+-+Return+Authorized+Operations+in+Describe+Responses) (include authorized ops in describe groups; 2.3.0)
- [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) (transaction changes to better support group changes; 2.5.0)
- [KIP-455](https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment) (admin replica reassignment; 2.4.0)
- [KIP-460](https://cwiki.apache.org/confluence/display/KAFKA/KIP-460%3A+Admin+Leader+Election+RPC) (admin leader election; 2.4.0)
- [KIP-464](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708722) (defaults for create topic; 2.4.0)
- [KIP-467](https://cwiki.apache.org/confluence/display/KAFKA/KIP-467%3A+Augment+ProduceResponse+error+messaging+for+specific+culprit+records) (produce response error change for per-record errors; 2.4.0)
- [KIP-480](https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner) (sticky partition producing; 2.4.0)
- [KIP-482](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields) (tagged fields; KAFKA-8885; 2.4.0)
- [KIP-496](https://cwiki.apache.org/confluence/display/KAFKA/KIP-496%3A+Administrative+API+to+delete+consumer+offsets) (offset delete admin command; 2.4.0)
- [KIP-497](https://cwiki.apache.org/confluence/display/KAFKA/KIP-497%3A+Add+inter-broker+API+to+alter+ISR) (new API to alter ISR; 2.7.0)
- [KIP-498](https://cwiki.apache.org/confluence/display/KAFKA/KIP-498%3A+Add+client-side+configuration+for+maximum+response+size+to+protect+against+OOM) (add max bound on reads; unimplemented in Kafka)
- [KIP-511](https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers) (add client name / version in apiversions req; 2.4.0)
- [KIP-518](https://cwiki.apache.org/confluence/display/KAFKA/KIP-518%3A+Allow+listing+consumer+groups+per+state) (list groups by state; 2.6.0)
- [KIP-525](https://cwiki.apache.org/confluence/display/KAFKA/KIP-525+-+Return+topic+metadata+and+configs+in+CreateTopics+response) (create topics v5 returns configs; 2.4.0)
- [KIP-526](https://cwiki.apache.org/confluence/display/KAFKA/KIP-526%3A+Reduce+Producer+Metadata+Lookups+for+Large+Number+of+Topics) (reduce metadata lookups; done minus part 2, which we wont do)
- [KIP-546](https://cwiki.apache.org/confluence/display/KAFKA/KIP-546%3A+Add+Client+Quota+APIs+to+the+Admin+Client) (client quota APIs; 2.5.0)
- [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API) (broker side SCRAM API; 2.7.0)
- [KIP-559](https://cwiki.apache.org/confluence/display/KAFKA/KIP-559%3A+Make+the+Kafka+Protocol+Friendlier+with+L7+Proxies) (protocol info in sync / join; 2.5.0)
- [KIP-569](https://cwiki.apache.org/confluence/display/KAFKA/KIP-569%3A+DescribeConfigsResponse+-+Update+the+schema+to+include+additional+metadata+information+of+the+field) (doc/type in describe configs; 2.6.0)
- [KIP-570](https://cwiki.apache.org/confluence/display/KAFKA/KIP-570%3A+Add+leader+epoch+in+StopReplicaRequest) (leader epoch in stop replica; 2.6.0)
- [KIP-580](https://cwiki.apache.org/confluence/display/KAFKA/KIP-580%3A+Exponential+Backoff+for+Kafka+Clients) (exponential backoff; 2.6.0)
- [KIP-588](https://cwiki.apache.org/confluence/display/KAFKA/KIP-588%3A+Allow+producers+to+recover+gracefully+from+transaction+timeouts) (producer recovery from txn timeout; 2.7.0)
- [KIP-590](https://cwiki.apache.org/confluence/display/KAFKA/KIP-590%3A+Redirect+Zookeeper+Mutation+Protocols+to+The+Controller) (support for forwarding admin requests; 2.7.0)
- [KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum) (new APIs for raft protocol; 2.7.0)
- [KIP-599](https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations) (throttle create/delete topic/partition; 2.7.0)
- [KIP-664](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions) (describe producers, describe / list transactions; mostly 2.8.0 [write txn markers missing])
- [KIP-700](https://cwiki.apache.org/confluence/display/KAFKA/KIP-700%3A+Add+Describe+Cluster+API) (describe cluster; 2.8.0)
