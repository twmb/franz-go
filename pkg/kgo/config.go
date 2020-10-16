package kgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
)

// Opt is an option to configure a client.
type Opt interface {
	apply(*cfg)
}

// ProducerOpt is a producer-specific option to configure a client.
// This is simply a namespaced Opt.
type ProducerOpt interface {
	Opt
	producerOpt()
}

// ConsumerOpt is a consumer-specific option to configure a client.
// This is simply a namespaced Opt.
type ConsumerOpt interface {
	Opt
	consumerOpt()
}

type clientOpt struct{ fn func(*cfg) }
type producerOpt struct{ fn func(*cfg) }
type consumerOpt struct{ fn func(*cfg) }

func (opt clientOpt) apply(cfg *cfg)   { opt.fn(cfg) }
func (opt producerOpt) apply(cfg *cfg) { opt.fn(cfg) }
func (opt consumerOpt) apply(cfg *cfg) { opt.fn(cfg) }
func (producerOpt) producerOpt()       {}
func (consumerOpt) consumerOpt()       {}

type cfg struct {
	// ***GENERAL SECTION***
	id                  *string
	dialFn              func(context.Context, string, string) (net.Conn, error)
	connTimeoutOverhead time.Duration

	softwareName    string // KIP-511
	softwareVersion string // KIP-511

	logger Logger

	seedBrokers []string
	maxVersions kversion.Versions

	retryBackoff          func(int) time.Duration
	retries               int
	retryTimeout          func(int16) time.Duration
	brokerConnDeadRetries int

	maxBrokerWriteBytes int32

	allowAutoTopicCreation bool

	metadataMaxAge time.Duration
	metadataMinAge time.Duration

	sasls []sasl.Mechanism

	hooks hooks

	// ***PRODUCER SECTION***
	txnID       *string
	txnTimeout  time.Duration
	acks        Acks
	compression []CompressionCodec // order of preference

	maxRecordBatchBytes int32
	maxBufferedRecords  int64
	produceTimeout      time.Duration
	linger              time.Duration
	recordTimeout       time.Duration
	manualFlushing      bool

	partitioner Partitioner

	stopOnDataLoss bool
	onDataLoss     func(string, int32)

	// ***CONSUMER SECTION***
	maxWait        int32
	maxBytes       int32
	maxPartBytes   int32
	resetOffset    Offset
	isolationLevel int8
	keepControl    bool
	rack           string
}

// TODO strengthen?
func (cfg *cfg) validate() error {
	if len(cfg.seedBrokers) == 0 {
		return errors.New("config erroneously has no seed brokers")
	}
	if cfg.maxBrokerWriteBytes < 1<<10 {
		return fmt.Errorf("max broker write bytes %d is less than min acceptable %d", cfg.maxBrokerWriteBytes, 1<<10)
	}
	// upper bound broker write bytes to avoid any problems with
	// overflowing numbers in calculations.
	if cfg.maxBrokerWriteBytes > 1<<30 {
		return fmt.Errorf("max broker write bytes %d is greater than max acceptable %d", cfg.maxBrokerWriteBytes, 1<<30)
	}

	if cfg.maxRecordBatchBytes < 1<<10 {
		return fmt.Errorf("max record batch bytes %d is less than min acceptable %d", cfg.maxRecordBatchBytes, 1<<10)
	}

	if cfg.maxBrokerWriteBytes < cfg.maxRecordBatchBytes {
		return fmt.Errorf("max broker write bytes %d is erroneously less than max record batch bytes %d",
			cfg.maxBrokerWriteBytes, cfg.maxRecordBatchBytes)
	}

	return nil
}

func defaultCfg() cfg {
	defaultID := "kgo"
	return cfg{
		id:     &defaultID,
		dialFn: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,

		softwareName:    "kgo",
		softwareVersion: "0.1.0",

		logger: new(nopLogger),

		seedBrokers: []string{"127.0.0.1"},

		retryBackoff: func() func(int) time.Duration {
			var rngMu sync.Mutex
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			return func(fails int) time.Duration {
				const (
					min = 100 * time.Millisecond
					max = time.Second
				)
				if fails <= 0 {
					return min
				}
				if fails > 10 {
					return max
				}

				backoff := min * time.Duration(1<<(fails-1))

				rngMu.Lock()
				jitter := 0.8 + 0.4*rng.Float64()
				rngMu.Unlock()

				backoff = time.Duration(float64(backoff) * jitter)

				if backoff > max {
					return max
				}
				return backoff
			}
		}(),
		retries: math.MaxInt32, // effectively unbounded
		retryTimeout: func(key int16) time.Duration {
			if key == 26 { // EndTxn key
				return 5 * time.Minute
			}
			return time.Minute
		},
		brokerConnDeadRetries: 20,

		maxBrokerWriteBytes: 100 << 20, // Kafka socket.request.max.bytes default is 100<<20

		metadataMaxAge: 5 * time.Minute,
		metadataMinAge: 10 * time.Second,

		txnTimeout:          60 * time.Second,
		acks:                AllISRAcks(),
		compression:         []CompressionCodec{SnappyCompression(), NoCompression()},
		maxRecordBatchBytes: 1000000, // Kafka max.message.bytes default is 1000012
		maxBufferedRecords:  math.MaxInt64,
		produceTimeout:      30 * time.Second,
		partitioner:         StickyKeyPartitioner(nil), // default to how Kafka partitions

		maxWait:        5000,
		maxBytes:       50 << 20,
		maxPartBytes:   10 << 20,
		resetOffset:    NewOffset().AtStart(),
		isolationLevel: 0,
	}
}

// ********** CLIENT CONFIGURATION **********

// ClientID uses id for all requests sent to Kafka brokers, overriding the
// default "kgo".
func ClientID(id string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.id = &id }}
}

// DisableClientID sets the client ID to null for all requests sent to Kafka
// brokers, overriding the default "kgo".
func DisableClientID() Opt {
	return clientOpt{func(cfg *cfg) { cfg.id = nil }}
}

// SoftwareNameAndVersion sets the client software name and version that will
// be sent to Kafka as part of the ApiVersions request as of Kafka 2.4.0,
// overriding the default "kgo" and internal version number.
//
// Kafka exposes this through metrics to help operators understand the impact
// of clients.
//
// It is generally not recommended to set this. As well, if you do, the name
// and version must match the following regular expression:
//
//     [a-zA-Z0-9](?:[a-zA-Z0-9\\-.]*[a-zA-Z0-9])?
//
// Note this means neither the name nor version can be empty.
func SoftwareNameAndVersion(name, version string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.softwareName = name; cfg.softwareVersion = version }}
}

// WithLogger sets the client to use the given logger, overriding the default
// to not use a logger.
//
// It is invalid to use a nil logger; doing so will cause panics.
func WithLogger(l Logger) Opt {
	return clientOpt{func(cfg *cfg) { cfg.logger = &wrappedLogger{l} }}
}

// ConnTimeoutOverhead uses the given time as overhead while deadlining
// requests, overriding the default overhead of 5s.
//
// For most requests, the overhead will simply be the timeout. However, for any
// request with a TimeoutMillis field, the overhead is added on top of the
// request's TimeoutMillis. This ensures that we give Kafka enough time to
// actually process the request given the timeout, while still having a
// deadline on the connection as a whole to ensure it does not hang.
//
// For writes, the timeout is always the overhead. We buffer writes in our
// client before one quick flush, so we always expect the write to be fast.
//
// Using 0 has the overhead disables timeouts.
func ConnTimeoutOverhead(overhead time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.connTimeoutOverhead = overhead }}
}

// Dialer uses fn to dial addresses, overriding the default dialer that uses a
// 10s dial timeout and no TLS.
//
// The context passed to the dial function is the context used in the request
// that caused the dial. If the request is a client-internal request, the
// context is the context on the client itself (which is canceled when the
// client is closed).
//
// This function has the same signature as net.Dialer's DialContext and
// tls.Dialer's DialContext, meaning you can use this function like so:
//
//     kgo.Dialer((&net.Dialer{Timeout: 10*time.Second}).DialContext)
//
// or
//
//     kgo.Dialer((&tls.Dialer{...})}.DialContext)
//
func Dialer(fn func(ctx context.Context, network, host string) (net.Conn, error)) Opt {
	return clientOpt{func(cfg *cfg) { cfg.dialFn = fn }}
}

// SeedBrokers sets the seed brokers for the client to use, overriding the
// default 127.0.0.1:9092.
//
// Any seeds that are missing a port use the default Kafka port 9092.
func SeedBrokers(seeds ...string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.seedBrokers = append(cfg.seedBrokers[:0], seeds...) }}
}

// MaxVersions sets the maximum Kafka version to try, overriding the
// internal unbounded (latest) versions.
//
// Note that specific max version pinning is required if trying to interact
// with versions pre 0.10.0. Otherwise, unless using more complicated requests
// that this client itself does not natively use, it is generally safe to opt
// for the latest version. If using the kmsg package directly to issue
// requests, it is recommended to pin versions so that new fields on requests
// do not get invalid default zero values before you update your usage.
func MaxVersions(versions kversion.Versions) Opt {
	return clientOpt{func(cfg *cfg) { cfg.maxVersions = versions }}
}

// RetryBackoff sets the backoff strategy for how long to backoff for a given
// amount of retries, overriding the default exponential backoff that ranges
// from 100ms min to 1s max.
//
// This (roughly) corresponds to Kafka's retry.backoff.ms setting and
// retry.backoff.max.ms (which is being introduced with KIP-500).
func RetryBackoff(backoff func(int) time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.retryBackoff = backoff }}
}

// RequestRetries sets the number of tries that retriable requests are allowed,
// overriding the unlimited default.
//
// This setting applies to all types of requests.
func RequestRetries(n int) Opt {
	return clientOpt{func(cfg *cfg) { cfg.retries = n }}
}

// RetryTimeout sets the upper limit on how long we allow requests to retry,
// overriding the default of 5m for EndTxn requests, 1m for all others.
//
// This timeout applies to any request issued through a client's Request
// function. It does not apply to fetches nor produces.
//
// The function is called with the request key that is being retried. While it
// is not expected that the request key will be used, including it gives users
// the opportinuty to have different retry timeouts for different keys.
//
// If the function returns zero, there is no retry timeout.
//
// The timeout is evaluated after a request is issued. If a retry backoff
// places the next request past the retry timeout deadline, the request will
// still be tried once more once the backoff expires.
func RetryTimeout(t func(int16) time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.retryTimeout = t }}
}

// BrokerConnDeadRetries sets the number of tries that are allowed for requests
// that fail before a response is even received, overriding the default 20.
//
// These retries are for cases where a connection to a broker fails during a
// request. Generally, you just must retry these requests anyway. However, if
// Kafka cannot understand our request, it closes a connection deliberately.
// Since the client cannot differentiate this case, we upper bound the number
// of times we allow a connection to be cut before we call it quits on the
// request, assuming that if a connection is cut *so many times* repeatedly,
// then it is likely not the network but instead Kafka indicating a problem.
//
// The only error to trigger this is ErrConnDead, indicating Kafka closed the
// connection (or the connection actually just died).
//
// This setting applies to all but internally generated fetch and produce
// requests.
func BrokerConnDeadRetries(n int) Opt {
	return clientOpt{func(cfg *cfg) { cfg.brokerConnDeadRetries = n }}
}

// AutoTopicCreation enables topics to be auto created if they do
// not exist when fetching their metadata.
func AutoTopicCreation() Opt {
	return clientOpt{func(cfg *cfg) { cfg.allowAutoTopicCreation = true }}
}

// BrokerMaxWriteBytes upper bounds the number of bytes written to a broker
// connection in a single write, overriding the default 100MiB.
//
// This number corresponds to the a broker's socket.request.max.bytes, which
// defaults to 100MiB.
//
// The only Kafka request that could come reasonable close to hitting this
// limit should be produce requests.
func BrokerMaxWriteBytes(v int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.maxBrokerWriteBytes = v }}
}

// MetadataMaxAge sets the maximum age for the client's cached metadata,
// overriding the default 5m, to allow detection of new topics, partitions,
// etc.
//
// This corresponds to Kafka's metadata.max.age.ms.
func MetadataMaxAge(age time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.metadataMaxAge = age }}
}

// MetadataMinAge sets the minimum time between metadata queries,
// overriding the default 10s. You may want to raise or lower this to reduce
// the number of metadata queries the client will make. Notably, if metadata
// detects an error in any topic or partition, it triggers itself to update as
// soon as allowed. Additionally, any connection failures causing backoff while
// producing or consuming trigger metadata updates, because the client must
// assume that maybe the connection died due to a broker dying.
func MetadataMinAge(age time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.metadataMinAge = age }}
}

// SASL appends sasl authentication options to use for all connections.
//
// SASL is tried in order; if the broker supports the first mechanism, all
// connections will use that mechanism. If the first mechanism fails, the
// client will pick the first supported mechanism. If the broker does not
// support any client mechanisms, connections will fail.
func SASL(sasls ...sasl.Mechanism) Opt {
	return clientOpt{func(cfg *cfg) { cfg.sasls = append(cfg.sasls, sasls...) }}
}

// WithHooks sets hooks to call whenever relevant.
//
// Hooks can be used to layer in metrics (such as prometheus hooks) or anything
// else. The client will call all hooks in order. See the Hooks interface for
// more information, as well as any interface that contains "Hook" in the name
// to know the available hooks. A single hook can implement zero or all hook
// interfaces, and only the hooks that it implements will be called.
func WithHooks(hooks ...Hook) Opt {
	return clientOpt{func(cfg *cfg) { cfg.hooks = append(cfg.hooks, hooks...) }}
}

// ********** PRODUCER CONFIGURATION **********

// Acks represents the number of acks a broker leader must have before
// a produce request is considered complete.
//
// This controls the durability of written records and corresponds to "acks" in
// Kafka's Producer Configuration documentation.
//
// The default is LeaderAck.
type Acks struct {
	val int16
}

// NoAck considers records sent as soon as they are written on the wire.
// The leader does not reply to records.
func NoAck() Acks { return Acks{0} }

// LeaderAck causes Kafka to reply that a record is written after only
// the leader has written a message. The leader does not wait for in-sync
// replica replies.
func LeaderAck() Acks { return Acks{1} }

// AllISRAcks ensures that all in-sync replicas have acknowledged they
// wrote a record before the leader replies success.
func AllISRAcks() Acks { return Acks{-1} }

// RequiredAcks sets the required acks for produced records,
// overriding the default RequireAllISRAcks.
func RequiredAcks(acks Acks) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.acks = acks }}
}

// BatchCompression sets the compression codec to use for producing records.
//
// Compression is chosen in the order preferred based on broker support.
// For example, zstd compression was introduced in Kafka 2.1.0, so the
// preference can be first zstd, fallback snappy, fallback none.
//
// The default preference is [snappy, none], which should be fine for all
// old consumers since snappy compression has existed since Kafka 0.8.0.
// To use zstd, your brokers must be at least 2.1.0 and all consumers must
// be upgraded to support decoding zstd records.
func BatchCompression(preference ...CompressionCodec) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.compression = preference }}
}

// BatchMaxBytes upper bounds the size of a record batch, overriding the
// default 1MB.
//
// This corresponds to Kafka's max.message.bytes, which defaults to 1,000,012
// bytes (just over 1MB).
//
// Record batches are independent of a ProduceRequest: a record batch is
// specific to a topic and partition, whereas the produce request can contain
// many record batches for many topics.
//
// If a single record encodes larger than this number (before compression), it
// will will not be written and a callback will have the appropriate error.
//
// Note that this is the maximum size of a record batch before compression.
// If a batch compresses poorly and actually grows the batch, the uncompressed
// form will be used.
func BatchMaxBytes(v int32) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.maxRecordBatchBytes = v }}
}

// MaxBufferedRecords sets the max amount of records the client will buffer,
// blocking produces until records are finished if this limit is reached.
// This overrides the unbounded default.
func MaxBufferedRecords(n int) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.maxBufferedRecords = int64(n) }}
}

// RecordPartitioner uses the given partitioner to partition records, overriding
// the default StickyKeyPartitioner.
func RecordPartitioner(partitioner Partitioner) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.partitioner = partitioner }}
}

// ProduceRequestTimeout sets how long Kafka broker's are allowed to respond to
// produce requests, overriding the default 30s. If a broker exceeds this
// duration, it will reply with a request timeout error.
//
// This corresponds to Kafka's request.timeout.ms setting, but only applies to
// produce requests.
func ProduceRequestTimeout(limit time.Duration) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.produceTimeout = limit }}
}

// StopOnDataLoss sets the client to stop producing if data loss is detected,
// overriding the default false.
//
// Note that if using this option, it is strongly recommended to not have a
// retry limit. Doing so may lead to errors where the client fails a batch on a
// recoverable error, which internally bumps the idempotent sequence number
// used for producing, which may then later cause an inadvertent out of order
// sequence number and false "data loss" detection.
func StopOnDataLoss() ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.stopOnDataLoss = true }}
}

// OnDataLoss sets a function to call if data loss is detected when
// producing records if the client is configured to continue on data loss.
// Thus, this option is mutually exclusive with StopOnDataLoss.
//
// The passed function will be called with the topic and partition that data
// loss was detected on.
func OnDataLoss(fn func(string, int32)) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.onDataLoss = fn }}
}

// Linger sets how long individual topic partitions will linger
// waiting for more records before triggering a request to be built.
//
// Note that this option should only be used in low volume producers. The only
// benefit of lingering is to potentially build a larger batch to reduce cpu
// usage on the brokers if you have many producers all producing small amounts.
//
// If a produce request is triggered by any topic partition, all partitions
// with a possible batch to be sent are used and all lingers are reset.
//
// As mentioned, the linger is specific to topic partition. A high volume
// producer will likely be producing to many partitions; it is both unnecessary
// to linger in this case and inefficient because the client will have many
// timers running (and stopping and restarting) unnecessarily.
func Linger(linger time.Duration) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.linger = linger }}
}

// ManualFlushing disables auto-flushing when producing. While you can still
// set lingering, it would be useless to do so.
//
// With manual flushing, producing while MaxBufferedRecords have already been
// produced and not flushed will return ErrMaxBuffered.
func ManualFlushing() ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.manualFlushing = true }}
}

// RecordTimeout sets a rough time of how long a record can sit
// around in a batch before timing out.
//
// Note that the timeout for all records in a batch inherit the timeout of the
// first record in that batch. That is, once the first record's timeout
// expires, all records in the batch are expired. This generally is a non-issue
// unless using this option with lingering. In that case, simply add the linger
// to the record timeout to avoid problems.
//
// Also note that the timeout is only evaluated after a produce response, and
// only for batches that need to be retried. Thus, a sink backoff may delay
// record timeout slightly. As with lingering, this also should generally be a
// non-issue.
func RecordTimeout(timeout time.Duration) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.recordTimeout = timeout }}
}

// TransactionalID sets a transactional ID for the client, ensuring that
// records are produced transactionally under this ID (exactly once semantics).
//
// For Kafka-to-Kafka transactions, the transactional ID is only one half of
// the equation. You must also assign a group to consume from.
//
// To produce transactionally, you first BeginTransaction, then produce records
// consumed from a group, then you EndTransaction. All records prodcued outside
// of a transaction will fail immediately with an error.
//
// After producing a batch, you must commit what you consumed. Auto committing
// offsets is disabled during transactional consuming / producing.
//
// Note that, unless using Kafka 2.5.0, a consumer group rebalance may be
// problematic. Production should finish and be committed before the client
// rejoins the group. It may be safer to use an eager group balancer and just
// abort the transaction. Alternatively, any time a partition is revoked, you
// could abort the transaction and reset offsets being consumed.
//
// If the client detects an unrecoverable error, all records produced
// thereafter will fail.
//
// Lastly, the default read level is READ_UNCOMMITTED. Be sure to use the
// ReadIsolationLevel option if you want to only read committed.
func TransactionalID(id string) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.txnID = &id }}
}

// TransactionTimeout sets the allowed for a transaction, overriding the
// default 60s. It may be a good idea to set this under the rebalance timeout
// for a group, so that a produce will not complete successfully after the
// consumer group has already moved the partitions the consumer/producer is
// working on from one group member to another.
//
// Transaction timeouts begin when the first record is produced within a
// transaction, not when a transaction begins.
func TransactionTimeout(timeout time.Duration) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.txnTimeout = timeout }}
}

// ********** CONSUMER CONFIGURATION **********

// FetchMaxWait sets the maximum amount of time a broker will wait for a
// fetch response to hit the minimum number of required bytes before returning,
// overriding the default 5s.
//
// This corresponds to the Java replica.fetch.wait.max.ms setting.
func FetchMaxWait(wait time.Duration) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.maxWait = int32(wait.Milliseconds()) }}
}

// FetchMaxBytes sets the maximum amount of bytes a broker will try to
// send during a fetch, overriding the default 50MiB. Note that brokers may not
// obey this limit if it has messages larger than this limit. Also note that
// this client sends a fetch to each broker concurrently, meaning the client
// will buffer up to <brokers * max bytes> worth of memory.
//
// This corresponds to the Java fetch.max.bytes setting.
func FetchMaxBytes(b int32) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.maxBytes = b }}
}

// FetchMaxPartitionBytes sets the maximum amount of bytes that will be
// consumed for a single partition in a fetch request, overriding the default
// 10MiB. Note that if a single batch is larger than this number, that batch
// will still be returned so the client can make progress.
//
// This corresponds to the Java max.partition.fetch.bytes setting.
func FetchMaxPartitionBytes(b int32) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.maxPartBytes = b }}
}

// ConsumeResetOffset sets the offset to restart consuming from when a
// partition has no commits (for groups) or when a fetch sees an
// OffsetOutOfRange error, overriding the default ConsumeStartOffset.
func ConsumeResetOffset(offset Offset) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.resetOffset = offset }}
}

// Rack specifies where the client is physically located and changes fetch
// requests to consume from the closest replica as opposed to the leader
// replica.
//
// Consuming from a preferred replica can increase latency but can decrease
// cross datacenter costs. See KIP-392 for more information.
func Rack(rack string) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.rack = rack }}
}

// IsolationLevel controls whether uncommitted or only committed records are
// returned from fetch requests.
type IsolationLevel struct {
	level int8
}

// ReadUncommitted (the default) is an isolation level that returns the latest
// produced records, be they committed or not.
func ReadUncommitted() IsolationLevel { return IsolationLevel{0} }

// ReadCommitted is an isolation level to only fetch committed records.
func ReadCommitted() IsolationLevel { return IsolationLevel{1} }

// FetchIsolationLevel sets the "isolation level" used for fetching
// records, overriding the default ReadUncommitted.
func FetchIsolationLevel(level IsolationLevel) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.isolationLevel = level.level }}
}

// KeepControlRecords sets the client to keep control messages and return
// them with fetches, overriding the default that discards them.
//
// Generally, control messages are not useful.
func KeepControlRecords() ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.keepControl = true }}
}
