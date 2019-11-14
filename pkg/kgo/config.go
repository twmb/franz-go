package kgo

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/twmb/kafka-go/pkg/kversion"
	"github.com/twmb/kafka-go/pkg/sasl"
)

// Opt is an option to configure a client.
type Opt interface {
	apply(*cfg)
}

type clientOpt struct{ fn func(*cfg) }

func (opt clientOpt) apply(cfg *cfg) { opt.fn(cfg) }

type cfg struct {
	client   clientCfg
	producer producerCfg
	consumer consumerCfg
}

func (cfg *cfg) validate() error {
	if err := cfg.client.validate(); err != nil {
		return err
	}
	if err := cfg.producer.validate(); err != nil {
		return err
	}
	if err := cfg.consumer.validate(); err != nil {
		return err
	}

	if cfg.client.maxBrokerWriteBytes < cfg.producer.maxRecordBatchBytes {
		return fmt.Errorf("max broker write bytes %d is erroneously less than max record batch bytes %d",
			cfg.client.maxBrokerWriteBytes, cfg.producer.maxRecordBatchBytes)
	}

	return nil
}

func defaultCfg() cfg {
	defaultID := "kgo"
	return cfg{
		client: clientCfg{
			id:     &defaultID,
			dialFn: stddial,

			seedBrokers: []string{"127.0.0.1"},

			retryBackoff: func(int) time.Duration { return 100 * time.Millisecond },
			retries:      math.MaxInt32, // effectively unbounded

			maxBrokerWriteBytes: 100 << 20, // Kafka socket.request.max.bytes default is 100<<20

			metadataMaxAge: 5 * time.Minute,
			metadataMinAge: 10 * time.Second,
		},

		producer: producerCfg{
			txnTimeout:          60 * time.Second,
			acks:                RequireAllISRAcks(),
			compression:         []CompressionCodec{NoCompression()},
			maxRecordBatchBytes: 1000000, // Kafka max.message.bytes default is 1000012
			maxBufferedRecords:  100000,
			requestTimeout:      30 * time.Second,
			partitioner:         StickyKeyPartitioner(),
		},

		consumer: consumerCfg{
			maxWait:        500,
			maxBytes:       50 << 20,
			maxPartBytes:   10 << 20,
			resetOffset:    NewOffset(AtStart()),
			isolationLevel: 0,
		},
	}
}

// ********** CLIENT CONFIGURATION **********

type clientCfg struct {
	id     *string
	dialFn func(string) (net.Conn, error)

	seedBrokers []string
	maxVersions kversion.Versions

	retryBackoff func(int) time.Duration
	retries      int

	maxBrokerWriteBytes int32

	metadataMaxAge time.Duration
	metadataMinAge time.Duration

	sasls []sasl.Mechanism
}

func (cfg *clientCfg) validate() error {
	if cfg.maxBrokerWriteBytes < 1<<10 {
		return fmt.Errorf("max broker write bytes %d is less than min acceptable %d", cfg.maxBrokerWriteBytes, 1<<10)
	}
	// upper bound broker write bytes to avoid any problems with
	// overflowing numbers in calculations.
	if cfg.maxBrokerWriteBytes > 1<<30 {
		return fmt.Errorf("max broker write bytes %d is greater than max acceptable %d", cfg.maxBrokerWriteBytes, 1<<30)
	}
	return nil
}

// WithClientID uses id for all requests sent to Kafka brokers, overriding the
// default "kgo".
func WithClientID(id string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.id = &id }}
}

// WithoutClientID sets the client ID to null for all requests sent to Kafka
// brokers, overriding the default "kgo".
func WithoutClientID() Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.id = nil }}
}

// WithDialFn uses fn to dial addresses, overriding the default dialer that
// uses a 10s timeout and no TLS.
func WithDialFn(fn func(string) (net.Conn, error)) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.dialFn = fn }}
}

// WithSeedBrokers sets the seed brokers for the client to use, overriding the
// default 127.0.0.1:9092.
func WithSeedBrokers(seeds ...string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.seedBrokers = append(cfg.client.seedBrokers[:0], seeds...) }}
}

// WithMaxVersions sets the maximum Kafka version to try, overriding the
// internal unbounded (latest) versions.
//
// Note that specific max version pinning is required if trying to interact
// with versions pre 0.10.0. Otherwise, unless using more complicated requests
// that this client itself does not natively use, it is generally safe to opt
// for the latest version.
func WithMaxVersions(versions kversion.Versions) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.maxVersions = versions }}
}

// WithRetryBackoff sets the backoff strategy for how long to backoff for a
// given amount of retries, overriding the default constant 100ms.
//
// The function is called with the number of failures that have occurred in a
// row. This can be used to implement exponential backoff if desired.
//
// This (roughly) corresponds to Kafka's retry.backoff.ms setting.
func WithRetryBackoff(backoff func(int) time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.retryBackoff = backoff }}
}

// WithRetries sets the number of tries that retriable requests are allowed,
// overriding the unlimited default.
//
// This setting applies to all types of requests.
func WithRetries(n int) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.retries = n }}
}

// WithBrokerMaxWriteBytes upper bounds the number of bytes written to a broker
// connection in a single write, overriding the default 100MiB.
//
// This number corresponds to the a broker's socket.request.max.bytes, which
// defaults to 100MiB.
//
// The only Kafka request that could come reasonable close to hitting this
// limit should be produce requests.
func WithBrokerMaxWriteBytes(v int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.maxBrokerWriteBytes = v }}
}

// WithMetadataMaxAge sets the maximum age for the client's cached metadata,
// overriding the default 5m, to allow detection of new topics, partitions,
// etc.
//
// This corresponds to Kafka's metadata.max.age.ms.
func WithMetadataMaxAge(age time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.metadataMaxAge = age }}
}

// WithMetadataMinAge sets the minimum time between metadata queries,
// overriding the default 10s. You may want to raise or lower this to reduce
// the number of metadata queries the client will make. Notably, if metadata
// detects an error in any topic or partition, it triggers itself to update as
// soon as allowed. Additionally, any connection failures causing backoff while
// producing or consuming trigger metadata updates, because the client must
// assume that maybe the connection died due to a broker dying.
func WithMetadataMinAge(age time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.metadataMinAge = age }}
}

// WithSASL appends sasl authentication options to use for all connections.
//
// SASL is tried in order; if the broker supports the first mechanism, all
// connections will use that mechanism. If the first mechanism fails, the
// client will pick the first supported mechanism. If the broker does not
// support any client mechanisms, connections will fail.
func WithSASL(sasls ...sasl.Mechanism) Opt {
	return clientOpt{func(cfg *cfg) { cfg.client.sasls = append(cfg.client.sasls, sasls...) }}
}

// ********** PRODUCER CONFIGURATION **********

type producerCfg struct {
	txnID       *string
	txnTimeout  time.Duration
	acks        RequiredAcks
	compression []CompressionCodec // order of preference

	allowAutoTopicCreation bool

	maxRecordBatchBytes int32
	maxBufferedRecords  int64
	requestTimeout      time.Duration
	linger              time.Duration
	recordTimeout       time.Duration

	partitioner Partitioner

	stopOnDataLoss bool
	onDataLoss     func(string, int32)
}

func (cfg *producerCfg) validate() error {
	if cfg.maxRecordBatchBytes < 1<<10 {
		return fmt.Errorf("max record batch bytes %d is less than min acceptable %d", cfg.maxRecordBatchBytes, 1<<10)
	}

	return nil
}

// RequiredAcks represents the number of acks a broker leader must have before
// a produce request is considered complete.
//
// This controls the durability of written records and corresponds to "acks" in
// Kafka's Producer Configuration documentation.
//
// The default is RequireLeaderAck.
type RequiredAcks struct {
	val int16
}

// RequireNoAck considers records sent as soon as they are written on the wire.
// The leader does not reply to records.
func RequireNoAck() RequiredAcks { return RequiredAcks{0} }

// RequireLeaderAck causes Kafka to reply that a record is written after only
// the leader has written a message. The leader does not wait for in-sync
// replica replies.
func RequireLeaderAck() RequiredAcks { return RequiredAcks{1} }

// RequireAllISRAcks ensures that all in-sync replicas have acknowledged they
// wrote a record before the leader replies success.
func RequireAllISRAcks() RequiredAcks { return RequiredAcks{-1} }

// WithProduceRequiredAcks sets the required acks for produced records,
// overriding the default RequireLeaderAck.
func WithProduceRequiredAcks(acks RequiredAcks) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.acks = acks }}
}

// WithProduceAutoTopicCreation enables topics to be auto created if they do
// not exist when sending messages to them.
func WithProduceAutoTopicCreation() Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.allowAutoTopicCreation = true }}
}

// WithProduceCompression sets the compression codec to use for records.
//
// Compression is chosen in the order preferred based on broker support.
// For example, zstd compression was introduced in Kafka 2.1.0, so the
// preference can be first zstd, fallback gzip, fallback none.
//
// The default preference is no compression.
func WithProduceCompression(preference ...CompressionCodec) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.compression = preference }}
}

// WithProduceMaxRecordBatchBytes upper bounds the size of a record batch,
// overriding the default 1MB.
//
// This corresponds to Kafka's max.message.bytes, which defaults to 1,000,012
// bytes (just over 1MB).
//
// RecordBatch's are independent of a ProduceRequest: a record batch is
// specific to a topic and partition, whereas the produce request can contain
// many record batches for many topics.
//
// If a single record encodes larger than this number (before compression), it
// will will not be written and a callback will have the appropriate error.
//
// Note that this is the maximum size of a record batch before compression.
// If a batch compresses poorly and actually grows the batch, the uncompressed
// form will be used.
func WithProduceMaxRecordBatchBytes(v int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.maxRecordBatchBytes = v }}
}

// WithProducePartitioner uses the given partitioner to partition records,
// overriding the default StickyKeyPartitioner.
func WithProducePartitioner(partitioner Partitioner) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.partitioner = partitioner }}
}

// WithProduceTimeout sets how long Kafka broker's are allowed to respond to
// produce requests, overriding the default 30s. If a broker exceeds this
// duration, it will reply with a request timeout error.
//
// This corresponds to Kafka's request.timeout.ms setting, but only applies to
// produce requests. The reason for this is that most Kafka requests do not
// actually have a timeout field.
func WithProduceTimeout(limit time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.requestTimeout = limit }}
}

// WithProduceStopOnDataLoss sets the client to stop producing if data loss is
// detected, overriding the default false.
func WithProduceStopOnDataLoss() Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.stopOnDataLoss = true }}
}

// WithProduceOnDataLoss sets a function to call if data loss is detected when
// producing records.
//
// The passed function will be called with the topic and partition that data
// loss was detected on.
func WithProduceOnDataLoss(fn func(string, int32)) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.onDataLoss = fn }}
}

// WithProduceLinger sets how long individual topic partitions will linger
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
func WithProduceLinger(linger time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.linger = linger }}
}

// WithProduceRecordTimeout sets a rough time of how long a record can sit
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
func WithProduceRecordTimeout(timeout time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.recordTimeout = timeout }}
}

// WithTransactionalID sets a transactional ID for the client, ensuring that
// records are produced transactionally under this ID (exactly once semantics).
//
// For transactions, the transactional ID is only one half of the equation. You
// must also assign a group to consume from.
//
// To produce transactionally, you first BeginTransaction, then produce records
// consumed from a group, then you EndTransaction. All records prodcued outside
// of a transaction will fail immediately with an error.
//
// After producing a batch, you must commit what you consumed. Autocommitting
// offsets is disabled during transactional consuming / producing.
//
// Note that, unless using Kafka 2.5.0, a consumer group rebalance may be
// problematic. Production should finish and be committed before the client
// rejoins the group. It may be safer to use an eager group balancer and just
// abort the transaction.
func WithTransactionalID(id string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.txnID = &id }}
}

// WithTransactionTimeout sets the allowed for a transaction, overriding the
// default 60s. It may be a good idea to set this under the rebalance timeout
// for a group, so that a produce will not complete successfully after the
// consumer group has already moved the partitions the consumer/producer is
// working on from one group member to another.
//
// Transaction timeouts begin when the first record is produced within a
// transaction, not when a transaction begins.
func WithTransactionTimeout(timeout time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.producer.txnTimeout = timeout }}
}

// ********** CONSUMER CONFIGURATION **********

type consumerCfg struct {
	maxWait        int32
	maxBytes       int32
	maxPartBytes   int32
	resetOffset    Offset
	isolationLevel int8
}

func (cfg *consumerCfg) validate() error {
	return nil
}

// WithConsumeMaxWait sets the maximum amount of time a broker will wait for a
// fetch response to hit the minimum number of required bytes before returning,
// overriding the default 500ms.
//
// This corresponds to the Java replica.fetch.wait.max.ms setting.
func WithConsumeMaxWait(wait time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.consumer.maxWait = int32(wait.Milliseconds()) }}
}

// WithConsumeMaxBytes sets the maximum amount of bytes a broker will try to
// send during a fetch, overriding the default 50MiB. Note that brokers may not
// obey this limit if it has messages larger than this limit. Also note that
// this client sends a fetch to each broker concurrently, meaning the client
// will buffer up to <brokers * max bytes> worth of memory.
//
// This corresponds to the Java fetch.max.bytes setting.
func WithConsumeMaxBytes(b int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.consumer.maxBytes = b }}
}

// WithConsumeMaxPartitionBytes sets the maximum amount of bytes that will be
// consumed for a single partition in a fetch request, overriding the default
// 10MiB. Note that if a single batch is larger than this number, that batch
// will still be returned so the client can make progress.
//
// This corresponds to the Java max.partition.fetch.bytes setting.
func WithConsumeMaxPartitionBytes(b int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.consumer.maxPartBytes = b }}
}

// WithConsumeResetOffset sets the offset to restart consuming from when a
// partition has no commits (for groups) or when a fetch sees an
// OffsetOutOfRange error, overriding the default ConsumeStartOffset.
func WithConsumeResetOffset(offset Offset) Opt {
	return clientOpt{func(cfg *cfg) { cfg.consumer.resetOffset = offset }}
}

// IsolationLevel controls whether uncommitted or only committed records are
// returned from fetch requests.
type IsolationLevel struct {
	level int8
}

// ReadUncommitted, the default, is an isolation level that returns the latest
// produced records, be they committed or not.
func ReadUncommitted() IsolationLevel { return IsolationLevel{0} }

// ReadCommitted is an isolation level to only fetch committed records.
func ReadCommitted() IsolationLevel { return IsolationLevel{1} }

// WithConsumeIsolationLevel sets the "isolation level" used for fetching
// records, overriding the default ReadUncommitted.
func WithConsumeIsolationLevel(level IsolationLevel) Opt {
	return clientOpt{func(cfg *cfg) { cfg.consumer.isolationLevel = level.level }}
}
