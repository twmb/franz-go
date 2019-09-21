package kgo

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/twmb/kgo/kversion"
)

type (
	// Opt is an option to configure a client.
	Opt interface {
		isopt()
	}

	cfg struct {
		client   clientCfg
		producer producerCfg
		consumer consumerCfg
	}
)

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
		},

		producer: producerCfg{
			acks:                RequireAllISRAcks(),
			compression:         []CompressionCodec{NoCompression()},
			maxRecordBatchBytes: 1000000, // Kafka max.message.bytes default is 1000012
			maxBufferedRecords:  100000,
			requestTimeout:      30 * time.Second,
			partitioner:         RandomPartitioner(),
		},

		consumer: consumerCfg{
			maxWait:      500,
			maxBytes:     50 << 20,
			maxPartBytes: 10 << 20,
			resetOffset:  ConsumeStartOffset(),
		},
	}
}

// ********** CLIENT CONFIGURATION **********

type (
	// OptClient is an option to configure client settings.
	OptClient interface {
		Opt
		apply(*clientCfg)
	}

	clientOpt struct{ fn func(cfg *clientCfg) }

	clientCfg struct {
		id     *string
		dialFn func(string) (net.Conn, error)

		seedBrokers []string
		maxVersions kversion.Versions

		retryBackoff func(int) time.Duration
		retries      int

		maxBrokerWriteBytes int32

		metadataMaxAge time.Duration

		// TODO SASL
	}
)

func (opt clientOpt) isopt()               {}
func (opt clientOpt) apply(cfg *clientCfg) { opt.fn(cfg) }

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
//
// This accepts a pointer to a string because Kafka allows differentiation
// between writing a null string and an empty string.
func WithClientID(id *string) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.id = id }}
}

// WithDialFn uses fn to dial addresses, overriding the default dialer that
// uses a 10s timeout and no TLS.
func WithDialFn(fn func(string) (net.Conn, error)) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.dialFn = fn }}
}

// WithSeedBrokers sets the seed brokers for the client to use, overriding the
// default 127.0.0.1:9092.
func WithSeedBrokers(seeds ...string) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.seedBrokers = append(cfg.seedBrokers[:0], seeds...) }}
}

// WithMaxVersions sets the maximum Kafka version to try, overriding the
// internal unbounded (latest) versions.
//
// Note that specific max version pinning is required if trying to interact
// with versions pre 0.10.0. Otherwise, unless using more complicated requests
// that this client itself does not natively use, it is generally safe to opt
// for the latest version.
func WithMaxVersions(versions kversion.Versions) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.maxVersions = versions }}
}

// WithRetryBackoff sets the backoff strategy for how long to backoff for a
// given amount of retries, overriding the default constant 100ms.
//
// The function is called with the number of failures that have occurred in a
// row. This can be used to implement exponential backoff if desired.
//
// This (roughly) corresponds to Kafka's retry.backoff.ms setting.
func WithRetryBackoff(backoff func(int) time.Duration) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.retryBackoff = backoff }}
}

// WithRetries sets the number of tries that retriable requests are allowed,
// overriding the unlimited default.
//
// This setting applies to all types of requests.
func WithRetries(n int) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.retries = n }}
}

// WithBrokerMaxWriteBytes upper bounds the number of bytes written to a broker
// connection in a single write, overriding the default 100MiB.
//
// This number corresponds to the a broker's socket.request.max.bytes, which
// defaults to 100MiB.
//
// The only Kafka request that could come reasonable close to hitting this
// limit should be produce requests.
func WithBrokerMaxWriteBytes(v int32) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.maxBrokerWriteBytes = v }}
}

// WithMetadataMaxAge sets the maximum age for the client's cached metadata,
// overriding the default 5m, to allow detection of new topics, partitions,
// etc.
//
// This corresponds to Kafka's metadata.max.age.ms.
func WithMetadataMaxAge(age time.Duration) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.metadataMaxAge = age }}
}

// ********** PRODUCER CONFIGURATION **********

type (
	// OptProducer is an option to configure how a client produces records.
	OptProducer interface {
		Opt
		apply(*producerCfg)
	}

	producerOpt struct{ fn func(cfg *producerCfg) }

	producerCfg struct {
		txnID       *string
		acks        RequiredAcks
		compression []CompressionCodec // order of preference

		allowAutoTopicCreation bool

		maxRecordBatchBytes int32
		maxBufferedRecords  int64
		requestTimeout      time.Duration

		partitioner Partitioner

		continueOnDataLoss bool
		onDataLoss         func(string, int32)
	}
)

func (opt producerOpt) isopt()                 {}
func (opt producerOpt) apply(cfg *producerCfg) { opt.fn(cfg) }

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
func WithProduceRequiredAcks(acks RequiredAcks) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.acks = acks }}
}

// WithProduceAutoTopicCreation enables topics to be auto created if they do
// not exist when sending messages to them.
func WithProduceAutoTopicCreation() OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.allowAutoTopicCreation = true }}
}

// WithProduceCompression sets the compression codec to use for records.
//
// Compression is chosen in the order preferred based on broker support.
// For example, zstd compression was introduced in Kafka 2.1.0, so the
// preference can be first zstd, fallback gzip, fallback none.
//
// The default preference is no compression.
func WithProduceCompression(preference ...CompressionCodec) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.compression = preference }}
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
func WithProduceMaxRecordBatchBytes(v int32) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.maxRecordBatchBytes = v }}
}

// WithProducePartitioner uses the given partitioner to partition records,
// overriding the default hash partitioner.
func WithProducePartitioner(partitioner Partitioner) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.partitioner = partitioner }}
}

// WithProduceTimeout sets how long Kafka broker's are allowed to respond to
// produce requests, overriding the default 30s. If a broker exceeds this
// duration, it will reply with a request timeout error.
//
// This corresponds to Kafka's request.timeout.ms setting, but only applies to
// produce requests. The reason for this is that most Kafka requests do not
// actually have a timeout field.
func WithProduceTimeout(limit time.Duration) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.requestTimeout = limit }}
}

// WithProduceContinueOnDataLoss sets the client to continue producing if data
// loss is detected, overriding the default false.
//
// This can be combined with WithProduceOnDataLoss to ensure client progress
// while being notified that data loss occurred.
func WithProduceContinueOnDataLoss() OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.continueOnDataLoss = true }}
}

// WithProduceOnDataLoss sets a function to call if data loss is detected when
// producing records.
//
// This can be combined with WithProduceContinueOnDataLoss to ensure client
// progress while being notified that data loss occurred.
//
// The passed function will be called with the topic and partition that data
// loss was detected on. Note that this option is unnecessary if you are not
// using WithProduceContinueOnDataLoss, as record production will fail with out
// of order sequence number or unknown producer id errors.
func WithProduceOnDataLoss(fn func(string, int32)) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.onDataLoss = fn }}
}

// ********** CONSUMER CONFIGURATION **********

type (
	// OptConsumer is an option to configure how a client consumes records.
	OptConsumer interface {
		Opt
		apply(*consumerCfg)
	}

	consumerOpt struct{ fn func(cfg *consumerCfg) }

	consumerCfg struct {
		maxWait      int32
		maxBytes     int32
		maxPartBytes int32
		resetOffset  Offset
	}
)

func (opt consumerOpt) isopt()                 {}
func (opt consumerOpt) apply(cfg *consumerCfg) { opt.fn(cfg) }

func (cfg *consumerCfg) validate() error {
	return nil
}

// WithConsumeMaxWait sets the maximum amount of time a broker will wait for a
// fetch response to hit the minimum number of required bytes before returning,
// overriding the default 500ms.
//
// This corresponds to the Java replica.fetch.wait.max.ms setting.
func WithConsumeMaxWait(wait time.Duration) OptConsumer {
	return consumerOpt{func(cfg *consumerCfg) { cfg.maxWait = int32(wait.Milliseconds()) }}
}

// WithConsumeMaxBytes sets the maximum amount of bytes a broker will try to
// send during a fetch, overriding the default 50MiB. Note that brokers may not
// obey this limit if it has messages larger than this limit. Also note that
// this client sends a fetch to each broker concurrently, meaning the client
// will buffer up to <brokers * max bytes> worth of memory.
//
// This corresponds to the Java fetch.max.bytes setting.
func WithConsumeMaxBytes(b int32) OptConsumer {
	return consumerOpt{func(cfg *consumerCfg) { cfg.maxBytes = b }}
}

// WithConsumeMaxPartitionBytes sets the maximum amount of bytes that will be
// consumed for a single partition in a fetch request, overriding the default
// 10MiB. Note that if a single batch is larger than this number, that batch
// will still be returned so the client can make progress.
//
// This corresponds to the Java max.partition.fetch.bytes setting.
func WithConsumeMaxPartitionBytes(b int32) OptConsumer {
	return consumerOpt{func(cfg *consumerCfg) { cfg.maxPartBytes = b }}
}

// WithConsumeResetOffset sets the offset to restart consuming from when a
// partition has no commits (for groups) or when a fetch sees an
// OffsetOutOfRange error, overriding the default ConsumeStartOffset.
func WithConsumeResetOffset(offset Offset) OptConsumer {
	return consumerOpt{func(cfg *consumerCfg) { cfg.resetOffset = offset }}
}
