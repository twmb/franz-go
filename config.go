package kgo

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

type (
	// Opt is an option to configure a client.
	Opt interface {
		isopt()
	}

	cfg struct {
		client   clientCfg
		producer producerCfg
	}
)

func (cfg *cfg) validate() error {
	if err := cfg.client.validate(); err != nil {
		return err
	}
	return cfg.producer.validate()
}

// domainRe validates domains: a label, and at least one dot-label.
var domainRe = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*(?:\.[a-z0-9]+(?:-[a-z0-9]+)*)+$`)

// stddialer is the default dialer for dialing connections.
var stddialer = net.Dialer{Timeout: 10 * time.Second}

func stddial(addr string) (net.Conn, error) { return stddialer.Dial("tcp", addr) }

func NewClient(seedBrokers []string, opts ...Opt) (*Client, error) {
	defaultID := "kgo"
	cfg := cfg{
		client: clientCfg{
			id:     &defaultID,
			dialFn: stddial,
		},
		producer: producerCfg{
			acks:        RequireLeaderAck(),
			compression: []CompressionCodec{NoCompression()},

			maxRecordBatchBytes: 1000000,   // Kafka max.message.bytes default is 1000012
			maxBrokerWriteBytes: 100 << 20, // Kafka socket.request.max.bytes default is 100<<20

			brokerBufBytes: 100 << 30, // "unbounded"; hard stop at maxBrokerWriteBytes
			brokerBufDur:   250 * time.Millisecond,

			// TODO partitioner
		},
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case OptClient:
			opt.apply(&cfg.client)
		case OptProducer:
			opt.apply(&cfg.producer)
		default:
			panic(fmt.Sprintf("unknown opt type: %#v", opt))
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	isAddr := func(addr string) bool { return net.ParseIP(addr) != nil }
	isDomain := func(domain string) bool {
		if len(domain) < 3 || len(domain) > 255 {
			return false
		}
		for _, label := range strings.Split(domain, ".") {
			if len(label) > 63 {
				return false
			}
		}
		return domainRe.MatchString(strings.ToLower(domain))
	}

	brokers := make([]string, 0, len(seedBrokers))
	for _, seedBroker := range seedBrokers {
		addr := seedBroker
		port := 9092 // default kafka port
		var err error
		if colon := strings.IndexByte(addr, ':'); colon > 0 {
			addr = addr[:colon]
			port, err = strconv.Atoi(addr[colon+1:])
			if err != nil {
				return nil, fmt.Errorf("unable to parse addr:port in %q", seedBroker)
			}
		}

		if !isAddr(addr) && !isDomain(addr) {
			return nil, fmt.Errorf("%q is neither an IP address nor a domain", addr)
		}

		brokers = append(brokers, net.JoinHostPort(addr, strconv.Itoa(port)))
	}

	c := &Client{
		cfg: cfg,

		rng: rand.New(new(rand.PCGSource)),

		seedBrokers:    brokers,
		untriedSeeds:   append([]string(nil), brokers...),
		untriedBrokers: make(map[int32]*broker),
		triedBrokers:   make(map[int32]struct{}),
	}
	c.rng.Seed(uint64(time.Now().UnixNano()))

	return c, (&broker{cl: c, addr: brokers[0]}).connect()
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
		tlsCfg *tls.Config

		// TODO Conn timeouts? Or, DialFn wrapper?
		// TODO SASL
		// TODO allow unsupported features
		// TODO kafka < 0.10.0.0 ? ( no API versions )
		// TODO kafka < 0.11.0.0 ? ( no record batch)
	}
)

func (opt clientOpt) isopt()               {}
func (opt clientOpt) apply(cfg *clientCfg) { opt.fn(cfg) }

func (cfg *clientCfg) validate() error {
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
// uses a 10s timeout.
func WithDialFn(fn func(string) (net.Conn, error)) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.dialFn = fn }}
}

// WithTLSCfg uses tlsCfg for all connections.
func WithTLSCfg(tlsCfg *tls.Config) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.tlsCfg = tlsCfg }}
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
		acks        RequiredAcks
		compression []CompressionCodec // order of preference

		maxRecordBatchBytes int
		maxBrokerWriteBytes int
		maxBrokerBufdRecs   int

		brokerBufBytes int
		brokerBufDur   time.Duration

		partitioner Partitioner

		// TODO:
		// retries
		// retry backoff

		// MAYBE:
		// idempotency
	}
)

func (opt producerOpt) isopt()                 {}
func (opt producerOpt) apply(cfg *producerCfg) { opt.fn(cfg) }

func (cfg *producerCfg) validate() error {
	for _, codec := range cfg.compression {
		if err := codec.validate(); err != nil {
			return err
		}
	}
	if cfg.maxRecordBatchBytes < 1<<10 {
		return fmt.Errorf("max record batch bytes %d is less than min acceptable %d", cfg.maxRecordBatchBytes, 1<<10)
	}
	if cfg.maxBrokerWriteBytes < 1<<10 {
		return fmt.Errorf("max broker write bytes %d is less than min acceptable %d", cfg.maxBrokerWriteBytes, 1<<10)
	}
	if cfg.maxBrokerWriteBytes < cfg.maxRecordBatchBytes {
		return fmt.Errorf("max broker write bytes %d is erroneously less than max record batch bytes %d", cfg.maxBrokerWriteBytes, cfg.maxRecordBatchBytes)
	}

	// upper bound broker write bytes to avoid any problems with
	// overflowing numbers in calculations.
	if cfg.maxBrokerWriteBytes > 1<<30 {
		return fmt.Errorf("max broker write bytes %d is greater than max acceptable %d", cfg.maxBrokerWriteBytes, 1<<30)
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

// WithRequiredAcks sets the required acks for produced records, overriding
// the default RequireLeaderAck.
func WithRequiredAcks(acks RequiredAcks) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.acks = acks }}
}

// CompressionCodec configures how records are compressed before being sent.
// TODO expand: batch? individually?
type CompressionCodec struct {
	codec int // 1: gzip, 2: snappy, 3: lz4, 4: zstd
	level int
}

func (c CompressionCodec) validate() error {
	// KIP-390
	var min, max int
	var name string
	switch c.codec {
	case 0:
		min, max, name = 0, 0, "no-compression"
	case 1:
		min, max, name = 0, 9, "gzip"
	case 3:
		min, max, name = 0, 12, "lz4" // 12 is LZ4HC_CLEVEL_MAX in C
	case 4: // can be negative? but we will not support that for now
		min, max, name = 1, 22, "zstd"
	default:
		return errors.New("unknown compression codec")
	}
	if c.level < min || c.level > max {
		return fmt.Errorf("invalid %s compression level %d (min %d, max %d)", name, c.level, min, max)
	}
	return nil
}

// NoCompression is the default compression used for messages and can be used
// as a fallback compression option.
func NoCompression() CompressionCodec { return CompressionCodec{0, 0} }

// GzipCompression enables gzip compression. Level must be between 0 and 9.
func GzipCompression(level int) CompressionCodec { return CompressionCodec{1, level} }

// SnappyCompression enables snappy compression.
func SnappyCompression() CompressionCodec { return CompressionCodec{2, 0} }

// Lz4Compression enables lz4 compression. Level must be between 0 and 12.
func Lz4Compression(level int) CompressionCodec { return CompressionCodec{3, level} }

// ZstdCompression enables zstd compression. Level must be between 1 and 22.
//
// TODO currently unsupported
func ZstdCompression(level int) CompressionCodec { return CompressionCodec{4, level} }

// WithCompressionPreference sets the compression codec to use for records.
//
// Compression is chosen in the order preferred based on broker support.
// For example, zstd compression was introduced in Kafka 2.1.0, so the
// preference can be first zstd, fallback gzip, fallback none.
//
// The default preference is no compression.
func WithCompressionPreference(preference ...CompressionCodec) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.compression = preference }}
}

// WithMaxRecordBatchBytes upper bounds the size of a record batch, overriding
// the default 100MB.
//
// This corresponds to Kafka's max.message.bytes, which defaults to 1,000,012
// bytes (just over 100MB).
//
// RecordBatch's are independent of a ProduceRequest: a record batch is
// specific to a topic, whereas the produce request can contain many record
// batches for many topics.
//
// Note that this is the maximum size of a record batch before compression.
// If a batch compresses poorly and actually grows the batch, the uncompressed
// form will be used.
func WithMaxRecordBatchBytes(v int) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.maxRecordBatchBytes = v }}
}

// WithBrokerMaxWriteBytes upper bounds the number of bytes written to a broker
// connection in a single write, overriding the default 100MiB.
//
// If a single record encodes larger than this number, it will will not be
// written and a callback will have the appropriate error.
//
// This number corresponds to the a broker's socket.request.max.bytes, which
// defaults to 100MiB.
func WithBrokerMaxWriteBytes(v int) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.maxBrokerWriteBytes = v }}
}

// WithBrokerBufferBytes sets when a broker will attempt to flush a produce
// request, overriding the unbounded default.
//
// Note that this setting can increase memory usage on a per broker basis,
// since each broker may buffer many records in memory before hitting the
// buffer byte limit.
//
// To disable record buffering, set this to zero.
func WithBrokerBufferBytes(v int) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.brokerBufBytes = v }}
}

// WithBrokerMaxBufferDuration sets the maximum amount of time that brokers
// will buffer records before writing, overriding the default 250ms.
func WithBrokerMaxBufferDuration(d time.Duration) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.brokerBufDur = d }}
}

// WithPartitioner uses the given partitioner to partition records, overriding
// the default hash partitioner.
func WithPartitioner(partitioner Partitioner) OptProducer {
	return producerOpt{func(cfg *producerCfg) { cfg.partitioner = partitioner }}
}

// ********** CONSUMER CONFIGURATION **********
