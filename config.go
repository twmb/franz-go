package kgo

import (
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

// TODO STRICT LENGTH VALIDATION
// max bytes: 1G
// client id length: int16
// delivery.timeout.ms? (120s, upper bound on how long til acknowledged)
// max.block.ms? (60s, upper bound on how long blocked on Produce)
// reconnect.backoff.max.ms? (1s)
// reconnect.backoff.ms

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
	if err := cfg.producer.validate(); err != nil {
		return err
	}

	if cfg.client.maxBrokerWriteBytes < cfg.producer.maxRecordBatchBytes {
		return fmt.Errorf("max broker write bytes %d is erroneously less than max record batch bytes %d",
			cfg.client.maxBrokerWriteBytes, cfg.producer.maxRecordBatchBytes)
	}

	return nil
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

			// TODO rename tries, tryBackoff
			retryBackoff:   func(int) time.Duration { return 100 * time.Millisecond },
			retries:        math.MaxInt32, // effectively unbounded
			requestTimeout: int32(30 * time.Second / 1e4),

			maxBrokerWriteBytes: 100 << 20, // Kafka socket.request.max.bytes default is 100<<20
		},
		producer: producerCfg{
			acks:        RequireAllISRAcks(),
			compression: []CompressionCodec{NoCompression()},

			maxRecordBatchBytes: 1000000, // Kafka max.message.bytes default is 1000012
			maxBufferedRecords:  100000,

			partitioner: RandomPartitioner(),
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

	seedAddrs := make([]string, 0, len(seedBrokers))
	for _, seedBroker := range seedBrokers {
		addr := seedBroker
		port := 9092 // default kafka port
		var err error
		if colon := strings.IndexByte(addr, ':'); colon > 0 {
			port, err = strconv.Atoi(addr[colon+1:])
			if err != nil {
				return nil, fmt.Errorf("unable to parse addr:port in %q", seedBroker)
			}
			addr = addr[:colon]
		}

		if addr == "localhost" {
			addr = "127.0.0.1"
		}

		if !isAddr(addr) && !isDomain(addr) {
			return nil, fmt.Errorf("%q is neither an IP address nor a domain", addr)
		}

		seedAddrs = append(seedAddrs, net.JoinHostPort(addr, strconv.Itoa(port)))
	}

	c := &Client{
		cfg: cfg,

		rng: rand.New(new(rand.PCGSource)),

		controllerID: unknownControllerID,

		brokers: make(map[int32]*broker),

		producer: producer{
			waitBuffer: make(chan struct{}, 100),
		},

		coordinators: make(map[coordinatorKey]int32),

		topics: make(map[string]*topicPartitions),

		metadataTicker:   time.NewTicker(time.Minute), // TODO configurable?
		updateMetadataCh: make(chan struct{}, 1),

		closedCh: make(chan struct{}),
	}
	c.rng.Seed(uint64(time.Now().UnixNano()))

	for i, seedAddr := range seedAddrs {
		b := c.newBroker(seedAddr, unknownSeedID(i))
		c.brokers[b.id] = b
		c.anyBroker = append(c.anyBroker, b)
	}
	go c.updateMetadataLoop()

	return c, nil
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

		retryBackoff   func(int) time.Duration
		retries        int
		requestTimeout int32

		maxBrokerWriteBytes int32

		// TODO dial fn convenience wrappers for tls, timeouts
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

// WithRequestTimeout sets how long Kafka broker's are allowed to respond
// to requests, overriding the default 30s. If a broker exceeds this duration,
// it will reply with a request timeout error.
//
// This corresponds to Kafka's request.timeout.ms setting. It is invalid to use
// >596h (math.MaxInt32 milliseconds).
func WithRequestTimeout(limit time.Duration) OptClient {
	return clientOpt{func(cfg *clientCfg) { cfg.requestTimeout = int32(limit / 1e6) }}
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

		partitioner Partitioner
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

	// TODO maxBrokerWriteBytes should be > 2*max.MathInt16 (client ID,
	// transactional ID) + maxRecordBatchBytes + 2+2+4+2+2+2+4+4 (message
	// request + producer thing) + 2 (transactional ID)

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

// ********** CONSUMER CONFIGURATION **********
