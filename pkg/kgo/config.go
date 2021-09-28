package kgo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
)

// Opt is an option to configure a client.
type Opt interface {
	apply(*cfg)
}

// ProducerOpt is a producer specific option to configure a client.
// This is simply a namespaced Opt.
type ProducerOpt interface {
	Opt
	producerOpt()
}

// ConsumerOpt is a consumer specific option to configure a client.
// This is simply a namespaced Opt.
type ConsumerOpt interface {
	Opt
	consumerOpt()
}

// GroupOpt is a consumer group specific option to configure a client.
// This is simply a namespaced Opt.
type GroupOpt interface {
	Opt
	groupOpt()
}

type (
	clientOpt   struct{ fn func(*cfg) }
	producerOpt struct{ fn func(*cfg) }
	consumerOpt struct{ fn func(*cfg) }
	groupOpt    struct{ fn func(*cfg) }
)

func (opt clientOpt) apply(cfg *cfg)   { opt.fn(cfg) }
func (opt producerOpt) apply(cfg *cfg) { opt.fn(cfg) }
func (opt consumerOpt) apply(cfg *cfg) { opt.fn(cfg) }
func (opt groupOpt) apply(cfg *cfg)    { opt.fn(cfg) }
func (producerOpt) producerOpt()       {}
func (consumerOpt) consumerOpt()       {}
func (groupOpt) groupOpt()             {}

// A cfg can be written to while initializing a client, and after that it is
// only ever read from. Some areas of initializing may follow options, but all
// initializing is done before NewClient returns.
type cfg struct {
	/////////////////////
	// GENERAL SECTION //
	/////////////////////

	id                     *string // client ID
	dialFn                 func(context.Context, string, string) (net.Conn, error)
	requestTimeoutOverhead time.Duration
	connIdleTimeout        time.Duration

	softwareName    string // KIP-511
	softwareVersion string // KIP-511

	logger Logger

	seedBrokers []string
	maxVersions *kversion.Versions
	minVersions *kversion.Versions

	retryBackoff func(int) time.Duration
	retries      int64
	retryTimeout func(int16) time.Duration

	maxBrokerWriteBytes int32
	maxBrokerReadBytes  int32

	allowAutoTopicCreation bool

	metadataMaxAge time.Duration
	metadataMinAge time.Duration

	sasls []sasl.Mechanism

	hooks hooks

	//////////////////////
	// PRODUCER SECTION //
	//////////////////////

	txnID              *string
	txnTimeout         time.Duration
	acks               Acks
	disableIdempotency bool
	compression        []CompressionCodec // order of preference

	defaultProduceTopic string
	maxRecordBatchBytes int32
	maxBufferedRecords  int64
	produceTimeout      time.Duration
	recordRetries       int64
	linger              time.Duration
	recordTimeout       time.Duration
	manualFlushing      bool

	partitioner Partitioner

	stopOnDataLoss bool
	onDataLoss     func(string, int32)

	//////////////////////
	// CONSUMER SECTION //
	//////////////////////

	maxWait        int32
	minBytes       int32
	maxBytes       int32
	maxPartBytes   int32
	resetOffset    Offset
	isolationLevel int8
	keepControl    bool
	rack           string

	maxConcurrentFetches int

	topics     map[string]*regexp.Regexp   // topics to consume; if regex is true, values are compiled regular expressions
	partitions map[string]map[int32]Offset // partitions to directly consume from
	regex      bool

	////////////////////////////
	// CONSUMER GROUP SECTION //
	////////////////////////////

	group      string          // group we are in
	instanceID *string         // optional group instance ID
	balancers  []GroupBalancer // balancers we can use
	protocol   string          // "consumer" by default, expected to never be overridden

	sessionTimeout    time.Duration
	rebalanceTimeout  time.Duration
	heartbeatInterval time.Duration
	requireStable     bool

	onAssigned func(context.Context, *Client, map[string][]int32)
	onRevoked  func(context.Context, *Client, map[string][]int32)
	onLost     func(context.Context, *Client, map[string][]int32)

	setAssigned       bool
	setRevoked        bool
	setLost           bool
	setCommitCallback bool

	autocommitDisable  bool // true if autocommit was disabled or we are transactional
	autocommitGreedy   bool
	autocommitMarks    bool
	autocommitInterval time.Duration
	commitCallback     func(*Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)
}

// cooperative is a helper that returns whether all group balancers in the
// config are cooperative.
func (cfg *cfg) cooperative() bool {
	cooperative := true
	for _, balancer := range cfg.balancers {
		cooperative = cooperative && balancer.IsCooperative()
	}
	return cooperative
}

func (cfg *cfg) validate() error {
	if len(cfg.seedBrokers) == 0 {
		return errors.New("config erroneously has no seed brokers")
	}

	// We clamp maxPartBytes to maxBytes because some fake Kafka endpoints
	// (Oracle) cannot handle the mismatch correctly.
	if cfg.maxPartBytes > cfg.maxBytes {
		cfg.maxPartBytes = cfg.maxBytes
	}

	if cfg.disableIdempotency && cfg.txnID != nil {
		return errors.New("cannot both disable idempotent writes and use transactional IDs")
	}
	if !cfg.disableIdempotency && cfg.acks.val != -1 {
		return errors.New("idempotency requires acks=all")
	}

	for _, limit := range []struct {
		name    string
		sp      **string // if field is a *string, we take addr to it
		s       string
		allowed int
	}{
		// A 256 byte ID / software name & version is good enough and
		// fits with our max broker write byte min of 1K.
		{name: "client id", sp: &cfg.id, allowed: 256},
		{name: "software name", s: cfg.softwareName, allowed: 256},
		{name: "software version", s: cfg.softwareVersion, allowed: 256},

		// The following is the limit transitioning from two byte
		// prefix for flexible stuff to three bytes; as with above, it
		// is more than reasonable.
		{name: "transactional id", sp: &cfg.txnID, allowed: 16382},

		{name: "rack", s: cfg.rack, allowed: 512},
	} {
		s := limit.s
		if limit.sp != nil && *limit.sp != nil {
			s = **limit.sp
		}
		if len(s) > limit.allowed {
			return fmt.Errorf("%s length %d is larger than max allowed %d", limit.name, len(s), limit.allowed)
		}
	}

	i64lt := func(l, r int64) (bool, string) { return l < r, "less" }
	i64gt := func(l, r int64) (bool, string) { return l > r, "larger" }
	for _, limit := range []struct {
		name    string
		v       int64
		allowed int64
		badcmp  func(int64, int64) (bool, string)

		fmt  string
		durs bool
	}{
		// Min write of 1K and max of 1G is reasonable.
		{name: "max broker write bytes", v: int64(cfg.maxBrokerWriteBytes), allowed: 1 << 10, badcmp: i64lt},
		{name: "max broker write bytes", v: int64(cfg.maxBrokerWriteBytes), allowed: 1 << 30, badcmp: i64gt},

		// Same for read bytes.
		{name: "max broker read bytes", v: int64(cfg.maxBrokerReadBytes), allowed: 1 << 10, badcmp: i64lt},
		{name: "max broker read bytes", v: int64(cfg.maxBrokerReadBytes), allowed: 1 << 30, badcmp: i64gt},

		// For batches, we want at least 512 (reasonable), and the
		// upper limit is the max num when a uvarint transitions from 4
		// to 5 bytes. The upper limit is also more than reasoanble
		// (268M).
		{name: "max record batch bytes", v: int64(cfg.maxRecordBatchBytes), allowed: 512, badcmp: i64lt},
		{name: "max record batch bytes", v: int64(cfg.maxRecordBatchBytes), allowed: 268435454, badcmp: i64gt},

		// We do not want the broker write bytes to be less than the
		// record batch bytes, nor the read bytes to be less than what
		// we indicate to fetch.
		//
		// We cannot enforce if a single batch is larger than the max
		// fetch bytes limit, but hopefully we do not run into that.
		{v: int64(cfg.maxBrokerWriteBytes), allowed: int64(cfg.maxRecordBatchBytes), badcmp: i64lt, fmt: "max broker write bytes %v is erroneously less than max record batch bytes %v"},
		{v: int64(cfg.maxBrokerReadBytes), allowed: int64(cfg.maxBytes), badcmp: i64lt, fmt: "max broker read bytes %v is erroneously less than max fetch bytes %v"},

		// 0 <= allowed concurrency
		{name: "max concurrent fetches", v: int64(cfg.maxConcurrentFetches), allowed: 0, badcmp: i64lt},

		// 1s <= request timeout overhead <= 15m
		{name: "request timeout max overhead", v: int64(cfg.requestTimeoutOverhead), allowed: int64(15 * time.Minute), badcmp: i64gt, durs: true},
		{name: "request timeout min overhead", v: int64(cfg.requestTimeoutOverhead), allowed: int64(time.Second), badcmp: i64lt, durs: true},

		// 1s <= conn idle <= 15m
		{name: "conn min idle timeout", v: int64(cfg.connIdleTimeout), allowed: int64(time.Second), badcmp: i64lt, durs: true},
		{name: "conn max idle timeout", v: int64(cfg.connIdleTimeout), allowed: int64(15 * time.Minute), badcmp: i64gt, durs: true},

		// 10ms <= metadata <= 1hr
		{name: "metadata max age", v: int64(cfg.metadataMaxAge), allowed: int64(time.Hour), badcmp: i64gt, durs: true},
		{name: "metadata min age", v: int64(cfg.metadataMinAge), allowed: int64(10 * time.Millisecond), badcmp: i64lt, durs: true},
		{v: int64(cfg.metadataMaxAge), allowed: int64(cfg.metadataMinAge), badcmp: i64lt, fmt: "metadata max age %v is erroneously less than metadata min age %v", durs: true},

		// Some random producer settings.
		{name: "max buffered records", v: int64(cfg.maxBufferedRecords), allowed: 1, badcmp: i64lt},
		{name: "linger", v: int64(cfg.linger), allowed: int64(time.Minute), badcmp: i64gt, durs: true},
		{name: "produce timeout", v: int64(cfg.produceTimeout), allowed: int64(time.Second), badcmp: i64lt, durs: true},
		{name: "record timeout", v: int64(cfg.recordTimeout), allowed: int64(time.Second), badcmp: func(l, r int64) (bool, string) {
			if l == 0 {
				return false, "" // we print nothing when things are good
			}
			return l < r, "less"
		}, durs: true},

		// Consumer settings. maxWait is stored as int32 milliseconds,
		// but we want the error message to be in the nice
		// time.Duration string format.
		{name: "max fetch wait", v: int64(cfg.maxWait) * int64(time.Millisecond), allowed: int64(10 * time.Millisecond), badcmp: i64lt, durs: true},

		// Group settings.
		{name: "number of balancers", v: int64(len(cfg.balancers)), allowed: 1, badcmp: i64lt},
		{name: "consumer protocol length", v: int64(len(cfg.protocol)), allowed: 1, badcmp: i64lt},

		{name: "session timeout", v: int64(cfg.sessionTimeout), allowed: int64(100 * time.Millisecond), badcmp: i64lt, durs: true},
		{name: "rebalance timeout", v: int64(cfg.rebalanceTimeout), allowed: int64(100 * time.Millisecond), badcmp: i64lt, durs: true},
		{name: "autocommit interval", v: int64(cfg.autocommitInterval), allowed: int64(100 * time.Millisecond), badcmp: i64lt, durs: true},

		{v: int64(cfg.heartbeatInterval), allowed: int64(cfg.rebalanceTimeout) * int64(time.Millisecond), badcmp: i64gt, durs: true, fmt: "heartbeat interval %v is erroneously larger than the session timeout %v"},
	} {
		bad, cmp := limit.badcmp(limit.v, limit.allowed)
		if bad {
			if limit.fmt != "" {
				if limit.durs {
					return fmt.Errorf(limit.fmt, time.Duration(limit.v), time.Duration(limit.allowed))
				}
				return fmt.Errorf(limit.fmt, limit.v, limit.allowed)
			}
			if limit.durs {
				return fmt.Errorf("%s %v is %s than allowed %v", limit.name, time.Duration(limit.v), cmp, time.Duration(limit.allowed))
			}
			return fmt.Errorf("%s %v is %s than allowed %v", limit.name, limit.v, cmp, limit.allowed)
		}
	}

	if len(cfg.group) > 0 {
		if len(cfg.topics) == 0 {
			return errors.New("unable to consume from a group when no topics are specified")
		}
		if len(cfg.partitions) != 0 {
			return errors.New("invalid direct-partition consuming option when consuming as a group")
		}
	}

	if cfg.regex {
		if len(cfg.partitions) != 0 {
			return errors.New("invalid direct-partition consuming option when consuming as regex")
		}
		for re := range cfg.topics {
			compiled, err := regexp.Compile(re)
			if err != nil {
				return fmt.Errorf("invalid regular expression %q", re)
			}
			cfg.topics[re] = compiled
		}
	}

	if cfg.autocommitDisable && cfg.autocommitGreedy {
		return errors.New("cannot both disable autocommitting and enable greedy autocommitting")
	}
	if cfg.autocommitDisable && cfg.autocommitMarks {
		return errors.New("cannot both disable autocommitting and enable marked autocommitting")
	}
	if cfg.autocommitGreedy && cfg.autocommitMarks {
		return errors.New("cannot enable both greedy autocommitting and marked autocommitting")
	}
	if (cfg.autocommitGreedy || cfg.autocommitDisable || cfg.autocommitMarks || cfg.setCommitCallback) && len(cfg.group) == 0 {
		return errors.New("invalid autocommit options specified when a group was not specified")
	}
	if (cfg.setLost || cfg.setRevoked || cfg.setAssigned) && len(cfg.group) == 0 {
		return errors.New("invalid group partition assigned/revoked/lost functions set when a group was not specified")
	}

	return nil
}

var defaultDialer = &net.Dialer{Timeout: 10 * time.Second}

func defaultCfg() cfg {
	defaultID := "kgo"
	return cfg{

		/////////////
		// general //
		/////////////

		id:     &defaultID,
		dialFn: defaultDialer.DialContext,

		requestTimeoutOverhead: 20 * time.Second,
		connIdleTimeout:        20 * time.Second,

		softwareName:    "kgo",
		softwareVersion: "0.1.0",

		logger: new(nopLogger),

		seedBrokers: []string{"127.0.0.1"},
		maxVersions: kversion.Stable(),

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
		retries: 20,
		retryTimeout: func(key int16) time.Duration {
			if key == 26 { // EndTxn key
				return 5 * time.Minute
			}
			return time.Minute
		},

		maxBrokerWriteBytes: 100 << 20, // Kafka socket.request.max.bytes default is 100<<20
		maxBrokerReadBytes:  100 << 20,

		metadataMaxAge: 5 * time.Minute,
		metadataMinAge: 10 * time.Second,

		//////////////
		// producer //
		//////////////

		txnTimeout:          40 * time.Second,
		acks:                AllISRAcks(),
		compression:         []CompressionCodec{SnappyCompression(), NoCompression()},
		maxRecordBatchBytes: 1000000, // Kafka max.message.bytes default is 1000012
		maxBufferedRecords:  math.MaxInt64,
		produceTimeout:      30 * time.Second,
		recordRetries:       math.MaxInt64,             // effectively unbounded
		partitioner:         StickyKeyPartitioner(nil), // default to how Kafka partitions

		//////////////
		// consumer //
		//////////////

		maxWait:        5000,
		minBytes:       1,
		maxBytes:       50 << 20,
		maxPartBytes:   10 << 20,
		resetOffset:    NewOffset().AtStart(),
		isolationLevel: 0,

		maxConcurrentFetches: 0, // unbounded default

		///////////
		// group //
		///////////

		balancers: []GroupBalancer{
			CooperativeStickyBalancer(),
		},
		protocol: "consumer",

		sessionTimeout:    45000 * time.Millisecond,
		rebalanceTimeout:  60000 * time.Millisecond,
		heartbeatInterval: 3000 * time.Millisecond,

		autocommitInterval: 5 * time.Second,
	}
}

//////////////////////////
// CLIENT CONFIGURATION //
//////////////////////////

// ClientID uses id for all requests sent to Kafka brokers, overriding the
// default "kgo".
func ClientID(id string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.id = &id }}
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

// RequestTimeoutOverhead uses the given time as overhead while deadlining
// requests, overriding the default overhead of 20s.
//
// For most requests, the overhead will simply be this timeout. However, for
// any request with a TimeoutMillis field, the overhead is added on top of the
// request's TimeoutMillis. This ensures that we give Kafka enough time to
// actually process the request given the timeout, while still having a
// deadline on the connection as a whole to ensure it does not hang.
//
// For writes, the timeout is always the overhead. We buffer writes in our
// client before one quick flush, so we always expect the write to be fast.
//
// Note that hitting the timeout kills a connection, which will fail any other
// active writes or reads on the connection.
//
// This option is roughly equivalent to request.timeout.ms, but grants
// additional time to requests that have timeout fields.
func RequestTimeoutOverhead(overhead time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.requestTimeoutOverhead = overhead }}
}

// ConnIdleTimeout is a rough amount of time to allow connections to idle
// before they are closed, overriding the default 20.
//
// In the worst case, a connection can be allowed to idle for up to 2x this
// time, while the average is expected to be 1.5x (essentially, a uniform
// distribution from this interval to 2x the interval).
//
// It is possible that a connection can be reaped just as it is about to be
// written to, but the client internally retries in these cases.
//
// Connections are not reaped if they are actively being written to or read
// from; thus, a request can take a really long time itself and not be reaped
// (however, this may lead to the RequestTimeoutOverhead).
func ConnIdleTimeout(timeout time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.connIdleTimeout = timeout }}
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
//     kgo.Dialer((&tls.Dialer{...}).DialContext)
//
func Dialer(fn func(ctx context.Context, network, host string) (net.Conn, error)) Opt {
	return clientOpt{func(cfg *cfg) { cfg.dialFn = fn }}
}

// DialTLSConfig opts in to dialing brokers with the given TLS config with a
// 10s dial timeout. This is a shortcut for manually specifying a tls dialer
// using the Dialer option.
//
// Every dial, the input config is cloned. If the config's ServerName is not
// specified, this function uses net.SplitHostPort to extract the host from the
// broker being dialed and sets the ServerName. In short, it is not necessary
// to set the ServerName.
func DialTLSConfig(c *tls.Config) Opt {
	return Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
		c := c.Clone()
		if c.ServerName == "" {
			server, _, err := net.SplitHostPort(host)
			if err != nil {
				return nil, fmt.Errorf("unable to split host:port for dialing: %w", err)
			}
			c.ServerName = server
		}
		return (&tls.Dialer{
			NetDialer: defaultDialer,
			Config:    c,
		}).DialContext(ctx, network, host)
	})
}

// SeedBrokers sets the seed brokers for the client to use, overriding the
// default 127.0.0.1:9092.
//
// Any seeds that are missing a port use the default Kafka port 9092.
func SeedBrokers(seeds ...string) Opt {
	return clientOpt{func(cfg *cfg) { cfg.seedBrokers = append(cfg.seedBrokers[:0], seeds...) }}
}

// MaxVersions sets the maximum Kafka version to try, overriding the
// internal unbounded (latest stable) versions.
//
// Note that specific max version pinning is required if trying to interact
// with versions pre 0.10.0. Otherwise, unless using more complicated requests
// that this client itself does not natively use, it is generally safe to opt
// for the latest version. If using the kmsg package directly to issue
// requests, it is recommended to pin versions so that new fields on requests
// do not get invalid default zero values before you update your usage.
func MaxVersions(versions *kversion.Versions) Opt {
	return clientOpt{func(cfg *cfg) { cfg.maxVersions = versions }}
}

// MinVersions sets the minimum Kafka version a request can be downgraded to,
// overriding the default of the lowest version.
//
// This option is useful if you are issuing requests that you absolutely do not
// want to be downgraded; that is, if you are relying on features in newer
// requests, and you are not sure if your brokers can handle those features.
// By setting a min version, if the client detects it needs to downgrade past
// the version, it will instead avoid issuing the request.
//
// Unlike MaxVersions, if a request is issued that is unknown to the min
// versions, the request is allowed. It is assumed that there is no lower bound
// for that request.
func MinVersions(versions *kversion.Versions) Opt {
	return clientOpt{func(cfg *cfg) { cfg.minVersions = versions }}
}

// RetryBackoffFn sets the backoff strategy for how long to backoff for a given
// amount of retries, overriding the default jittery exponential backoff that
// ranges from 100ms min to 1s max.
//
// This (roughly) corresponds to Kafka's retry.backoff.ms setting and
// retry.backoff.max.ms (which is being introduced with KIP-500).
func RetryBackoffFn(backoff func(int) time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.retryBackoff = backoff }}
}

// RequestRetries sets the number of tries that retriable requests are allowed,
// overriding the default of 20.
//
// This option does not apply to produce requests; to limit produce request
// retries / record retries, see RecordRetries.
func RequestRetries(n int) Opt {
	return clientOpt{func(cfg *cfg) { cfg.retries = int64(n) }}
}

// RetryTimeout sets the upper limit on how long we allow requests to retry,
// overriding the default of 5m for EndTxn requests, 1m for all others.
//
// This timeout applies to any request issued through a client's Request
// function. It does not apply to fetches nor produces.
//
// A value of zero indicates no request timeout.
//
// The timeout is evaluated after a request is issued. If a retry backoff
// places the next request past the retry timeout deadline, the request will
// still be tried once more once the backoff expires.
func RetryTimeout(t time.Duration) Opt {
	return RetryTimeoutFn(func(int16) time.Duration { return t })
}

// RetryTimeoutFn sets the per-request upper limit on how long we allow
// requests to retry, overriding the default of 5m for EndTxn requests, 1m for
// all others.
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
func RetryTimeoutFn(t func(int16) time.Duration) Opt {
	return clientOpt{func(cfg *cfg) { cfg.retryTimeout = t }}
}

// AllowAutoTopicCreation enables topics to be auto created if they do
// not exist when fetching their metadata.
func AllowAutoTopicCreation() Opt {
	return clientOpt{func(cfg *cfg) { cfg.allowAutoTopicCreation = true }}
}

// BrokerMaxWriteBytes upper bounds the number of bytes written to a broker
// connection in a single write, overriding the default 100MiB.
//
// This number corresponds to the a broker's socket.request.max.bytes, which
// defaults to 100MiB.
//
// The only Kafka request that could come reasonable close to hitting this
// limit should be produce requests, and thus this limit is only enforced for
// produce requests.
func BrokerMaxWriteBytes(v int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.maxBrokerWriteBytes = v }}
}

// BrokerMaxReadBytes sets the maximum response size that can be read from
// Kafka, overriding the default 100MiB.
//
// This is a safety measure to avoid OOMing on invalid responses. This is
// slightly double FetchMaxBytes; if bumping that, consider bump this. No other
// response should run the risk of hitting this limit.
func BrokerMaxReadBytes(v int32) Opt {
	return clientOpt{func(cfg *cfg) { cfg.maxBrokerReadBytes = v }}
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
// Hooks can be used to layer in metrics (such as Prometheus hooks) or anything
// else. The client will call all hooks in order. See the Hooks interface for
// more information, as well as any interface that contains "Hook" in the name
// to know the available hooks. A single hook can implement zero or all hook
// interfaces, and only the hooks that it implements will be called.
func WithHooks(hooks ...Hook) Opt {
	return clientOpt{func(cfg *cfg) { cfg.hooks = append(cfg.hooks, hooks...) }}
}

////////////////////////////
// PRODUCER CONFIGURATION //
////////////////////////////

// DefaultProduceTopic sets the default topic to produce to if the topic field
// is empty in a Record.
//
// If this option is not used, if a record has an empty topic, the record
// cannot be produced and will be failed immediately.
func DefaultProduceTopic(t string) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.defaultProduceTopic = t }}
}

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

// DisableIdempotentWrite disables idempotent produce requests, opting out of
// Kafka server-side deduplication in the face of reissued requests due to
// transient network problems.
//
// Idempotent production is strictly a win, but does require the
// IDEMPOTENT_WRITE permission on CLUSTER (pre Kafka 3.0), and not all clients
// can have that permission.
//
// This option is incompatible with specifying a transactional id.
func DisableIdempotentWrite() ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.disableIdempotency = true }}
}

// ProducerBatchCompression sets the compression codec to use for producing
// records.
//
// Compression is chosen in the order preferred based on broker support.  For
// example, zstd compression was introduced in Kafka 2.1.0, so the preference
// can be first zstd, fallback snappy, fallback none.
//
// The default preference is [snappy, none], which should be fine for all old
// consumers since snappy compression has existed since Kafka 0.8.0.  To use
// zstd, your brokers must be at least 2.1.0 and all consumers must be upgraded
// to support decoding zstd records.
func ProducerBatchCompression(preference ...CompressionCodec) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.compression = preference }}
}

// ProducerBatchMaxBytes upper bounds the size of a record batch, overriding
// the default 1MB.
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
// Note that this is the maximum size of a record batch before compression. If
// a batch compresses poorly and actually grows the batch, the uncompressed
// form will be used.
func ProducerBatchMaxBytes(v int32) ProducerOpt {
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

// RecordRetries sets the number of tries for producing records, overriding the
// unlimited default.
//
// If idempotency is enabled (as it is by default), this option is only
// enforced if it is safe to do so without creating invalid sequence numbers.
// It is safe to enforce if a record was never issued in a request to Kafka, or
// if it was requested and received a response.
//
// If a record fails due to retries, all records buffered in the same partition
// are failed as well. This ensures gapless ordering: the client will not fail
// one record only to produce a later one successfully. This also allows for
// easier sequence number ordering internally.
//
// If a topic repeatedly fails to load with UNKNOWN_TOPIC_OR_PARTITION, it has
// a different, internal retry limit. All records for a topic that repeatedly
// cannot be loaded are failed when the internal limit is hit.
//
// This option is different from RequestRetries to allow finer grained control
// of when to fail when producing records.
func RecordRetries(n int) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.recordRetries = int64(n) }}
}

// StopProducerOnDataLossDetected sets the client to stop producing if data
// loss is detected, overriding the default false.
//
// Note that if using this option, it is strongly recommended to not have a
// retry limit. Doing so may lead to errors where the client fails a batch on a
// recoverable error, which internally bumps the idempotent sequence number
// used for producing, which may then later cause an inadvertent out of order
// sequence number and false "data loss" detection.
func StopProducerOnDataLossDetected() ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.stopOnDataLoss = true }}
}

// ProducerOnDataLossDetected sets a function to call if data loss is detected
// when producing records if the client is configured to continue on data loss.
// Thus, this option is mutually exclusive with StopProducerOnDataLossDetected.
//
// The passed function will be called with the topic and partition that data
// loss was detected on.
func ProducerOnDataLossDetected(fn func(string, int32)) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.onDataLoss = fn }}
}

// ProducerLinger sets how long individual topic partitions will linger waiting
// for more records before triggering a request to be built.
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
func ProducerLinger(linger time.Duration) ProducerOpt {
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

// RecordDeliveryTimeout sets a rough time of how long a record can sit around
// in a batch before timing out, overriding the unlimited default.
//
// If idempotency is enabled (as it is by default), this option is only
// enforced if it is safe to do so without creating invalid sequence numbers.
// It is safe to enforce if a record was never issued in a request to Kafka, or
// if it was requested and received a response.
//
// The timeout for all records in a batch inherit the timeout of the first
// record in that batch. That is, once the first record's timeout expires, all
// records in the batch are expired. This generally is a non-issue unless using
// this option with lingering. In that case, simply add the linger to the
// record timeout to avoid problems.
//
// If a record times out, all records buffered in the same partition are failed
// as well. This ensures gapless ordering: the client will not fail one record
// only to produce a later one successfully. This also allows for easier
// sequence number ordering internally.
//
// The timeout is only evaluated evaluated before writing a request or after a
// produce response. Thus, a sink backoff may delay record timeout slightly.
//
// This option is roughly equivalent to delivery.timeout.ms.
func RecordDeliveryTimeout(timeout time.Duration) ProducerOpt {
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
// Note that unless using Kafka 2.5.0, a consumer group rebalance may be
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
// default 40s. It is a good idea to keep this less than a group's session
// timeout, so that a group member will always be alive for the duration of a
// transaction even if connectivity dies. This helps prevent a transaction
// finishing after a rebalance, which is problematic pre-Kafka 2.5.0. If you
// are on Kafka 2.5.0+, then you can use the RequireStableFetchOffsets option
// when assigning the group, and you can set this to whatever you would like.
//
// Transaction timeouts begin when the first record is produced within a
// transaction, not when a transaction begins.
func TransactionTimeout(timeout time.Duration) ProducerOpt {
	return producerOpt{func(cfg *cfg) { cfg.txnTimeout = timeout }}
}

////////////////////////////
// CONSUMER CONFIGURATION //
////////////////////////////

// FetchMaxWait sets the maximum amount of time a broker will wait for a
// fetch response to hit the minimum number of required bytes before returning,
// overriding the default 5s.
//
// This corresponds to the Java replica.fetch.wait.max.ms setting.
func FetchMaxWait(wait time.Duration) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.maxWait = int32(wait.Milliseconds()) }}
}

// FetchMaxBytes sets the maximum amount of bytes a broker will try to send
// during a fetch, overriding the default 50MiB. Note that brokers may not obey
// this limit if it has records larger than this limit. Also note that this
// client sends a fetch to each broker concurrently, meaning the client will
// buffer up to <brokers * max bytes> worth of memory.
//
// This corresponds to the Java fetch.max.bytes setting.
//
// If bumping this, consider bumping BrokerMaxReadBytes.
//
// If what you are consuming is compressed, and compressed well, it is strongly
// recommended to set this option so that decompression does not eat all of
// your RAM.
func FetchMaxBytes(b int32) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.maxBytes = b }}
}

// FetchMinBytes sets the minimum amount of bytes a broker will try to send
// during a fetch, overriding the default 1 byte.
//
// With the default of 1, data is sent as soon as it is available. By bumping
// this, the broker will try to wait for more data, which may improve server
// throughput at the expense of added latency.
//
// This corresponds to the Java fetch.min.bytes setting.
func FetchMinBytes(b int32) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.minBytes = b }}
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

// MaxConcurrentFetches sets the maximum number of fetch requests to allow in
// flight or buffered at once, overriding the unbounded (i.e. number of
// brokers) default.
//
// This setting, paired with FetchMaxBytes, can upper bound the maximum amount
// of memory that the client can use for consuming.
//
// Requests are issued to brokers in a FIFO order: once the client is ready to
// issue a request to a broker, it registers that request and issues it in
// order with other registrations.
//
// If Kafka replies with any data, the client does not track the fetch as
// completed until the user has polled the buffered fetch. Thus, a concurrent
// fetch is not considered complete until all data from it is done being
// processed and out of the client itself.
//
// Note that brokers are allowed to hang for up to FetchMaxWait before replying
// to a request, so if this option is too constrained and you are consuming a
// low throughput topic, the client may take a long time before requesting a
// broker that has new data. For high throughput topics, or if the allowed
// concurrent fetches is large enough, this should not be a concern.
//
// A value of 0 implies the allowed concurrency is unbounded and will be
// limited only by the number of brokers in the cluster.
func MaxConcurrentFetches(n int) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.maxConcurrentFetches = n }}
}

// ConsumeResetOffset sets the offset to restart consuming from when a
// partition has no commits (for groups) or when beginning to consume a
// partition (for direct partition consuming), or when a fetch sees an
// OffsetOutOfRange error, overriding the default ConsumeStartOffset.
//
// Defaults to: NewOffset().AtStart() / Earliest Offset
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

// ConsumeTopics adds topics to use for consuming.
//
// By default, consuming will start at the beginning of partitions. To change
// this, use the ConsumeResetOffset option.
func ConsumeTopics(topics ...string) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) {
		cfg.topics = make(map[string]*regexp.Regexp, len(topics))
		for _, topic := range topics {
			cfg.topics[topic] = nil
		}
	}}
}

// ConsumePartitions sets partitions to consume from directly and the offsets
// to start consuming those partitions from.
//
// This option is basically a way to explicitly consume from subsets of partitions
// in topics, or to consume at exact offsets. Offsets from this option have
// higher precedence than the ConsumeResetOffset.
//
// This option is not compatible with group consuming and regex consuming.
func ConsumePartitions(partitions map[string]map[int32]Offset) ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.partitions = partitions }}
}

// ConsumeRegex sets the client to parse all topics passed to ConsumeTopics as
// regular expressions.
//
// When consuming via regex, every metadata request loads *all* topics, so that
// all topics can be passed to any regular expressions. Every topic is
// evaluated only once ever across all regular expressions; either it
// permanently is known to match, or is permanently known to not match.
func ConsumeRegex() ConsumerOpt {
	return consumerOpt{func(cfg *cfg) { cfg.regex = true }}
}

//////////////////////////////////
// CONSUMER GROUP CONFIGURATION //
//////////////////////////////////

// ConsumerGroup sets the consumer group for the client to join and consume in.
// This option is required if using any other group options.
//
// Note that when group consuming, the default is to autocommit every 5s. To be
// safe, autocommitting only commits what is *previously* polled. If you poll
// once, nothing will be committed. If you poll again, the first poll is
// available to be committed. This ensures at-least-once processing, but does
// mean there is likely some duplicate processing during rebalances. When your
// client shuts down, you should issue one final synchronous commit before
// leaving the group (because you will not be polling again, and you are not
// waiting for an autocommit).
func ConsumerGroup(group string) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.group = group }}
}

// Balancers sets the group balancers to use for dividing topic partitions
// among group members, overriding the current default [cooperative-sticky].
// This option is equivalent to Kafka's partition.assignment.strategies option.
//
// For balancing, Kafka chooses the first protocol that all group members agree
// to support.
//
// Note that if you opt in to cooperative-sticky rebalancing, cooperative group
// balancing is incompatible with eager (classical) rebalancing and requires a
// careful rollout strategy (see KIP-429).
func Balancers(balancers ...GroupBalancer) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.balancers = balancers }}
}

// SessionTimeout sets how long a member in the group can go between
// heartbeats, overriding the default 45,000ms. If a member does not heartbeat
// in this timeout, the broker will remove the member from the group and
// initiate a rebalance.
//
// If you are using a GroupTransactSession for EOS, wish to lower this, and are
// talking to a Kafka cluster pre 2.5.0, consider lowering the
// TransactionTimeout. If you do not, you risk a transaction finishing after a
// group has rebalanced, which could lead to duplicate processing. If you are
// talking to a Kafka 2.5.0+ cluster, you can safely use the
// RequireStableFetchOffsets group option and prevent any problems.
//
// This option corresponds to Kafka's session.timeout.ms setting and must be
// within the broker's group.min.session.timeout.ms and
// group.max.session.timeout.ms.
func SessionTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.sessionTimeout = timeout }}
}

// RebalanceTimeout sets how long group members are allowed to take when a a
// rebalance has begun, overriding the default 60,000ms. This timeout is how
// long all members are allowed to complete work and commit offsets, minus the
// time it took to detect the rebalance (from a heartbeat).
//
// Kafka uses the largest rebalance timeout of all members in the group. If a
// member does not rejoin within this timeout, Kafka will kick that member from
// the group.
//
// This corresponds to Kafka's rebalance.timeout.ms.
func RebalanceTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.rebalanceTimeout = timeout }}
}

// HeartbeatInterval sets how long a group member goes between heartbeats to
// Kafka, overriding the default 3,000ms.
//
// Kafka uses heartbeats to ensure that a group member's session stays active.
// This value can be any value lower than the session timeout, but should be no
// higher than 1/3rd the session timeout.
//
// This corresponds to Kafka's heartbeat.interval.ms.
func HeartbeatInterval(interval time.Duration) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.heartbeatInterval = interval }}
}

// RequireStableFetchOffsets sets the group consumer to require "stable" fetch
// offsets before consuming from the group. Proposed in KIP-447 and introduced
// in Kafka 2.5.0, stable offsets are important when consuming from partitions
// that a transactional producer could be committing to.
//
// With this option, Kafka will block group consumers from fetching offsets for
// partitions that are in an active transaction.
//
// Because this can block consumption, it is strongly recommended to set
// transactional timeouts to a small value (10s) rather than the default 60s.
// Lowering the transactional timeout will reduce the chance that consumers are
// entirely blocked.
func RequireStableFetchOffsets() GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.requireStable = true }}
}

// OnPartitionsAssigned sets the function to be called when a group is joined
// after partitions are assigned before fetches for those partitions begin.
//
// This function combined with OnPartitionsRevoked should not exceed the
// rebalance interval. It is possible for the group, immediately after
// finishing a balance, to re-enter a new balancing session.
//
// The OnPartitionsAssigned function is passed the client's context, which is
// only canceled if the client is closed.
//
// This function is not called concurrent with any other On callback, and this
// function is given a new map that the user is free to modify.
func OnPartitionsAssigned(onAssigned func(context.Context, *Client, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.onAssigned, cfg.setAssigned = onAssigned, true }}
}

// OnPartitionsRevoked sets the function to be called once this group member
// has partitions revoked.
//
// This function combined with OnPartitionsAssigned should not exceed the
// rebalance interval. It is possible for the group, immediately after
// finishing a balance, to re-enter a new balancing session.
//
// If autocommit is enabled, the default OnPartitionsRevoked is a blocking
// commit all non-dirty offsets (where dirty is the most recent poll). The
// reason for a blocking commit is so that no later commit cancels the blocking
// commit. If the commit in OnPartitionsRevoked were canceled, then the
// rebalance would proceed immediately, the commit that canceled the blocking
// commit would fail, and duplicates could be consumed after the rebalance
// completes.
//
// The OnPartitionsRevoked function is passed the client's context, which is
// only canceled if the client is closed. OnPartitionsRevoked function is
// called at the end of a group session even if there are no partitions being
// revoked. If you are committing offsets manually (have disabled
// autocommitting), it is highly recommended to do a proper blocking commit in
// OnPartitionsRevoked.
//
// This function is not called concurrent with any other On callback, and this
// function is given a new map that the user is free to modify.
func OnPartitionsRevoked(onRevoked func(context.Context, *Client, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.onRevoked, cfg.setRevoked = onRevoked, true }}
}

// OnPartitionsLost sets the function to be called on "fatal" group errors,
// such as IllegalGeneration, UnknownMemberID, and authentication failures.
// This function differs from OnPartitionsRevoked in that it is unlikely that
// commits will succeed when partitions are outright lost, whereas commits
// likely will succeed when revoking partitions.
//
// If this is not set, you will not know when a group error occurs that
// forcefully loses all partitions. If you wish to use the same callback for
// lost and revoked, you can use OnPartitionsLostAsRevoked as a shortcut.
//
// This function is not called concurrent with any other On callback, and this
// function is given a new map that the user is free to modify.
func OnPartitionsLost(onLost func(context.Context, *Client, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.onLost, cfg.setLost = onLost, true }}
}

// DisableAutoCommit disable auto committing.
//
// If you disable autocommitting, you may want to use a custom
// OnPartitionsRevoked, otherwise you may end up doubly processing records
// (which is fine, just leads to duplicate processing). Consider the scenario:
// you, member A, are processing partition 0, and previously committed offset 4
// and have now locally processed through offset 30. A rebalance happens, and
// partition 0 moves to member B. If you use OnPartitionsRevoked, you can
// detect that you are losing this partition and commit your work through
// offset 30, so that member B can start processing at offset 30. If you do not
// commit (i.e. you do not use a custom OnPartitionsRevoked), the other member
// will start processing at offset 4. It may process through offset 50, leading
// to double processing of offsets 4 through 29. Worse, you, member A, can
// rewind member B's commit, because member B may commit offset 50 and you may
// finally eventually commit offset 30. If a rebalance happens, then even more
// duplicate processing will occur of offsets 30 through 49.
//
// Again, OnPartitionsRevoked is not necessary, and not using it just means
// double processing, which for most workloads is fine since a simple group
// consumer is not EOS / transactional, only at-least-once. But, this is
// something to be aware of.
func DisableAutoCommit() GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.autocommitDisable = true }}
}

// GreedyAutoCommit opts in to committing everything that has been polled when
// autocommitting (the dirty offsets), rather than committing what has
// previously been polled. This option may result in message loss if your
// application crashes.
func GreedyAutoCommit() GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.autocommitGreedy = true }}
}

// AutoCommitInterval sets how long to go between autocommits, overriding the
// default 5s.
func AutoCommitInterval(interval time.Duration) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.autocommitInterval = interval }}
}

// AutoCommitMarks switches the autocommitting behavior to only commit "marked"
// records, which can be done with the MarkCommitRecords method.
//
// This option is basically a halfway point between autocommitting and manually
// committing. If you have slow batch processing of polls, then you can
// manually mark records to be autocommitted before you poll again. This way,
// if you usually take a long time between polls, your partial work can still
// be automatically checkpointed through autocommitting.
func AutoCommitMarks() GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.autocommitMarks = true }}
}

// InstanceID sets the group consumer's instance ID, switching the group member
// from "dynamic" to "static".
//
// Prior to Kafka 2.3.0, joining a group gave a group member a new member ID.
// The group leader could not tell if this was a rejoining member. Thus, any
// join caused the group to rebalance.
//
// Kafka 2.3.0 introduced the concept of an instance ID, which can persist
// across restarts. This allows for avoiding many costly rebalances and allows
// for stickier rebalancing for rejoining members (since the ID for balancing
// stays the same). The main downsides are that you, the user of a client, have
// to manage instance IDs properly, and that it may take longer to rebalance in
// the event that a client legitimately dies.
//
// When using an instance ID, the client does NOT send a leave group request
// when closing. This allows for the client ot restart with the same instance
// ID and rejoin the group to avoid a rebalance. It is strongly recommended to
// increase the session timeout enough to allow time for the restart (remember
// that the default session timeout is 10s).
//
// To actually leave the group, you must use an external admin command that
// issues a leave group request on behalf of this instance ID (see kcl), or you
// can manually use the kmsg package with a proper LeaveGroupRequest.
//
// NOTE: Leaving a group with an instance ID is only supported in Kafka 2.4.0+.
func InstanceID(id string) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.instanceID = &id }}
}

// GroupProtocol sets the group's join protocol, overriding the default value
// "consumer". The only reason to override this is if you are implementing
// custom join and sync group logic.
func GroupProtocol(protocol string) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.protocol = protocol }}
}

// AutoCommitCallback sets the callback to use if autocommitting is enabled.
// This overrides the default callback that logs errors and continues.
func AutoCommitCallback(fn func(*Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)) GroupOpt {
	return groupOpt{func(cfg *cfg) { cfg.commitCallback, cfg.setCommitCallback = fn, true }}
}
