package kgo

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var errSkipChecks848 = errors.New("848 stale commit; skip checks")

var (
	adm             *Client
	testrf          = 3
	testRecordLimit = 500000

	// Redpanda is a bit more strict with transactions: we must wait for
	// EndTxn to return successfully before beginning a new transaction. We
	// cannot use EndAndBeginTransaction with EndBeginTxnUnsafe.
	allowUnsafe = false

	// Static membership (KIP-345) requires JoinGroup v5+.
	allowStaticMembership = false

	// KIP-848 requires ConsumerGroupHeartbeat (key 68).
	allow848 = false

	// KGO_TEST_TLS: DSL syntax is ({ca|cert|key}:path),{1,3}
	testCert *tls.Config

	// KGO_TEST_SCRAM: DSL is user:pass(:num); we assume 256
	saslScram sasl.Mechanism

	// KGO_TEST_SCRAM_SEEDS: separate broker address for SCRAM testing
	scramSeeds string

	// We create topics with a different number of partitions to exercise
	// a few extra code paths; we index into npartitions with npartitionsAt,
	// an atomic that we modulo after load.
	//
	// We can lower bound these numbers with KGO_TEST_MAX_TOPIC_PARTS.
	npartitions   = []int{7, 11, 31}
	npartitionsAt int64
)

func wait(tb testing.TB, dur time.Duration, cond func() error) {
	tb.Helper()

	start := time.Now()
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Since(start) > dur {
			tb.Error(err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

type slowConn struct {
	net.Conn
}

func (s *slowConn) Write(p []byte) (int, error) {
	time.Sleep(100 * time.Millisecond)
	return s.Conn.Write(p)
}

func (s *slowConn) Read(p []byte) (int, error) {
	time.Sleep(100 * time.Millisecond)
	return s.Conn.Read(p)
}

type slowDialer struct {
	d net.Dialer
}

func (s *slowDialer) DialContext(ctx context.Context, network, host string) (net.Conn, error) {
	c, err := s.d.DialContext(ctx, network, host)
	if err != nil {
		return nil, err
	}
	return &slowConn{c}, nil
}

func init() {
	var err error
	if n, _ := strconv.Atoi(os.Getenv("KGO_TEST_RF")); n > 0 {
		testrf = n
	}
	if n, _ := strconv.Atoi(os.Getenv("KGO_TEST_RECORDS")); n > 0 {
		testRecordLimit = n
	}
	if _, exists := os.LookupEnv("KGO_TEST_UNSAFE"); exists {
		allowUnsafe = true
	}
	if paths, exists := os.LookupEnv("KGO_TEST_TLS"); exists {
		var caPath, certPath, keyPath string
		for _, path := range strings.Split(paths, ",") {
			switch {
			case strings.HasPrefix(path, "ca:"):
				caPath = path[3:]
			case strings.HasPrefix(path, "cert:"):
				certPath = path[5:]
			case strings.HasPrefix(path, "key:"):
				keyPath = path[4:]
			default:
				panic(fmt.Sprintf("invalid tls format %q", path))
			}
		}
		inittls := func() {
			if testCert == nil {
				testCert = &tls.Config{MinVersion: tls.VersionTLS12}
			}
		}
		if caPath != "" {
			ca, err := os.ReadFile(caPath) //nolint:gosec // we are deliberately including a file from a variable
			if err != nil {
				panic(fmt.Sprintf("unable to read ca: %v", err))
			}
			inittls()
			testCert.RootCAs = x509.NewCertPool()
			if !testCert.RootCAs.AppendCertsFromPEM(ca) {
				panic("unable to append ca")
			}
		}
		if certPath != "" || keyPath != "" {
			if certPath == "" || keyPath == "" {
				panic("both cert path and key path must be specified")
			}
			cert, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				panic(fmt.Sprintf("unable to load cert/key pair: %v", err))
			}
			inittls()
			testCert.Certificates = append(testCert.Certificates, cert)
		}
	}
	if saslStr, exists := os.LookupEnv("KGO_TEST_SCRAM"); exists {
		split := strings.Split(saslStr, ":")
		if len(split) != 2 && len(split) != 3 {
			panic(fmt.Sprintf("invalid scram format %q", saslStr))
		}
		a := scram.Auth{
			User: split[0],
			Pass: split[1],
		}
		saslScram = a.AsSha256Mechanism()
		if len(split) == 3 {
			n, err := strconv.Atoi(split[2])
			if err != nil {
				panic(fmt.Sprintf("invalid scram alg %q: %v", split[2], err))
			}
			if n != 256 && n != 512 {
				panic(fmt.Sprintf("invalid scram alg %q: must be 256 or 512", split[2]))
			}
			if n == 512 {
				saslScram = a.AsSha512Mechanism()
			}
		}
	}
	scramSeeds = os.Getenv("KGO_TEST_SCRAM_SEEDS")
	if maxTParts, exists := os.LookupEnv("KGO_TEST_MAX_TOPIC_PARTS"); exists {
		n, err := strconv.Atoi(maxTParts)
		if err != nil {
			panic(fmt.Sprintf("invalid max topic parts %q: %v", maxTParts, err))
		}
		if n < 1 {
			n = 1
		}
		for i, v := range npartitions {
			if v > n {
				npartitions[i] = n
			}
		}
	}
	adm, err = newTestClient()
	if err != nil {
		panic(fmt.Sprintf("unable to create admin client: %v", err))
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := adm.Request(context.Background(), kmsg.NewPtrApiVersionsRequest())
		if err == nil {
			versions := kversion.FromApiVersionsResponse(resp.(*kmsg.ApiVersionsResponse))
			if v, ok := versions.LookupMaxKeyVersion(11); ok && v >= 5 { // 11 = JoinGroup
				allowStaticMembership = true
			}
			if _, ok := versions.LookupMaxKeyVersion(68); ok { // 68 = ConsumerGroupHeartbeat
				allow848 = true
			}
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("unable to issue ApiVersions: %v", err))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func testClientOpts(opts ...Opt) []Opt {
	opts = append(opts, getSeedBrokers())
	if testCert != nil {
		opts = append(opts, DialTLSConfig(testCert))
	}
	return opts
}

func newTestClient(opts ...Opt) (*Client, error) {
	return NewClient(testClientOpts(opts...)...)
}

func getSeedBrokers() Opt {
	seeds := os.Getenv("KGO_SEEDS")
	if seeds == "" {
		seeds = "127.0.0.1:9092"
	}
	return SeedBrokers(strings.Split(seeds, ",")...)
}

var loggerNum atomic.Int64

var testLogLevel = func() LogLevel {
	level := strings.ToLower(os.Getenv("KGO_LOG_LEVEL"))
	switch level {
	case "debug":
		return LogLevelDebug
	case "none":
		return LogLevelNone
	}
	return LogLevelInfo
}()

func testLogger() Logger {
	num := loggerNum.Add(1)
	pfx := strconv.Itoa(int(num))
	return BasicLogger(os.Stderr, testLogLevel, func() string {
		return time.Now().UTC().Format("[15:04:05.999 ") + pfx + "]"
	})
}

var randsha = func() func() string {
	var mu sync.Mutex
	last := time.Now().UnixNano()
	return func() string {
		mu.Lock()
		defer mu.Unlock()

		now := time.Now().UnixNano()
		if now <= last {
			now = last + 1
		}

		last = now

		sum := sha256.Sum256([]byte(strconv.Itoa(int(now))))
		return hex.EncodeToString(sum[:])
	}
}()

func tmpTopic(tb testing.TB) (string, func()) {
	partitions := npartitions[int(atomic.AddInt64(&npartitionsAt, 1))%len(npartitions)]
	topic := randsha()
	return tmpNamedTopicPartitions(tb, topic, partitions)
}

func tmpTopicPartitions(tb testing.TB, partitions int) (string, func()) {
	topic := randsha()
	return tmpNamedTopicPartitions(tb, topic, partitions)
}

func tmpNamedTopicPartitions(tb testing.TB, topic string, partitions int) (string, func()) {
	tb.Helper()

	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = int32(partitions)
	reqTopic.ReplicationFactor = int16(testrf)
	req.Topics = append(req.Topics, reqTopic)

	start := time.Now()
issue:
	resp, err := req.RequestWith(context.Background(), adm)

	// If we run tests in a container _immediately_ after the container
	// starts, we can receive dial errors for a bit if the container is not
	// fully initialized. Handle this by retrying specifically dial errors.
	if ne := (*net.OpError)(nil); errors.As(err, &ne) && ne.Op == "dial" && time.Since(start) < 30*time.Second {
		time.Sleep(time.Second)
		goto issue
	}

	if err == nil {
		err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
		// Topic names are randomly generated, so TopicAlreadyExists
		// means our prior CreateTopics request succeeded but the
		// response was lost (e.g. connection blip caused a retry).
		// Treat it as success.
		if errors.Is(err, kerr.TopicAlreadyExists) {
			err = nil
		}
	}
	if err != nil {
		tb.Fatalf("unable to create topic %q: %v", topic, err)
	}

	var cleaned bool
	return topic, func() {
		tb.Helper()

		if cleaned {
			return
		}
		cleaned = true

		if tb.Failed() {
			tb.Logf("FAILED TESTING -- NOT DELETING TOPIC %s", topic)
			return
		}

		tb.Logf("deleting topic %s", topic)

		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic}
		reqTopic := kmsg.NewDeleteTopicsRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), adm)
		if err == nil {
			err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
		}
		if err != nil {
			tb.Logf("unable to delete topic %q: %v", topic, err)
		}
	}
}

func tmpGroup(tb testing.TB) (string, func()) {
	tb.Helper()

	group := randsha()

	return group, func() {
		tb.Helper()

		if tb.Failed() {
			tb.Logf("FAILED TESTING -- NOT DELETING GROUP %s", group)
			return
		}
		tb.Logf("deleting group %s", group)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{group}
		resp, err := req.RequestWith(context.Background(), adm)
		if err == nil {
			err = kerr.ErrorForCode(resp.Groups[0].ErrorCode)
		}
		if err != nil {
			tb.Logf("unable to delete group %q: %v", group, err)
		}
	}
}

// The point of this pool is that, if used, it *will* return something from the
// Get's after the Put's have been put into. Using it in txn test allows us to
// detect races.
type primitivePool struct {
	bytes sync.Pool
	krecs sync.Pool
	recs  sync.Pool
}

func (p *primitivePool) GetDecompressBytes([]byte, CompressionCodecType) []byte {
	get := p.bytes.Get()
	if get != nil {
		return get.([]byte)
	}
	return nil
}

func (p *primitivePool) PutDecompressBytes(put []byte) {
	p.bytes.Put(put) //nolint:staticcheck // ...
}

func (p *primitivePool) GetKRecords(int) []kmsg.Record {
	get := p.krecs.Get()
	if get != nil {
		return get.([]kmsg.Record)
	}
	return nil
}

func (p *primitivePool) PutKRecords(put []kmsg.Record) {
	p.krecs.Put(put) //nolint:staticcheck // ...
}

func (p *primitivePool) GetRecords(int) []kmsg.Record {
	get := p.recs.Get()
	if get != nil {
		return get.([]kmsg.Record)
	}
	return nil
}

func (p *primitivePool) PutRecords(put []kmsg.Record) {
	p.recs.Put(put) //nolint:staticcheck // ...
}

// testConsumer performs ETL inside or outside transactions;
// the ETL functions are defined in group_test.go and txn_test.go
type testConsumer struct {
	errCh chan<- error

	consumeFrom string
	produceTo   string

	group    string
	balancer GroupBalancer

	enable848  bool   // opt into KIP-848 consumer group protocol
	instanceID string // if non-empty, each goroutine gets a unique instanceID with this prefix

	expBody []byte // what every record body should be

	consumed atomic.Uint64 // shared atomically

	instanceIDCounter atomic.Int64 // generates unique suffixes for instanceID

	wg sync.WaitGroup
	mu sync.Mutex

	// part2key tracks keys we have consumed from which partition
	part2key map[int32][]int

	// partOffsets tracks partition offsets we have consumed to ensure no
	// duplicates
	partOffsets map[partOffset]struct{}
}

type partOffset struct {
	part   int32
	offset int64
}

func newTestConsumer(
	errCh chan error,
	consumeFrom string,
	produceTo string,
	group string,
	balancer GroupBalancer,
	expBody []byte,
	enable848 bool,
	instanceID string,
) *testConsumer {
	return &testConsumer{
		errCh: errCh,

		consumeFrom: consumeFrom,
		produceTo:   produceTo,

		group:    group,
		balancer: balancer,

		enable848:  enable848,
		instanceID: instanceID,

		expBody: expBody,

		part2key:    make(map[int32][]int),
		partOffsets: make(map[partOffset]struct{}),
	}
}

func (c *testConsumer) wait() {
	c.wg.Wait()
}

// leaveGroupStatic fully removes a static member after the client has
// closed. For classic groups this sends a raw LeaveGroup (since kgo's
// LeaveGroup is a no-op for classic static members). For 848 groups
// this sends a ConsumerGroupHeartbeat with MemberEpoch=-1 (full leave).
// For dynamic members (empty instanceID), this is a no-op since the
// normal LeaveGroup path handles cleanup.
func (c *testConsumer) leaveGroupStatic(cl *Client, instanceID string) {
	if instanceID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c.enable848 {
		// Look up the server-assigned memberID via ConsumerGroupDescribe.
		dreq := kmsg.NewPtrConsumerGroupDescribeRequest()
		dreq.Groups = []string{c.group}
		dresp, err := dreq.RequestWith(ctx, cl)
		if err != nil {
			return
		}
		var memberID string
		for _, g := range dresp.Groups {
			for _, m := range g.Members {
				if m.InstanceID != nil && *m.InstanceID == instanceID {
					memberID = m.MemberID
					break
				}
			}
		}
		if memberID == "" {
			return
		}
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = c.group
		req.MemberID = memberID
		req.MemberEpoch = -1
		req.RequestWith(ctx, cl) //nolint:errcheck // best-effort cleanup
		return
	}
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = c.group
	member := kmsg.NewLeaveGroupRequestMember()
	member.InstanceID = &instanceID
	member.Reason = kmsg.StringPtr("test cleanup")
	req.Members = append(req.Members, member)
	req.RequestWith(ctx, cl) //nolint:errcheck // best-effort cleanup
}

func (c *testConsumer) goRun(transactional bool, etlsBeforeQuit int) {
	if transactional {
		c.goTransact(etlsBeforeQuit)
	} else {
		c.goGroupETL(etlsBeforeQuit)
	}
}

// etlSem limits the number of concurrent ETL subtests to avoid
// overwhelming the broker cluster. Too many concurrent transactional
// clients can cause transaction timeouts: broker contention causes
// produce draining to stall, the transaction exceeds its timeout, and
// Kafka's coordinator fences the producer with INVALID_PRODUCER_EPOCH.
//
// The semaphore is acquired in the for loop of TestGroupETL/TestTxnEtl
// BEFORE t.Run so that the subtest timer reflects actual work rather
// than time spent waiting for a semaphore slot.
var etlSem = make(chan struct{}, 4)

func testChainETL(
	t *testing.T,
	topic1 string,
	body []byte,
	transactional bool,
	balancer GroupBalancer,
	enable848 bool,
	instanceID string,
) {
	if instanceID != "" && !allowStaticMembership {
		t.Skip("broker does not support static membership (requires JoinGroup v5+, KIP-345)")
	}
	if enable848 && !allow848 {
		t.Skip("broker does not support KIP-848 (requires ConsumerGroupHeartbeat, key 68)")
	}
	errs := make(chan error)
	var (
		/////////////
		// LEVEL 1 //
		/////////////

		group1, groupCleanup1 = tmpGroup(t)
		topic2, topic2Cleanup = tmpTopic(t)

		consumers1 = newTestConsumer(
			errs,
			topic1,
			topic2,
			group1,
			balancer,
			body,
			enable848,
			instanceID,
		)

		/////////////
		// LEVEL 2 //
		/////////////

		group2, groupCleanup2 = tmpGroup(t)
		topic3, topic3Cleanup = tmpTopic(t)

		consumers2 = newTestConsumer(
			errs,
			topic2,
			topic3,
			group2,
			balancer,
			body,
			enable848,
			instanceID,
		)

		/////////////
		// LEVEL 3 //
		/////////////

		group3, groupCleanup3 = tmpGroup(t)
		topic4, topic4Cleanup = tmpTopic(t)

		consumers3 = newTestConsumer(
			errs,
			topic3,
			topic4,
			group3,
			balancer,
			body,
			enable848,
			instanceID,
		)
	)

	t.Cleanup(func() {
		groupCleanup1()
		groupCleanup2()
		groupCleanup3()
		topic2Cleanup()
		topic3Cleanup()
		topic4Cleanup()
	})

	////////////////////
	// CONSUMER START //
	////////////////////

	for range 3 { // three consumers start with standard poll&commit behavior
		consumers1.goRun(transactional, -1)
		consumers2.goRun(transactional, -1)
		consumers3.goRun(transactional, -1)
	}

	consumers1.goRun(transactional, 0) // bail immediately
	consumers1.goRun(transactional, 2) // bail after two txns
	consumers2.goRun(transactional, 2) // same

	time.Sleep(5 * time.Second)
	for range 3 { // trigger rebalance after 5s with more consumers
		consumers1.goRun(transactional, -1)
		consumers2.goRun(transactional, -1)
		consumers3.goRun(transactional, -1)
	}

	consumers2.goRun(transactional, 0) // bail immediately
	consumers1.goRun(transactional, 1) // bail after one txn

	doneConsume := make(chan struct{})
	go func() {
		consumers1.wait()
		consumers2.wait()
		consumers3.wait()
		close(doneConsume)
	}()

	//////////////////////
	// FINAL VALIDATION //
	//////////////////////

	var skipChecks bool
out:
	for {
		select {
		case err := <-errs:
			if skipChecks {
				continue
			}
			if errors.Is(err, errSkipChecks848) {
				t.Log("pre-KIP-1251 stale commit encountered, skipping validation")
				skipChecks = true
				continue
			}
			t.Fatal(err)
		case <-doneConsume:
			break out
		}
	}

	if skipChecks {
		return
	}

	for level, part2key := range []map[int32][]int{
		consumers1.part2key,
		consumers2.part2key,
		consumers3.part2key,
	} {
		allKeys := make([]int, 0, testRecordLimit) // did we receive everything we sent?
		for _, keys := range part2key {
			allKeys = append(allKeys, keys...)
		}

		if len(allKeys) != testRecordLimit {
			t.Errorf("consumers %d: got %d keys != exp %d", level, len(allKeys), testRecordLimit)
		}

		sort.Ints(allKeys)
		for i := range testRecordLimit {
			if allKeys[i] != i {
				t.Errorf("consumers %d: got key %d != exp %d, first 100: %v", level, allKeys[i], i, allKeys[:100])
			}
		}
	}
}
