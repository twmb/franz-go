package kgo

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type readSignalConn struct {
	net.Conn
	readStarted chan struct{}
	once        sync.Once
}

type brokerReadHook func()

func (h brokerReadHook) OnBrokerRead(BrokerMetadata, int16, int, time.Duration, time.Duration, error) {
	h()
}

func (c *readSignalConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.readStarted) })
	return c.Conn.Read(p)
}

func TestRetiredBrokerConnectionClassifiedAsDead(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { serverConn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := newRingLogger(new(nopLogger), 10)
	cfg := defaultCfg()
	cfg.logger = logger
	cl := &Client{cfg: cfg, ctx: ctx}
	b := cl.newBroker(1, "127.0.0.1", 9092, nil)
	cxn := &brokerCxn{
		conn: &readSignalConn{
			Conn:        clientConn,
			readStarted: make(chan struct{}),
		},
		cl:     cl,
		b:      b,
		addr:   b.addr,
		deadCh: make(chan struct{}),
	}
	b.cxnFetch = cxn

	errCh := make(chan error, 1)
	go cxn.handleResp(promisedResp{
		ctx:         ctx,
		resp:        kmsg.NewPtrFetchResponse(),
		readEnqueue: time.Now(),
		promise: func(_ kmsg.Response, err error) {
			errCh <- err
		},
	})

	<-cxn.conn.(*readSignalConn).readStarted
	b.stopForever()

	if err := <-errCh; !errors.Is(err, errChosenBrokerDead) {
		t.Fatalf("retired broker request error = %v, want %v", err, errChosenBrokerDead)
	}
	for _, entry := range logger.buf {
		if entry.level == LogLevelWarn {
			t.Fatalf("retired broker logged warning %q", entry.msg)
		}
		if entry.msg == "read from broker canceled, closing connection and killing any other in-flight requests on this connection" {
			t.Fatalf("retired broker logged misleading cancellation message")
		}
	}
}

func TestRetiredBrokerDoesNotMaskResponseError(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { serverConn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := defaultCfg()
	cl := &Client{cfg: cfg, ctx: ctx}
	b := cl.newBroker(1, "127.0.0.1", 9092, nil)
	cxn := &brokerCxn{
		conn:   clientConn,
		cl:     cl,
		b:      b,
		addr:   b.addr,
		deadCh: make(chan struct{}),
	}
	cl.cfg.hooks = hooks{brokerReadHook(cxn.die)}

	writeErrCh := make(chan error, 1)
	go func() {
		response := make([]byte, 8)
		binary.BigEndian.PutUint32(response, 4)
		binary.BigEndian.PutUint32(response[4:], 2)
		_, err := serverConn.Write(response)
		writeErrCh <- err
	}()

	var (
		promiseErr    error
		deadAtPromise bool
	)
	cxn.handleResp(promisedResp{
		ctx:         ctx,
		resp:        kmsg.NewPtrMetadataResponse(),
		corrID:      1,
		readEnqueue: time.Now(),
		promise: func(_ kmsg.Response, err error) {
			promiseErr = err
			deadAtPromise = cxn.dead.Load()
		},
	})

	if writeErr := <-writeErrCh; writeErr != nil {
		t.Fatalf("write response: %v", writeErr)
	}
	if !deadAtPromise {
		t.Fatal("broker read hook did not retire connection")
	}
	if !errors.Is(promiseErr, errCorrelationIDMismatch) {
		t.Fatalf("retired broker response error = %v, want %v", promiseErr, errCorrelationIDMismatch)
	}
}

func TestUnexpectedFirstReadStillWarns(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { serverConn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := newRingLogger(new(nopLogger), 10)
	cfg := defaultCfg()
	cfg.logger = logger
	cl := &Client{cfg: cfg, ctx: ctx}
	b := cl.newBroker(1, "127.0.0.1", 9092, nil)
	cxn := &brokerCxn{
		conn: &readSignalConn{
			Conn:        clientConn,
			readStarted: make(chan struct{}),
		},
		cl:     cl,
		b:      b,
		addr:   b.addr,
		deadCh: make(chan struct{}),
	}

	errCh := make(chan error, 1)
	go cxn.handleResp(promisedResp{
		ctx:         ctx,
		resp:        kmsg.NewPtrFetchResponse(),
		readEnqueue: time.Now(),
		promise: func(_ kmsg.Response, err error) {
			errCh <- err
		},
	})

	<-cxn.conn.(*readSignalConn).readStarted
	serverConn.Close()

	err := <-errCh
	var firstReadErr *ErrFirstReadEOF
	if !errors.As(err, &firstReadErr) {
		t.Fatalf("unexpected first read error = %v, want *ErrFirstReadEOF", err)
	}
	for _, entry := range logger.buf {
		if entry.level == LogLevelWarn && entry.msg == "read from broker errored, killing connection after 0 successful responses (is SASL missing?)" {
			return
		}
	}
	t.Fatal("unexpected first read did not log the SASL warning")
}

// resetThenServeListener implements just enough of the wire protocol to
// exercise loadConnection's ApiVersions-reset retry: it RSTs (SO_LINGER 0)
// the first two connections after reading one full request, then serves
// ApiVersions v0 responses on later connections. It records the request
// version of the first request on every connection.
type resetThenServeListener struct {
	ln net.Listener

	mu            sync.Mutex
	firstVersions []int16
}

func newResetThenServeListener(t *testing.T) *resetThenServeListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &resetThenServeListener{ln: ln}
	t.Cleanup(func() { ln.Close() })
	go l.run()
	return l
}

func (l *resetThenServeListener) versions() []int16 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]int16(nil), l.firstVersions...)
}

// readReq reads one length-prefixed request and returns the request version
// and correlation ID from its header (key int16, version int16, corr int32 --
// fixed offsets regardless of header flexibility).
func readReq(conn net.Conn) (version int16, corr int32, err error) {
	var size [4]byte
	if _, err = io.ReadFull(conn, size[:]); err != nil {
		return 0, 0, err
	}
	body := make([]byte, binary.BigEndian.Uint32(size[:]))
	if _, err = io.ReadFull(conn, body); err != nil {
		return 0, 0, err
	}
	if len(body) < 8 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return int16(binary.BigEndian.Uint16(body[2:4])), int32(binary.BigEndian.Uint32(body[4:8])), nil
}

func writeResp(conn net.Conn, corr int32, resp kmsg.Response) error {
	body := resp.AppendTo(nil)
	buf := make([]byte, 0, 8+len(body))
	buf = binary.BigEndian.AppendUint32(buf, uint32(4+len(body)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(corr))
	buf = append(buf, body...)
	_, err := conn.Write(buf)
	return err
}

func (l *resetThenServeListener) run() {
	var conns int
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return
		}
		conns++
		go l.serve(conn, conns)
	}
}

func (l *resetThenServeListener) serve(conn net.Conn, n int) {
	defer conn.Close()
	first := true
	for {
		version, corr, err := readReq(conn)
		if err != nil {
			return
		}
		if first {
			first = false
			l.mu.Lock()
			l.firstVersions = append(l.firstVersions, version)
			l.mu.Unlock()
		}

		// First two connections: read the request, then RST. EventHubs
		// shape: the connection resets during ApiVersions.
		if n <= 2 {
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.SetLinger(0)
			}
			return // deferred Close sends RST due to linger 0
		}

		// Later connections: serve ApiVersions v0, advertising only
		// ApiVersions itself at v0 so every subsequent request on the
		// connection stays v0 and non-flexible.
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.Version = 0
		key := kmsg.NewApiVersionsResponseApiKey()
		key.ApiKey = 18
		key.MinVersion = 0
		key.MaxVersion = 0
		resp.ApiKeys = append(resp.ApiKeys, key)
		if err := writeResp(conn, corr, resp); err != nil {
			return
		}
	}
}

// The EventHubs ApiVersions-reset retry (loadConnection's goto) abandoned the
// reset connection without closing it: the fd leaked until the runtime
// finalizer and HookBrokerDisconnect never fired for a connection whose
// HookBrokerConnect did fire. The retry also double-incremented its attempt
// counter, downgrading to ApiVersions v0 on the first retry instead of
// retrying at max version as documented (losing FinalizedFeatures for the
// connection's lifetime on any transient RST).
//
// Post-fix expectations: attempts go max, max, v0 (three connections, first
// request versions [4, 4, 0]), the two reset connections get disconnect
// hooks, and the request then succeeds. Pre-fix: two connections with
// versions [4, 0] and zero disconnect hooks.
func TestAuditApiVersionsResetRetryClosesConns(t *testing.T) {
	t.Parallel()
	l := newResetThenServeListener(t)

	var connects, disconnects atomic.Int32
	hook := brokerConnDisconnHook{&connects, &disconnects}

	cl, err := NewClient(
		SeedBrokers(l.ln.Addr().String()),
		WithHooks(hook),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seeds := cl.SeedBrokers()
	if len(seeds) != 1 {
		t.Fatalf("expected 1 seed, got %d", len(seeds))
	}
	if _, err := seeds[0].Request(ctx, kmsg.NewPtrApiVersionsRequest()); err != nil {
		t.Fatalf("request failed after reset retries: %v", err)
	}

	if got := l.versions(); len(got) != 3 || got[0] != 4 || got[1] != 4 || got[2] != 0 {
		t.Errorf("expected connection first-request versions [4 4 0] (max, max, then v0 downgrade), got %v", got)
	}

	// The live third connection stays open; both reset connections must
	// have been closed (with their disconnect hooks fired).
	if c, d := connects.Load(), disconnects.Load(); c != 3 || d != 2 {
		t.Errorf("expected 3 connects and 2 disconnects (two abandoned reset connections closed), got %d connects, %d disconnects", c, d)
	}
}

type brokerConnDisconnHook struct {
	connects, disconnects *atomic.Int32
}

// A connection was opened whenever conn is non-nil, even if initialization
// subsequently failed (the hook's err then carries the init error); opened
// connections are what must balance against disconnects.
func (h brokerConnDisconnHook) OnBrokerConnect(_ BrokerMetadata, _ time.Duration, conn net.Conn, _ error) {
	if conn != nil {
		h.connects.Add(1)
	}
}

func (h brokerConnDisconnHook) OnBrokerDisconnect(BrokerMetadata, net.Conn) {
	h.disconnects.Add(1)
}
