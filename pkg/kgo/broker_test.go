package kgo

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

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
