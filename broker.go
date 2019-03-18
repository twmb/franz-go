package kgo

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type broker struct {
	cl *Client

	id   int32
	addr string
	conn net.Conn

	versions apiVersions

	reqs  chan promisedReq
	resps chan promisedResp

	/*
		bufdRecs    bufferedRecords
		flushRecs   bool         // forces a flush
		overflowRec writeAndThen // stores a record that would have overflown bufdRecs

		flushTimer        *time.Timer
		flushTimerRunning bool
		mustFlushBuf      bool
		mustFlushRecs     bool
	*/

	dieMu sync.RWMutex
	dead  int64 // atomic
}

type promisedReq struct {
	req     func(*broker) messageRequestKind
	promise func(messageResponseKind, error)
}

type promisedResp struct {
	correlationID int32
	resp          messageResponseKind
	promise       func(messageResponseKind, error)
}

// called under broker lock
// TODO
// Should we not wait for handshake? Should we not do any network IO?
// Should we not wait for api versions response?
func (b *broker) connect() error {
	conn, err := b.cl.cfg.client.dialFn(b.addr)
	if err != nil {
		return err
	}
	if b.cl.cfg.client.tlsCfg != nil {
		tlsconn := tls.Client(conn, b.cl.cfg.client.tlsCfg)
		// TODO SetDeadline, then clear
		if err = tlsconn.Handshake(); err != nil {
			conn.Close()
			return err
		}
		conn = tlsconn
	}

	b.conn = conn

	b.reqs = make(chan promisedReq, 100)
	b.resps = make(chan promisedResp, 100)

	go b.handleReqs()

	var api *apiVersionsResponse
	b.wait(
		func(*broker) messageRequestKind { return new(apiVersionsRequest) },
		func(resp messageResponseKind, respErr error) {
			if err = respErr; err != nil {
				return
			}
			api = resp.(*apiVersionsResponse)
			err = kafkaErr(api.errCode)
		},
	)
	if err != nil {
		b.dieOnce()
		return err
	}
	b.versions = api.keys

	return nil
}

func (b *broker) dieOnce() {
	if atomic.SwapInt64(&b.dead, 1) == 1 {
		return
	}

	b.conn.Close() // kill anything writing / reading

	// begin draining reqs before lock/unlocking to ensure nothing
	// sitting on the rlock will block our lock
	go func() {
		for pr := range b.reqs {
			pr.promise(nil, errClientClosing)
		}
	}()
	go func() {
		for pr := range b.resps {
			pr.promise(nil, errClientClosing)
		}
	}()

	b.dieMu.Lock()
	b.dieMu.Unlock()

	// after mu, nothing will be sent down reqs
	close(b.reqs)
}

func (b *broker) do(
	req func(*broker) messageRequestKind,
	promise func(messageResponseKind, error),
) {
	b.dieMu.RLock()
	if atomic.LoadInt64(&b.dead) == 0 {
		b.reqs <- promisedReq{req, promise}
	}
	b.dieMu.RUnlock()
}

func (b *broker) wait(
	req func(*broker) messageRequestKind,
	promise func(messageResponseKind, error),
) {
	done := make(chan struct{})
	wait := func(k messageResponseKind, err error) {
		defer close(done)
		promise(k, err)
	}
	b.do(req, wait)
	<-done
}

type errWrite struct {
	wrote int
	err   error
}

func (e *errWrite) Error() string {
	return fmt.Sprintf("err after %d bytes written: %v", e.wrote, e.err)
}

func (b *broker) handleReqs() {
	defer b.dieOnce()

	go b.handleResps()
	defer close(b.resps)

	var buf []byte
	var correlationID int32
	for pr := range b.reqs {
		req := pr.req(b)
		brokerMax := b.versions[req.key()]
		ourMax := req.maxVersion()
		version := brokerMax
		if brokerMax > ourMax {
			version = ourMax
		}
		if mv, ok := req.(messageRequestMinVersioner); ok {
			if version < mv.minVersion() {
				pr.promise(nil, errBrokerTooOld)
			}
		}
		req.setVersion(version) // always go for highest version
		buf = appendMessageRequest(
			buf[:0],
			req,
			version,
			correlationID,
			b.cl.cfg.client.id,
		)

		n, err := b.conn.Write(buf)
		if err != nil {
			pr.promise(nil, &errWrite{n, err})
			return
		}

		b.resps <- promisedResp{
			correlationID,
			req.responseKind(),
			pr.promise,
		}
		correlationID++
	}
}

func (b *broker) handleResps() {
	defer b.dieOnce()

	rawSize := make([]byte, 4)
	var buf []byte // TODO opt to disable buf reuse
	for pr := range b.resps {
		if _, err := io.ReadFull(b.conn, rawSize); err != nil {
			pr.promise(nil, err)
			return
		}
		size := int32(binary.BigEndian.Uint32(rawSize))

		buf = append(buf[:0], make([]byte, size)...)
		if _, err := io.ReadFull(b.conn, buf); err != nil {
			pr.promise(nil, err)
			return
		}

		if len(buf) < 4 {
			pr.promise(nil, errNotEnoughData)
		}
		correlationID := int32(binary.BigEndian.Uint32(buf))
		buf = buf[4:]
		if correlationID != pr.correlationID {
			pr.promise(nil, errCorrelationIDMismatch)
			return
		}

		pr.promise(pr.resp, pr.resp.readFrom(buf))
	}
}
