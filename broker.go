package kgo

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/twmb/kgo/kmsg"
)

type promisedReq struct {
	req     kmsg.Request
	promise func(kmsg.Response, error)
}

type promisedResp struct {
	correlationID int32
	resp          kmsg.Response
	promise       func(kmsg.Response, error)
}

type waitingResp struct {
	resp    kmsg.Response
	promise func(kmsg.Response, error)
}

type apiVersions [kmsg.MaxKey + 1]int16

// broker manages the concept how a client would interact with a broker.
type broker struct {
	cl *Client

	// id and addr are the Kafka broker ID and addr for this broker.
	id   int32
	addr string

	// cxn manages a single tcp connection to a broker.
	// This field is managed serially in handleReqs.
	cxn *brokerCxn

	bt *brokerToppars

	// seqResps, guarded by seqRespsMu, contains responses that must be
	// handled sequentially. These responses are handled asyncronously,
	// but sequentially.
	seqRespsMu sync.Mutex
	seqResps   []waitingResp

	// dieMu guards sending to reqs in case the broker has been
	// permanently stopped.
	dieMu sync.RWMutex
	// reqs manages incoming message requests.
	reqs chan promisedReq
	// dead is an atomic so a backed up reqs cannot block broker stoppage.
	dead int64
}

const unknownControllerID = -1

// broker IDs are all positive, but Kafka uses -1 to signify unknown
// controllers. To avoid issues where a client broker ID map knows of
// a -1 ID controller, we start unknown seeds at MinInt32.
func unknownSeedID(seedNum int) int32 {
	return int32(math.MinInt32 + seedNum)
}

func (c *Client) newBroker(addr string, id int32) *broker {
	br := &broker{
		cl: c,

		id:   id,
		addr: addr,

		reqs: make(chan promisedReq, 100), // TODO size limitation for max inflight reqs?
	}
	br.bt = newBrokerToppars(br)
	go br.handleReqs()

	return br
}

// stopForever permanently disables this broker.
func (b *broker) stopForever() {
	if atomic.SwapInt64(&b.dead, 1) == 1 {
		return
	}

	// begin draining reqs before lock/unlocking to ensure nothing
	// sitting on the rlock will block our lock
	go func() {
		for pr := range b.reqs {
			pr.promise(nil, errClientClosing)
		}
	}()

	b.dieMu.Lock()
	b.dieMu.Unlock()

	// after dieMu, nothing will be sent down reqs
	close(b.reqs)
}

// do issues a request to the broker, eventually calling the response
// once a the request either fails or is responded to (with failure or not).
//
// The promise will block broker processing.
func (b *broker) do(
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	dead := false

	b.dieMu.RLock()
	if atomic.LoadInt64(&b.dead) == 1 {
		dead = true
	} else {
		b.reqs <- promisedReq{req, promise}
	}
	b.dieMu.RUnlock()

	if dead {
		promise(nil, errBrokerDead)
	}
}

// doAsyncPromise is the same as do, but the promise is ran concurrently
// so as to not block the broker's request/response processing.
func (b *broker) doAsyncPromise(
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	b.do(req, func(resp kmsg.Response, err error) {
		go promise(resp, err)
	})
}

// doSequencedAsyncPromise is the same as do, but all requests using this
// function have their responses handled sequentially.
//
// This is important for example for odering of produce requests.
func (b *broker) doSequencedAsyncPromise(
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	b.do(req, func(resp kmsg.Response, err error) {
		if err != nil {
			go promise(nil, err)
			return
		}

		b.seqRespsMu.Lock()
		defer b.seqRespsMu.Unlock()

		b.seqResps = append(b.seqResps, waitingResp{resp, promise})
		if len(b.seqResps) == 1 {
			go b.handleSeqResp()
		}
	})
}

// handleSeqResp handles a sequenced response while there is one.
func (b *broker) handleSeqResp() {
start:

	b.seqRespsMu.Lock()
	waitingResp := b.seqResps[0]
	b.seqResps = b.seqResps[1:]
	more := len(b.seqResps) > 0
	b.seqRespsMu.Unlock()

	waitingResp.promise(waitingResp.resp, nil)
	if more {
		goto start
	}
}

// wait is the same as do, but this waits for the response to finish.
//
// This does not block the broker's request/response processing because this is
// inherently already tied to a running goroutine.
func (b *broker) wait(
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	var resp kmsg.Response
	var err error
	done := make(chan struct{})
	wait := func(k kmsg.Response, kerr error) {
		resp, err = k, kerr
		close(done)
	}
	b.do(req, wait)
	<-done
	promise(resp, err)
}

// handleReqs manages the intake of message requests for a broker.
//
// This creates connections as appropriate, serializes the request, and sends
// awaiting responses with the request promise to be handled as appropriate.
//
// If any of these steps fail, the promise is called with the relevant error.
func (b *broker) handleReqs() {
	defer func() {
		if b.cxn != nil {
			b.cxn.die()
		}
	}()

	for pr := range b.reqs {
		cxn, err := b.loadConnection()
		if err != nil {
			pr.promise(nil, err)
			continue
		}

		// version bound our request
		req := pr.req
		brokerMax := cxn.versions[req.Key()]
		ourMax := req.MaxVersion()
		version := brokerMax
		if brokerMax > ourMax {
			version = ourMax
		}
		if version < req.MinVersion() {
			pr.promise(nil, errBrokerTooOld)
			continue
		}
		req.SetVersion(version) // always go for highest version

		correlationID, err := cxn.writeRequest(req)
		if err != nil {
			pr.promise(nil, err)
			cxn.die()
			continue
		}

		cxn.waitResp(promisedResp{
			correlationID,
			req.ResponseKind(),
			pr.promise,
		})
	}
}

// loadConection returns the broker's connection, creating it if necessary
// and returning an error of if that fails.
func (b *broker) loadConnection() (*brokerCxn, error) {
	if b.cxn != nil {
		return b.cxn, nil
	}

	conn, err := b.connect()
	if err != nil {
		return nil, err
	}

	cxn := &brokerCxn{
		conn:     conn,
		clientID: b.cl.cfg.client.id,
	}
	if err = cxn.init(); err != nil {
		conn.Close()
		return nil, err
	}

	b.cxn = cxn
	return cxn, nil
}

// connect connects to the broker's addr, returning the new connection.
func (b *broker) connect() (net.Conn, error) {
	conn, err := b.cl.cfg.client.dialFn(b.addr)
	if err != nil {
		return nil, maybeRetriableConnErr(err)
	}
	if b.cl.cfg.client.tlsCfg != nil {
		tlsconn := tls.Client(conn, b.cl.cfg.client.tlsCfg)
		// TODO SetDeadline, then clear
		if err = tlsconn.Handshake(); err != nil {
			conn.Close()
			return nil, maybeRetriableConnErr(err)
		}
		conn = tlsconn
	}

	return conn, nil
}

// brokerCxn manages an actual connection to a Kafka broker. This is separate
// the broker struct to allow lazy connection (re)creation.
type brokerCxn struct {
	conn     net.Conn
	versions apiVersions

	// reqBuf, correlationID, and clientID are used in writing requests.
	reqBuf        []byte
	correlationID int32
	clientID      *string

	// dieMu guards sending to resps in case the connection has died.
	dieMu sync.RWMutex
	// resps manages reading kafka responses.
	resps chan promisedResp
	// dead is an atomic so that a backed up resps cannot block cxn death.
	dead int64
}

func (cx *brokerCxn) init() error {
	// TODO sasl
	if err := cx.requestAPIVersions(); err != nil {
		return err
	}
	cx.resps = make(chan promisedResp, 100)
	go cx.handleResps()
	return nil
}

func (cx *brokerCxn) requestAPIVersions() (err error) {
	req := new(kmsg.ApiVersionsRequest)
	corrID, err := cx.writeRequest(req)
	if err != nil {
		return err
	}

	rawResp, err := readResponse(cx.conn, corrID)
	if err != nil {
		return err
	}
	resp := req.ResponseKind().(*kmsg.ApiVersionsResponse)
	if err = resp.ReadFrom(rawResp); err != nil {
		return maybeRetriableConnErr(err)
	}

	for _, keyVersions := range resp.ApiVersions {
		if keyVersions.ApiKey > kmsg.MaxKey {
			continue
		}
		cx.versions[keyVersions.ApiKey] = keyVersions.MaxVersion
	}
	return nil
}

// errWrite is the error returned when a message request write fails.
type errWrite struct {
	wrote int
	err   error
}

func (e *errWrite) Error() string {
	return fmt.Sprintf("err after %d bytes written: %v", e.wrote, e.err)
}

// writeRequest writes a message request to the broker connection, bumping the
// connection's correlation ID as appropriate for the next write.
func (cx *brokerCxn) writeRequest(req kmsg.Request) (int32, error) {
	cx.reqBuf = kmsg.AppendRequest(
		cx.reqBuf[:0],
		req,
		cx.correlationID,
		cx.clientID,
	)
	if _, err := cx.conn.Write(cx.reqBuf); err != nil {
		return 0, maybeRetriableConnErr(err)
	}
	id := cx.correlationID
	cx.correlationID++
	return id, nil
}

// readResponse reads a response from conn, ensures the correlation ID is
// correct, and returns a newly allocated slice on success.
func readResponse(conn io.Reader, correlationID int32) ([]byte, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf[:4]); err != nil {
		return nil, maybeRetriableConnErr(err)
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:4]))
	if size < 0 {
		return nil, errInvalidResp
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, maybeRetriableConnErr(err)
	}

	if len(buf) < 4 {
		return nil, errNotEnoughData
	}
	gotID := int32(binary.BigEndian.Uint32(buf))
	buf = buf[4:]
	if gotID != correlationID {
		return nil, errCorrelationIDMismatch
	}
	return buf, nil
}

// die kills a broker connection (which could be dead already) and replies to
// all requests awaiting responses appropriately.
func (cx *brokerCxn) die() {
	if atomic.SwapInt64(&cx.dead, 1) == 1 {
		return
	}

	cx.conn.Close()

	go func() {
		for pr := range cx.resps {
			pr.promise(nil, errBrokerConnectionDied)
		}
	}()

	cx.dieMu.Lock()
	cx.dieMu.Unlock()

	close(cx.resps) // after lock, nothing sends down resps
}

// waitResp, called serially by a broker's handleReqs, manages handling a
// message requests's response.
func (cx *brokerCxn) waitResp(pr promisedResp) {
	dead := false

	cx.dieMu.RLock()
	if atomic.LoadInt64(&cx.dead) == 1 {
		dead = true
	} else {
		cx.resps <- pr
	}
	cx.dieMu.RUnlock()

	if dead {
		pr.promise(nil, errBrokerConnectionDied)
	}
}

// handleResps handles all broker responses serially.
func (cx *brokerCxn) handleResps() {
	defer cx.die() // always track our death

	for pr := range cx.resps {
		raw, err := readResponse(cx.conn, pr.correlationID)
		if err != nil {
			pr.promise(nil, err)
			return
		}
		pr.promise(pr.resp, pr.resp.ReadFrom(raw))
	}
}
