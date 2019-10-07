package kgo

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/twmb/kafka-go/pkg/kbin"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/kversion"
)

type promisedReq struct {
	ctx     context.Context
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
	err     error
}

type apiVersions [kmsg.MaxKey + 1]int16

// broker manages the concept how a client would interact with a broker.
type broker struct {
	client *Client

	// id and addr are the Kafka broker ID and addr for this broker.
	id   int32
	addr string

	// The cxn fields each manage a single tcp connection to one broker.
	// Each field is managed serially in handleReqs. This means that only
	// one write can happen at a time, regardless of which connection the
	// write goes to, but the write is expected to be fast whereas the wait
	// for the response is expected to be slow.
	//
	// Produce requests go to cxnProduce, fetch to cxnFetch, and all others
	// to cxnNormal.
	cxnNormal  *brokerCxn
	cxnProduce *brokerCxn
	cxnFetch   *brokerCxn

	// recordSink and recordSource exist so that metadata updates can
	// copy these pointers to a topicPartition's record's sink field
	// and consumption's source field.
	//
	// Brokers are created with these two fields initialized; when
	// a topic partition wants to use the broker, it copies these
	// pointers.
	recordSink   *recordSink
	recordSource *recordSource

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
		client: c,

		id:   id,
		addr: addr,

		reqs: make(chan promisedReq, 10),
	}
	br.recordSink = newRecordSink(br)
	br.recordSource = newRecordSource(br)
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
			pr.promise(nil, ErrBrokerDead)
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
	ctx context.Context,
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	dead := false

	b.dieMu.RLock()
	if atomic.LoadInt64(&b.dead) == 1 {
		dead = true
	} else {
		b.reqs <- promisedReq{ctx, req, promise}
	}
	b.dieMu.RUnlock()

	if dead {
		promise(nil, ErrBrokerDead)
	}
}

// doSequencedAsyncPromise is the same as do, but all requests using this
// function have their responses handled sequentially.
//
// This is important for example for ordering of produce requests.
//
// Note that the requests may finish out of order (e.g. dead connection kills
// latter request); this is handled appropriately in producing.
func (b *broker) doSequencedAsyncPromise(
	ctx context.Context,
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	b.do(ctx, req, func(resp kmsg.Response, err error) {
		b.seqRespsMu.Lock()
		b.seqResps = append(b.seqResps, waitingResp{resp, promise, err})
		if len(b.seqResps) == 1 {
			go b.handleSeqResp(b.seqResps[0])
		}
		b.seqRespsMu.Unlock()
	})
}

// handleSeqResp handles a sequenced response while there is one.
func (b *broker) handleSeqResp(wr waitingResp) {
more:
	wr.promise(wr.resp, wr.err)

	b.seqRespsMu.Lock()
	b.seqResps = b.seqResps[1:]
	if len(b.seqResps) > 0 {
		wr = b.seqResps[0]
		b.seqRespsMu.Unlock()
		goto more
	}
	b.seqRespsMu.Unlock()
}

// waitResp runs a req, waits for the resp and returns the resp and err.
func (b *broker) waitResp(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	var resp kmsg.Response
	var err error
	done := make(chan struct{})
	wait := func(kresp kmsg.Response, kerr error) {
		resp, err = kresp, kerr
		close(done)
	}
	b.do(ctx, req, wait)
	<-done
	return resp, err
}

// handleReqs manages the intake of message requests for a broker.
//
// This creates connections as appropriate, serializes the request, and sends
// awaiting responses with the request promise to be handled as appropriate.
//
// If any of these steps fail, the promise is called with the relevant error.
func (b *broker) handleReqs() {
	defer func() {
		for _, cxn := range []*brokerCxn{
			b.cxnNormal,
			b.cxnProduce,
			b.cxnFetch,
		} {
			if cxn != nil {
				cxn.die()
			}
		}
	}()

	for pr := range b.reqs {
		req := pr.req
		cxn, err := b.loadConnection(req.Key())
		if err != nil {
			pr.promise(nil, err)
			continue
		}

		// version bound our request:
		// If we have no versions, then a max versions bound prevented
		// us from requesting versions at all.
		// We also check the max version bound.
		if int(req.Key()) > len(cxn.versions[:]) ||
			b.client.cfg.client.maxVersions != nil &&
				int(req.Key()) >= len(b.client.cfg.client.maxVersions) {
			pr.promise(nil, ErrUnknownRequestKey)
			continue
		}
		ourMax := req.MaxVersion()
		if b.client.cfg.client.maxVersions != nil {
			userMax := b.client.cfg.client.maxVersions[req.Key()]
			if userMax < ourMax {
				ourMax = userMax
			}
		}

		// If brokerMax is negative, we have no api versions because
		// the client is pinned pre 0.10.0 and we stick with our max.
		version := ourMax
		if brokerMax := cxn.versions[req.Key()]; brokerMax >= 0 && brokerMax < ourMax {
			version = brokerMax
		}
		req.SetVersion(version) // always go for highest version

		// Juuuust before we issue the request, we check if it was
		// canceled. If it is not, we do not cancel hereafter.
		// We only check the promised req's ctx, not our clients.
		// The client ctx is closed on shutdown, which kills the
		// cxn anyway.
		select {
		case <-pr.ctx.Done():
			pr.promise(nil, pr.ctx.Err())
			continue
		default:
		}

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
func (b *broker) loadConnection(reqKey int16) (*brokerCxn, error) {
	pcxn := &b.cxnNormal
	if reqKey == 0 {
		pcxn = &b.cxnProduce
	} else if reqKey == 1 {
		pcxn = &b.cxnFetch
	}

	if *pcxn != nil && atomic.LoadInt64(&(*pcxn).dead) == 0 {
		return *pcxn, nil
	}

	conn, err := b.connect()
	if err != nil {
		return nil, err
	}

	cxn := &brokerCxn{
		conn:     conn,
		clientID: b.client.cfg.client.id,
	}
	if err = cxn.init(b.client.cfg.client.maxVersions); err != nil {
		conn.Close()
		return nil, err
	}

	*pcxn = cxn
	return cxn, nil
}

// connect connects to the broker's addr, returning the new connection.
func (b *broker) connect() (net.Conn, error) {
	conn, err := b.client.cfg.client.dialFn(b.addr)
	if err != nil {
		if _, ok := err.(net.Error); ok {
			return nil, ErrConnDead
		}
		return nil, err
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

func (cx *brokerCxn) init(maxVersions kversion.Versions) error {
	for i := 0; i < kmsg.MaxKey; i++ {
		cx.versions[i] = -1
	}

	// TODO sasl
	if maxVersions == nil || len(maxVersions) > 18 {
		if err := cx.requestAPIVersions(); err != nil {
			return err
		}
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
		return ErrConnDead
	}

	for _, keyVersions := range resp.ApiVersions {
		if keyVersions.ApiKey > kmsg.MaxKey {
			continue
		}
		cx.versions[keyVersions.ApiKey] = keyVersions.MaxVersion
	}
	return nil
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
		return 0, ErrConnDead
	}
	id := cx.correlationID
	cx.correlationID++
	return id, nil
}

// readResponse reads a response from conn, ensures the correlation ID is
// correct, and returns a newly allocated slice on success.
func readResponse(conn io.ReadCloser, correlationID int32) ([]byte, error) {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf[:4]); err != nil {
		return nil, ErrConnDead
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf[:4]))
	if size < 0 {
		return nil, ErrInvalidRespSize
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, ErrConnDead
	}

	if len(buf) < 4 {
		return nil, kbin.ErrNotEnoughData
	}
	gotID := int32(binary.BigEndian.Uint32(buf))
	buf = buf[4:]
	if gotID != correlationID {
		conn.Close()
		return nil, ErrCorrelationIDMismatch
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
			pr.promise(nil, ErrConnDead)
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
		pr.promise(nil, ErrConnDead)
	}
}

// handleResps serially handles all broker responses for an single connection.
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
