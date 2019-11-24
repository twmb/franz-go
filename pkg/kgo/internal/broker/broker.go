// Package broker talks to Kafka brokers.
package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kbin"
	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/kversion"
	"github.com/twmb/kafka-go/pkg/sasl"
)

var (
	// ErrConnDead is a temporary error returned when any read or write to
	// a broker connection errors.
	ErrConnDead = errors.New("connection is dead")

	// ErrBrokerDead is a potentially temporary error returned when a
	// broker chosen for a request is stopped due to a concurrent metadata
	// response.
	ErrBrokerDead = errors.New("broker has died - the broker id either migrated or no longer exists")

	// ErrUnknownRequestKey is returned when using a kmsg.Request with a
	// key larger than kmsg.MaxKey.
	ErrUnknownRequestKey = errors.New("request key is unknown")

	// ErrInvalidRespSize is a potentially temporary error returned when
	// the client reads an invalid message response size from Kafka.
	//
	// If this error happens, the client closes the broker connection.
	ErrInvalidRespSize = errors.New("invalid response size less than zero")

	// ErrCorrelationIDMismatch is a temporary error returned when Kafka
	// replies with a different correlation ID than we were expecting for
	// the request the client issued.
	//
	// If this error happens, the client closes the broker connection.
	ErrCorrelationIDMismatch = errors.New("correlation ID mismatch")
)

type promisedReq struct {
	ctx     context.Context
	req     kmsg.Request
	promise func(kmsg.Response, error)
}

type promisedResp struct {
	corrID   int32
	flexible bool
	resp     kmsg.Response
	promise  func(kmsg.Response, error)
}

type waitingResp struct {
	resp    kmsg.Response
	promise func(kmsg.Response, error)
	err     error
}

// Broker talks to Kafka brokers.
type Broker struct {
	dial     func(string) (net.Conn, error)
	addr     string
	clientID *string

	maxVersions kversion.Versions

	saslCtx context.Context
	sasls   []sasl.Mechanism

	// The cxn fields each manage a single tcp connection to one broker.
	// Each field is managed serially in handleReqs. This means that only
	// one write can happen at a time, regardless of which connection the
	// write goes to, but the write is expected to be fast whereas the wait
	// for the response is expected to be slow.
	//
	// Produce requests go to cxnProduce, fetch to cxnFetch, and all others
	// to cxnNormal.
	cxnNormal  *cxn
	cxnProduce *cxn
	cxnFetch   *cxn

	// seqResps, guarded by seqRespsMu, contains responses that are handled
	// sequentially in their own goroutine.
	seqRespsMu sync.Mutex
	seqResps   []waitingResp

	// dieMu guards sending to reqs in case the broker has been
	// permanently stopped.
	dieMu sync.RWMutex
	reqs  chan promisedReq // manages incoming requests
	dead  int32            // atomic so a backed up reqs cannot block broker stoppage
}

// New returns a new Broker that talks to Kafka brokers using dial to open
// connections to addr and adding clientID to all requests.
//
// If maxVersions is nonempty, request versions are bounded before being
// issued.
//
// If sasls is nonempty, SASL is attempted immediately after a connection
// is dialed using saslCtx.
func New(
	dial func(string) (net.Conn, error),
	addr string,
	clientID *string,
	maxVersions kversion.Versions,
	saslCtx context.Context,
	sasls []sasl.Mechanism,
) *Broker {
	b := &Broker{
		dial:     dial,
		addr:     addr,
		clientID: clientID,

		maxVersions: maxVersions,

		saslCtx: saslCtx,
		sasls:   sasls,

		reqs: make(chan promisedReq, 10),
	}

	go b.handleReqs()

	return b
}

// Stop permanently stops this broker, ensuring all pending requests and
// requests awaiting responses are errored.
func (b *Broker) Stop() {
	if atomic.SwapInt32(&b.dead, 1) == 1 {
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

// do issues a request.
func (b *Broker) do(
	ctx context.Context,
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	dead := false

	b.dieMu.RLock()
	if atomic.LoadInt32(&b.dead) == 1 {
		dead = true
	} else {
		b.reqs <- promisedReq{ctx, req, promise}
	}
	b.dieMu.RUnlock()

	if dead {
		promise(nil, ErrBrokerDead)
	}
}

// SeqReq issues a request to the broker, calling promise in a dedicated
// goroutine. All requests to SeqReq are handled sequentially.
func (b *Broker) SeqReq(
	ctx context.Context,
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	b.do(ctx, req, func(req kmsg.Response, err error) {
		b.seqRespsMu.Lock()
		b.seqResps = append(b.seqResps, waitingResp{req, promise, err})
		if len(b.seqResps) == 1 {
			go b.handleSeqResp(b.seqResps[0])
		}
		b.seqRespsMu.Unlock()
	})
}

// handleSeqResp handles sequenced responses while there is one.
func (b *Broker) handleSeqResp(wr waitingResp) {
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

// WaitReq issues a request to the broker and waits for the response.
func (b *Broker) WaitReq(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
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
func (b *Broker) handleReqs() {
	defer func() {
		b.cxnNormal.die()
		b.cxnProduce.die()
		b.cxnFetch.die()
	}()

	for pr := range b.reqs {
		req := pr.req
		cxn, err := b.loadConnection(req.Key())
		if err != nil {
			pr.promise(nil, err)
			continue
		}

		if int(req.Key()) > len(cxn.versions[:]) || b.maxVersions != nil && int(req.Key()) >= len(b.maxVersions) {
			pr.promise(nil, ErrUnknownRequestKey)
			continue
		}
		ourMax := req.MaxVersion()
		if b.maxVersions != nil {
			userMax := b.maxVersions[req.Key()]
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
		req.SetVersion(version)

		// If we are after the reauth time, try to reauth. We can only
		// have an expiry if we went the authenticate flow, so we know
		// we are authenticating again. (KIP-368)
		if !cxn.expiry.IsZero() && time.Now().After(cxn.expiry) {
			if err = cxn.doSasl(true); err != nil {
				pr.promise(nil, err)
				cxn.die()
				continue
			}
		}

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

		corrID, err := cxn.writeRequest(req)
		if err != nil {
			pr.promise(nil, err)
			cxn.die()
			continue
		}

		cxn.waitResp(promisedResp{
			corrID,
			req.IsFlexible(),
			req.ResponseKind(),
			pr.promise,
		})
	}
}

// loadConection returns the broker's connection, creating it if necessary
// and returning an error of if that fails.
func (b *Broker) loadConnection(reqKey int16) (*cxn, error) {
	cxnPtr := &b.cxnNormal
	if reqKey == 0 {
		cxnPtr = &b.cxnProduce
	} else if reqKey == 1 {
		cxnPtr = &b.cxnFetch
	}

	if *cxnPtr != nil && atomic.LoadInt32(&(*cxnPtr).dead) == 0 {
		return *cxnPtr, nil
	}

	conn, err := b.connect()
	if err != nil {
		return nil, err
	}

	cxn := &cxn{
		conn:     conn,
		clientID: b.clientID,
		saslCtx:  b.saslCtx,
		sasls:    b.sasls,
	}
	if err = cxn.init(b.maxVersions); err != nil {
		conn.Close()
		return nil, err
	}

	*cxnPtr = cxn
	return cxn, nil
}

// connect connects to the broker's addr, returning the new connection.
func (b *Broker) connect() (net.Conn, error) {
	conn, err := b.dial(b.addr)
	if err != nil {
		if _, ok := err.(net.Error); ok {
			return nil, ErrConnDead
		}
		return nil, err
	}
	return conn, nil
}

// cxn manages an actual connection to a Kafka broker. This is separate
// the broker struct to allow lazy connection (re)creation.
type cxn struct {
	conn     net.Conn
	versions [kmsg.MaxKey + 1]int16

	saslCtx context.Context
	sasls   []sasl.Mechanism

	mechanism sasl.Mechanism
	expiry    time.Time

	// reqBuf, corrID, and clientID are used in writing requests.
	reqBuf   []byte
	corrID   int32
	clientID *string

	// dieMu guards sending to resps in case the connection has died.
	dieMu sync.RWMutex
	// resps manages reading kafka responses.
	resps chan promisedResp
	// dead is an atomic so that a backed up resps cannot block cxn death.
	dead int32
}

func (cxn *cxn) init(maxVersions kversion.Versions) error {
	for i := 0; i < len(cxn.versions[:]); i++ {
		cxn.versions[i] = -1
	}

	if maxVersions == nil || len(maxVersions) >= 19 { // 19 == api versions request
		if err := cxn.requestAPIVersions(); err != nil {
			return err
		}
	}

	if err := cxn.sasl(); err != nil {
		return err
	}

	cxn.resps = make(chan promisedResp, 10)
	go cxn.handleResps()
	return nil
}

func (cxn *cxn) requestAPIVersions() error {
	maxVersion := int16(3)
start:
	req := &kmsg.ApiVersionsRequest{
		Version:               maxVersion,
		ClientSoftwareName:    "kgo",
		ClientSoftwareVersion: "0.0.1",
	}
	corrID, err := cxn.writeRequest(req)
	if err != nil {
		return err
	}

	rawResp, err := readResponse(cxn.conn, corrID)
	if err != nil {
		return err
	}
	if len(rawResp) < 2 {
		return ErrConnDead
	}

	resp := req.ResponseKind().(*kmsg.ApiVersionsResponse)

	// If we used a version larger than Kafka supports, Kafka replies with
	// Version 0 and an UNSUPPORTED_VERSION error.
	//
	// Pre Kafka 2.4.0, we have to retry the request with version 0.
	// Post, Kafka replies with all versions.
	if rawResp[1] == 35 {
		if maxVersion == 0 {
			return ErrConnDead
		}
		if string(rawResp) == "\x00\x23\x00\x00\x00\x00" {
			maxVersion = 0
			goto start
		}
		resp.Version = 0
	}

	if err = resp.ReadFrom(rawResp); err != nil {
		return ErrConnDead
	}
	if len(resp.ApiKeys) == 0 {
		return ErrConnDead
	}

	for _, key := range resp.ApiKeys {
		if key.ApiKey > kmsg.MaxKey {
			continue
		}
		cxn.versions[key.ApiKey] = key.MaxVersion
	}
	return nil
}

func (cxn *cxn) sasl() error {
	if len(cxn.sasls) == 0 {
		return nil
	}
	mechanism := cxn.sasls[0]
	retried := false
	authenticate := false
	const handshakeKey = 17

start:
	if mechanism.Name() != "GSSAPI" && cxn.versions[handshakeKey] >= 0 {
		req := &kmsg.SASLHandshakeRequest{
			Version:   cxn.versions[handshakeKey],
			Mechanism: mechanism.Name(),
		}
		corrID, err := cxn.writeRequest(req)
		if err != nil {
			return err
		}

		rawResp, err := readResponse(cxn.conn, corrID)
		if err != nil {
			return err
		}
		resp := req.ResponseKind().(*kmsg.SASLHandshakeResponse)
		if err = resp.ReadFrom(rawResp); err != nil {
			return err
		}

		err = kerr.ErrorForCode(resp.ErrorCode)
		if err != nil {
			if !retried && err == kerr.UnsupportedSaslMechanism {
				for _, ours := range cxn.sasls[1:] {
					for _, supported := range resp.SupportedMechanisms {
						if supported == ours.Name() {
							mechanism = ours
							retried = true
							goto start
						}
					}
				}
			}
			return err
		}
		authenticate = req.Version == 1
	}
	cxn.mechanism = mechanism
	return cxn.doSasl(authenticate)
}

func (cxn *cxn) doSasl(authenticate bool) error {
	session, clientWrite, err := cxn.mechanism.Authenticate(cxn.saslCtx)
	if err != nil {
		return err
	}
	if len(clientWrite) == 0 {
		return fmt.Errorf("unexpected server-write sasl with mechanism %s", cxn.mechanism.Name())
	}

	var lifetimeMillis int64

	for done := false; !done; {
		var challenge []byte

		if !authenticate {
			cxn.reqBuf = append(cxn.reqBuf[:0], 0, 0, 0, 0)
			binary.BigEndian.PutUint32(cxn.reqBuf, uint32(len(clientWrite)))
			cxn.reqBuf = append(cxn.reqBuf, clientWrite...)
			if _, err = cxn.conn.Write(cxn.reqBuf); err != nil {
				return ErrConnDead
			}
			if challenge, err = readConn(cxn.conn); err != nil {
				return err
			}

		} else {
			const authenticateKey = 37
			req := &kmsg.SASLAuthenticateRequest{
				Version:       cxn.versions[authenticateKey],
				SASLAuthBytes: clientWrite,
			}
			corrID, err := cxn.writeRequest(req)
			if err != nil {
				return err
			}
			rawResp, err := readResponse(cxn.conn, corrID)
			if err != nil {
				return err
			}
			resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)
			if err = resp.ReadFrom(rawResp); err != nil {
				return err
			}

			if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
				if resp.ErrorMessage != nil {
					return fmt.Errorf("%s: %v", *resp.ErrorMessage, err)
				}
				return err
			}
			challenge = resp.SASLAuthBytes
			lifetimeMillis = resp.SessionLifetimeMillis
		}

		if done, clientWrite, err = session.Challenge(challenge); err != nil {
			return err
		}
	}

	if lifetimeMillis > 0 {
		// If we have a lifetime, we take 1s off of it to account
		// for some processing lag or whatever.
		// A better thing to return in the auth response would
		// have been the deadline, but we are here now.
		if lifetimeMillis < 5000 {
			return fmt.Errorf("invalid short sasl lifetime millis %d", lifetimeMillis)
		}
		cxn.expiry = time.Now().Add(time.Duration(lifetimeMillis)*time.Millisecond - time.Second)
	}
	return nil
}

// writeRequest writes a message request to the broker connection, bumping the
// connection's correlation ID as appropriate for the next write.
func (cxn *cxn) writeRequest(req kmsg.Request) (int32, error) {
	cxn.reqBuf = kmsg.AppendRequest(
		cxn.reqBuf[:0],
		req,
		cxn.corrID,
		cxn.clientID,
	)
	if _, err := cxn.conn.Write(cxn.reqBuf); err != nil {
		return 0, ErrConnDead
	}
	id := cxn.corrID
	cxn.corrID++
	return id, nil
}

func readConn(conn io.ReadCloser) ([]byte, error) {
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
	return buf, nil
}

// readResponse reads a response from conn, ensures the correlation ID is
// correct, and returns a newly allocated slice on success.
func readResponse(conn io.ReadCloser, corrID int32) ([]byte, error) {
	buf, err := readConn(conn)
	if err != nil {
		return nil, err
	}
	if len(buf) < 4 {
		return nil, kbin.ErrNotEnoughData
	}
	gotID := int32(binary.BigEndian.Uint32(buf))
	if gotID != corrID {
		conn.Close()
		return nil, ErrCorrelationIDMismatch
	}
	return buf[4:], nil
}

// die kills a broker connection (which could be dead already) and replies to
// all requests awaiting responses appropriately.
func (cxn *cxn) die() {
	if cxn == nil {
		return
	}
	if atomic.SwapInt32(&cxn.dead, 1) == 1 {
		return
	}

	cxn.conn.Close()

	go func() {
		for pr := range cxn.resps {
			pr.promise(nil, ErrConnDead)
		}
	}()

	cxn.dieMu.Lock()
	cxn.dieMu.Unlock()

	close(cxn.resps) // after lock, nothing sends down resps
}

// waitResp, called serially by a broker's handleReqs, manages handling a
// message requests's response.
func (cxn *cxn) waitResp(pr promisedResp) {
	dead := false

	cxn.dieMu.RLock()
	if atomic.LoadInt32(&cxn.dead) == 1 {
		dead = true
	} else {
		cxn.resps <- pr
	}
	cxn.dieMu.RUnlock()

	if dead {
		pr.promise(nil, ErrConnDead)
	}
}

// handleResps serially handles all broker responses for an single connection.
func (cxn *cxn) handleResps() {
	defer cxn.die() // always track our death

	for pr := range cxn.resps {
		raw, err := readResponse(cxn.conn, pr.corrID)
		if err != nil {
			pr.promise(nil, err)
			return
		}
		pr.promise(pr.resp, pr.resp.ReadFrom(raw))
	}
}
