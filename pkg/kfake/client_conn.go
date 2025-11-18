package kfake

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type (
	clientConn struct {
		c      *Cluster
		b      *broker
		conn   net.Conn
		respCh chan clientResp

		saslStage saslStage
		s0        *scramServer0
	}

	clientReq struct {
		cc   *clientConn
		kreq kmsg.Request
		at   time.Time
		cid  string
		corr int32
		seq  uint32
	}

	clientResp struct {
		kresp kmsg.Response
		corr  int32
		err   error
		seq   uint32
	}
)

func (creq *clientReq) empty() bool { return creq == nil || creq.cc == nil || creq.kreq == nil }

func (cc *clientConn) read() {
	defer cc.conn.Close()

	type read struct {
		body []byte
		err  error
	}
	var (
		who    = cc.conn.RemoteAddr()
		size   = make([]byte, 4)
		readCh = make(chan read, 1)
		seq    uint32
	)
	for {
		go func() {
			if _, err := io.ReadFull(cc.conn, size); err != nil {
				readCh <- read{err: err}
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(size))
			_, err := io.ReadFull(cc.conn, body)
			readCh <- read{body: body, err: err}
		}()

		var read read
		select {
		case <-cc.c.die:
			return
		case read = <-readCh:
		}

		if err := read.err; err != nil {
			cc.c.cfg.logger.Logf(LogLevelDebug, "client %s disconnected from read: %v", who, err)
			return
		}

		var (
			body     = read.body
			reader   = kbin.Reader{Src: body}
			key      = reader.Int16()
			version  = reader.Int16()
			corr     = reader.Int32()
			clientID = reader.NullableString()
			kreq     = kmsg.RequestForKey(key)
		)
		kreq.SetVersion(version)
		if kreq.IsFlexible() {
			kmsg.SkipTags(&reader)
		}
		if err := kreq.ReadFrom(reader.Src); err != nil {
			cc.c.cfg.logger.Logf(LogLevelDebug, "client %s unable to parse request (key=%d, version=%d): %v", who, key, version, err)
			return
		}

		// Within Kafka, a null client ID is treated as an empty string.
		var cid string
		if clientID != nil {
			cid = *clientID
		}

		select {
		case cc.c.reqCh <- &clientReq{cc, kreq, time.Now(), cid, corr, seq}:
			seq++
		case <-cc.c.die:
			return
		}
	}
}

func (cc *clientConn) write() {
	defer cc.conn.Close()

	var (
		who     = cc.conn.RemoteAddr()
		writeCh = make(chan error, 1)
		buf     []byte
		seq     uint32

		// If a request is by necessity slow (join&sync), and the
		// client sends another request down the same conn, we can
		// actually handle them out of order because group state is
		// managed independently in its own loop. To ensure
		// serialization, we capture out of order responses and only
		// send them once the prior requests are replied to.
		//
		// (this is also why there is a seq in the clientReq)
		oooresp = make(map[uint32]clientResp)
	)
	for {
		resp, ok := oooresp[seq]
		if !ok {
			select {
			case resp = <-cc.respCh:
				if resp.seq != seq {
					oooresp[resp.seq] = resp
					continue
				}
				seq = resp.seq + 1
			case <-cc.c.die:
				return
			}
		} else {
			delete(oooresp, seq)
			seq++
		}
		if err := resp.err; err != nil {
			cc.c.cfg.logger.Logf(LogLevelInfo, "client %s request unable to be handled: %v", who, err)
			return
		}

		buf = buf[:0]
		buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
		hasFlexHeader := resp.kresp.IsFlexible() && resp.kresp.Key() != 18
		if hasFlexHeader {
			buf = append(buf, 0) // zero tagged fields
		}
		buf = resp.kresp.AppendTo(buf)

		binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))
		binary.BigEndian.PutUint32(buf[4:8], uint32(resp.corr))

		payload := append([]byte(nil), buf...)
		go func() {
			_, err := cc.conn.Write(payload)
			writeCh <- err
		}()

		var err error
		select {
		case <-cc.c.die:
			return
		case err = <-writeCh:
		}
		if err != nil {
			cc.c.cfg.logger.Logf(LogLevelDebug, "client %s disconnected from write: %v", who, err)
			return
		}
	}
}
