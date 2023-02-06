package kfake

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type clientConn struct {
	c      *Cluster
	b      *broker
	conn   net.Conn
	respCh chan clientResp
}

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
		corr   int32
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
			body    = read.body
			reader  = kbin.Reader{Src: body}
			key     = reader.Int16()
			version = reader.Int16()
			ccorr   = reader.Int32()
			_       = reader.NullableString()
			kreq    = kmsg.RequestForKey(key)
		)
		kreq.SetVersion(version)
		if ccorr != corr {
			cc.c.cfg.logger.Logf(LogLevelDebug, "client %s incorrect correlation, saw %d expecting %d: %v", who, ccorr, corr)
			return
		}
		corr++
		if kreq.IsFlexible() {
			kmsg.SkipTags(&reader)
		}
		if err := kreq.ReadFrom(reader.Src); err != nil {
			cc.c.cfg.logger.Logf(LogLevelDebug, "client %s unable to parse request: %v", who, err)
			return
		}

		select {
		case cc.c.reqCh <- clientReq{cc, kreq, time.Now()}:
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
		corr    int32
		buf     []byte
	)
	for {
		var resp clientResp
		select {
		case resp = <-cc.respCh:
		case <-cc.c.die:
			return
		}
		if err := resp.err; err != nil {
			cc.c.cfg.logger.Logf(LogLevelInfo, "client %s request unable to be handled: %v", who, err)
			return
		}

		// Size, corr, and empty tag section if flexible: 9 bytes max.
		buf = append(buf[:0], 0, 0, 0, 0, 0, 0, 0, 0, 0)
		buf = resp.kresp.AppendTo(buf)

		start := 0
		l := len(buf) - 4
		if !resp.kresp.IsFlexible() || resp.kresp.Key() == 18 {
			l--
			start++
		}
		binary.BigEndian.PutUint32(buf[start:], uint32(l))
		binary.BigEndian.PutUint32(buf[start+4:], uint32(corr))
		corr++

		go func() {
			_, err := cc.conn.Write(buf[start:])
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
