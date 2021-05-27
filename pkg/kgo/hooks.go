package kgo

import (
	"net"
	"time"
)

// Hook is a hook to be called when something happens in kgo.
//
// The base Hook interface is useless, but wherever a hook can occur in kgo,
// the client checks if your hook implements an appropriate interface. If so,
// your hook is called.
//
// This allows you to only hook in to behavior you care about, and it allows
// the client to add more hooks in the future.
//
// All hook interfaces in this package have Hook in the name. Hooks must be
// safe for concurrent use. It is expected that hooks are fast; if a hook needs
// to take time, then copy what you need and ensure the hook is async.
type Hook interface{}

type hooks []Hook

func (hs hooks) each(fn func(Hook)) {
	for _, h := range hs {
		fn(h)
	}
}

// HookBrokerConnect is called after a connection to a broker is opened.
type HookBrokerConnect interface {
	// OnConnect is passed the broker metadata, how long it took to dial,
	// and either the dial's resulting net.Conn or error.
	OnConnect(meta BrokerMetadata, dialDur time.Duration, conn net.Conn, err error)
}

// HookBrokerDisconnect is called when a connection to a broker is closed.
type HookBrokerDisconnect interface {
	// OnDisconnect is passed the broker metadata and the connection that
	// is closing.
	OnDisconnect(meta BrokerMetadata, conn net.Conn)
}

// HookBrokerWrite is called after a write to a broker.
//
// Kerberos SASL does not cause write hooks, since it directly writes to the
// connection.
type HookBrokerWrite interface {
	// OnWrite is passed the broker metadata, the key for the request that
	// was written, the number of bytes that were written (may not be the
	// whole request if there was an error), how long the request waited
	// before being written (including throttling waiting), how long it
	// took to write the request, and any error.
	//
	// The bytes written does not count any tls overhead.
	OnWrite(meta BrokerMetadata, key int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error)
}

// HookBrokerRead is called after a read from a broker.
//
// Kerberos SASL does not cause read hooks, since it directly reads from the
// connection.
type HookBrokerRead interface {
	// OnRead is passed the broker metadata, the key for the response that
	// was read, the number of bytes read (may not be the whole read if
	// there was an error), how long the client waited before reading the
	// response, how long it took to read the response, and any error.
	//
	// The bytes read does not count any tls overhead.
	OnRead(meta BrokerMetadata, key int16, bytesRead int, readWait, timeToRead time.Duration, err error)
}

// E2EInfo tracks complete information for a write of a request followed by a
// read of that requests's response.
//
// Note that if this is for a produce request with no acks, there will be no
// read wait / time to read.
type E2EInfo struct {
	// BytesWritten is the number of bytes written for this request.
	//
	// This may not be the whole request if there was an error while writing.
	BytesWritten int

	// BytesRead is the number of bytes read for this requests's response.
	//
	// This may not be the whole response if there was an error while
	// reading, and this will be zero if there was a write error.
	BytesRead int

	// WriteWait is the time spent waiting from when this request was
	// generated internally in the client to just before the request is
	// written to the connection. This number is not included in the
	// DurationE2E method.
	WriteWait time.Duration
	// TimeToWrite is how long a request took to be written on the wire.
	// This specifically tracks only how long conn.Write takes.
	TimeToWrite time.Duration
	// ReadWait tracks the span of time immediately following conn.Write
	// until conn.Read begins.
	ReadWait time.Duration
	// TimeToRead tracks how long conn.Read takes for this request to be
	// entirely read. This includes the time it takes to allocate a buffer
	// for the response after the initial four size bytes are read.
	TimeToRead time.Duration

	// WriteErr is any error encountered during writing. If a write error is
	// encountered, no read will be attempted.
	WriteErr error
	// ReadErr is any error encountered during reading.
	ReadErr error
}

// DurationE2E returns the e2e time from the start of when a request is written
// to the end of when the response for that request was fully read. If a write
// or read error occurs, this hook is called with all information possible at
// the time (e.g., if a write error occurs, all write info is specified).
//
// Kerberos SASL does not cause this hook, since it directly reads from the
// connection.
func (e *E2EInfo) DurationE2E() time.Duration {
	return e.TimeToWrite + e.ReadWait + e.TimeToRead
}

// Err returns the first of either the write err or the read err. If this
// return is non-nil, the request/response had an error.
func (e *E2EInfo) Err() error {
	if e.WriteErr != nil {
		return e.WriteErr
	}
	return e.ReadErr
}

// HookBrokerE2E is called after a write to a broker that errors, or after a
// read to a broker.
//
// This differs from HookBrokerRead and HookBrokerWrite by tracking all E2E
// info for a write and a read, which allows for easier e2e metrics. This hook
// can replace both the read and write hook.
type HookBrokerE2E interface {
	// OnE2E is passed the broker metadata, the key for the
	// request/response that was written/read, and the e2e info for the
	// request and response.
	OnE2E(meta BrokerMetadata, key int16, e2e E2EInfo)
}

// HookBrokerThrottle is called after a response to a request is read
// from a broker, and the response identifies throttling in effect.
type HookBrokerThrottle interface {
	// OnThrottle is passed the broker metadata, the imposed throttling
	// interval, and whether the throttle was applied before Kafka
	// responded to them request or after.
	//
	// For Kafka < 2.0.0, the throttle is applied before issuing a response.
	// For Kafka >= 2.0.0, the throttle is applied after issuing a response.
	//
	// If throttledAfterResponse is false, then Kafka already applied the
	// throttle. If it is true, the client internally will not send another
	// request until the throttle deadline has passed.
	OnThrottle(meta BrokerMetadata, throttleInterval time.Duration, throttledAfterResponse bool)
}

// HookGroupManageError is called after every error that causes the client,
// operating as a group member, to break out of the group managing loop and
// backoff temporarily.
//
// Specifically, any error that would result in OnLost being called will result
// in this hook being called.
type HookGroupManageError interface {
	// OnGroupManageError is passed the error that killed a group session.
	// This can be used to detect potentially fatal errors and act on them
	// at runtime to recover (such as group auth errors, or group max size
	// reached).
	OnGroupManageError(error)
}
