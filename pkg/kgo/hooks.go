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
// connection. This may change in the future such that the sasl authenticate
// key is used (even though sasl authenticate requests are not being issued).
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
// connection. This may change in the future such that the sasl authenticate
// key is used (even though sasl authenticate requests are not being issued).
type HookBrokerRead interface {
	// OnRead is passed the broker metadata, the key for the response that
	// was read, the number of bytes read (may not be the whole read if
	// there was an error), how long the client waited before reading the
	// response, how long it took to read the response, and any error.
	//
	// The bytes read does not count any tls overhead.
	OnRead(meta BrokerMetadata, key int16, bytesRead int, readWait, timeToRead time.Duration, err error)
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
