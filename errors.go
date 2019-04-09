package kgo

import (
	"errors"

	"github.com/twmb/kgo/kerr"
)

type clientErr struct {
	err       error
	retriable bool
}

func (c *clientErr) Error() string {
	return c.err.Error()
}

var (
	// ErrUnknownRequestKey is returned when using a kmsg.Request with a
	// key larger than kmsg.MaxKey.
	ErrUnknownRequestKey = errors.New("request key is unknown")

	// ErrBrokerTooOld is returned when issuing a kmsg.Request to a broker
	// that does not understand the request (either the broker cannot
	// handle the request due to not knowing of the key, or the broker does
	// not support the client minimum supported version of the request).
	ErrBrokerTooOld = errors.New("broker is too old; this client does not support the broker")

	errNoResp = errors.New("message was not replied to in a produce response")

	errNoPartitionIDs         = &clientErr{err: errors.New("topic currently has no known partition IDs"), retriable: true}
	errUnknownPartition       = &clientErr{err: errors.New("unknown partition"), retriable: true}
	errUnknownBrokerForLeader = &clientErr{err: errors.New("no broker is known for partition leader id"), retriable: true}
	errUnknownController      = &clientErr{err: errors.New("controller is unknown"), retriable: true}

	// ErrBrokerDead is a temporary error returned when a broker chosen for
	// a request is stopped due to a concurrent metadata response.
	ErrBrokerDead = errors.New("broker has died - the broker id either migrated or no longer exists")

	// ErrConnDead is a temporary error returned when any read or write to
	// a broker connection errors.
	ErrConnDead = errors.New("connection is dead")

	// ErrInvalidRespSize is a potentially temporary error returned when
	// the client reads an invalid message response size from Kafka.
	//
	// If this error happens, the client closes the broker connection.
	ErrInvalidRespSize = errors.New("invalid response size less than zero")

	// ErrInvalidResp is a generic error used when Kafka responded
	// unexpectedly.
	ErrInvalidResp = errors.New("invalid response")

	// ErrCorrelationIDMismatch is a temporary error returned when Kafka
	// replies with a different correlation ID than we were expecting for
	// the request the client issued.
	//
	// If this error happens, the client closes the broker connection.
	ErrCorrelationIDMismatch = errors.New("correlation ID mismatch")

	ErrPartitionDeleted = errors.New("TODO")
)

// TODO ErrBrokerDead is retriable, but topics need to be remapped.
func isRetriableBrokerErr(err error) bool {
	switch err {
	case ErrBrokerDead,
		ErrConnDead,
		ErrCorrelationIDMismatch,
		ErrInvalidRespSize:
		return true
	}
	return false
}

func isRetriableErr(err error) bool {
	if err, ok := err.(*kerr.Error); ok {
		return kerr.IsRetriable(err)
	}
	return isRetriableBrokerErr(err)
}
