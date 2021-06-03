package kgo

import (
	"errors"
	"fmt"
	"os"
)

type errDeadConn struct {
	err error
}

func (e *errDeadConn) Error() string {
	return e.err.Error()
}
func (e *errDeadConn) Temporary() bool {
	return true
}

func isRetriableBrokerErr(err error) bool {
	// https://github.com/golang/go/issues/45729
	//
	// Temporary is relatively useless. We will still check for the
	// temporary interface, and in all cases, even with timeouts, we want
	// to retry.
	//
	// More generally, we will retry for any error that unwraps into an
	// os.SyscallError. Looking at Go's net package, the error we care
	// about is net.OpError. Looking into that further, any error that
	// reaches into the operating system return a syscall error, which is
	// then put in net.OpError's Err field as an os.SyscallError. There are
	// a few non-os.SyscallError errors, these are where Go itself detects
	// a hard failure. We do not retry those.
	//
	// We blanket retry os.SyscallError because a lot of the times, what
	// appears as a hard failure can actually be retried. For example, a
	// failed dial can be retried, maybe the resolver temporarily had a
	// problem.
	//
	// We favor testing os.SyscallError first, because net.OpError _always_
	// implements Temporary, so if we test that first, it'll return false
	// in many cases when we want to return true from os.SyscallError.
	var se *os.SyscallError
	if errors.As(err, &se) {
		return true
	}
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) {
		return tempErr.Temporary()
	}
	switch err {
	case errChosenBrokerDead,
		errCorrelationIDMismatch:
		return true
	}
	return false
}

var (
	//////////////
	// INTERNAL // -- when used multiple times or checked in different areas of the client
	//////////////

	// A temporary error returned when a broker chosen for a request is
	// stopped due to a concurrent metadata response.
	errChosenBrokerDead = errors.New("the internal broker struct chosen to issue this requesthas died--either the broker id is migrating or no longer exists")

	errProducerIDLoadFail = errors.New("unable to initialize a producer ID due to request failures")

	// A temporary error returned when Kafka replies with a different
	// correlation ID than we were expecting for the request the client
	// issued.
	//
	// If this error happens, the client closes the broker connection.
	errCorrelationIDMismatch = errors.New("correlation ID mismatch")

	// Returned when using a kmsg.Request with a key larger than kmsg.MaxKey.
	errUnknownRequestKey = errors.New("request key is unknown")

	// Returned if a connection has loaded broker ApiVersions and knows
	// that the broker cannot handle the request to-be-issued request.
	errBrokerTooOld = errors.New("broker is too old; the broker has already indicated it will not know how to handle the request")

	// Returned when trying to call group functions when the client is not
	// assigned a group.
	errNotGroup = errors.New("invalid group function call when not assigned a group")

	// Returned when trying to begin a transaction with a client that does
	// not have a transactional ID.
	errNotTransactional = errors.New("invalid attempt to begin a transaction with a non-transactional client")

	// Returned when trying to produce a record outside of a transaction.
	errNotInTransaction = errors.New("cannot produce record transactionally if not in a transaction")

	// Returned when issuing a request to a broker that the client does not
	// know about.
	errUnknownBroker = errors.New("unknown broker")

	// Returned when records are unable to be produced and they hit the
	// configured record timeout limit.
	errRecordTimeout = errors.New("records have timed out before they were able to be produced")

	errRecordRetries = errors.New("record failed after being retried too many times")

	errClientClosing = errors.New("client closing")

	//////////////
	// EXTERNAL //
	//////////////

	// ErrMaxBuffered is returned when producing with manual flushing
	// enabled and the maximum amount of records are buffered.
	ErrMaxBuffered = errors.New("manual flushing is enabled and the maximum amount of records are buffered, cannot buffer more")

	// ErrAborting is returned for all buffered records while
	// AbortBufferedRecords is being called.
	ErrAborting = errors.New("client is aborting buffered records")
)

// ErrDataLoss is returned for Kafka >=2.1.0 when data loss is detected and the
// client is able to reset to the last valid offset.
type ErrDataLoss struct {
	// Topic is the topic data loss was detected on.
	Topic string
	// Partition is the partition data loss was detected on.
	Partition int32
	// ConsumedTo is what the client had consumed to for this partition before
	// data loss was detected.
	ConsumedTo int64
	// ResetTo is what the client reset the partition to; everything from
	// ResetTo to ConsumedTo was lost.
	ResetTo int64
}

func (e *ErrDataLoss) Error() string {
	return fmt.Sprintf("topic %s partition %d lost records;"+
		" the client consumed to offset %d but was reset to offset %d",
		e.Topic, e.Partition, e.ConsumedTo, e.ResetTo)
}

type errUnknownController struct {
	id int32
}

func (e *errUnknownController) Error() string {
	return fmt.Sprintf("Kafka replied that the controller broker is %d,"+
		" but did not reply with that broker in the broker list", e.id)
}

type errUnknownCoordinator struct {
	coordinator int32
	key         coordinatorKey
}

func (e *errUnknownCoordinator) Error() string {
	switch e.key.typ {
	case coordinatorTypeGroup:
		return fmt.Sprintf("Kafka replied that group %s has broker coordinator %d,"+
			" but did not reply with that broker in the broker list",
			e.key.name, e.coordinator)
	case coordinatorTypeTxn:
		return fmt.Sprintf("Kafka replied that txn id %s has broker coordinator %d,"+
			" but did not reply with that broker in the broker list",
			e.key.name, e.coordinator)
	default:
		return fmt.Sprintf("Kafka replied to an unknown coordinator key %s (type %d) that it has a broker coordinator %d,"+
			" but did not reply with that broker in the broker list", e.key.name, e.key.typ, e.coordinator)
	}
}
