package kgo

import (
	"errors"
	"fmt"

	"github.com/twmb/kafka-go/pkg/kerr"
)

var (
	// ErrUnknownRequestKey is returned when using a kmsg.Request with a
	// key larger than kmsg.MaxKey.
	ErrUnknownRequestKey = errors.New("request key is unknown")

	// ErrClientToOld is returned when issuing request that are unknown or
	// use an unknown version.
	ErrClientTooOld = errors.New("client is too old; this client does not know what to do with this")

	// ErrNoResp is the error used if Kafka does not reply to a topic or
	// partition in a produce request. This error should never be seen.
	ErrNoResp = errors.New("message was not replied to in a response")

	// ErrUnknownBroker is returned when issuing a request to a broker that
	// the client does not know about.
	ErrUnknownBroker = errors.New("unknown broker")

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

	// ErrNoPartitionsAvailable is returned immediately when producing a
	// non-consistent record to a topic that has no writable partitions.
	ErrNoPartitionsAvailable = errors.New("no partitions available")

	// ErrPartitionDeleted is returned when a partition that was being
	// written to disappears in a metadata update.
	//
	// Kafka does not allow downsizing partition counts in Kafka, so this
	// error should generally not appear. This will only appear if a topic
	// is deleted and recreated with fewer partitions.
	ErrPartitionDeleted = errors.New("partition no longer exists")

	// ErrRecordTimeout is returned when records are unable to be produced
	// and they hit the configured record timeout limit.
	ErrRecordTimeout = errors.New("records have timed out before they were able to be produced")

	// ErrNotTransactional is returned when trying to begin a transaction
	// with a client that does not have a transactional ID.
	ErrNotTransactional = errors.New("invalid attempt to begin a transaction with a non-transactional client")

	// ErrAlreadyInTransaction is returned if trying to begin a transaction
	// while the producer is already in a transaction.
	ErrAlreadyInTransaction = errors.New("invalid attempt to begin a transaction while already in a transaction")

	// ErrNotInTransaction is returned when trying to produce a record
	// outside of a transaction.
	ErrNotInTransaction = errors.New("cannot produce record transactionally if not in a transaction")

	// ErrAborting is returned for all buffered records while
	// AbortBufferedRecords is being called.
	ErrAborting = errors.New("client is aborting buffered records")

	// ErrCommitWithFatalID is returned when trying to commit in
	// EndTransaction with a producer ID that has failed.
	ErrCommitWithFatalID = errors.New("cannot commit with a fatal producer id; retry with an abort")
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

type errUnknownBrokerForPartition struct {
	topic     string
	partition int32
	broker    int32
}

func (e *errUnknownBrokerForPartition) Error() string {
	return fmt.Sprintf("Kafka replied that topic %s partition %d has broker leader %d,"+
		" but did not reply with that broker in the broker list",
		e.topic, e.partition, e.broker)
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
