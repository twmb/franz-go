package kgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
)

// TODO KIP-359 leader epoch in produce request when it is released

// BeginTransaction sets the client to a transactional state, erroring if there
// is no transactional ID or if the client is already in a transaction.
func (cl *Client) BeginTransaction() error {
	if cl.cfg.txnID == nil {
		return ErrNotTransactional
	}

	cl.producer.txnMu.Lock()
	defer cl.producer.txnMu.Unlock()
	if cl.producer.inTxn {
		return ErrAlreadyInTransaction
	}
	cl.producer.inTxn = true
	atomic.StoreUint32(&cl.producer.producingTxn, 1) // allow produces for txns now
	return nil
}

// AbortBufferedRecords fails all unflushed records with ErrAborted and waits
// for there to be no buffered records.
//
// This accepts a context to quit the wait early, but it is strongly
// recommended to always wait for all records to be flushed. Waits should only
// occur when waiting for currently in flight produce requests to finish.  The
// only case where this function returns an error is if the context is canceled
// while flushing.
//
// The intent of this function is to provide a way to clear the client's
// production backlog before aborting a transaction and beginning a new one; it
// would be erroneous to not wait for the backlog to clear before beginning a
// new transaction, since anything not cleared may be a part of the new
// transaction.
//
// Records produced during or after a call to this function may not be failed;
// it is not recommended to call this function concurrent with producing
// records.
//
// It is invalid to call this method multiple times concurrently.
func (cl *Client) AbortBufferedRecords(ctx context.Context) error {
	var unaborting []*broker
	cl.brokersMu.Lock()
	for _, broker := range cl.brokers {
		broker.sink.setAborting()
		unaborting = append(unaborting, broker)
	}
	cl.brokersMu.Unlock()

	defer func() {
		for _, broker := range unaborting {
			broker.sink.clearAborting()
		}
	}()

	// Like in client closing, we must manually fail all partitions
	// that never had a sink.
	for _, partitions := range cl.loadTopics() {
		for _, partition := range partitions.load().all {
			partition.records.mu.Lock()
			if partition.records.sink == nil {
				partition.records.failAllRecords(ErrAborting)
			}
			partition.records.mu.Unlock()
		}
	}

	return cl.Flush(ctx)
}

// EndTransaction ends a transaction and resets the client's internal state to
// not be in a transaction.
//
// Flush and CommitOffsetsForTransaction must be called before this function;
// this function does not flush and does not itself ensure that all buffered
// records are flushed. If no record yet has caused a partition to be added to
// the transaction, this function does nothing and returns nil. Alternatively,
// ErrorBufferedRecords should be called before aborting a transaction to
// ensure that any buffered records not yet flushed will not be a part of a new
// transaction.
//
// If records failed with UnknownProducerID and your Kafka version is at least
// 2.5.0, then aborting here will potentially allow the client to recover for
// more production.
func (cl *Client) EndTransaction(ctx context.Context, commit bool) error {
	cl.producer.txnMu.Lock()
	defer cl.producer.txnMu.Unlock()

	atomic.StoreUint32(&cl.producer.producingTxn, 0) // forbid any new produces while ending txn

	defer func() {
		cl.consumer.mu.Lock()
		defer cl.consumer.mu.Unlock()
		if cl.consumer.typ == consumerTypeGroup {
			cl.consumer.group.offsetsAddedToTxn = false
		}
	}()

	if !cl.producer.inTxn {
		return ErrNotInTransaction
	}
	cl.producer.inTxn = false

	// After the flush, no records are being produced to, and we can set
	// addedToTxn to false outside of any mutex.
	var anyAdded bool
	for _, parts := range cl.loadTopics() {
		for _, part := range parts.load().all {
			if part.records.addedToTxn {
				part.records.addedToTxn = false
				anyAdded = true
			}
		}
	}
	// If no partition was added to a transaction, then we have nothing to
	// commit. If any were added, we know we have a producer id.
	if !anyAdded {
		return nil
	}

	id, epoch, err := cl.producerID()
	if err != nil {
		if commit {
			return ErrCommitWithFatalID
		}

		switch err.(type) {
		case *kerr.Error:
			if err != kerr.UnknownProducerID || cl.producer.idVersion <= 2 {
				return err // fatal, unrecoverable
			}

			// At this point, nothing is being produced and the
			// producer ID is loads with an error. Before we allow
			// production to continue, we reset all sequence
			// numbers. Storing errReloadProducerID will reset the
			// id / epoch appropriately and everything will work as
			// per KIP-360.
			for _, tp := range cl.loadTopics() {
				for _, tpd := range tp.load().all {
					tpd.records.resetSeq()
				}
			}

			// With UnknownProducerID and v3 init id, we can recover.
			// No sense issuing an abort request, though.
			cl.producer.id.Store(&producerID{
				id:    id,
				epoch: epoch,
				err:   errReloadProducerID,
			})
			return nil

		default:
			// If this is not a kerr.Error, then it was some arbitrary
			// client error. We can try the EndTxnRequest in the hopes
			// that our id / epoch is valid, but the id / epoch may
			// have never loaded (and thus will be -1 / -1).
		}
	}

	kresp, err := cl.Request(ctx, &kmsg.EndTxnRequest{
		TransactionalID: *cl.cfg.txnID,
		ProducerID:      id,
		ProducerEpoch:   epoch,
		Commit:          commit,
	})
	if err != nil {
		return err
	}
	return kerr.ErrorForCode(kresp.(*kmsg.EndTxnResponse).ErrorCode)
}

type producer struct {
	bufferedRecords int64

	id           atomic.Value
	producingTxn uint32 // 1 if in txn
	flushing     int32  // >0 if flushing, can Flush many times concurrently

	idMu       sync.Mutex
	idVersion  int16
	waitBuffer chan struct{}

	flushingMu   sync.Mutex
	flushingCond *sync.Cond

	txnMu sync.Mutex
	inTxn bool
}

func noPromise(*Record, error) {}

// Produce sends a Kafka record to the topic in the record's Topic field,
// calling promise with the record or an error when Kafka replies.
//
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly.
//
// If the record is too large, this will return an error.  For simplicity, this
// function considers messages too large if they are within 512 bytes of the
// record batch byte limit. This may be made more precise in the future if
// necessary.
//
// The context is used if the client currently currently has the max amount of
// buffered records. If so, the client waits for some records to complete or
// for the context or client to quit.
//
// The first buffered record for an unknown topic begins a timeout for the
// configured record timeout limit; all records buffered within the wait will
// expire with the same timeout if the topic does not load in time. For
// simplicity, any time spent waiting for the topic to load is not persisted
// through once the topic loads, meaning the record may further wait once
// buffered. This may be changed in the future if necessary, however, the only
// reason for a topic to not load promptly is if it does not exist.
func (cl *Client) Produce(
	ctx context.Context,
	r *Record,
	promise func(*Record, error),
) error {
	if len(r.Key)+len(r.Value) > int(cl.cfg.maxRecordBatchBytes)-512 {
		return kerr.MessageTooLarge
	}

	if cl.cfg.txnID != nil && atomic.LoadUint32(&cl.producer.producingTxn) != 1 {
		return ErrNotInTransaction
	}

	if atomic.AddInt64(&cl.producer.bufferedRecords, 1) > cl.cfg.maxBufferedRecords {
		select {
		case <-cl.producer.waitBuffer:
		case <-cl.ctx.Done():
			return cl.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if promise == nil {
		promise = noPromise
	}
	pr := promisedRec{promise, r}
	cl.partitionRecord(pr)
	return nil
}

func (cl *Client) finishRecordPromise(pr promisedRec, err error) {
	buffered := atomic.AddInt64(&cl.producer.bufferedRecords, -1)
	if buffered >= cl.cfg.maxBufferedRecords {
		go func() { cl.producer.waitBuffer <- struct{}{} }()
	} else if buffered == 0 && atomic.LoadInt32(&cl.producer.flushing) > 0 {
		cl.producer.flushingMu.Lock()
		cl.producer.flushingMu.Unlock()
		cl.producer.flushingCond.Broadcast()
	}
	pr.promise(pr.Record, err)
}

// partitionRecord loads the partitions for a topic and produce to them. If
// the topic does not currently exist, the record is buffered in unknownTopics
// for a metadata update to deal with.
func (cl *Client) partitionRecord(pr promisedRec) {
	parts, partsData := cl.partitionsForTopicProduce(pr)
	if parts == nil {
		return
	}
	cl.doPartitionRecord(parts, partsData, pr)
}

// doPartitionRecord is the logic behind record partitioning and producing if
// the client knows of the topic's partitions.
func (cl *Client) doPartitionRecord(parts *topicPartitions, partsData *topicPartitionsData, pr promisedRec) {
	if partsData.loadErr != nil && !kerr.IsRetriable(partsData.loadErr) {
		cl.finishRecordPromise(pr, partsData.loadErr)
		return
	}

	parts.partsMu.Lock()
	defer parts.partsMu.Unlock()
	if parts.partitioner == nil {
		parts.partitioner = cl.cfg.partitioner.forTopic(pr.Topic)
	}

	mapping := partsData.writable
	possibilities := partsData.writablePartitions
	if parts.partitioner.requiresConsistency(pr.Record) {
		mapping = partsData.all
		possibilities = partsData.partitions
	}
	if len(possibilities) == 0 {
		cl.finishRecordPromise(pr, ErrNoPartitionsAvailable)
		return
	}

	idIdx := parts.partitioner.partition(pr.Record, len(possibilities))
	id := possibilities[idIdx]
	partition := mapping[id]

	appended := partition.records.bufferRecord(pr, true) // KIP-480
	if !appended {
		parts.partitioner.onNewBatch()
		idIdx = parts.partitioner.partition(pr.Record, len(possibilities))
		id = possibilities[idIdx]
		partition = mapping[id]
		partition.records.bufferRecord(pr, false) // KIP-480
	}
}

type producerID struct {
	id    int64
	epoch int16
	err   error
}

var errReloadProducerID = errors.New("producer id needs reloading")

// initProducerID initalizes the client's producer ID for idempotent
// producing only (no transactions, which are more special). After the first
// load, this clears all buffered unknown topics.
func (cl *Client) producerID() (int64, int16, error) {
	id := cl.producer.id.Load().(*producerID)
	if id.err == errReloadProducerID {
		cl.producer.idMu.Lock()
		defer cl.producer.idMu.Unlock()

		if id = cl.producer.id.Load().(*producerID); id.err == errReloadProducerID {
			id = cl.doInitProducerID(id.id, id.epoch)
			cl.producer.id.Store(id)
		}
	}

	return id.id, id.epoch, id.err
}

func (cl *Client) failProducerID(id int64, epoch int16, err error) {
	cl.producer.idMu.Lock()
	defer cl.producer.idMu.Unlock()

	current := cl.producer.id.Load().(*producerID)
	if current.id != id || current.epoch != epoch {
		return // failed an old id
	}

	// If this is not UnknownProducerID, then we cannot recover production.
	//
	// If this is UnknownProducerID without a txnID, then we are here from
	// stopOnDataLoss in sink.go (see large comment there).
	if err != kerr.UnknownProducerID || cl.cfg.txnID == nil {
		cl.producer.id.Store(&producerID{
			id:    id,
			epoch: epoch,
			err:   err,
		})
		return // fatal error
	}

	// need to not produce until everything failed?

	// UNKNOWN_PRODUCER_ID
	// if txn id,
	// store abortable error,

	// which can be cleared in EndTransaction (store "need reload" error)
	// when cleared, reset sequence numbers if abortable error

	return
}

// doInitProducerID inits the idempotent ID and potentially the transactional
// producer epoch.
func (cl *Client) doInitProducerID(lastID int64, lastEpoch int16) *producerID {
	req := &kmsg.InitProducerIDRequest{
		TransactionalID: cl.cfg.txnID,
		ProducerID:      lastID,
		ProducerEpoch:   lastEpoch,
	}
	if cl.cfg.txnID != nil {
		req.TransactionTimeoutMillis = int32(cl.cfg.txnTimeout.Milliseconds())
	}

	kresp, err := cl.Request(cl.ctx, req)
	if err != nil {
		// If our broker is too old, then well...
		//
		// Note this is dependent on the first broker we hit;
		// there are other areas in this client where we assume
		// what we hit first is the default.
		if err == ErrUnknownRequestKey {
			return &producerID{-1, -1, nil}
		}
		return &producerID{-1, -1, err}
	}
	resp := kresp.(*kmsg.InitProducerIDResponse)
	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return &producerID{-1, -1, err}
	}

	// We track if this was v3. We do not need to gate this behind a mutex,
	// since no request is issued before the ID is loaded, meaning nothing
	// checks this value in a racy way.
	if cl.producer.idVersion == -1 {
		cl.producer.idVersion = req.Version
	}

	return &producerID{resp.ProducerID, resp.ProducerEpoch, nil}
}

// partitionsForTopicProduce returns the topic partitions for a record.
// If the topic is not loaded yet, this buffers the record.
func (cl *Client) partitionsForTopicProduce(pr promisedRec) (*topicPartitions, *topicPartitionsData) {
	topic := pr.Topic

	// 1) if the topic exists and there are partitions, then we can simply
	// return the parts.
	topics := cl.loadTopics()
	parts, exists := topics[topic]
	if exists {
		v := parts.load()
		if len(v.partitions) > 0 {
			return parts, v
		}
	}

	if !exists {
		// 2) if the topic does not exist, we check again under the
		// topics mu.
		cl.topicsMu.Lock()
		topics = cl.loadTopics()
		if _, exists = topics[topic]; !exists {
			// 2a) the topic definitely does not exist; we create it.
			//
			// Before we release the topic mu, we lock unknownTopics
			// and add our record to ensure ordering.
			parts = newTopicPartitions(topic)
			newTopics := cl.cloneTopics()
			newTopics[topic] = parts
			cl.topics.Store(newTopics)

			cl.unknownTopicsMu.Lock()
			cl.topicsMu.Unlock()
			cl.addUnknownTopicRecord(pr)
			cl.unknownTopicsMu.Unlock()

		} else {
			// 2b) the topic exists now; exists is true and we fall
			// into the logic below.
			cl.topicsMu.Unlock()
		}
	}

	if exists {
		// 3) if the topic does exist, either partitions were loaded,
		// meaning we can just return the load since we are guaranteed
		// (single goroutine) sequential now, or they were not loaded
		// and we must add our record in order under unknownTopicsMu.
		cl.unknownTopicsMu.Lock()
		topics = cl.loadTopics()
		parts = topics[topic]
		v := parts.load()
		if len(v.partitions) > 0 {
			cl.unknownTopicsMu.Unlock()
			return parts, v
		}
		cl.addUnknownTopicRecord(pr)
		cl.unknownTopicsMu.Unlock()
	}

	cl.triggerUpdateMetadataNow()

	// Our record is buffered waiting for a metadata update to discover
	// the topic. We return nil here.
	return nil, nil
}

func (cl *Client) addUnknownTopicRecord(pr promisedRec) {
	existing := cl.unknownTopics[pr.Topic]
	existing = append(existing, pr)
	cl.unknownTopics[pr.Topic] = existing
	if len(existing) != 1 {
		return
	}

	wait := make(chan struct{}, 1)
	cl.unknownTopicsWait[pr.Topic] = wait
	go cl.waitUnknownTopic(pr.Topic, wait)
}

func (cl *Client) waitUnknownTopic(topic string, wait chan struct{}) {
	var after <-chan time.Time
	if timeout := cl.cfg.recordTimeout; timeout > 0 {
		timer := time.NewTimer(cl.cfg.recordTimeout)
		defer timer.Stop()
		after = timer.C
	}
	var tries int
	var err error
	for err == nil {
		select {
		case <-cl.ctx.Done():
			err = ErrBrokerDead
		case <-after:
			err = ErrRecordTimeout
		case _, ok := <-wait:
			if !ok {
				return // metadata was successful!
			}
			tries++
			if tries >= cl.cfg.retries {
				err = ErrNoPartitionsAvailable
			}
		}
	}

	// We only get down here if we errored above. Clear everything waiting
	// and call the promises with our error.
	cl.unknownTopicsMu.Lock()
	prs, ok := cl.unknownTopics[topic]
	delete(cl.unknownTopics, topic)
	delete(cl.unknownTopicsWait, topic)
	cl.unknownTopicsMu.Unlock()

	// We could have raced with a metadata update successfully clearing the
	// partitions.
	if ok {
		for _, pr := range prs {
			cl.finishRecordPromise(pr, err)
		}
	}
}

// Flush hangs waiting for all buffered records to be flushed, stopping all
// lingers if necessary.
//
// If the context finishes (Done), this returns the context's error.
func (cl *Client) Flush(ctx context.Context) error {
	// Signal to finishRecord that we want to be notified once buffered hits 0.
	// Also forbid any new producing to start a linger.
	atomic.AddInt32(&cl.producer.flushing, 1)
	defer atomic.AddInt32(&cl.producer.flushing, -1)

	// At this point, if lingering is configured, nothing will _start_ a
	// linger because the producer's flushing atomic int32 is nonzero.  We
	// must wake anything that could be lingering up, after which all sinks
	// will loop draining.
	if cl.cfg.linger > 0 {
		for _, parts := range cl.loadTopics() {
			for _, part := range parts.load().all {
				part.records.unlinger()
			}
		}
	}

	quit := false
	done := make(chan struct{})
	go func() {
		cl.producer.flushingMu.Lock()
		defer cl.producer.flushingMu.Unlock()
		defer close(done)

		for !quit && atomic.LoadInt64(&cl.producer.bufferedRecords) > 0 {
			cl.producer.flushingCond.Wait()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		quit = true
		cl.producer.flushingCond.Broadcast()
		return ctx.Err()
	}
}
