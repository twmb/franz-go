package kgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/krec"
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
		broker.recordSink.mu.Lock()
		broker.recordSink.aborting = true
		broker.recordSink.mu.Unlock()
		broker.recordSink.maybeBeginDraining() // awaken anything in backoff
		unaborting = append(unaborting, broker)
	}
	cl.brokersMu.Unlock()

	defer func() {
		for _, broker := range unaborting {
			broker.recordSink.mu.Lock()
			broker.recordSink.aborting = false
			broker.recordSink.mu.Unlock()
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

	kresp, err := cl.Request(ctx, &kmsg.EndTxnRequest{
		TransactionalID: *cl.cfg.txnID,
		ProducerID:      cl.producer.id,
		ProducerEpoch:   cl.producer.epoch,
		Commit:          commit,
	})
	if err != nil {
		return err
	}
	return kerr.ErrorForCode(kresp.(*kmsg.EndTxnResponse).ErrorCode)
}

type producer struct {
	bufferedRecords int64

	id           int64
	epoch        int16
	idLoaded     uint32 // 1 if loaded
	producingTxn uint32 // 1 if in txn
	flushing     int32  // >0 if flushing, can Flush many times concurrently

	idMu        sync.Mutex
	idLoadingCh chan struct{} // exists if id is loading
	waitBuffer  chan struct{}

	flushingMu   sync.Mutex
	flushingCond *sync.Cond

	txnMu sync.Mutex
	inTxn bool
}

func noPromise(*krec.Rec, error) {}

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
	r *krec.Rec,
	promise func(*krec.Rec, error),
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
	pr := promisedRecord{promise, r}

	if atomic.LoadUint32(&cl.producer.idLoaded) == 0 {
		var buffered bool
		cl.producer.idMu.Lock()
		if atomic.LoadUint32(&cl.producer.idLoaded) == 0 {
			// unknownTopics is guarded under either the
			// unknownTopicsMu or producer idMu. Since
			// we must have an ID before we move to the
			// loading topics stage, we will always have
			// producer idMu and then unknownTopicsMu
			// non overlapping.
			cl.addUnknownTopicRecord(pr)
			buffered = true

			if cl.producer.idLoadingCh == nil {
				cl.producer.idLoadingCh = make(chan struct{})
				go cl.initProducerID()
			}
		}
		cl.producer.idMu.Unlock()
		if buffered {
			return nil
		}
	}
	cl.partitionRecord(pr)
	return nil
}

func (cl *Client) finishRecordPromise(pr promisedRecord, err error) {
	buffered := atomic.AddInt64(&cl.producer.bufferedRecords, -1)
	if buffered >= cl.cfg.maxBufferedRecords {
		go func() { cl.producer.waitBuffer <- struct{}{} }()
	} else if buffered == 0 && atomic.LoadInt32(&cl.producer.flushing) > 0 {
		cl.producer.flushingMu.Lock()
		cl.producer.flushingMu.Unlock()
		cl.producer.flushingCond.Broadcast()
	}
	pr.promise(pr.Rec, err)
}

// partitionRecord loads the partitions for a topic and produce to them. If
// the topic does not currently exist, the record is buffered in unknownTopics
// for a metadata update to deal with.
func (cl *Client) partitionRecord(pr promisedRecord) {
	parts, partsData := cl.partitionsForTopicProduce(pr)
	if parts == nil {
		return
	}
	cl.doPartitionRecord(parts, partsData, pr)
}

// doPartitionRecord is the logic behind record partitioning and producing if
// the client knows of the topic's partitions.
func (cl *Client) doPartitionRecord(parts *topicPartitions, partsData *topicPartitionsData, pr promisedRecord) {
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
	if parts.partitioner.requiresConsistency(pr.Rec) {
		mapping = partsData.all
		possibilities = partsData.partitions
	}
	if len(possibilities) == 0 {
		cl.finishRecordPromise(pr, ErrNoPartitionsAvailable)
		return
	}

	idIdx := parts.partitioner.partition(pr.Rec, len(possibilities))
	id := possibilities[idIdx]
	partition := mapping[id]

	appended := partition.records.bufferRecord(pr, true) // KIP-480
	if !appended {
		parts.partitioner.onNewBatch()
		idIdx = parts.partitioner.partition(pr.Rec, len(possibilities))
		id = possibilities[idIdx]
		partition = mapping[id]
		partition.records.bufferRecord(pr, false) // KIP-480
	}
}

// initProducerID initalizes the client's producer ID for idempotent
// producing only (no transactions, which are more special). After the first
// load, this clears all buffered unknown topics.
func (cl *Client) initProducerID() {
	err := cl.doInitProducerID()

	// Grab our lock. We need to block producing until this function
	// returns to ensure order.
	cl.producer.idMu.Lock()
	defer cl.producer.idMu.Unlock()

	// close idLoadingCh before setting idLoaded to 1 to ensure anything
	// waiting sees the loaded.
	defer close(cl.producer.idLoadingCh)
	cl.producer.idLoadingCh = nil

	// If we were successful, we have to store that the ID is loaded before
	// we release the mutex. Otherwise, something may grab the mu and still
	// see the id is not loaded just before we store it is, and then it
	// will forever sit buffered waiting for a load that will not happen.
	//
	// We cannot guard against two concurrent produces to the same topic,
	// but that is fine.
	if err == nil {
		defer atomic.StoreUint32(&cl.producer.idLoaded, 1)
	}

	unknown := cl.unknownTopics
	unknownWait := cl.unknownTopicsWait
	cl.unknownTopics = make(map[string][]promisedRecord)
	cl.unknownTopicsWait = make(map[string]chan struct{})

	if err != nil {
		for i, prs := range unknown {
			close(unknownWait[i])
			for _, pr := range prs {
				cl.finishRecordPromise(pr, err)
			}
		}
		return
	}
	for i, prs := range unknown {
		close(unknownWait[i])
		for _, pr := range prs {
			cl.partitionRecord(pr)
		}
	}
}

// doInitProducerID inits the idempotent ID and potentially the transactional
// producer epoch.
func (cl *Client) doInitProducerID() error {
	req := new(kmsg.InitProducerIDRequest)
	if cl.cfg.txnID != nil {
		req.TransactionalID = cl.cfg.txnID
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
			return nil
		}
		return err
	}
	resp := kresp.(*kmsg.InitProducerIDResponse)
	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return err
	}
	cl.producer.id = resp.ProducerID
	cl.producer.epoch = resp.ProducerEpoch
	return nil
}

// partitionsForTopicProduce returns the topic partitions for a record.
// If the topic is not loaded yet, this buffers the record.
func (cl *Client) partitionsForTopicProduce(pr promisedRecord) (*topicPartitions, *topicPartitionsData) {
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

func (cl *Client) addUnknownTopicRecord(pr promisedRecord) {
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
	// Signal to finishRecord that we want to be notified once thins hit 0.
	atomic.AddInt32(&cl.producer.flushing, 1)
	defer atomic.AddInt32(&cl.producer.flushing, -1)

	// At this point, if lingering,
	// nothing new will fall into a timer waiting,
	// so we can just wake everything up through the lock,
	// and be sure that all sinks will loop draining.
	if cl.cfg.linger > 0 {
		for _, parts := range cl.loadTopics() {
			for _, part := range parts.load().all {
				part.records.mu.Lock()
				part.records.lockedStopLinger()
				part.records.sink.maybeBeginDraining()
				part.records.mu.Unlock()
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
