package kgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type producer struct {
	topicsMu sync.Mutex // locked to prevent concurrent updates; reads are always atomic
	topics   *topicsPartitions

	// unknownTopics buffers all records for topics that are not loaded.
	// The map is to a pointer to a slice for reasons documented in
	// waitUnknownTopic.
	unknownTopicsMu sync.Mutex
	unknownTopics   map[string]*unknownTopicProduces

	bufferedRecords int64

	id           atomic.Value
	producingTxn uint32 // 1 if in txn

	// We must have a producer field for flushing; we cannot just have a
	// field on recBufs that is toggled on flush. If we did, then a new
	// recBuf could be created and records sent to while we are flushing.
	flushing int32 // >0 if flushing, can Flush many times concurrently

	aborting uint32 // 1 means yes
	workers  int32  // number of sinks draining / number of in flight produce requests

	idMu       sync.Mutex
	idVersion  int16
	waitBuffer chan struct{}

	// notifyMu and notifyCond are used for flush and drain notifications.
	notifyMu   sync.Mutex
	notifyCond *sync.Cond

	txnMu sync.Mutex
	inTxn bool
}

type unknownTopicProduces struct {
	buffered []promisedRec
	wait     chan error
}

func (p *producer) init() {
	p.topics = newTopicsPartitions()
	p.unknownTopics = make(map[string]*unknownTopicProduces)
	p.waitBuffer = make(chan struct{}, 100)
	p.idVersion = -1
	p.id.Store(&producerID{
		id:    -1,
		epoch: -1,
		err:   errReloadProducerID,
	})
	p.notifyCond = sync.NewCond(&p.notifyMu)
}

func (p *producer) incWorkers() { atomic.AddInt32(&p.workers, 1) }
func (p *producer) decWorkers() { p.decAbortNotify(&p.workers) }

func (p *producer) decAbortNotify(v *int32) {
	if atomic.AddInt32(v, -1) != 0 || atomic.LoadUint32(&p.aborting) == 0 {
		return
	}
	p.notifyMu.Lock()
	p.notifyMu.Unlock()
	p.notifyCond.Broadcast()
}

func (p *producer) isAborting() bool { return atomic.LoadUint32(&p.aborting) == 1 }

func noPromise(*Record, error) {}

// Produce sends a Kafka record to the topic in the record's Topic field,
// calling promise with the record or an error when Kafka replies.
//
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly.
//
// If the record is too large to fit in a batch on its own in a produce
// request, the promise is called immediately before this function returns with
// kerr.MessageToLarge.
//
// The context is used if the client currently has the max amount of buffered
// records. If so, the client waits for some records to complete or for the
// context or client to quit. If the context / client quits, this returns an
// error.
//
// The first buffered record for an unknown topic begins a timeout for the
// configured record timeout limit; all records buffered within the wait will
// expire with the same timeout if the topic does not load in time. For
// simplicity, any time spent waiting for the topic to load is not persisted
// through once the topic loads, meaning the record may further wait once
// buffered. This may be changed in the future if necessary, however, the only
// reason for a topic to not load promptly is if it does not exist.
//
// If manually flushing and there are already MaxBufferedRecords buffered, this
// will return ErrMaxBuffered.
//
// If the client is transactional and a transaction has not been begun, this
// returns an error corresponding to not being in a transaction.
//
// Thus, there are only three possible errors: the non-transaction error, and
// then either a context error or ErrMaxBuffered.
func (cl *Client) Produce(
	ctx context.Context,
	r *Record,
	promise func(*Record, error),
) error {
	p := &cl.producer

	if cl.cfg.txnID != nil && atomic.LoadUint32(&p.producingTxn) != 1 {
		return errNotInTransaction
	}

	if atomic.AddInt64(&p.bufferedRecords, 1) > cl.cfg.maxBufferedRecords {
		// If the client ctx cancels or the produce ctx cancels, we
		// need to un-count our buffering of this record. As well, to
		// be safe, we need to drain a slot from the waitBuffer chan,
		// which will be sent to. Thus, for easiness, if either ctx is
		// canceled, we just finish a fake promise with no record
		// (i.e., pretending we finished this record) and drain the
		// waitBuffer as normal.
		drainBuffered := func() {
			go func() { <-p.waitBuffer }()
			cl.finishRecordPromise(promisedRec{noPromise, nil}, nil)
		}
		if cl.cfg.manualFlushing {
			drainBuffered()
			return ErrMaxBuffered
		}
		select {
		case <-p.waitBuffer:
		case <-cl.ctx.Done():
			drainBuffered()
			return cl.ctx.Err()
		case <-ctx.Done():
			drainBuffered()
			return ctx.Err()
		}
	}

	if promise == nil {
		promise = noPromise
	}
	cl.partitionRecord(promisedRec{promise, r})
	return nil
}

func (cl *Client) finishRecordPromise(pr promisedRec, err error) {
	p := &cl.producer

	// We call the promise before finishing the record; this allows users
	// of Flush to know that all buffered records are completely done
	// before Flush returns.
	pr.promise(pr.Record, err)

	buffered := atomic.AddInt64(&p.bufferedRecords, -1)
	if buffered >= cl.cfg.maxBufferedRecords {
		go func() { p.waitBuffer <- struct{}{} }()
	} else if buffered == 0 && atomic.LoadInt32(&p.flushing) > 0 {
		p.notifyMu.Lock()
		p.notifyMu.Unlock()
		p.notifyCond.Broadcast()
	}
}

// partitionRecord loads the partitions for a topic and produce to them. If
// the topic does not currently exist, the record is buffered in unknownTopics
// for a metadata update to deal with.
func (cl *Client) partitionRecord(pr promisedRec) {
	parts, partsData := cl.partitionsForTopicProduce(pr)
	if parts == nil { // saved in unknownTopics
		return
	}
	cl.doPartitionRecord(parts, partsData, pr)
}

// doPartitionRecord is separate so that metadata updates that load unknown
// partitions can call this directly.
func (cl *Client) doPartitionRecord(parts *topicPartitions, partsData *topicPartitionsData, pr promisedRec) {
	if partsData.loadErr != nil && !kerr.IsRetriable(partsData.loadErr) {
		cl.finishRecordPromise(pr, partsData.loadErr)
		return
	}

	parts.partsMu.Lock()
	defer parts.partsMu.Unlock()
	if parts.partitioner == nil {
		parts.partitioner = cl.cfg.partitioner.ForTopic(pr.Topic)
	}

	mapping := partsData.writablePartitions
	if parts.partitioner.RequiresConsistency(pr.Record) {
		mapping = partsData.partitions
	}
	if len(mapping) == 0 {
		cl.finishRecordPromise(pr, errors.New("unable to partition record due to no usable partitions"))
		return
	}

	pick := parts.partitioner.Partition(pr.Record, len(mapping))
	if pick < 0 || pick >= len(mapping) {
		cl.finishRecordPromise(pr, fmt.Errorf("invalid record partitioning choice of %d from %d available", pick, len(mapping)))
		return
	}

	partition := mapping[pick]

	processed := partition.records.bufferRecord(pr, true) // KIP-480
	if !processed {
		parts.partitioner.OnNewBatch()
		pick = parts.partitioner.Partition(pr.Record, len(mapping))
		if pick < 0 || pick >= len(mapping) {
			cl.finishRecordPromise(pr, fmt.Errorf("invalid record partitioning choice of %d from %d available", pick, len(mapping)))
			return
		}
		partition = mapping[pick]
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
	p := &cl.producer

	id := p.id.Load().(*producerID)
	if id.err == errReloadProducerID {
		p.idMu.Lock()
		defer p.idMu.Unlock()

		if id = p.id.Load().(*producerID); id.err == errReloadProducerID {

			if cl.cfg.disableIdempotency {
				cl.cfg.logger.Log(LogLevelInfo, "skipping producer id initialization because the client was configured to disable idempotent writes")
				id = &producerID{
					id:    -1,
					epoch: -1,
					err:   nil,
				}
				p.id.Store(id)

				// For the idempotent producer, as specified in KIP-360,
				// if we had an ID, we can bump the epoch locally.
				// If we are at the max epoch, we will ask for a new ID.
			} else if cl.cfg.txnID == nil && id.id >= 0 && id.epoch < math.MaxInt16-1 {
				cl.resetAllProducerSequences()

				id = &producerID{
					id:    id.id,
					epoch: id.epoch + 1,
					err:   nil,
				}
				p.id.Store(id)

			} else {
				newID, keep := cl.doInitProducerID(id.id, id.epoch)
				if keep {
					id = newID
					p.id.Store(id)
				} else {
					// If we are not keeping the producer ID,
					// we will return our old ID but with a
					// static error that we can check or bubble
					// up where needed.
					id = &producerID{
						id:    id.id,
						epoch: id.epoch,
						err:   errProducerIDLoadFail,
					}
				}
			}
		}
	}

	return id.id, id.epoch, id.err
}

// As seen in KAFKA-12152, if we bump an epoch, we have to reset sequence nums
// for every partition. Otherwise, we will use a new id/epoch for a partition
// and trigger OOOSN errors.
//
// Pre 2.5.0, this function is only be called if it is acceptable to continue
// on data loss (idempotent producer with no StopOnDataLoss option).
//
// 2.5.0+, it is safe to call this if the producer ID can be reset (KIP-360),
// in EndTransaction.
func (cl *Client) resetAllProducerSequences() {
	for _, tp := range cl.producer.topics.load() {
		for _, p := range tp.load().partitions {
			p.records.mu.Lock()
			p.records.needSeqReset = true
			p.records.mu.Unlock()
		}
	}
}

func (cl *Client) failProducerID(id int64, epoch int16, err error) {
	p := &cl.producer

	p.idMu.Lock()
	defer p.idMu.Unlock()

	current := p.id.Load().(*producerID)
	if current.id != id || current.epoch != epoch {
		cl.cfg.logger.Log(LogLevelInfo, "ignoring a fail producer id request due to current id being different",
			"current_id", current.id,
			"current_epoch", current.epoch,
			"current_err", current.err,
			"fail_id", id,
			"fail_epoch", epoch,
			"fail_err", err,
		)
		return // failed an old id
	}

	// If this is not UnknownProducerID, then we cannot recover production.
	//
	// If this is UnknownProducerID without a txnID, then we are here from
	// stopOnDataLoss in sink.go (see large comment there).
	//
	// If this is UnknownProducerID with a txnID, then EndTransaction will
	// recover us.
	p.id.Store(&producerID{
		id:    id,
		epoch: epoch,
		err:   err,
	})
}

// doInitProducerID inits the idempotent ID and potentially the transactional
// producer epoch, returning whether to keep the result.
func (cl *Client) doInitProducerID(lastID int64, lastEpoch int16) (*producerID, bool) {
	cl.cfg.logger.Log(LogLevelInfo, "initializing producer id")
	req := &kmsg.InitProducerIDRequest{
		TransactionalID: cl.cfg.txnID,
		ProducerID:      lastID,
		ProducerEpoch:   lastEpoch,
	}
	if cl.cfg.txnID != nil {
		req.TransactionTimeoutMillis = int32(cl.cfg.txnTimeout.Milliseconds())
	}

	resp, err := req.RequestWith(cl.ctx, cl)
	if err != nil {
		if err == errUnknownRequestKey || err == errBrokerTooOld {
			cl.cfg.logger.Log(LogLevelInfo, "unable to initialize a producer id because the broker is too old or the client is pinned to an old version, continuing without a producer id")
			return &producerID{-1, -1, nil}, true
		}
		if err == errChosenBrokerDead {
			select {
			case <-cl.ctx.Done():
				cl.cfg.logger.Log(LogLevelInfo, "producer id initialization failure due to dying client", "err", err)
				return &producerID{lastID, lastEpoch, errClientClosing}, true
			default:
			}
		}
		cl.cfg.logger.Log(LogLevelInfo, "producer id initialization failure, discarding initialization attempt", "err", err)
		return &producerID{lastID, lastEpoch, err}, false
	}

	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		if kerr.IsRetriable(err) { // TODO handle ConcurrentTransactions collision?
			cl.cfg.logger.Log(LogLevelInfo, "producer id initialization resulted in retriable error, discarding initialization attempt", "err", err)
			return &producerID{lastID, lastEpoch, err}, false
		}
		cl.cfg.logger.Log(LogLevelInfo, "producer id initialization errored", "err", err)
		return &producerID{lastID, lastEpoch, err}, true
	}

	cl.cfg.logger.Log(LogLevelInfo, "producer id initialization success", "id", resp.ProducerID, "epoch", resp.ProducerEpoch)

	// We track if this was v3. We do not need to gate this behind a mutex,
	// because the only other use is EndTransaction's read, which is
	// documented to only be called sequentially after producing.
	if cl.producer.idVersion == -1 {
		cl.producer.idVersion = req.Version
	}

	return &producerID{resp.ProducerID, resp.ProducerEpoch, nil}, true
}

// partitionsForTopicProduce returns the topic partitions for a record.
// If the topic is not loaded yet, this buffers the record and returns
// nil, nil.
func (cl *Client) partitionsForTopicProduce(pr promisedRec) (*topicPartitions, *topicPartitionsData) {
	p := &cl.producer
	topic := pr.Topic

	topics := p.topics.load()
	parts, exists := topics[topic]
	if exists {
		if v := parts.load(); len(v.partitions) > 0 {
			return parts, v
		}
	}

	if !exists { // topic did not exist: check again under mu and potentially create it
		p.topicsMu.Lock()
		defer p.topicsMu.Unlock()

		if parts, exists = p.topics.load()[topic]; !exists { // update parts for below
			// Before we store the new topic, we lock unknown
			// topics to prevent a concurrent metadata update
			// seeing our new topic before we are waiting from the
			// addUnknownTopicRecord fn. Otherwise, we would wait
			// and never be re-notified.
			p.unknownTopicsMu.Lock()
			defer p.unknownTopicsMu.Unlock()

			p.topics.storeTopics([]string{topic})
			cl.addUnknownTopicRecord(pr)
			cl.triggerUpdateMetadataNow()
			return nil, nil
		}
	}

	// Here, the topic existed, but maybe has not loaded partitions yet. We
	// have to lock unknown topics first to ensure ordering just in case a
	// load has not happened.
	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()

	if v := parts.load(); len(v.partitions) > 0 {
		return parts, v
	}
	cl.addUnknownTopicRecord(pr)
	cl.triggerUpdateMetadata()

	return nil, nil // our record is buffered waiting for metadata update; nothing to return
}

// addUnknownTopicRecord adds a record to a topic whose partitions are
// currently unknown. This is always called with the unknownTopicsMu held.
func (cl *Client) addUnknownTopicRecord(pr promisedRec) {
	unknown := cl.producer.unknownTopics[pr.Topic]
	if unknown == nil {
		unknown = &unknownTopicProduces{
			buffered: make([]promisedRec, 0, 100),
			wait:     make(chan error, 5),
		}
		cl.producer.unknownTopics[pr.Topic] = unknown
	}
	unknown.buffered = append(unknown.buffered, pr)
	if len(unknown.buffered) == 1 {
		go cl.waitUnknownTopic(pr.Topic, unknown)
	}
}

// waitUnknownTopic waits for a notification
func (cl *Client) waitUnknownTopic(
	topic string,
	unknown *unknownTopicProduces,
) {
	cl.cfg.logger.Log(LogLevelInfo, "waiting for metadata to produce to unknown topic", "topic", topic)
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
			err = errClientClosing
		case <-after:
			err = errRecordTimeout
		case retriableErr, ok := <-unknown.wait:
			if !ok {
				cl.cfg.logger.Log(LogLevelInfo, "done waiting for unknown topic", "topic", topic)
				return // metadata was successful!
			}
			cl.cfg.logger.Log(LogLevelInfo, "unknown topic wait failed, retrying wait", "topic", topic, "err", retriableErr)
			tries++
			if int64(tries) >= cl.cfg.retries {
				err = fmt.Errorf("no partitions available after refreshing metadata %d times, last err: %w", tries, retriableErr)
			}
		}
	}

	// If we errored above, we come down here to potentially clear the
	// topic wait and fail all buffered records. However, under some
	// extreme conditions, a quickly following metadata update could delete
	// our unknown topic, and then a produce could recreate a new unknown
	// topic. We only delete and finish promises if the pointer in the
	// unknown topic map is still the same.
	p := &cl.producer

	p.topicsMu.Lock()
	defer p.topicsMu.Unlock()
	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()

	nowUnknown := p.unknownTopics[topic]
	if nowUnknown != unknown {
		return
	}
	cl.cfg.logger.Log(LogLevelInfo, "unknown topic wait failed, done retrying, failing all records", "topic", topic)

	delete(p.unknownTopics, topic)
	cl.deleteUnknownTopic(topic, unknown, err)
}

// Called under both topic and unknown mu's, this clears the topic from the
// producer and finishes all buffered promises.
//
// We do not need to clear recBufs in sinks when deleting unknown topics
// because unknown topics implies that partitions were never loaded, thus no
// recBufs in sinks.
func (cl *Client) deleteUnknownTopic(topic string, unknown *unknownTopicProduces, err error) {
	topics := cl.producer.topics.clone()
	delete(topics, topic)
	cl.producer.topics.storeData(topics)

	go func() {
		for _, pr := range unknown.buffered {
			cl.finishRecordPromise(pr, err)
		}
	}()
}

// Flush hangs waiting for all buffered records to be flushed, stopping all
// lingers if necessary.
//
// If the context finishes (Done), this returns the context's error.
func (cl *Client) Flush(ctx context.Context) error {
	p := &cl.producer

	// Signal to finishRecord that we want to be notified once buffered hits 0.
	// Also forbid any new producing to start a linger.
	atomic.AddInt32(&p.flushing, 1)
	defer atomic.AddInt32(&p.flushing, -1)

	cl.cfg.logger.Log(LogLevelInfo, "flushing")
	defer cl.cfg.logger.Log(LogLevelDebug, "flushed")

	// At this point, if lingering is configured, nothing will _start_ a
	// linger because the producer's flushing atomic int32 is nonzero. We
	// must wake anything that could be lingering up, after which all sinks
	// will loop draining.
	if cl.cfg.linger > 0 || cl.cfg.manualFlushing {
		for _, parts := range p.topics.load() {
			for _, part := range parts.load().partitions {
				part.records.unlingerAndManuallyDrain()
			}
		}
	}

	quit := false
	done := make(chan struct{})
	go func() {
		p.notifyMu.Lock()
		defer p.notifyMu.Unlock()
		defer close(done)

		for !quit && atomic.LoadInt64(&p.bufferedRecords) > 0 {
			p.notifyCond.Wait()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		p.notifyMu.Lock()
		quit = true
		p.notifyMu.Unlock()
		p.notifyCond.Broadcast()
		return ctx.Err()
	}
}

// Bumps the tries for all buffered records in the client.
//
// This is called whenever there is a problematic error that would affect the
// state of all buffered records as a whole:
//
//   - if we cannot init a producer ID due to RequestWith errors, producing is useless
//   - if we cannot add partitions to a txn due to RequestWith errors, producing is useless
//
// Note that these are specifically due to RequestWith errors, not due to
// receiving a response that has a retriable error code. That is, if our
// request keeps dying.
func (cl *Client) bumpRepeatedLoadErr(err error) {
	p := &cl.producer

	for _, partitions := range p.topics.load() {
		for _, partition := range partitions.load().partitions {
			partition.records.bumpRepeatedLoadErr(err)
		}
	}
	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()
	for _, unknown := range p.unknownTopics {
		select {
		case unknown.wait <- err:
		default:
		}
	}
}

// Clears all buffered records in the client with the given error.
//
// - closing client
// - aborting transaction
// - fatal AddPartitionsToTxn
//
// Because the error fails everything, we also empty our unknown topics and
// delete any topics that were still unknown from the producer's topics.
func (cl *Client) failBufferedRecords(err error) {
	p := &cl.producer

	for _, partitions := range p.topics.load() {
		for _, partition := range partitions.load().partitions {
			recBuf := partition.records
			recBuf.mu.Lock()
			recBuf.failAllRecords(err)
			recBuf.mu.Unlock()
		}
	}

	p.topicsMu.Lock()
	defer p.topicsMu.Unlock()
	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()

	toStore := p.topics.clone()
	defer p.topics.storeData(toStore)

	var toFail [][]promisedRec
	for topic, unknown := range p.unknownTopics {
		delete(toStore, topic)
		delete(p.unknownTopics, topic)
		close(unknown.wait)
		toFail = append(toFail, unknown.buffered)
	}

	go func() {
		for _, fail := range toFail {
			for _, pr := range fail {
				cl.finishRecordPromise(pr, err)
			}
		}
	}()
}
