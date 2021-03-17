package kgo

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type producer struct {
	bufferedRecords int64

	id           atomic.Value
	producingTxn uint32 // 1 if in txn
	flushing     int32  // >0 if flushing, can Flush many times concurrently
	aborting     uint32 // 1 means yes
	drains       int32  // number of sinks draining

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
	p.waitBuffer = make(chan struct{}, 100)
	p.idVersion = -1
	p.id.Store(&producerID{
		id:    -1,
		epoch: -1,
		err:   errReloadProducerID,
	})
	p.notifyCond = sync.NewCond(&p.notifyMu)
}

func (p *producer) incDrains() {
	atomic.AddInt32(&p.drains, 1)
}

func (p *producer) decDrains() {
	if atomic.AddInt32(&p.drains, -1) != 0 || atomic.LoadUint32(&p.aborting) == 0 {
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
// request, the promise is called immediately before this function returns
// with kerr.MessageToLarge.
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
// returns ErrNotInTransaction.
//
// Thus, there are only three possible errors: ErrNotInTransaction, and then
// either a context error or ErrMaxBuffered.
func (cl *Client) Produce(
	ctx context.Context,
	r *Record,
	promise func(*Record, error),
) error {
	if cl.cfg.txnID != nil && atomic.LoadUint32(&cl.producer.producingTxn) != 1 {
		return ErrNotInTransaction
	}

	if atomic.AddInt64(&cl.producer.bufferedRecords, 1) > cl.cfg.maxBufferedRecords {
		// If the client ctx cancels or the produce ctx cancels, we
		// need to un-count our buffering of this record. As well, to
		// be safe, we need to drain a slot from the waitBuffer chan,
		// which will be sent to. Thus, for easiness, if either ctx is
		// canceled, we just finish a fake promise with no record
		// (i.e., pretending we finished this record) and drain the
		// waitBuffer as normal.
		drainBuffered := func() {
			go func() { <-cl.producer.waitBuffer }()
			cl.finishRecordPromise(promisedRec{noPromise, nil}, nil)
		}
		if cl.cfg.manualFlushing {
			drainBuffered()
			return ErrMaxBuffered
		}
		select {
		case <-cl.producer.waitBuffer:
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
	// We call the promise before finishing the record; this allows users
	// of Flush to know that all buffered records are completely done
	// before Flush returns.
	pr.promise(pr.Record, err)

	buffered := atomic.AddInt64(&cl.producer.bufferedRecords, -1)
	if buffered >= cl.cfg.maxBufferedRecords {
		go func() { cl.producer.waitBuffer <- struct{}{} }()
	} else if buffered == 0 && atomic.LoadInt32(&cl.producer.flushing) > 0 {
		cl.producer.notifyMu.Lock()
		cl.producer.notifyMu.Unlock()
		cl.producer.notifyCond.Broadcast()
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
		cl.finishRecordPromise(pr, ErrNoPartitionsAvailable)
		return
	}

	pick := parts.partitioner.Partition(pr.Record, len(mapping))
	if pick < 0 || pick >= len(mapping) {
		cl.finishRecordPromise(pr, ErrInvalidPartition)
		return
	}

	partition := mapping[pick]

	processed := partition.records.bufferRecord(pr, true) // KIP-480
	if !processed {
		parts.partitioner.OnNewBatch()
		pick = parts.partitioner.Partition(pr.Record, len(mapping))
		if pick < 0 || pick >= len(mapping) {
			cl.finishRecordPromise(pr, ErrInvalidPartition)
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
	id := cl.producer.id.Load().(*producerID)
	if id.err == errReloadProducerID {
		cl.producer.idMu.Lock()
		defer cl.producer.idMu.Unlock()

		if id = cl.producer.id.Load().(*producerID); id.err == errReloadProducerID {

			if cl.cfg.disableIdempotency {
				cl.cfg.logger.Log(LogLevelInfo, "skipping producer id initialization because the client was configured to disable idempotent writes")
				id = &producerID{
					id:    -1,
					epoch: -1,
					err:   nil,
				}
				cl.producer.id.Store(id)

				// For the idempotent producer, as specified in KIP-360,
				// if we had an ID, we can bump the epoch locally.
				// If we are at the max epoch, we will ask for a new ID.
			} else if cl.cfg.txnID == nil && id.id >= 0 && id.epoch < math.MaxInt16-1 {
				// As seen in KAFKA-12152, if we are simply bumping the
				// epoch for the idempotent producer, we actually need to
				// reset the sequence number for **all** partitions.
				// Otherwise, we will use a new epoch and a partition
				// that did not reset will have OOOSN.
				for _, tp := range cl.loadTopics() {
					for _, tpd := range tp.load().partitions {
						tpd.records.resetSeq()
					}
				}
				cl.producer.id.Store(&producerID{
					id:    id.id,
					epoch: id.epoch + 1,
					err:   nil,
				})

			} else {
				newID, keep := cl.doInitProducerID(id.id, id.epoch)
				if keep {
					id = newID
					cl.producer.id.Store(id)
				}
			}
		}
	}

	return id.id, id.epoch, id.err
}

func (cl *Client) failProducerID(id int64, epoch int16, err error) {
	cl.producer.idMu.Lock()
	defer cl.producer.idMu.Unlock()

	current := cl.producer.id.Load().(*producerID)
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
	cl.producer.id.Store(&producerID{
		id:    id,
		epoch: epoch,
		err:   err,
	})
}

// doInitProducerID inits the idempotent ID and potentially the transactional
// producer epoch.
//
// This returns false only if our request failed and not due to the key being
// unknown to the broker.
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
		// If our broker is too old, or our client is pinned to an old
		// version, then well...
		//
		// Note this is dependent on the first broker we hit; there are
		// other areas in this client where we assume what we hit first
		// is the default.
		if err == ErrUnknownRequestKey || err == ErrBrokerTooOld {
			cl.cfg.logger.Log(LogLevelInfo, "unable to initialize a producer id because the broker is too old, continuing without a producer id")
			return &producerID{-1, -1, nil}, true
		}
		cl.cfg.logger.Log(LogLevelInfo, "producer id initialization failure, discarding initialization attempt", "err", err)
		return &producerID{lastID, lastEpoch, err}, false
	}
	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		cl.cfg.logger.Log(LogLevelInfo, "producer id initialization errored", "err", err)
		return &producerID{lastID, lastEpoch, err}, true
	}
	cl.cfg.logger.Log(LogLevelInfo, "producer id initialization success", "id", resp.ProducerID, "epoch", resp.ProducerEpoch)

	// We track if this was v3. We do not need to gate this behind a mutex,
	// since no request is issued before the ID is loaded, meaning nothing
	// checks this value in a racy way.
	if cl.producer.idVersion == -1 {
		cl.producer.idVersion = req.Version
	}

	return &producerID{resp.ProducerID, resp.ProducerEpoch, nil}, true
}

// partitionsForTopicProduce returns the topic partitions for a record.
// If the topic is not loaded yet, this buffers the record and returns
// nil, nil.
func (cl *Client) partitionsForTopicProduce(pr promisedRec) (*topicPartitions, *topicPartitionsData) {
	topic := pr.Topic

	// If the topic exists and there are partitions, then we can simply
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
		// If the topic does not exist, we check again under the topics
		// mu.
		cl.topicsMu.Lock()
		topics = cl.loadTopics()
		if _, exists = topics[topic]; !exists {
			// The topic definitely does not exist; we create it.
			//
			// Before we store the new topics and release the topic
			// mu, we lock unknownTopicsMu. We cannot allow a
			// concurrent metadata update to see our new topic and
			// store partitions for it before we are waiting from
			// the addUnknownTopicRecord func. Otherwise, we would
			// fall into the wait and never be re-notified.
			parts = newTopicPartitions()
			newTopics := cl.cloneTopics()
			newTopics[topic] = parts

			cl.unknownTopicsMu.Lock() // lock before store and topicsMu release
			cl.topics.Store(newTopics)
			cl.topicsMu.Unlock()

			cl.addUnknownTopicRecord(pr)
			cl.unknownTopicsMu.Unlock()

		} else {
			// Topic existed: fall into the logic below.
			cl.topicsMu.Unlock()
		}
	}

	if exists {
		// If the topic does exist, either partitions were loaded,
		// meaning we can just return the load since we are guaranteed
		// sequential now (if producing to this topic in a single
		// goroutine), or they were not loaded and we must add our
		// record in order under unknownTopicsMu.
		//
		// See comment in storePartitionsUpdate to the ordering
		// of our lock then load, or the comment above.
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

// addUnknownTopicRecord adds a record to a topic whose partitions are
// currently unknown. This is always called with the unknownTopicsMu held.
func (cl *Client) addUnknownTopicRecord(pr promisedRec) {
	unknown := cl.unknownTopics[pr.Topic]
	if unknown == nil {
		unknown = &unknownTopicProduces{
			buffered: make([]promisedRec, 0, 100),
			wait:     make(chan error, 1),
		}
		cl.unknownTopics[pr.Topic] = unknown
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
		var ok bool
		select {
		case <-cl.ctx.Done():
			err = ErrBrokerDead
		case <-after:
			err = ErrRecordTimeout
		case err, ok = <-unknown.wait:
			if !ok {
				cl.cfg.logger.Log(LogLevelInfo, "done waiting for unknown topic, metadata was successful", "topic", topic)
				return // metadata was successful!
			}
			cl.cfg.logger.Log(LogLevelInfo, "unknown topic wait failed, retrying wait", "topic", topic, "err", err)
			tries++
			if tries >= cl.cfg.retries {
				err = ErrNoPartitionsAvailable
			}
		}
	}

	cl.cfg.logger.Log(LogLevelInfo, "unknown topic wait failed, done retrying, failing all records", "topic", topic)

	// If we errored above, we come down here to potentially clear the
	// topic wait and fail all buffered records. However, we could have
	// some weird racy interactions with storePartitionsUpdate.
	//
	// If the pointer in the unknownTopics map is different than the one
	// that started this goroutine, then we raced. A partitions update
	// cleared the unknownTopics, and then a new produce went and set a
	// completely new pointer-to-slice in unknownTopics. We do not want to
	// fail everything in that new slice.
	cl.unknownTopicsMu.Lock()
	nowUnknown := cl.unknownTopics[topic]
	if nowUnknown != unknown {
		cl.unknownTopicsMu.Unlock()
		return
	}
	delete(cl.unknownTopics, topic)
	cl.unknownTopicsMu.Unlock()

	for _, pr := range unknown.buffered {
		cl.finishRecordPromise(pr, err)
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

	cl.cfg.logger.Log(LogLevelDebug, "flushing")
	defer cl.cfg.logger.Log(LogLevelDebug, "done flushing")

	// At this point, if lingering is configured, nothing will _start_ a
	// linger because the producer's flushing atomic int32 is nonzero. We
	// must wake anything that could be lingering up, after which all sinks
	// will loop draining.
	if cl.cfg.linger > 0 || cl.cfg.manualFlushing {
		for _, parts := range cl.loadTopics() {
			for _, part := range parts.load().partitions {
				part.records.unlingerAndManuallyDrain()
			}
		}
	}

	quit := false
	done := make(chan struct{})
	go func() {
		cl.producer.notifyMu.Lock()
		defer cl.producer.notifyMu.Unlock()
		defer close(done)

		for !quit && atomic.LoadInt64(&cl.producer.bufferedRecords) > 0 {
			cl.producer.notifyCond.Wait()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		cl.producer.notifyMu.Lock()
		quit = true
		cl.producer.notifyMu.Unlock()
		cl.producer.notifyCond.Broadcast()
		return ctx.Err()
	}
}
