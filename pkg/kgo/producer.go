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

	aborting int32 // >0 if aborting, can abort many times concurrently

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
	p.waitBuffer = make(chan struct{}, 32)
	p.idVersion = -1
	p.id.Store(&producerID{
		id:    -1,
		epoch: -1,
		err:   errReloadProducerID,
	})
	p.notifyCond = sync.NewCond(&p.notifyMu)
}

func (p *producer) isAborting() bool { return atomic.LoadInt32(&p.aborting) > 0 }

func noPromise(*Record, error) {}

// ProduceResult is the result of producing a record in a synchronous manner.
type ProduceResult struct {
	// Record is the produced record. It is always non-nil.
	//
	// If this record was produced successfully, its attrs / offset / id /
	// epoch / etc. fields are filled in on return if possible (i.e. when
	// producing with acks required).
	Record *Record

	// Err is a potential produce error. If this is non-nil, the record was
	// not produced successfully.
	Err error
}

// ProduceResults is a collection of produce results.
type ProduceResults []ProduceResult

// FirstErr returns the first erroring result, if any.
func (rs ProduceResults) FirstErr() error {
	for _, r := range rs {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// First the first record and error in the produce results.
//
// This function is useful if you only passed one record to ProduceSync.
func (rs ProduceResults) First() (*Record, error) {
	return rs[0].Record, rs[0].Err
}

// ProduceSync is a synchronous produce. Please see the Produce documentation
// for an in depth description of how producing works.
//
// This function produces all records in one range loop and waits for them all
// to be produced before returning.
func (cl *Client) ProduceSync(ctx context.Context, rs ...*Record) ProduceResults {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make(ProduceResults, 0, len(rs))
		promise = func(r *Record, err error) {
			mu.Lock()
			results = append(results, ProduceResult{r, err})
			mu.Unlock()
			wg.Done()
		}
	)

	wg.Add(len(rs))
	for _, r := range rs {
		cl.Produce(ctx, r, promise)
	}
	wg.Wait()

	return results
}

// FirstErrPromise is a helper type to capture only the first failing error
// when producing a batch of records with this type's Promise function.
//
// This is useful for when you only care about any record failing, and can use
// that as a signal (i.e., to abort a batch). The AbortingFirstErrPromise
// function can be used to abort all records as soon as the first error is
// encountered. If you do not need to abort, you can use this type with no
// constructor.
//
// This is similar to using ProduceResult's FirstErr function.
type FirstErrPromise struct {
	wg   sync.WaitGroup
	once uint32
	err  error
	cl   *Client
}

// AbortingFirstErrPromise returns a FirstErrPromise that will call the
// client's AbortBufferedRecords function if an error is encountered.
//
// This can be used to quickly exit when any error is encountered, rather than
// waiting while flushing only to discover things errored.
func AbortingFirstErrPromise(cl *Client) *FirstErrPromise {
	return &FirstErrPromise{
		cl: cl,
	}
}

// Promise is a promise for producing that will store the first error
// encountered.
func (f *FirstErrPromise) promise(_ *Record, err error) {
	defer f.wg.Done()
	if err != nil && atomic.SwapUint32(&f.once, 1) == 0 {
		f.err = err
		if f.cl != nil {
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.cl.AbortBufferedRecords(context.Background())
			}()
		}
	}
}

// Promise returns a promise for producing that will store the first error
// encountered.
//
// The returned promise must eventually be called, because a FirstErrPromise
// does not return from 'Err' until all promises are completed.
func (f *FirstErrPromise) Promise() func(*Record, error) {
	f.wg.Add(1)
	return f.promise
}

// Err waits for all promises to complete and then returns any stored error.
func (f *FirstErrPromise) Err() error {
	f.wg.Wait()
	return f.err
}

// Produce sends a Kafka record to the topic in the record's Topic field,
// calling promise with the record or an error when Kafka replies. For a
// synchronous produce, see ProduceSync.
//
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly. Records are produced in per-partition order and
// promises are called in per-partition order if the record is buffered to a
// topic that has loaded successfully. Topics that fail loading, or records
// that cannot be buffered, may not have their promises called in order.
// Promises may be called concurrently.
//
// If a record is produced successfully, the record's attrs / offset / etc.
// fields are updated appropriately before a promise is called.
//
// If the record has an empty Topic field, the client will use a default topic
// if the client was configured with one via ProduceTopic, otherwise the record
// will be failed immediately.
//
// If the record is too large to fit in a batch on its own in a produce
// request, the promise will be called with kerr.MessageTooLarge and there will
// be no attempt to produce the record.
//
// The context is used if the client currently has the max amount of buffered
// records. If so, the client waits for some records to complete or for the
// context or client to quit. If the context / client quits, the promise is
// called with ctx.Err().
//
// The context is also used on a per-partition basis to abort buffered records.
// If the context is done for the first record buffered in a partition, and if
// it is valid to abort records (i.e., we can avoid invalid sequence numbers),
// then all buffered records for a partition are aborted. The context checked
// for doneness is always the first buffered record's context. The context is
// evaluated before or after writing a request.
//
// The first buffered record for an unknown topic begins a timeout for the
// configured record timeout limit; all records buffered within the wait will
// expire with the same timeout if the topic does not load in time. For
// simplicity, any time spent waiting for the topic to load is not persisted
// through once the topic loads, meaning the record may further wait once
// buffered. This may be changed in the future if necessary, however, the only
// reason for a topic to not load promptly is if it does not exist.
//
// If manual flushing is configured and there are already MaxBufferedRecords
// buffered, the promise is immediately called with ErrMaxBuffered.
//
// If the client is transactional and a transaction has not been begun, the
// promise is immediately called with an error corresponding to not being in
// a transaction.
func (cl *Client) Produce(
	ctx context.Context,
	r *Record,
	promise func(*Record, error),
) {
	if promise == nil {
		promise = noPromise
	}

	if r.Topic == "" {
		if def := cl.cfg.defaultProduceTopic; def != "" {
			r.Topic = def
		} else {
			go promise(r, errors.New("cannot produce to a record that does not have a topic set"))
			return
		}
	}

	p := &cl.producer

	if cl.cfg.txnID != nil && atomic.LoadUint32(&p.producingTxn) != 1 {
		go promise(r, errNotInTransaction) // see comment just below for why we 'go' this
		return
	}

	if atomic.AddInt64(&p.bufferedRecords, 1) > cl.cfg.maxBufferedRecords {
		// If the client ctx cancels or the produce ctx cancels, we
		// need to un-count our buffering of this record. We also need
		// to drain a slot from the waitBuffer chan, which could be
		// sent to right when we are erroring.
		//
		// We issue the waitBuffer drain in a goroutine because we do
		// not want to block sending to it.
		//
		// We issue the promise finishing in a goroutine because we do
		// not want to block Produce on executing the promise. The user
		// could be consuming from a channel that is sent to in the
		// promise only *after* Produce returns; not executing the
		// promise in a goroutine would lead to a deadlock.
		drainBuffered := func(err error) {
			go func() { <-p.waitBuffer }()
			go cl.finishRecordPromise(promisedRec{ctx, promise, r}, err)
		}
		if cl.cfg.manualFlushing {
			drainBuffered(ErrMaxBuffered)
			return
		}
		select {
		case <-p.waitBuffer:
		case <-cl.ctx.Done():
			drainBuffered(cl.ctx.Err())
			return
		case <-ctx.Done():
			drainBuffered(ctx.Err())
			return
		}
	}

	cl.partitionRecord(promisedRec{ctx, promise, r})
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
	cl.triggerUpdateMetadata(false)

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
			if int64(tries) >= cl.cfg.produceRetries {
				err = fmt.Errorf("no partitions available after attempting to refresh metadata %d times, last err: %w", tries, retriableErr)
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

	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()

	nowUnknown := p.unknownTopics[topic]
	if nowUnknown != unknown {
		return
	}
	cl.cfg.logger.Log(LogLevelInfo, "unknown topic wait failed, done retrying, failing all records", "topic", topic)

	delete(p.unknownTopics, topic)
	cl.failUnknownTopicRecords(topic, unknown, err)
}

// Called under the unknown mu, this finishes promises for an unknown topic.
//
// We do not delete from the producer's topics due to potential concurrent
// metadata updating issues: if the metadata has an active request loading for
// a topic we are actively deleting now, and that request finally loads the
// topic successfully, it will create recBuf pointers that will not be cleaned
// up.
//
// We could work around this using the same blockingMetadataFn type logic that
// we use when unsetting a consumer, but it's more finnicky for a producer
// because we want to knife out a single topic.
//
// Leaving a topic buffered even if we failed it as unknown should be of no
// consequence because clients should not really be producing to loads of
// unknown topics.
func (cl *Client) failUnknownTopicRecords(topic string, unknown *unknownTopicProduces, err error) {
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
//
// This function is safe to call multiple times concurrently, and safe to call
// concurrent with Flush.
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
