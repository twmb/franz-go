package kgo

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo/internal/xsync"
)

type metawait struct {
	mu         xsync.Mutex
	c          *sync.Cond
	lastUpdate time.Time
	updates    uint64 // completed updates; see metadataUpdates
}

func (m *metawait) init() { m.c = sync.NewCond(&m.mu) }
func (m *metawait) signal() {
	m.mu.Lock()
	m.lastUpdate = time.Now()
	m.updates++
	m.mu.Unlock()
	m.c.Broadcast()
}

// metadataUpdates returns how many metadata updates have completed. A
// cursor rejected for its topic ID waits for the count to grow before it
// fetches again; a count cannot go backward the way a clock can.
func (cl *Client) metadataUpdates() uint64 {
	cl.metawait.mu.Lock()
	defer cl.metawait.mu.Unlock()
	return cl.metawait.updates
}

// ForceMetadataRefresh triggers the client to update the metadata that is
// currently used for producing & consuming.
//
// Internally, the client already properly triggers metadata updates whenever a
// partition is discovered to be out of date (leader moved, epoch is old, etc).
// However, when partitions are added to a topic through a CreatePartitions
// request, it may take up to MetadataMaxAge for the new partitions to be
// discovered. In this case, you may want to forcefully refresh metadata
// manually to discover these new partitions sooner.
func (cl *Client) ForceMetadataRefresh() {
	cl.triggerUpdateMetadataNow("from user ForceMetadataRefresh")
}

// PartitionLeader returns the given topic partition's leader, leader epoch and
// load error. This returns -1, -1, nil if the partition has not been loaded.
func (cl *Client) PartitionLeader(topic string, partition int32) (leader, leaderEpoch int32, err error) {
	if partition < 0 {
		return -1, -1, errors.New("invalid negative partition")
	}

	var t *topicPartitions

	m := cl.producer.topics.load()
	if len(m) > 0 {
		t = m[topic]
	}
	if t == nil {
		if cl.consumer.g != nil {
			if m = cl.consumer.g.tps.load(); len(m) > 0 {
				t = m[topic]
			}
		} else if cl.consumer.d != nil {
			if m = cl.consumer.d.tps.load(); len(m) > 0 {
				t = m[topic]
			}
		}
		if t == nil {
			return -1, -1, nil
		}
	}

	tv := t.load()
	if len(tv.partitions) <= int(partition) {
		return -1, -1, tv.loadErr
	}
	p := tv.partitions[partition]
	return p.leader, p.leaderEpoch, p.loadErr
}

var noid2t = make(map[[16]byte]string)

func (cl *Client) id2tMap() map[[16]byte]string {
	v := cl.id2t.Load()
	if v == nil {
		return noid2t
	}
	m := v.(map[[16]byte]string)
	if m == nil {
		return noid2t
	}
	return m
}

// waitmeta returns immediately if metadata was updated within the last second,
// otherwise this waits for up to wait for a metadata update to complete.
func (cl *Client) waitmeta(ctx context.Context, wait time.Duration, why string) {
	cl.dowaitmeta(ctx, wait, false, why)
}

func (cl *Client) dowaitmeta(ctx context.Context, wait time.Duration, force bool, why string) {
	now := time.Now()

	if !force {
		cl.metawait.mu.Lock()
		if now.Sub(cl.metawait.lastUpdate) < cl.cfg.metadataMinAge {
			cl.metawait.mu.Unlock()
			return
		}
		cl.metawait.mu.Unlock()
	}

	cl.triggerUpdateMetadataNow(why)

	quit := false
	done := make(chan struct{})
	timeout := time.NewTimer(wait)
	defer timeout.Stop()

	go func() {
		defer close(done)
		cl.metawait.mu.Lock()
		defer cl.metawait.mu.Unlock()

		for !quit {
			if now.Sub(cl.metawait.lastUpdate) < cl.cfg.metadataMinAge {
				return
			}
			cl.metawait.c.Wait()
		}
	}()

	select {
	case <-done:
		return
	case <-timeout.C:
	case <-ctx.Done():
	case <-cl.ctx.Done():
	}

	cl.metawait.mu.Lock()
	quit = true
	cl.metawait.mu.Unlock()
	cl.metawait.c.Broadcast()
}

func (cl *Client) triggerUpdateMetadata(must bool, why string) bool {
	if !must {
		cl.metawait.mu.Lock()
		defer cl.metawait.mu.Unlock()
		if time.Since(cl.metawait.lastUpdate) < cl.cfg.metadataMinAge {
			return false
		}
	}

	select {
	case cl.updateMetadataCh <- why:
	default:
	}
	return true
}

func (cl *Client) triggerUpdateMetadataNow(why string) {
	select {
	case cl.updateMetadataNowCh <- why:
	default:
	}
}

func (cl *Client) blockingMetadataFn(fn func()) {
	var wg sync.WaitGroup
	wg.Add(1)
	waitfn := func() {
		defer wg.Done()
		fn()
	}
	select {
	case cl.blockingMetadataFnCh <- waitfn:
		wg.Wait()
	case <-cl.ctx.Done():
	}
}

// updateMetadataLoop updates metadata whenever the update ticker ticks,
// or whenever deliberately triggered.
func (cl *Client) updateMetadataLoop() {
	defer close(cl.metadone)
	var consecutiveErrors int
	var lastAt time.Time

	ticker := time.NewTicker(cl.cfg.metadataMaxAge)
	defer ticker.Stop()
loop:
	for {
		var now bool
		select {
		case <-cl.ctx.Done():
			return
		case <-ticker.C:
			// We do not log on the standard update case.
		case why := <-cl.updateMetadataCh:
			cl.cfg.logger.Log(LogLevelInfo, "metadata update triggered", "why", why)
		case why := <-cl.updateMetadataNowCh:
			cl.cfg.logger.Log(LogLevelInfo, "immediate metadata update triggered", "why", why)
			now = true
		case fn := <-cl.blockingMetadataFnCh:
			fn()
			continue loop
		}

		var nowTries int
	start:
		nowTries++
		if !now {
			if wait := cl.cfg.metadataMinAge - time.Since(lastAt); wait > 0 {
				timer := time.NewTimer(wait)
			prewait:
				select {
				case <-cl.ctx.Done():
					timer.Stop()
					return
				case why := <-cl.updateMetadataNowCh:
					timer.Stop()
					cl.cfg.logger.Log(LogLevelInfo, "immediate metadata update triggered, bypassing normal wait", "why", why)
				case <-timer.C:
				case fn := <-cl.blockingMetadataFnCh:
					fn()
					goto prewait
				}
			}
		}

		// Even with an "update now", we sleep just a bit to allow some
		// potential pile on now triggers.
		time.Sleep(time.Until(lastAt.Add(10 * time.Millisecond)))

		// Drain any refires that occurred during our waiting.
	out:
		for {
			select {
			case <-cl.updateMetadataCh:
			case <-cl.updateMetadataNowCh:
			case fn := <-cl.blockingMetadataFnCh:
				fn()
			default:
				break out
			}
		}

		retryWhy, err := cl.updateMetadata()
		lastAt = time.Now()
		if retryWhy != nil || err != nil {
			// If err is non-nil, the metadata request failed
			// itself and already retried 3x; we do not loop more.
			//
			// If err is nil, the a topic or partition had a load
			// error and is perhaps still being created. We retry a
			// few more times to give Kafka a chance to figure
			// things out. By default this will put us at 2s of
			// looping+waiting (250ms per wait, 8x), and if things
			// still fail we will fall into the slower update below
			// which waits (default) 5s between tries.
			if now && err == nil && nowTries < 8 {
				// This round merged: partitions took any new topic ID
				// it carried. Count it and wake anything waiting on a
				// metadata update before we loop, otherwise a cursor
				// paused for its topic ID to be re-decided sits through
				// every one of these rounds without seeing the updates
				// that already fixed it.
				cl.metawait.signal()
				cl.consumer.doOnMetadataUpdate()
				wait := min(cl.cfg.metadataMinAge, 250*time.Millisecond)
				cl.cfg.logger.Log(LogLevelDebug, "immediate metadata update had inner errors, re-updating",
					"errors", retryWhy.reason(""),
					"update_after", wait,
				)
				timer := time.NewTimer(wait)
			quickbackoff:
				select {
				case <-cl.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				case fn := <-cl.blockingMetadataFnCh:
					fn()
					goto quickbackoff
				}
				goto start
			}
			if err != nil {
				cl.triggerUpdateMetadata(true, fmt.Sprintf("re-updating metadata due to err: %s", err))
			} else {
				cl.triggerUpdateMetadata(true, retryWhy.reason("re-updating due to inner errors"))
			}
		}
		if err == nil {
			cl.metawait.signal()
			cl.consumer.doOnMetadataUpdate()
			consecutiveErrors = 0
			continue
		}

		consecutiveErrors++
		// We sleep a bit in case the max metadata age is very small;
		// typically this sleep is inconsequential.
		after := time.NewTimer(cl.cfg.retryBackoff(consecutiveErrors))
	backoff:
		select {
		case <-cl.ctx.Done():
			after.Stop()
			return
		case <-after.C:
		case fn := <-cl.blockingMetadataFnCh:
			fn()
			goto backoff
		}
	}
}

var errMissingTopic = errors.New("topic_missing")

// Updates all producer and consumer partition data, returning whether a new
// update needs scheduling or if an error occurred.
//
// The producer and consumer use different topic maps and underlying
// topicPartitionsData pointers, but we update those underlying pointers
// equally.
func (cl *Client) updateMetadata() (retryWhy multiUpdateWhy, err error) {
	var (
		tpsProducerLoad = cl.producer.topics.load()
		tpsConsumer     *topicsPartitions
		groupExternal   *groupExternal
		all             = cl.cfg.regex
		reqTopics       []string
	)
	c := &cl.consumer
	switch {
	case c.g != nil:
		tpsConsumer = c.g.tps
		groupExternal = c.g.loadExternal()
	case c.s != nil:
		tpsConsumer = c.s.tps
	case c.d != nil:
		tpsConsumer = c.d.tps
	}

	if !all {
		reqTopicsSet := make(map[string]struct{})
		for _, m := range []map[string]*topicPartitions{
			tpsProducerLoad,
			tpsConsumer.load(),
		} {
			for topic := range m {
				reqTopicsSet[topic] = struct{}{}
			}
		}
		groupExternal.eachTopic(func(t string) {
			reqTopicsSet[t] = struct{}{}
		})
		reqTopics = make([]string, 0, len(reqTopicsSet))
		for topic := range reqTopicsSet {
			reqTopics = append(reqTopics, topic)
		}
	}

	// If the user has auto topic creation while producing AND is consuming
	// via regex, we need to send a separate metadata request with unknown
	// produce topics before our standard metadata request. The standard
	// metadata request is send with a nil Topics field (requesting all),
	// which prevents us from ever creating the topics.
	var unknownCreateResp map[string]*metadataTopic
	if all && cl.cfg.allowAutoTopicCreation {
		cl.producer.unknownTopicsMu.Lock()
		if len(cl.producer.unknownTopics) > 0 {
			unknownTopics := make([]string, 0, len(cl.producer.unknownTopics))
			for unknown := range cl.producer.unknownTopics {
				unknownTopics = append(unknownTopics, unknown)
			}
			var err error
			unknownCreateResp, err = cl.fetchTopicMetadata(false, unknownTopics, false) // prune: no; unknown produce topics only, the fetch below covers the rest
			if err != nil {
				// We bump all produce topics even though we
				// only explicitly requested unknown ones; this
				// is a general request failure and we want to
				// note it (also this is simpler...).
				cl.bumpMetadataFailForTopics(
					tpsProducerLoad,
					err,
				)
			}
		}
		cl.producer.unknownTopicsMu.Unlock()
	}

	latest, err := cl.fetchTopicMetadata(all, reqTopics, true) // prune: yes; everything we track, so anything else is unwanted
	if err != nil {
		cl.bumpMetadataFailForTopics( // bump load failures for all topics
			tpsProducerLoad,
			err,
		)
		return nil, err
	}
	groupExternal.updateLatest(latest)

	// If regex consuming AND we issued a metadata request to forcefully
	// create topics, we merge any topics missing into the all-request from
	// the create-request. It is possible we want to keep failed creation
	// errors.
	for t, mt := range unknownCreateResp {
		if _, ok := latest[t]; !ok {
			latest[t] = mt
		}
	}

	tpsConsumerLoad := tpsConsumer.load()

	// Merge topic ID mappings into the existing id2t map. We clone
	// rather than rebuild so that topics missing from this particular
	// metadata response (transient broker omission, non-all request)
	// retain their ID mapping from prior responses.
	//
	// If a topic was deleted and recreated, the broker returns a new
	// ID for the same name. We adopt the new ID and drop the old one.
	// KIP-848 assignments name topics by ID, so the new topic's
	// assignment resolves through this map. Nothing needs the old entry.
	// A response carrying the old ID is resolved through the request
	// that sent it: fetchOffsets scans its request, and OffsetCommit,
	// fetch sessions, and produce batches keep their own maps. A KIP-848
	// assignment still naming the old ID stays in unresolvedAssigned
	// until the coordinator assigns the new one. The metadata cache's
	// byID map drops the old ID the same way.
	//
	// An ID the consumer's topic holds as a prior ID is a lagging
	// broker's report; the merge below refuses it, and mapping the name
	// back onto it here would leave a KIP-848 assignment naming the
	// current ID unresolved for a round.
	{
		old := cl.id2tMap()
		t2id := make(map[string][16]byte, len(old))
		for id, name := range old {
			t2id[name] = id
		}
		merged := make(map[[16]byte]string, len(old)+len(latest))
		maps.Copy(merged, old)
		for _, mt := range latest {
			if mt.id == noID {
				continue
			}
			if tp, ok := tpsConsumerLoad[mt.topic]; ok && tp.load().priorIDs.has(mt.id) {
				continue
			}
			if prior, ok := t2id[mt.topic]; ok && prior != mt.id {
				delete(merged, prior)
			}
			merged[mt.id] = mt.topic
		}
		cl.id2t.Store(merged)
	}

	// If we are consuming with regex and fetched all topics, the metadata
	// may have returned topics the consumer is not yet tracking. We ensure
	// that we will store the topics at the end of our metadata update.
	if all {
		// We filter out topics will not match any of our regex's.
		// This ensures that the `tps` field does not contain topics
		// we will never use (the client works with misc. topics in
		// there, but it's better to avoid it -- and allows us to use
		// `tps` in GetConsumeTopics).
		allTopics := c.filterMetadataAllTopics(latest)

		tpsConsumerLoad = tpsConsumer.ensureTopics(allTopics)
		defer tpsConsumer.storeData(tpsConsumerLoad)

		// For regex consuming, if a topic is not returned in the
		// response and for at least missingTopicDelete from when we
		// first discovered it, we assume the topic has been deleted
		// and purge it. We allow for missingTopicDelete because (in
		// testing locally) Kafka can originally broadcast a newly
		// created topic exists and then fail to broadcast that info
		// again for a while.
		var purgeTopics []string
		for topic, tps := range tpsConsumerLoad {
			if _, ok := latest[topic]; !ok {
				if td := tps.load(); td.when != 0 && time.Since(time.Unix(td.when, 0)) > cl.cfg.missingTopicDelete {
					purgeTopics = append(purgeTopics, td.topic)
				} else {
					retryWhy.add(topic, -1, errMissingTopic)
				}
			}
		}
		if len(purgeTopics) > 0 {
			// We have to `go` because Purge issues a blocking
			// metadata fn; this will wait for our current
			// execution to finish then purge.
			cl.cfg.logger.Log(LogLevelInfo, "regex consumer purging topics that were previously consumed because they are missing in a metadata response, we are assuming they are deleted", "topics", purgeTopics)
			go cl.PurgeTopicsFromClient(purgeTopics...)
		}
	}

	css := &consumerSessionStopper{cl: cl}
	defer css.maybeRestart()

	consumerKind := partitionKindConsume
	if cl.consumer.s != nil {
		consumerKind = partitionKindShare
	}
	var missingProduceTopics []*topicPartitions
	for _, m := range []struct {
		priors map[string]*topicPartitions
		kind   partitionKind
	}{
		{tpsProducerLoad, partitionKindProduce},
		{tpsConsumerLoad, consumerKind},
	} {
		for topic, priorParts := range m.priors {
			newParts, exists := latest[topic]
			if !exists {
				if m.kind == partitionKindProduce {
					missingProduceTopics = append(missingProduceTopics, priorParts)
				}
				continue
			}
			cl.mergeTopicPartitions(
				topic,
				priorParts,
				newParts,
				m.kind,
				css,
				&retryWhy,
			)
		}
	}

	// For all produce topics that were missing, we want to bump their
	// retries that a failure happened. However, if we are regex consuming,
	// then it is possible in a rare scenario for the broker to not return
	// a topic that actually does exist and that we previously received a
	// metadata response for. This is handled above for consuming, we now
	// handle it the same way for consuming.
	if len(missingProduceTopics) > 0 {
		var bumpFail []string
		for _, tps := range missingProduceTopics {
			if all {
				if td := tps.load(); td.when != 0 && time.Since(time.Unix(td.when, 0)) > cl.cfg.missingTopicDelete {
					bumpFail = append(bumpFail, td.topic)
				} else {
					retryWhy.add(td.topic, -1, errMissingTopic)
				}
			} else {
				bumpFail = append(bumpFail, tps.load().topic)
			}
		}
		if len(bumpFail) > 0 {
			cl.bumpMetadataFailForTopics(
				tpsProducerLoad,
				fmt.Errorf("metadata request did not return topics: %v", bumpFail),
				bumpFail...,
			)
		}
	}

	return retryWhy, nil
}

// We use a special structure to represent metadata before we *actually* convert
// it to topicPartitionsData. This helps avoid any pointer reuse problems
// because we want to keep the client's producer and consumer maps completely
// independent.  If we just returned map[string]*topicPartitionsData, we could
// end up in some really weird pointer reuse scenario that ultimately results
// in a bug.
//
// See #190 for more details, as well as the commit message introducing this.
type metadataTopic struct {
	loadErr    error
	isInternal bool
	topic      string
	id         [16]byte
	partitions []metadataPartition
}

// newPartitions builds the partitions of a metadata response. Every
// partition is born with id, the ID the merge decided the topic has, which
// is the response's ID unless the response carried none.
func (mt *metadataTopic) newPartitions(cl *Client, kind partitionKind, id [16]byte) *topicPartitionsData {
	n := len(mt.partitions)
	ps := &topicPartitionsData{
		loadErr:            mt.loadErr,
		isInternal:         mt.isInternal,
		partitions:         make([]*topicPartition, 0, n),
		writablePartitions: make([]*topicPartition, 0, n),
		topic:              mt.topic,
		id:                 id,
		when:               time.Now().Unix(),
	}
	for i := range mt.partitions {
		p := mt.partitions[i].newPartition(cl, kind, id)
		ps.partitions = append(ps.partitions, p)
		if p.loadErr == nil {
			ps.writablePartitions = append(ps.writablePartitions, p)
		}
	}
	return ps
}

type metadataPartition struct {
	topic       string
	partition   int32
	loadErr     int16
	leader      int32
	leaderEpoch int32
	sns         sinkAndSource
}

func (mp metadataPartition) newPartition(cl *Client, kind partitionKind, id [16]byte) *topicPartition {
	td := topicPartitionData{
		leader:      mp.leader,
		leaderEpoch: mp.leaderEpoch,
	}
	p := &topicPartition{
		loadErr:            kerr.ErrorForCode(mp.loadErr),
		topicPartitionData: td,
	}
	switch kind {
	case partitionKindProduce:
		r := &recBuf{
			cl:                  cl,
			topic:               mp.topic,
			topicID:             id,
			partition:           mp.partition,
			maxRecordBatchBytes: cl.maxRecordBatchBytesForTopic(mp.topic),
			recBufsIdx:          -1,
			failing:             mp.loadErr != 0,
			sink:                mp.sns.sink,
			topicPartitionData:  td,
			lastAckedOffset:     -1,
		}
		r.lingerFn = r.unlingerAndManuallyDrain
		p.records = r
	case partitionKindShare:
		p.shareCursor = &shareCursor{
			topic:      mp.topic,
			topicID:    id,
			partition:  mp.partition,
			cursorsIdx: -1, // sentinel: not yet added to a source
		}
		p.shareCursor.source.Store(mp.sns.source)
	default:
		p.cursor = &cursor{
			topic:              mp.topic,
			topicID:            id,
			partition:          mp.partition,
			keepControl:        cl.cfg.keepControl,
			cursorsIdx:         -1,
			source:             mp.sns.source,
			topicPartitionData: td,
			cursorOffset: cursorOffset{
				offset:            -1, // required to not consume until needed
				lastConsumedEpoch: -1, // required sentinel
			},
		}
	}
	return p
}

// fetchTopicMetadata fetches metadata for all reqTopics and returns new
// topicPartitionsData for each topic.
func (cl *Client) fetchTopicMetadata(all bool, reqTopics []string, prune bool) (map[string]*metadataTopic, error) {
	_, meta, err := cl.fetchMetadataByName(cl.ctx, all, reqTopics, prune, nil)
	if err != nil {
		return nil, err
	}

	topics := make(map[string]*metadataTopic, len(meta.Topics))

	// Even if metadata returns a leader epoch, we do not use it unless we
	// can validate it per OffsetForLeaderEpoch. Some brokers may have an
	// odd set of support.
	useLeaderEpoch := cl.supportsOffsetForLeaderEpoch()

	for i := range meta.Topics {
		topicMeta := &meta.Topics[i]
		if topicMeta.Topic == nil {
			cl.cfg.logger.Log(LogLevelWarn, "metadata response contained nil topic name even though we did not request with topic IDs, skipping")
			continue
		}
		topic := *topicMeta.Topic

		mt := &metadataTopic{
			loadErr:    kerr.ErrorForCode(topicMeta.ErrorCode),
			isInternal: topicMeta.IsInternal,
			topic:      topic,
			id:         topicMeta.TopicID,
			partitions: make([]metadataPartition, 0, len(topicMeta.Partitions)),
		}

		topics[topic] = mt

		if mt.loadErr != nil {
			continue
		}

		// This 249 limit is in Kafka itself, we copy it here to rely on it while producing.
		if len(topic) > 249 {
			mt.loadErr = fmt.Errorf("invalid long topic name of (len %d) greater than max allowed 249", len(topic))
			continue
		}

		// Kafka partitions are strictly increasing from 0. We enforce
		// that here; if any partition is missing, we consider this
		// topic a load failure.
		sort.Slice(topicMeta.Partitions, func(i, j int) bool {
			return topicMeta.Partitions[i].Partition < topicMeta.Partitions[j].Partition
		})
		for i := range topicMeta.Partitions {
			if got := topicMeta.Partitions[i].Partition; got != int32(i) {
				mt.loadErr = fmt.Errorf("kafka did not reply with a comprehensive set of partitions for a topic; we expected partition %d but saw %d", i, got)
				break
			}
		}

		if mt.loadErr != nil {
			continue
		}

		for i := range topicMeta.Partitions {
			partMeta := &topicMeta.Partitions[i]
			leaderEpoch := partMeta.LeaderEpoch
			if meta.Version < 7 || !useLeaderEpoch {
				leaderEpoch = -1
			}
			mp := metadataPartition{
				topic:       topic,
				partition:   partMeta.Partition,
				loadErr:     partMeta.ErrorCode,
				leader:      partMeta.Leader,
				leaderEpoch: leaderEpoch,
			}
			if mp.loadErr != 0 {
				mp.leader = unknownSeedID(0) // ensure every records & cursor can use a sink or source
			}
			cl.sinksAndSourcesMu.Lock()
			sns, exists := cl.sinksAndSources[mp.leader]
			if !exists {
				sns = sinkAndSource{
					sink:   cl.newSink(mp.leader),
					source: cl.newSource(mp.leader),
				}
				cl.sinksAndSources[mp.leader] = sns
			}
			for _, replica := range partMeta.Replicas {
				if replica < 0 {
					continue
				}
				if _, exists = cl.sinksAndSources[replica]; !exists {
					cl.sinksAndSources[replica] = sinkAndSource{
						sink:   cl.newSink(replica),
						source: cl.newSource(replica),
					}
				}
			}
			cl.sinksAndSourcesMu.Unlock()
			mp.sns = sns
			mt.partitions = append(mt.partitions, mp)
		}
	}

	return topics, nil
}

// mergeTopicPartitions merges a new topicPartition into an old and returns
// whether the metadata update that caused this merge needs to be retried.
//
// Retries are necessary if the topic or any partition has a retryable error.
func (cl *Client) mergeTopicPartitions(
	topic string,
	l *topicPartitions,
	mt *metadataTopic,
	kind partitionKind,
	css *consumerSessionStopper,
	retryWhy *multiUpdateWhy,
) {
	isProduce := kind == partitionKindProduce
	// The logger is an interface, so the variadic slice and the interface
	// boxes for every argument are built at the call site even when the
	// logger drops the line. The logs below fire once per partition per
	// metadata refresh, so for a client with many partitions that is
	// continuous garbage forever; we only pay it if debug is on.
	debug := cl.cfg.logger.Level() >= LogLevelDebug
	lv := *l.load() // copy so our field writes do not collide with reads

	// Producers must store the update through a special function that
	// manages unknown topic waiting, whereas consumers can just simply
	// store the update.
	if isProduce {
		hadPartitions := len(lv.partitions) != 0
		defer func() { cl.storePartitionsUpdate(topic, l, &lv, hadPartitions) }()
	} else {
		defer l.v.Store(&lv)
	}

	lv.loadErr = mt.loadErr
	lv.isInternal = mt.isInternal
	lv.topic = mt.topic
	if lv.when == 0 {
		lv.when = time.Now().Unix()
	}

	// If the load had an error for the entire topic, we set the load error
	// but keep our stale partition information. For anything being
	// produced, we bump the respective error or fail everything. There is
	// nothing to be done in a consumer.
	if mt.loadErr != nil {
		if isProduce {
			for _, topicPartition := range lv.partitions {
				topicPartition.records.bumpRepeatedLoadErr(lv.loadErr)
			}
		} else if !kerr.IsRetriable(mt.loadErr) || cl.cfg.keepRetryableFetchErrors {
			cl.consumer.addFakeReadyForDraining(topic, -1, mt.loadErr, "metadata refresh has a load error on this entire topic")
		}
		retryWhy.add(topic, -1, mt.loadErr)
		return
	}

	// Topic IDs are random and never reused, so a new ID for a name we
	// hold means the topic was deleted and recreated. We adopt an ID the
	// topic never held immediately: a partition being consumed restarts
	// below, and one being produced to continues under the new ID. We
	// refuse an ID the topic held previously until the ID we hold has
	// been rejected recreationRejectionLimit times: a broker that has
	// not yet learned of the recreation still reports the old ID.
	now := time.Now()
	lv.priorIDs.dropExpired(now)
	var recreated bool
	switch {
	case mt.id == lv.id, lv.id == noID: // same ID, or no ID yet: copy the ID over
		lv.id = mt.id
	case mt.id == noID:
		// A broker that reports no ID cannot tell us whether the topic
		// was recreated, so we keep the ID we hold. The rest of the
		// response is still usable: we merge leaders and epochs below
		// rather than discarding the whole update. This is a cluster
		// mid-upgrade to 2.8 or a proxy; there is nothing to retry.
		cl.cfg.logger.Log(LogLevelDebug, "metadata update is missing the topic ID when we previously had one, keeping our ID",
			"topic", topic,
		)
	case kind == partitionKindShare:
		// Share cursors keep their ID: migrateShareCursorTo copies the
		// cursor. Share consuming does not restart on a recreation.
		lv.id = mt.id
	case lv.priorIDs.has(mt.id) && !lv.unknownIDLimitReached(kind):
		// The broker still reports an ID the topic held before, so it
		// still lags. We keep refusing the ID for as long as any broker
		// reports it, and forget it once none has for the expiry.
		cl.cfg.logger.Log(LogLevelDebug, "metadata update reports a topic ID this topic held previously, ignoring update until our ID is rejected",
			"topic", topic,
			"reported_id", topicID(mt.id),
			"our_id", topicID(lv.id),
		)
		lv.priorIDs.refresh(mt.id, now)
		lv.clearFailing(kind)
		return
	default:
		what := "topic recreation detected, adopting the new topic ID"
		switch kind {
		case partitionKindProduce:
			what += " for producing"
		case partitionKindConsume:
			what += " for consuming"
		}
		cl.cfg.logger.Log(LogLevelInfo, what,
			"topic", topic,
			"old_id", topicID(lv.id),
			"new_id", topicID(mt.id),
		)
		lv.priorIDs.add(lv.id, now)
		lv.id = mt.id
		// The adopted ID is now the current one, not a prior one. Drop it
		// from the prior set so that a later update reporting it (the common
		// case once a broker catches up) is not taken for a lagging broker
		// and skipped in id2t, which would stall 848/share reassignment.
		lv.priorIDs = lv.priorIDs.without(mt.id)
		recreated = true
		cl.sawRecreation.Store(true)

		// The new topic's log has no producer state for us, and the
		// idempotent producer's next batch would carry a continued
		// sequence number: Kafka 2.5 to 4.4 accept it and 4.5 rejects it
		// as out of order (KAFKA-15591). We bump the producer epoch
		// locally instead (KIP-360, as after data loss), so that the
		// first batch to every partition of the new topic starts at
		// sequence 0, which every version accepts. The bump waits for
		// requests in flight, so a batch staged behind one rejected for
		// the old ID cannot land before it. This runs before any
		// partition takes the new ID, so no request goes out under the
		// new ID at the old epoch. A transaction is handled below: its
		// writes to the old topic are gone and it must abort.
		if isProduce && cl.cfg.txnID == nil && !cl.cfg.disableIdempotency {
			if cur := cl.producer.id.Load().(*producerID); cur.err == nil && cur.id >= 0 {
				cl.failProducerID(cur.id, cur.epoch, errReloadProducerID)
			}
		}
	}

	// The partitions are built after the ID is decided so that every one
	// is born with the topic's ID: a cursor's ID is the topic's, whatever
	// the response carried.
	r := mt.newPartitions(cl, kind, lv.id)

	// Before the atomic update, we keep the latest partitions / writable
	// partitions. All updates happen in r's slices, and we keep the
	// results and store them in lv.
	defer func() {
		lv.partitions = r.partitions
		lv.writablePartitions = r.writablePartitions
	}()

	// We should have no deleted partitions, but there are two cases where
	// we could.
	//
	// 1) an admin added partitions, we saw, then we re-fetched metadata
	// from an out of date broker that did not have the new partitions
	//
	// 2) a topic was deleted and recreated with fewer partitions
	//
	// Case 1 is temporary and heals on a later refresh; case 2 is
	// permanent. Below, we keep the missing partition around either way.
	// For producers we bump its load error, which fails buffered records
	// only once the unknown fail limit trips (so case 1 does not fail
	// records); consumers keep consuming through the existing cursor.

	// Migrating topicPartitions is a little tricky because we have to
	// worry about underlying pointers that may currently be loaded.
	var (
		swapped     int      // cursors restarted or stopped by a recreation
		swappedFrom [16]byte // the ID they were swapped from
		txnExposed  int      // partitions whose recreation a transaction is exposed to
	)
	for part, oldTP := range lv.partitions {
		exists := part < len(r.partitions)
		if !exists {
			// This is the "deleted" case; see the comment above.
			//
			// We need to keep the partition around. For producing,
			// the partition could be loaded and a record could be
			// added to it after we bump the load error. For
			// consuming, the partition is part of a group or part
			// of what was loaded for direct consuming.
			//
			// We only clear a partition if the topic is purged from
			// the client, either manually via PurgeTopicsFromClient
			// or automatically for regex consumers when the topic
			// has been missing from metadata for longer than
			// ConsiderMissingTopicDeletedAfter.
			dup := *oldTP
			newTP := &dup
			newTP.loadErr = errMissingMetadataPartition

			r.partitions = append(r.partitions, newTP)

			cl.cfg.logger.Log(LogLevelDebug, "metadata update is missing partition in topic, we are keeping the partition around for safety -- use PurgeTopicsFromClient if you wish to remove the topic",
				"topic", topic,
				"partition", part,
			)
			if isProduce {
				oldTP.records.bumpRepeatedLoadErr(errMissingMetadataPartition)
			}
			retryWhy.add(topic, int32(part), errMissingMetadataPartition)
			continue
		}
		newTP := r.partitions[part]

		// Like above for the entire topic, an individual partition
		// can have a load error. Unlike for the topic, individual
		// partition errors are always retryable.
		//
		// If the load errored, we keep all old information minus the
		// load error itself (the new load will have no information).
		// A cursor created before the topic had an ID keeps noID and
		// fetches by name until a recreation swaps it, so a cursor whose
		// ID differs from the topic's is one an earlier update recreated
		// and skipped for a load error. We can read the ID: after
		// creation only the swap writes it, on this goroutine. A
		// recreated topic's leader epoch restarts from 0, so it is often
		// below the old topic's: the swap comes before the epoch
		// comparison below, which would otherwise take this for a stale
		// broker. A partition whose load errored, as a recreated topic's
		// can while its leaders are elected, is swapped too, on its old
		// source: a fetch request carries one ID per topic, so every
		// cursor of a topic must hold the topic's ID.
		swapCursor := func() bool {
			c := oldTP.cursor
			return recreated || c.topicID != noID && c.topicID != lv.id
		}

		if newTP.loadErr != nil {
			err := newTP.loadErr
			*newTP = *oldTP
			newTP.loadErr = err
			if isProduce {
				// A recreated topic's partitions can load with an
				// error while their leaders are elected. The
				// partition takes the topic's ID now, so that it
				// does not produce under the old one meanwhile.
				rb := newTP.records
				rb.mu.Lock()
				exposed := rb.setTopicID(lv.id)
				rb.mu.Unlock()
				if exposed {
					txnExposed++
				}
				rb.bumpRepeatedLoadErr(newTP.loadErr)
			} else if !kerr.IsRetriable(newTP.loadErr) || cl.cfg.keepRetryableFetchErrors {
				cl.consumer.addFakeReadyForDraining(topic, int32(part), newTP.loadErr, "metadata refresh has a load error on this partition")
			}
			retryWhy.add(topic, int32(part), newTP.loadErr)
			if kind == partitionKindConsume && swapCursor() {
				if from, ok := oldTP.swapRecreatedCursorTo(newTP, lv.id, css); ok {
					swappedFrom, swapped = from, swapped+1
				}
			}
			continue
		}

		switch kind {
		case partitionKindProduce:
			// A recreated topic's leader epoch restarts from 0, so it is
			// often below the old topic's. The partition takes the new
			// topic's leader and ID now rather than going through the
			// epoch comparison below, which would keep the old leader,
			// and the old ID, for maxEpochRewinds updates.
			if recreated {
				var exposed bool
				if newTP.topicPartitionData == oldTP.topicPartitionData {
					newTP.records = oldTP.records
					exposed = newTP.records.setTopicIDClearFailing(lv.id)
				} else {
					exposed = oldTP.migrateProductionTo(newTP, lv.id)
				}
				if exposed {
					txnExposed++
				}
				continue
			}

		case partitionKindConsume:
			if swapCursor() {
				if from, ok := oldTP.swapRecreatedCursorTo(newTP, lv.id, css); ok {
					swappedFrom, swapped = from, swapped+1
				}
				continue
			}
		}

		// If the new partition has an older leader epoch, then we
		// fetched from an out of date broker. We just keep the old
		// information.
		if newTP.leaderEpoch < oldTP.leaderEpoch {
			// A negative leader epoch is the "no leader" sentinel
			// (Kafka uses -1): the partition is momentarily
			// leaderless, e.g. mid-election after every replica
			// restarted in a full cluster bounce. This is not a
			// genuinely older epoch, so we must not treat it as a
			// rewind. If we counted it toward maxEpochRewinds, then
			// after enough leaderless refreshes we would fall through
			// below and accept -1 as the partition's leader epoch --
			// which is unsafe. A cursor at leader epoch -1 opts out of
			// KIP-320 fencing: migrateCursorTo skips
			// OffsetForLeaderEpoch validation for a negative new epoch,
			// so a genuine log truncation during the leaderless window
			// would go undetected (no ErrDataLoss), and the consumer
			// would then fetch at a stale offset with currentLeaderEpoch
			// -1 (which brokers never fence) and stall at the high
			// watermark. Instead we keep our last known real leader and
			// epoch and signal a retry; once a real epoch (>= old)
			// reappears, normal validation runs and detects any
			// truncation.
			if newTP.leaderEpoch < 0 {
				cl.cfg.logger.Log(LogLevelDebug, "metadata has a leader epoch of -1 (no leader); keeping our last known leader and epoch until a leader is elected",
					"topic", topic,
					"partition", part,
					"old_leader_epoch", oldTP.leaderEpoch,
				)
				*newTP = *oldTP
				retryWhy.add(topic, int32(part), errNoLeaderEpoch)
				continue
			}

			// Otherwise newTP.leaderEpoch is a real (>= 0) epoch that
			// is merely lower than ours. That can be the current
			// reality: issue #119 saw an unclean leader election
			// briefly surface a higher epoch from a broker that then
			// died, leaving the surviving cluster on a real, lower
			// epoch. Permanently refusing it stranded the client in an
			// unrecoverable metadata loop, so if we repeatedly rewind we
			// accept it to allow the client to continue. Unlike the -1
			// sentinel handled above, a real lower epoch self-corrects
			// downstream: consume sees FENCED_LEADER_EPOCH (and reports
			// data loss) and produce sees NOT_LEADER_FOR_PARTITION.
			//
			// Five is a pretty low amount of retries, but since
			// we iterate through known brokers, this basically
			// means we keep stale metadata if five brokers all
			// agree things rewound.
			const maxEpochRewinds = 5
			if oldTP.epochRewinds < maxEpochRewinds {
				cl.cfg.logger.Log(LogLevelDebug, "metadata leader epoch went backwards, ignoring update",
					"topic", topic,
					"partition", part,
					"old_leader_epoch", oldTP.leaderEpoch,
					"new_leader_epoch", newTP.leaderEpoch,
					"current_num_rewinds", oldTP.epochRewinds+1,
				)
				*newTP = *oldTP
				newTP.epochRewinds++
				retryWhy.add(topic, int32(part), errEpochRewind)
				continue
			}

			cl.cfg.logger.Log(LogLevelInfo, "metadata leader epoch went backwards repeatedly, we are now keeping the metadata to allow forward progress",
				"topic", topic,
				"partition", part,
				"old_leader_epoch", oldTP.leaderEpoch,
				"new_leader_epoch", newTP.leaderEpoch,
			)
		}

		// If the tp data is the same, we simply copy over the records
		// and cursor pointers.
		//
		// If the tp data equals the old, then the sink / source is the
		// same, because the sink/source is from the tp leader.
		if newTP.topicPartitionData == oldTP.topicPartitionData {
			if debug {
				cl.cfg.logger.Log(LogLevelDebug, "metadata refresh has identical topic partition data",
					"topic", topic,
					"partition", part,
					"leader", newTP.leader,
					"leader_epoch", newTP.leaderEpoch,
				)
			}
			switch kind {
			case partitionKindProduce:
				// The partition takes the topic's ID and clears its
				// failing state under one lock. The ID only changes here
				// when the topic gained one, or when the partition was
				// skipped for a load error on the adopting update.
				newTP.records = oldTP.records
				// The exposure return is ignored: this arm runs only
				// when the ID did not change, so setTopicID reports
				// no exposure. The recreated arm above counts it.
				newTP.records.setTopicIDClearFailing(lv.id)
			case partitionKindShare:
				newTP.shareCursor = oldTP.shareCursor
			default:
				newTP.cursor = oldTP.cursor // unlike records, there is no failing state for a cursor
			}
		} else {
			if debug {
				cl.cfg.logger.Log(LogLevelDebug, "metadata refresh topic partition data changed",
					"topic", topic,
					"partition", part,
					"new_leader", newTP.leader,
					"new_leader_epoch", newTP.leaderEpoch,
					"old_leader", oldTP.leader,
					"old_leader_epoch", oldTP.leaderEpoch,
				)
			}
			switch kind {
			case partitionKindProduce:
				// Ignored for the same reason as the sibling call
				// above: no ID change here, so no exposure to report.
				oldTP.migrateProductionTo(newTP, lv.id)
			case partitionKindShare:
				oldTP.migrateShareCursorTo(cl, newTP)
			default:
				oldTP.migrateCursorTo(newTP, css)
			}
		}
	}

	// A transaction that produced to a recreated topic, or has records for
	// it buffered or in flight, cannot commit: its writes to the old topic
	// were deleted with it, and a commit would report them as committed.
	// We fail the transaction once for the topic. The producer ID error
	// gate is for the log line: failProducerID itself no-ops once the ID
	// has failed, but we would otherwise log on every update until you
	// abort.
	if txnExposed > 0 && cl.cfg.txnID != nil {
		if cur := cl.producer.id.Load().(*producerID); cur.err == nil {
			cl.cfg.logger.Log(LogLevelWarn, "topic recreation observed with an active transaction exposed to it; failing the transaction",
				"topic", topic,
				"exposed_partitions", txnExposed,
			)
			cl.failProducerID(cur.id, cur.epoch, errRecreationAbortTxn)
		}
	}

	// The swaps above log one line for the topic; the per partition detail
	// is at debug. Whether a swapped cursor restarts or stops is our own
	// config, so every swap in this update is one or the other.
	if swapped > 0 {
		restarted, stopped := swapped, 0
		msg := "restarting partitions from the recreated topic's beginning"
		if cl.cfg.resetOffset.noReset {
			restarted, stopped = 0, swapped
			msg = "the topic was recreated and NoResetOffset is set, so these partitions are stopped until they are assigned again or you purge and re-add the topic"
		}
		cl.cfg.logger.Log(LogLevelInfo, msg,
			"topic", topic,
			"old_id", topicID(swappedFrom),
			"new_id", topicID(lv.id),
			"restarted_partitions", restarted,
			"stopped_partitions", stopped,
		)
	}

	// For any partitions **not currently in use**, we need to add them to
	// the sink or source. If they are in use, they could be getting
	// managed or moved by the sink or source itself, so we should not
	// check the index field (which may be concurrently modified).
	if len(lv.partitions) > len(r.partitions) {
		return
	}
	newPartitions := r.partitions[len(lv.partitions):]

	// Anything left with a negative recBufsIdx / cursorsIdx is a new topic
	// partition and must be added to the sink / source.
	for _, newTP := range newPartitions {
		if newTP.loadErr != nil {
			if isProduce {
				newTP.records.bumpRepeatedLoadErr(newTP.loadErr)
			} else if !kerr.IsRetriable(newTP.loadErr) || cl.cfg.keepRetryableFetchErrors {
				cl.consumer.addFakeReadyForDraining(topic, newTP.partition(), newTP.loadErr, "metadata refresh has a load error on a new partition")
			}
			retryWhy.add(topic, newTP.partition(), newTP.loadErr)
		}
		switch kind {
		case partitionKindProduce:
			if newTP.records.recBufsIdx == -1 {
				newTP.records.sink.addRecBuf(newTP.records)
				if debug {
					cl.cfg.logger.Log(LogLevelDebug, "metadata refresh new produce partition",
						"topic", topic,
						"partition", newTP.partition(),
						"leader", newTP.leader,
						"leader_epoch", newTP.leaderEpoch,
					)
				}
			}
		case partitionKindShare:
			if newTP.shareCursor.cursorsIdx == -1 {
				newTP.shareCursor.source.Load().addShareCursor(newTP.shareCursor)
				if debug {
					cl.cfg.logger.Log(LogLevelDebug, "metadata refresh new share consume partition",
						"topic", topic,
						"partition", newTP.partition(),
						"leader", newTP.leader,
						"leader_epoch", newTP.leaderEpoch,
					)
				}
			}
		default:
			if newTP.cursor.cursorsIdx == -1 {
				newTP.cursor.source.addCursor(newTP.cursor)
				if debug {
					cl.cfg.logger.Log(LogLevelDebug, "metadata refresh new consume partition",
						"topic", topic,
						"partition", newTP.partition(),
						"leader", newTP.leader,
						"leader_epoch", newTP.leaderEpoch,
					)
				}
			}
		}
	}
}

var (
	errEpochRewind   = errors.New("epoch rewind")
	errNoLeaderEpoch = errors.New("no leader epoch")
)

type multiUpdateWhy map[kerrOrString]map[string]map[int32]struct{}

type kerrOrString struct {
	k *kerr.Error
	s string
}

// isOnly reports whether every reason is one of errs.
func (m *multiUpdateWhy) isOnly(errs ...error) bool {
	if m == nil {
		return false
	}
	for e := range *m {
		if !slices.ContainsFunc(errs, func(err error) bool { return errors.Is(err, e.k) }) {
			return false
		}
	}
	return true
}

func (m *multiUpdateWhy) add(t string, p int32, err error) {
	if err == nil {
		return
	}

	if *m == nil {
		*m = make(map[kerrOrString]map[string]map[int32]struct{})
	}
	var ks kerrOrString
	if ke := (*kerr.Error)(nil); errors.As(err, &ke) {
		ks = kerrOrString{k: ke}
	} else {
		ks = kerrOrString{s: err.Error()}
	}

	ts := (*m)[ks]
	if ts == nil {
		ts = make(map[string]map[int32]struct{})
		(*m)[ks] = ts
	}

	ps := ts[t]
	if ps == nil {
		ps = make(map[int32]struct{})
		ts[t] = ps
	}
	// -1 signals that the entire topic had an error.
	if p != -1 {
		ps[p] = struct{}{}
	}
}

// err{topic[1 2 3] topic2[4 5 6]} err2{...}
func (m multiUpdateWhy) reason(reason string) string {
	if len(m) == 0 {
		return ""
	}

	ksSorted := make([]kerrOrString, 0, len(m))
	for err := range m {
		ksSorted = append(ksSorted, err)
	}
	sort.Slice(ksSorted, func(i, j int) bool { // order by non-nil kerr's code, otherwise the string
		l, r := ksSorted[i], ksSorted[j]
		return l.k != nil && (r.k == nil || l.k.Code < r.k.Code) || r.k == nil && l.s < r.s
	})

	var errorStrings []string
	for _, ks := range ksSorted {
		ts := m[ks]
		tsSorted := make([]string, 0, len(ts))
		for t := range ts {
			tsSorted = append(tsSorted, t)
		}
		sort.Strings(tsSorted)

		var topicStrings []string
		for _, t := range tsSorted {
			ps := ts[t]
			if len(ps) == 0 {
				topicStrings = append(topicStrings, t)
			} else {
				psSorted := make([]int32, 0, len(ps))
				for p := range ps {
					psSorted = append(psSorted, p)
				}
				slices.Sort(psSorted)
				topicStrings = append(topicStrings, fmt.Sprintf("%s%v", t, psSorted))
			}
		}

		if ks.k != nil {
			errorStrings = append(errorStrings, fmt.Sprintf("%s{%s}", ks.k.Message, strings.Join(topicStrings, " ")))
		} else {
			errorStrings = append(errorStrings, fmt.Sprintf("%s{%s}", ks.s, strings.Join(topicStrings, " ")))
		}
	}
	if reason == "" {
		return strings.Join(errorStrings, " ")
	}
	return reason + ": " + strings.Join(errorStrings, " ")
}
