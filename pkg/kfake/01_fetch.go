package kfake

import (
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Fetch: v4-18
//
// Behavior:
// * A partition that is unauthorized or that we do not have does not end
//   the wait; any other partition error, such as a partition on a different
//   broker or an out of range offset, answers at once
// * A fetch with no partition we can read answers at once
// * A topic created while we wait does not end the wait, and the final
//   response includes it
// * Raw bytes of batch counts against wait bytes
//
// For read_committed (IsolationLevel=1):
// * We only count stable bytes (inTx=false) toward MinBytes - this includes
//   committed txn batches, aborted txn batches, and non-txn batches
// * Uncommitted txn batches (inTx=true) are not counted and not returned
// * MinBytes is evaluated across all partitions, so stable data from
//   other partitions can satisfy MinBytes even if one partition is all in-txn
// * When a transaction commits/aborts, waiting read_committed fetches are woken
//
// Fetch sessions (KIP-227):
// * Sessions allow incremental fetches where clients only send changed partitions
// * We track session state per broker and merge with request to get full partition list
// * Incremental responses omit unchanged partitions (no records, same HWM/logStartOffset, no error)
//
// Version notes:
// * v4: RecordBatch format, IsolationLevel, LastStableOffset, AbortedTransactions
// * v5: LogStartOffset (KIP-107)
// * v7: Fetch sessions (KIP-227)
// * v9: CurrentLeaderEpoch for epoch fencing (KIP-320)
// * v11: Rack in request (KIP-392) - ignored
// * v12: LastFetchedEpoch for divergence detection - not implemented
// * v13: TopicID (KIP-516)
// * v15: ReplicaState (KIP-903) - broker-only, ignored
// * v18: HighWatermark tag (KIP-1166) - broker-only, ignored

func init() { regKey(1, 4, 18) }

func (c *Cluster) handleFetch(creq *clientReq, w *watchFetch) (kmsg.Response, error) {
	var (
		req  = creq.kreq.(*kmsg.FetchRequest)
		resp = req.ResponseKind().(*kmsg.FetchResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// Handle fetch sessions (v7+)
	var session *fetchSession
	var newSession bool
	if req.Version >= 7 && w != nil {
		// A parked fetch comes back with the session it set up when it
		// arrived; setting up again would create a second session.
		session, newSession = w.session, w.newSession
	} else if req.Version >= 7 {
		var errCode int16
		session, newSession, errCode = c.fetchSessions.getOrCreate(creq.cc.b.node, req.SessionID, req.SessionEpoch, int(c.fetchSessionCacheSlots()))
		if errCode != 0 {
			resp.ErrorCode = errCode
			return resp, nil
		}

		// Handle ForgottenTopics - remove partitions from session. Kafka
		// forgets an entry by the ID it was added under, and the entry
		// of a deleted topic keeps its name, which we find by that ID.
		var byID map[idp]string
		for _, ft := range req.ForgottenTopics {
			topic := ft.Topic
			if req.Version >= 13 {
				topic = c.data.id2t[ft.TopicID]
				if topic == "" && session != nil && byID == nil {
					byID = make(map[idp]string, len(session.partitions))
					for key, sp := range session.partitions {
						byID[idp{sp.topicID, key.p}] = key.t
					}
				}
			}
			for _, p := range ft.Partitions {
				name := topic
				if req.Version >= 13 {
					session.forgetUnknownID(ft.TopicID, p)
					if name == "" {
						name = byID[idp{ft.TopicID, p}]
					}
				}
				session.forgetPartition(name, p)
			}
		}
		session.dropForgotten()
	}
	if session != nil {
		resp.SessionID = session.id
	}

	// Build the list of partitions to fetch. With sessions, we need to merge:
	// 1. Partitions from the request (may be updates or new partitions)
	// 2. Partitions already in the session (for incremental fetches)
	type fetchPartition struct {
		topic        string
		topicID      uuid
		partition    int32
		fetchOffset  int64
		maxBytes     int32
		currentEpoch int32
		// staleID marks a session entry whose topic ID no longer
		// resolves while its cached name still does: the topic was
		// recreated under that name. Kafka resolves a session entry's
		// name once, at insertion, so the entry reaches the log by
		// name and fails the ID check there.
		staleID bool
	}
	var toFetch []fetchPartition

	// First, add partitions from the request and update session
	for i, rt := range req.Topics {
		if req.Version >= 13 {
			rt.Topic = c.data.id2t[rt.TopicID]
			req.Topics[i].Topic = rt.Topic
		}
		for _, rp := range rt.Partitions {
			fp := fetchPartition{
				topic:        rt.Topic,
				topicID:      rt.TopicID,
				partition:    rp.Partition,
				fetchOffset:  rp.FetchOffset,
				maxBytes:     rp.PartitionMaxBytes,
				currentEpoch: rp.CurrentLeaderEpoch,
			}
			// Update session with this partition's state. Kafka
			// matches a v13 request partition to a session entry
			// by topic ID and keeps the name the entry was added
			// under, so an ID that stopped resolving still reaches
			// the log by that name. An unresolvable ID with no
			// entry is tracked by ID: Kafka caches it with no name
			// and answers UNKNOWN_TOPIC_ID for it on every fetch of
			// the session.
			if fp.topic == "" && req.Version >= 13 {
				fp.topic = session.nameOfID(rt.TopicID, rp.Partition)
				fp.staleID = fp.topic != ""
			}
			if fp.topic != "" || req.Version < 13 {
				session.updatePartition(fp.topic, rt.TopicID, rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.CurrentLeaderEpoch)
			} else {
				session.updateUnknownID(rt.TopicID, rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.CurrentLeaderEpoch)
			}
			toFetch = append(toFetch, fp)
		}
	}

	// An incremental fetch (session exists and not new) answers every
	// session partition, in session order as Kafka does, not request order.
	if session != nil && !newSession {
		fromRequest := make(map[tp]fetchPartition, len(toFetch))
		inRequestUnknown := make(map[idp]bool)
		var unknown []fetchPartition
		for _, fp := range toFetch {
			if fp.topic == "" {
				inRequestUnknown[idp{fp.topicID, fp.partition}] = true
				unknown = append(unknown, fp)
				continue
			}
			fromRequest[tp{fp.topic, fp.partition}] = fp
		}
		for key, sp := range session.unknownIDs {
			if inRequestUnknown[key] {
				continue
			}
			// An ID that resolves now was created after the entry was
			// added; it becomes a normal entry under its name, which
			// the loop below picks up.
			if topic := c.data.id2t[key.id]; topic != "" {
				delete(session.unknownIDs, key)
				if _, ok := session.partitions[tp{topic, key.p}]; !ok {
					session.order = append(session.order, tp{topic, key.p})
				}
				session.partitions[tp{topic, key.p}] = sp
				continue
			}
			unknown = append(unknown, fetchPartition{
				topicID:      key.id,
				partition:    key.p,
				fetchOffset:  sp.fetchOffset,
				maxBytes:     sp.maxBytes,
				currentEpoch: sp.currentEpoch,
			})
		}
		ordered := make([]fetchPartition, 0, len(session.order)+len(unknown))
		for _, key := range session.order {
			if fp, ok := fromRequest[key]; ok {
				ordered = append(ordered, fp)
				continue
			}
			sp := session.partitions[key]
			var staleID bool
			if sp.topicID != (uuid{}) {
				// v13+ entries are addressed by the ID they
				// were added with. If it no longer resolves,
				// the entry keeps the name it was inserted
				// under, like a real broker: a deleted topic
				// answers UNKNOWN_TOPIC_OR_PARTITION, a
				// recreated one INCONSISTENT_TOPIC_ID from a
				// broker hosting the new incarnation.
				if _, ok := c.data.id2t[sp.topicID]; !ok {
					staleID = true
				}
			}
			ordered = append(ordered, fetchPartition{
				topic:        key.t,
				topicID:      sp.topicID,
				partition:    key.p,
				fetchOffset:  sp.fetchOffset,
				maxBytes:     sp.maxBytes,
				currentEpoch: sp.currentEpoch,
				staleID:      staleID,
			})
		}
		toFetch = append(ordered, unknown...)
	}

	// An unknown v13 topic ID has no name to authorize.
	denyp := func(fp fetchPartition) *kerr.Error {
		k := c.fetchFaultKey(creq, fp.topic, fp.topicID, fp.partition)
		if req.Version >= 13 && fp.topic == "" {
			return creq.faults.check(k)
		}
		return c.deny(creq, fp.topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead, k)
	}

	var (
		readCommitted = req.IsolationLevel == 1
		nbytes        int
		returnEarly   bool
		needp         tps[int]
		syn           = c.cfg.synthetic
	)
	if syn != nil {
		// A synthetic cluster serves its canned batch from any offset,
		// so there is always data: we answer now rather than waiting.
		returnEarly = true
	} else if w == nil {
		// As on a real broker, a partition that fails before its log
		// is read (unauthorized, or a topic or partition we do not
		// have) leaves the rest to wait for data. An error reading the
		// log answers at once, as does a fetch with nothing to read.
		var readable bool
	out:
		for _, fp := range toFetch {
			if e := denyp(fp); e != nil {
				if !faultEndsWait(e) {
					continue
				}
				returnEarly = true // the fault's error
				break out
			}
			t, ok := c.data.tps.gett(fp.topic)
			if !ok {
				continue // UnknownTopicID or UnknownTopicOrPartition
			}
			pd, ok := t[fp.partition]
			if !ok {
				continue // UnknownTopicOrPartition
			}
			readable = true
			if fp.staleID || pd.failed {
				returnEarly = true // InconsistentTopicID, NotLeaderForPartition, or KafkaStorageError
				break out
			}
			if pd.leader != creq.cc.b && !slices.Contains(pd.followers, creq.cc.b.node) {
				returnEarly = true // NotLeaderForPartition
				break out
			}
			if le := fp.currentEpoch; le != -1 && le != pd.epoch {
				returnEarly = true // FencedLeaderEpoch or UnknownLeaderEpoch
				break out
			}
			segIdx, metaIdx, ok, atEnd := pd.searchOffset(fp.fetchOffset)
			if atEnd {
				continue
			}
			if !ok {
				returnEarly = true // OffsetOutOfRange
				break out
			}
			pbytes := 0
			// Walk batches starting from the found position (segIdx, metaIdx).
			// The first segment starts at metaIdx; subsequent segments start at 0.
			for si := segIdx; si < len(pd.segments); si++ {
				seg := &pd.segments[si]
				startIdx := 0
				if si == segIdx {
					startIdx = metaIdx
				}
				for batchIdx := startIdx; batchIdx < len(seg.index); batchIdx++ {
					m := &seg.index[batchIdx]
					if readCommitted && m.firstOffset >= pd.lastStableOffset {
						break
					}
					nbytes += int(m.nbytes)
					pbytes += int(m.nbytes)
					if pbytes >= int(fp.maxBytes) {
						returnEarly = true
						break out
					}
				}
			}
			needp.set(fp.topic, fp.partition, int(fp.maxBytes)-pbytes)
		}
		if !readable {
			returnEarly = true
		}
	}

	// Like Kafka, the wait starts when we handle the fetch.
	wait := time.Duration(req.MaxWaitMillis) * time.Millisecond
	minBytes := int(min(req.MinBytes, req.MaxBytes)) // as Kafka caps it
	if w == nil && !returnEarly && nbytes < minBytes && wait > 0 {
		w := &watchFetch{
			need:          minBytes - nbytes,
			needp:         needp,
			readCommitted: readCommitted,
			creq:          creq,
			session:       session,
			newSession:    newSession,
		}
		w.cb = func() {
			select {
			case c.watchFetchCh <- w:
			case <-c.die:
			}
		}
		for _, fp := range toFetch {
			t, ok := c.data.tps.gett(fp.topic)
			if !ok {
				continue
			}
			pd, ok := t[fp.partition]
			if !ok {
				continue
			}
			pd.watch[w] = struct{}{}
			w.in = append(w.in, pd)
		}
		w.t = time.AfterFunc(wait, w.cb)
		return nil, nil
	}

	// Kafka merges a partition into the previous topic entry only when
	// that entry is the same topic and ID, so a topic whose partitions
	// are not adjacent in the answer order appears in more than one entry.
	donet := func(t string, id uuid) *kmsg.FetchResponseTopic {
		if n := len(resp.Topics); n > 0 {
			if last := &resp.Topics[n-1]; last.Topic == t && last.TopicID == id {
				return last
			}
		}
		st := kmsg.NewFetchResponseTopic()
		st.Topic = t
		st.TopicID = id
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, id uuid, p int32, errCode int16) *kmsg.FetchResponseTopicPartition {
		sp := kmsg.NewFetchResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		if errCode != 0 {
			sp.HighWatermark = -1 // Kafka sends no offsets with an error
		}
		// Encode empty record sets as an empty-but-present byte slice rather
		// than null. Technically the protocol allows nullable records here,
		// but some clients (e.g. librdkafka) interpret a nil slice (length -1)
		// as an invalid MessageSet size.
		sp.RecordBatches = []byte{}
		st := donet(t, id)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}

	var includeBrokers bool
	defer func() {
		if includeBrokers {
			for _, b := range c.bs {
				sb := kmsg.NewFetchResponseBroker()
				sb.NodeID = b.node
				sb.Host, sb.Port = b.hostport()
				sb.Rack = &brokerRack
				resp.Brokers = append(resp.Brokers, sb)
			}
		}
	}()

	var batchesAdded int
	nbytes = 0
full:
	for _, fp := range toFetch {
		if e := denyp(fp); e != nil {
			donep(fp.topic, fp.topicID, fp.partition, e.Code)
			continue
		}
		pd, ok := c.data.tps.getp(fp.topic, fp.partition)
		if !ok {
			// A v13 partition with a name got it from its session
			// entry; the entry reaches the log by that name, as on
			// a real broker, and fails as a missing partition.
			if req.Version >= 13 && fp.topic == "" {
				donep(fp.topic, fp.topicID, fp.partition, kerr.UnknownTopicID.Code)
			} else {
				donep(fp.topic, fp.topicID, fp.partition, kerr.UnknownTopicOrPartition.Code)
			}
			continue
		}
		if pd.failed {
			donep(fp.topic, fp.topicID, fp.partition, kerr.KafkaStorageError.Code)
			continue
		}
		if fp.staleID {
			// The session entry's name now belongs to a new
			// incarnation. A broker hosting it rejects the stale ID
			// as inconsistent; one that does not is simply not the
			// leader.
			if pd.leader != creq.cc.b && !slices.Contains(pd.followers, creq.cc.b.node) {
				donep(fp.topic, fp.topicID, fp.partition, kerr.NotLeaderForPartition.Code)
			} else {
				donep(fp.topic, fp.topicID, fp.partition, kerr.InconsistentTopicID.Code)
			}
			continue
		}
		if pd.leader != creq.cc.b && !slices.Contains(pd.followers, creq.cc.b.node) {
			p := donep(fp.topic, fp.topicID, fp.partition, kerr.NotLeaderForPartition.Code)
			p.CurrentLeader.LeaderID = pd.leader.node
			p.CurrentLeader.LeaderEpoch = pd.epoch
			includeBrokers = true
			continue
		}
		// CurrentLeaderEpoch validation (KIP-320, v9+)
		if le := fp.currentEpoch; le != -1 {
			if le < pd.epoch {
				donep(fp.topic, fp.topicID, fp.partition, kerr.FencedLeaderEpoch.Code)
				continue
			} else if le > pd.epoch {
				donep(fp.topic, fp.topicID, fp.partition, kerr.UnknownLeaderEpoch.Code)
				continue
			}
		}
		sp := donep(fp.topic, fp.topicID, fp.partition, 0)
		if syn != nil {
			sp.HighWatermark = syntheticEnd
			sp.LastStableOffset = syntheticEnd
			sp.LogStartOffset = 0
			room := min(int(fp.maxBytes), int(req.MaxBytes)-nbytes)
			if nbytes > 0 && room < len(syn.batch) {
				continue // full: only the first batch we return may exceed the limits
			}
			sp.RecordBatches = syn.appendBatches(sp.RecordBatches, fp.fetchOffset, pd.epoch, room)
			nbytes += len(sp.RecordBatches)
			continue
		}
		sp.HighWatermark = pd.highWatermark
		sp.LastStableOffset = pd.lastStableOffset
		sp.LogStartOffset = pd.logStartOffset
		segIdx, metaIdx, ok, atEnd := pd.searchOffset(fp.fetchOffset)
		if atEnd {
			continue
		}
		if !ok {
			sp.ErrorCode = kerr.OffsetOutOfRange.Code
			sp.HighWatermark = -1
			sp.LastStableOffset = -1
			sp.LogStartOffset = -1
			continue
		}

		var pbytes, pBatchesAdded int
		var lastMeta *batchMeta
		// Walk batches starting from the found position (segIdx, metaIdx).
		// The first segment starts at metaIdx; subsequent segments start at 0.
	segments:
		for si := segIdx; si < len(pd.segments); si++ {
			seg := &pd.segments[si]
			startIdx := 0
			if si == segIdx {
				startIdx = metaIdx
			}
			for batchIdx := startIdx; batchIdx < len(seg.index); batchIdx++ {
				m := &seg.index[batchIdx]
				if readCommitted && m.firstOffset >= pd.lastStableOffset {
					break segments
				}
				if nbytes += int(m.nbytes); nbytes > int(req.MaxBytes) && batchesAdded > 0 {
					break full
				}
				if pbytes += int(m.nbytes); pbytes > int(fp.maxBytes) && batchesAdded > 0 {
					break segments
				}
				batchesAdded++
				pBatchesAdded++
				lastMeta = m
				raw, err := c.readBatchRaw(pd, si, m)
				if err != nil {
					sp.ErrorCode = kerr.UnknownServerError.Code
					break segments
				}
				sp.RecordBatches = append(sp.RecordBatches, raw...)
			}
		}

		// For read_committed, look up aborted transactions that overlap
		// the returned data range using the partition's aborted txn index.
		if readCommitted && req.Version >= 4 && pBatchesAdded > 0 {
			upperBound := lastMeta.firstOffset + int64(lastMeta.lastOffsetDelta) + 1
			// Binary search for the first entry whose abort marker
			// is at or after the fetch offset.
			j := sort.Search(len(pd.abortedTxns), func(k int) bool {
				return pd.abortedTxns[k].lastOffset >= fp.fetchOffset
			})
			for ; j < len(pd.abortedTxns); j++ {
				e := pd.abortedTxns[j]
				if e.firstOffset >= upperBound {
					continue
				}
				at := kmsg.NewFetchResponseTopicPartitionAbortedTransaction()
				at.ProducerID = e.producerID
				at.FirstOffset = e.firstOffset
				sp.AbortedTransactions = append(sp.AbortedTransactions, at)
			}
		}
	}

	// Record partition state in the session. For incremental fetches
	// (not new), also filter out unchanged partitions.
	session.updateAndFilterResponse(resp, !newSession)

	// Bump session epoch after successful fetch, but not for newly created
	// sessions. The client will send epoch=1 for its first incremental fetch
	// after receiving the new session ID.
	if !newSession {
		session.bumpEpoch()
	}

	return resp, nil
}

type watchFetch struct {
	need  int
	needp tps[int]
	creq  *clientReq

	in []*partData
	cb func()
	t  *time.Timer

	readCommitted bool

	// session and newSession are what the fetch set up when it
	// arrived, for its rerun once it wakes.
	session    *fetchSession
	newSession bool

	once    sync.Once
	cleaned bool
}

func (w *watchFetch) push(pd *partData, nbytes int, inTx bool) {
	// For readCommitted consumers, skip counting bytes from uncommitted transactional batches.
	// These bytes will be counted when the transaction commits via addBytes.
	if w.readCommitted && inTx {
		return
	}
	w.addBytes(pd, nbytes)
}

// addBytes counts nbytes toward MinBytes satisfaction for a partition.
// Called by push() for normal produces, and directly from txns.go when
// a transaction commits (to count previously-skipped transactional bytes).
func (w *watchFetch) addBytes(pd *partData, nbytes int) {
	w.need -= nbytes
	needp, _ := w.needp.getp(pd.t, pd.p)
	if needp != nil {
		*needp -= nbytes
	}
	if w.need <= 0 || needp != nil && *needp <= 0 {
		w.do()
	}
}

func (w *watchFetch) deleted() { w.do() }

func (w *watchFetch) do() {
	w.once.Do(func() {
		go w.cb()
	})
}

func (w *watchFetch) cleanup() {
	w.cleaned = true
	for _, in := range w.in {
		delete(in.watch, w)
	}
	w.t.Stop()
}

// Fetch sessions (KIP-227)

// fetchSessions manages fetch sessions per KIP-227. Sessions are scoped per
// broker; since kfake simulates multiple brokers in one process, we key by
// broker node ID.
type fetchSessions struct {
	nextID   atomic.Int32
	sessions map[int32]map[int32]*fetchSession // broker node -> session ID -> session
}

// fetchSession tracks the state of a single fetch session.
type fetchSession struct {
	id         int32
	epoch      int32
	partitions map[tp]fetchSessionPartition
	// order is the order we answer an incremental fetch in, as Kafka
	// does: a partition is added at the end, and a partition that
	// returns records moves to the end.
	order []tp
	// unknownIDs holds v13+ entries whose topic ID did not resolve when
	// they were added. Kafka caches these with no name and answers
	// UNKNOWN_TOPIC_ID for them on every fetch of the session; a client
	// that keeps such an entry in its session sees the error on every
	// fetch rather than once.
	unknownIDs map[idp]fetchSessionPartition
	lastUsed   time.Time
}

// idp keys a session entry by topic ID and partition.
type idp struct {
	id uuid
	p  int32
}

// fetchSessionPartition tracks per-partition state within a session.
type fetchSessionPartition struct {
	// topicID is what the requester addressed this partition by (zero
	// below fetch v13). v13+ sessions are topic-ID addressed: if this ID
	// stops resolving, the entry keeps its name, and answers
	// UNKNOWN_TOPIC_OR_PARTITION for a deleted topic and
	// INCONSISTENT_TOPIC_ID for one recreated under a new ID.
	topicID      uuid
	fetchOffset  int64
	maxBytes     int32
	currentEpoch int32

	// Cached from last response sent. Used to detect changes for
	// incremental filtering. Initialized to -1 to force inclusion
	// in the first response after the partition is added.
	lastHighWatermark  int64
	lastLogStartOffset int64
}

const defMaxFetchSessionCacheSlots = 1000

func (fs *fetchSessions) init(brokerNode int32) {
	if fs.sessions == nil {
		fs.sessions = make(map[int32]map[int32]*fetchSession)
		fs.nextID.Store(1)
	}
	if fs.sessions[brokerNode] == nil {
		fs.sessions[brokerNode] = make(map[int32]*fetchSession)
	}
}

// getOrCreate returns an existing session or creates a new one based on the
// request's SessionID and SessionEpoch. Returns (nil, false, 0) for legacy
// sessionless fetches.
func (fs *fetchSessions) getOrCreate(brokerNode, sessionID, sessionEpoch int32, maxSlots int) (*fetchSession, bool, int16) {
	fs.init(brokerNode)

	// SessionEpoch=-1: Full fetch, no session (legacy mode).
	// If sessionID>0, this is the client unregistering/killing the session
	// (e.g., during source shutdown via killSessionOnClose).
	if sessionEpoch == -1 {
		if sessionID > 0 {
			delete(fs.sessions[brokerNode], sessionID)
		}
		return nil, false, 0
	}

	// SessionEpoch=0: Create new session (or unregister existing and create new).
	// When sessionID>0 with epoch=0, the client is requesting to unregister
	// the old session and start fresh (per KIP-227).
	if sessionEpoch == 0 {
		if sessionID > 0 {
			delete(fs.sessions[brokerNode], sessionID)
		}
		// Evict the oldest session if we're at the limit.
		brokerSessions := fs.sessions[brokerNode]
		if len(brokerSessions) >= maxSlots {
			var oldestID int32
			var oldestTime time.Time
			for id, s := range brokerSessions {
				if oldestTime.IsZero() || s.lastUsed.Before(oldestTime) {
					oldestID = id
					oldestTime = s.lastUsed
				}
			}
			delete(brokerSessions, oldestID)
		}
		now := time.Now()
		id := fs.nextID.Add(1) - 1
		session := &fetchSession{
			id:         id,
			epoch:      1,
			partitions: make(map[tp]fetchSessionPartition),
			unknownIDs: make(map[idp]fetchSessionPartition),
			lastUsed:   now,
		}
		fs.sessions[brokerNode][id] = session
		return session, true, 0
	}

	// SessionID>0, SessionEpoch>0: Look up existing session
	session, ok := fs.sessions[brokerNode][sessionID]
	if !ok {
		return nil, false, kerr.FetchSessionIDNotFound.Code
	}
	if sessionEpoch != session.epoch {
		return nil, false, kerr.InvalidFetchSessionEpoch.Code
	}
	session.lastUsed = time.Now()
	return session, false, 0
}

func (s *fetchSession) updatePartition(topic string, topicID uuid, partition int32, fetchOffset int64, maxBytes, currentEpoch int32) {
	if s == nil {
		return
	}
	key := tp{topic, partition}
	if existing, ok := s.partitions[key]; ok {
		existing.topicID = topicID
		existing.fetchOffset = fetchOffset
		existing.maxBytes = maxBytes
		existing.currentEpoch = currentEpoch
		s.partitions[key] = existing
	} else {
		s.partitions[key] = fetchSessionPartition{
			topicID:            topicID,
			fetchOffset:        fetchOffset,
			maxBytes:           maxBytes,
			currentEpoch:       currentEpoch,
			lastHighWatermark:  -1,
			lastLogStartOffset: -1,
		}
		s.order = append(s.order, key)
	}
}

func (s *fetchSession) forgetPartition(topic string, partition int32) {
	if s == nil {
		return
	}
	delete(s.partitions, tp{topic, partition})
}

// dropForgotten removes forgotten partitions from order. We call it once per
// request rather than once per forgotten partition, which would rewrite order
// once per partition.
func (s *fetchSession) dropForgotten() {
	if s == nil || len(s.order) == len(s.partitions) {
		return
	}
	s.order = slices.DeleteFunc(s.order, func(k tp) bool {
		_, ok := s.partitions[k]
		return !ok
	})
}

// nameOfID returns the name of the session entry added under topicID for
// partition, if any.
func (s *fetchSession) nameOfID(topicID uuid, partition int32) string {
	if s == nil {
		return ""
	}
	for key, sp := range s.partitions {
		if sp.topicID == topicID && key.p == partition {
			return key.t
		}
	}
	return ""
}

func (s *fetchSession) updateUnknownID(topicID uuid, partition int32, fetchOffset int64, maxBytes, currentEpoch int32) {
	if s == nil {
		return
	}
	s.unknownIDs[idp{topicID, partition}] = fetchSessionPartition{
		topicID:            topicID,
		fetchOffset:        fetchOffset,
		maxBytes:           maxBytes,
		currentEpoch:       currentEpoch,
		lastHighWatermark:  -1,
		lastLogStartOffset: -1,
	}
}

func (s *fetchSession) forgetUnknownID(topicID uuid, partition int32) {
	if s == nil {
		return
	}
	delete(s.unknownIDs, idp{topicID, partition})
}

func (s *fetchSession) bumpEpoch() {
	if s == nil {
		return
	}
	s.epoch++
	if s.epoch < 0 {
		s.epoch = 1
	}
}

// updateAndFilterResponse records the HWM and logStartOffset from each
// partition as baseline state. When filter is true, unchanged partitions
// (no records, same HWM/logStartOffset, no error) are removed from the
// response for incremental fetch, and partitions that returned records
// move to the end of the session's order.
func (s *fetchSession) updateAndFilterResponse(resp *kmsg.FetchResponse, filter bool) {
	if s == nil {
		return
	}
	var (
		moved    map[tp]struct{}
		movedSeq []tp
	)
	n := 0
	for i := range resp.Topics {
		rt := &resp.Topics[i]
		np := 0
		for j := range rt.Partitions {
			rp := &rt.Partitions[j]
			key := tp{rt.Topic, rp.Partition}
			sp, ok := s.partitions[key]
			if filter && ok && len(rp.RecordBatches) > 0 {
				if moved == nil {
					moved = make(map[tp]struct{})
				}
				moved[key] = struct{}{}
				movedSeq = append(movedSeq, key)
			}

			include := !filter || !ok ||
				len(rp.RecordBatches) > 0 ||
				rp.ErrorCode != 0 ||
				rp.HighWatermark != sp.lastHighWatermark ||
				rp.LogStartOffset != sp.lastLogStartOffset

			if ok {
				if rp.ErrorCode != 0 {
					sp.lastHighWatermark = -1
					sp.lastLogStartOffset = -1
				} else {
					sp.lastHighWatermark = rp.HighWatermark
					sp.lastLogStartOffset = rp.LogStartOffset
				}
				s.partitions[key] = sp
			}

			if include {
				rt.Partitions[np] = *rp
				np++
			}
		}
		rt.Partitions = rt.Partitions[:np]
		if len(rt.Partitions) == 0 {
			continue
		}
		// Kafka drops unchanged partitions before it groups the rest
		// into topic entries, so two entries of one topic that are
		// adjacent only now are one entry.
		if n > 0 && resp.Topics[n-1].Topic == rt.Topic && resp.Topics[n-1].TopicID == rt.TopicID {
			resp.Topics[n-1].Partitions = append(resp.Topics[n-1].Partitions, rt.Partitions...)
			continue
		}
		resp.Topics[n] = *rt
		n++
	}
	resp.Topics = resp.Topics[:n]

	// Kafka moves an incremental session's partition that returned records
	// to the end of the session's order, so the next fetch answers the
	// other partitions first.
	if len(movedSeq) > 0 {
		s.order = slices.DeleteFunc(s.order, func(k tp) bool {
			_, ok := moved[k]
			return ok
		})
		s.order = append(s.order, movedSeq...)
	}
}

// fetchFaultKey is the fault key for a fetched partition. A fetch is served
// by the leader or a follower; anywhere else we answer NOT_LEADER, so the
// partition is misrouted.
func (c *Cluster) fetchFaultKey(creq *clientReq, topic string, topicID uuid, partition int32) faultKey {
	pd, ok := c.data.tps.getp(topic, partition)
	misrouted := ok && pd.leader != creq.cc.b && !slices.Contains(pd.followers, creq.cc.b.node)
	return faultKey{topic: topic, topicID: topicID, misrouted: misrouted}.part(partition)
}

// faultEndsWait reports whether a fault answering e answers a fetch or share
// fetch at once. Kafka answers an unauthorized or unknown topic or partition
// before it reads the log, which leaves the rest of the request to wait.
func faultEndsWait(e *kerr.Error) bool {
	switch e {
	case kerr.TopicAuthorizationFailed, kerr.UnknownTopicID, kerr.UnknownTopicOrPartition:
		return false
	}
	return true
}
