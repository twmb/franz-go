package kfake

import (
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
// * If topic does not exist, we hang
// * Topic created while waiting is not returned in final response
// * If any partition is on a different broker, we return immediately
// * Out of range fetch causes early return
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
// * We always return full results (not incremental diffs) which is compliant behavior
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
	if req.Version >= 7 {
		var errCode int16
		session, newSession, errCode = c.fetchSessions.getOrCreate(creq.cc.b.node, req.SessionID, req.SessionEpoch)
		if errCode != 0 {
			resp.ErrorCode = errCode
			return resp, nil
		}

		// Handle ForgottenTopics - remove partitions from session
		for _, ft := range req.ForgottenTopics {
			topic := ft.Topic
			if req.Version >= 13 {
				topic = c.data.id2t[ft.TopicID]
			}
			for _, p := range ft.Partitions {
				session.forgetPartition(topic, p)
			}
		}

		// Set session ID in response
		if session != nil {
			resp.SessionID = session.id
		}
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
	}
	var toFetch []fetchPartition

	// First, add partitions from the request and update session
	for i, rt := range req.Topics {
		if req.Version >= 13 {
			rt.Topic = c.data.id2t[rt.TopicID]
			req.Topics[i].Topic = rt.Topic
		}
		for _, rp := range rt.Partitions {
			toFetch = append(toFetch, fetchPartition{
				topic:        rt.Topic,
				topicID:      rt.TopicID,
				partition:    rp.Partition,
				fetchOffset:  rp.FetchOffset,
				maxBytes:     rp.PartitionMaxBytes,
				currentEpoch: rp.CurrentLeaderEpoch,
			})
			// Update session with this partition's state
			session.updatePartition(rt.Topic, rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.CurrentLeaderEpoch)
		}
	}

	// For incremental fetches (session exists and not new), add session partitions
	// that weren't in the request
	if session != nil && !newSession {
		inRequest := make(map[tp]bool)
		for _, fp := range toFetch {
			inRequest[tp{fp.topic, fp.partition}] = true
		}
		for key, sp := range session.partitions {
			if !inRequest[key] {
				toFetch = append(toFetch, fetchPartition{
					topic:        key.t,
					topicID:      c.data.t2id[key.t],
					partition:    key.p,
					fetchOffset:  sp.fetchOffset,
					maxBytes:     sp.maxBytes,
					currentEpoch: sp.currentEpoch,
				})
			}
		}
	}

	var (
		readCommitted = req.IsolationLevel == 1
		nbytes        int
		returnEarly   bool
		needp         tps[int]
	)
	if w == nil {
	out:
		for _, fp := range toFetch {
			t, ok := c.data.tps.gett(fp.topic)
			if !ok {
				continue
			}
			pd, ok := t[fp.partition]
			if !ok {
				continue
			}
			if pd.leader != creq.cc.b && !pd.followers.has(creq.cc.b) {
				returnEarly = true // NotLeaderForPartition
				break out
			}
			i, ok, atEnd := pd.searchOffset(fp.fetchOffset)
			if atEnd {
				continue
			}
			if !ok {
				returnEarly = true // OffsetOutOfRange
				break out
			}
			pbytes := 0
			for _, b := range pd.batches[i:] {
				if readCommitted && b.inTx {
					break
				}
				nbytes += b.nbytes
				pbytes += b.nbytes
				if pbytes >= int(fp.maxBytes) {
					returnEarly = true
					break out
				}
			}
			needp.set(fp.topic, fp.partition, int(fp.maxBytes)-pbytes)
		}
	}

	wait := time.Duration(req.MaxWaitMillis) * time.Millisecond
	deadline := creq.at.Add(wait)
	if w == nil && !returnEarly && nbytes < int(req.MinBytes) && time.Now().Before(deadline) {
		w := &watchFetch{
			need:          int(req.MinBytes) - nbytes,
			needp:         needp,
			deadline:      deadline,
			readCommitted: readCommitted,
			creq:          creq,
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

	id2t := make(map[uuid]string)
	tidx := make(map[string]int)

	donet := func(t string, id uuid) *kmsg.FetchResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		id2t[id] = t
		tidx[t] = len(resp.Topics)
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
		if !c.allowedACL(creq, fp.topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			donep(fp.topic, fp.topicID, fp.partition, kerr.TopicAuthorizationFailed.Code)
			continue
		}
		pd, ok := c.data.tps.getp(fp.topic, fp.partition)
		if !ok {
			if req.Version >= 13 {
				donep(fp.topic, fp.topicID, fp.partition, kerr.UnknownTopicID.Code)
			} else {
				donep(fp.topic, fp.topicID, fp.partition, kerr.UnknownTopicOrPartition.Code)
			}
			continue
		}
		if pd.leader != creq.cc.b && !pd.followers.has(creq.cc.b) {
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
		sp.HighWatermark = pd.highWatermark
		sp.LastStableOffset = pd.lastStableOffset
		sp.LogStartOffset = pd.logStartOffset
		i, ok, atEnd := pd.searchOffset(fp.fetchOffset)
		if atEnd {
			continue
		}
		if !ok {
			sp.ErrorCode = kerr.OffsetOutOfRange.Code
			continue
		}

		// Track aborted transactions per producer. The kgo client expects
		// AbortedTransactions to be sorted by FirstOffset per producer.
		var abortedByProducer map[int64][]int64

		var pbytes int
		for _, b := range pd.batches[i:] {
			if readCommitted && b.inTx {
				break
			}
			if nbytes = nbytes + b.nbytes; nbytes > int(req.MaxBytes) && batchesAdded > 0 {
				break full
			}
			if pbytes = pbytes + b.nbytes; pbytes > int(fp.maxBytes) && batchesAdded > 0 {
				break
			}
			batchesAdded++

			// Track aborted transactions in returned batches
			if readCommitted && req.Version >= 4 && b.aborted {
				if abortedByProducer == nil {
					abortedByProducer = make(map[int64][]int64)
				}
				// Only add if we haven't seen this txnFirstOffset for this producer
				offsets := abortedByProducer[b.ProducerID]
				found := false
				for _, o := range offsets {
					if o == b.txnFirstOffset {
						found = true
						break
					}
				}
				if !found {
					abortedByProducer[b.ProducerID] = append(offsets, b.txnFirstOffset)
				}
			}

			sp.RecordBatches = b.AppendTo(sp.RecordBatches)
		}

		// Add aborted transactions for read_committed consumers
		// Sorted by FirstOffset per producer (as expected by kgo client)
		for producerID, offsets := range abortedByProducer {
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
			for _, firstOffset := range offsets {
				at := kmsg.NewFetchResponseTopicPartitionAbortedTransaction()
				at.ProducerID = producerID
				at.FirstOffset = firstOffset
				sp.AbortedTransactions = append(sp.AbortedTransactions, at)
			}
		}
	}

	// Bump session epoch after successful fetch, but not for newly created
	// sessions. The client will send epoch=1 for its first incremental fetch
	// after receiving the new session ID.
	if !newSession {
		session.bumpEpoch()
	}

	return resp, nil
}

type watchFetch struct {
	need     int
	needp    tps[int]
	deadline time.Time
	creq     *clientReq

	in []*partData
	cb func()
	t  *time.Timer

	readCommitted bool

	once    sync.Once
	cleaned bool
}

func (w *watchFetch) push(pd *partData, nbytes int) {
	// For readCommitted consumers, skip counting bytes from uncommitted transactional batches.
	// These bytes will be counted when the transaction commits via addBytes.
	if w.readCommitted && pd.inTx {
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
}

// fetchSessionPartition tracks per-partition state within a session.
type fetchSessionPartition struct {
	fetchOffset  int64
	maxBytes     int32
	currentEpoch int32
}

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
func (fs *fetchSessions) getOrCreate(brokerNode, sessionID, sessionEpoch int32) (*fetchSession, bool, int16) {
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
		id := fs.nextID.Add(1) - 1
		session := &fetchSession{
			id:         id,
			epoch:      1,
			partitions: make(map[tp]fetchSessionPartition),
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
	return session, false, 0
}

func (s *fetchSession) updatePartition(topic string, partition int32, fetchOffset int64, maxBytes int32, currentEpoch int32) {
	if s == nil {
		return
	}
	s.partitions[tp{topic, partition}] = fetchSessionPartition{fetchOffset, maxBytes, currentEpoch}
}

func (s *fetchSession) forgetPartition(topic string, partition int32) {
	if s == nil {
		return
	}
	delete(s.partitions, tp{topic, partition})
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
