package kfake

import (
	"hash/crc32"
	"hash/fnv"
	"math"
	"math/rand"
	"regexp"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// * Add heap of last use, add index to pidwindow, and remove pidwindow as they exhaust max # of pids configured.

// kfake transaction state simplifications:
//
// EndTxn v0-4 (pre-KIP-890) does not bump the producer epoch; the
// same epoch is reused across transactions. EndTxn v5+ (KIP-890)
// bumps the epoch on each commit/abort and returns the new epoch.
// Produce v12+ implicitly adds partitions to the transaction.
//
// kfake completes a transaction synchronously unless a marker fails to
// write. The transaction then stays open with its decision in ending,
// DescribeTransactions shows PrepareCommit or PrepareAbort, and EndTxn,
// InitProducerID, AddPartitionsToTxn, and AddOffsetsToTxn answer
// CONCURRENT_TRANSACTIONS until a retry writes the rest. With DataDir, a
// restart also sees the prepare entry that pids.log keeps for a crash
// during EndTxn.
//
// State tracking: instead of the full transaction state machine
// (Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit,
// CompleteAbort, Dead, PrepareEpochFence), kfake uses:
//   - inTx (bool): true = Ongoing
//   - ending (*bool): non-nil = PrepareCommit or PrepareAbort
//   - ended (bool): false = Empty (no transaction ended since the
//     last InitProducerID), true = CompleteCommit or CompleteAbort
//   - lastWasCommit (bool): which of the two Complete states

type (
	pids struct {
		ids     map[int64]*pidinfo
		byTxid  map[string]*pidinfo   // txid -> pidinfo, for expiration
		txs     map[*pidinfo]struct{} // active transactions being tracked for timeout
		txTimer *time.Timer
		c       *Cluster

		producerCheck time.Time // when expireProducers last ran
	}

	// Seq/txn info for a given individual producer ID.
	pidinfo struct {
		pids *pids

		id      int64
		epoch   int16
		windows tps[pidwindow] // topic/partition 5-window pid sequences

		// The epoch before the last bump. A KIP-360 InitProducerID at
		// this epoch gets our epoch without a bump, and -1 fences every
		// older epoch.
		lastEpoch int16

		// The producer ID before an epoch overflow rotated to id, so
		// that we accept a retry from a client that missed the
		// rotation. As in Kafka, the next transition clears it: a
		// transaction start, an end, or an InitProducerID.
		prevID int64

		txid         string
		txTimeout    int32                        // millis
		txParts      tps[partData]                // partitions in the transaction, if transactional
		txBatchCount int                          // number of batches in the transaction
		txGroups     []string                     // consumer groups in the transaction
		txOffsets    map[string]tps[offsetCommit] // pending offset commits per group

		// Track per-partition first offset for this transaction.
		// Used for AbortedTransactions in fetch response.
		txPartFirstOffsets tps[int64]

		// Track bytes per partition for this transaction.
		// Used to count committed bytes for readCommitted watchers.
		txPartBytes tps[int]

		txStart    time.Time // start of the current or last transaction, zero after InitProducerID
		lastActive time.Time // last time this transactional ID was used
		inTx       bool

		// The decision of an end whose markers are not all written, and
		// their epoch. The transaction stays open until a retry of the
		// end, an InitProducerID, or the transaction timer writes the
		// rest. endClearsLast is set while our own abort below
		// transaction version 2 is pending: completing the abort clears
		// the last epoch.
		ending        *bool
		endEpoch      int16
		endClearsLast bool

		// Whether a transaction ended since the last InitProducerID,
		// and if so, whether it was a commit. EndTxn accepts a retry
		// of the end only in the same direction (commit after commit,
		// abort after abort); with no end to retry, an EndTxn at an
		// old epoch is fenced.
		ended         bool
		lastWasCommit bool
	}

	// Independent dup-detection entry: stores (firstSeq, nextSeq,
	// offset) for one committed batch. Real Kafka stores 5
	// independent BatchMetadata records; we match that.
	pidEntry struct {
		firstSeq int32
		nextSeq  int32
		offset   int64
	}

	// Sequence ID window for duplicate detection. 5 independent
	// entries (not overlapping pairs), so all 5 in-flight batches
	// allowed by maxInFlight=5 are deduplicatable after a
	// connection death.
	pidwindow struct {
		entries [5]pidEntry
		at      uint8 // next write position (circular)
		count   uint8 // number of valid entries (0-5)
		seen    bool  // true after the first batch has been accepted
		epoch   int16 // last seen epoch; when epoch changes, seq 0 is accepted
		nextSeq int32 // next expected firstSequence (authoritative, survives window rotation)

		// lastTimestamp is what DescribeProducers reports and what
		// producer.id.expiration.ms counts from: the max timestamp of
		// the producer's last batch here, or the time of its last
		// marker, as in Kafka.
		lastTimestamp int64
	}
)

// hasUnstableOffsets returns whether an open transaction holds a pending
// offset commit for the group. Offset expiry does not delete such a group.
func (pids *pids) hasUnstableOffsets(group string) bool {
	for pidinf := range pids.txs {
		if _, hasGroup := pidinf.txOffsets[group]; hasGroup {
			return true
		}
	}
	return false
}

// hasUnstableOffset reports whether an open transaction holds a pending
// offset commit for the group's partition. Kafka answers OffsetFetch
// with RequireStable per partition.
func (pids *pids) hasUnstableOffset(group, topic string, part int32) bool {
	for pidinf := range pids.txs {
		if offsets, ok := pidinf.txOffsets[group]; ok && offsets.checkp(topic, part) {
			return true
		}
	}
	return false
}

// updateTimer reschedules the transaction timeout timer based on the
// next expiring transaction. This also handles any already-expired
// transactions inline: under high request throughput, this function
// runs after every request and drains the timer channel. Without the
// inline check, the timer fires (negative duration for expired txns)
// but gets drained here before mainSelect can read it, creating a
// livelock where expired transactions are never aborted.
func (pids *pids) updateTimer() {
	if !pids.txTimer.Stop() {
		select {
		case <-pids.txTimer.C:
		default:
		}
	}

	// Abort any already-expired transactions inline.
	now := time.Now()
	for {
		var minPid *pidinfo
		var minExpire time.Time
		for pidinf := range pids.txs {
			expire := pidinf.txExpire()
			if minPid == nil || expire.Before(minExpire) {
				minPid = pidinf
				minExpire = expire
			}
		}
		if minPid == nil || now.Before(minExpire) {
			break
		}
		if minPid.ending != nil {
			if p, ok := minPid.endTx(*minPid.ending, minPid.endEpoch); ok {
				p.persist()
			}
			continue
		}
		elapsed := now.Sub(minPid.txStart)
		pids.c.cfg.logger.Logf(LogLevelWarn,
			"txn timeout abort: txn_id=%s producer_id=%d epoch=%d timeout=%dms elapsed=%v",
			minPid.txid, minPid.id, minPid.epoch, minPid.txTimeout, elapsed)
		if p, ok := minPid.fenceAbort(); ok {
			p.persist()
		}
	}

	// Schedule timer for the next future expiry.
	var nextExpire time.Time
	var found bool
	for pidinf := range pids.txs {
		expire := pidinf.txExpire()
		if !found || expire.Before(nextExpire) {
			nextExpire = expire
			found = true
		}
	}
	// Also schedule for the next transactional ID expiration, and the
	// next producer expiration check.
	expirationMs := int64(pids.c.txnIDExpirationMs())
	for _, pidinf := range pids.byTxid {
		if pidinf.inTx {
			continue
		}
		expire := pidinf.lastActive.Add(time.Duration(expirationMs) * time.Millisecond)
		if !found || expire.Before(nextExpire) {
			nextExpire = expire
			found = true
		}
	}
	if pids.producerCheck.IsZero() {
		pids.producerCheck = now
	}
	if check := pids.producerCheck.Add(producerExpirationCheckInterval); len(pids.ids) > 0 && (!found || check.Before(nextExpire)) {
		nextExpire = check
		found = true
	}
	if found {
		pids.txTimer.Reset(time.Until(nextExpire))
	}
}

// handleTimeout is called when the transaction timer fires in
// mainSelect. It expires transactional IDs and reschedules the timer.
// Transaction abort for expired txns is handled inline by updateTimer,
// which runs after every request - this ensures aborts happen even
// under high throughput when the timer channel gets starved.
func (pids *pids) handleTimeout() {
	pids.expireTransactionalIDs()
	if time.Since(pids.producerCheck) >= producerExpirationCheckInterval {
		pids.expireProducers()
	}
	pids.updateTimer()
}

// producerExpirationCheckInterval is Kafka's default
// producer.id.expiration.check.interval.ms.
const producerExpirationCheckInterval = 10 * time.Minute

// expireProducers drops a producer's state on each partition where it has
// no open transaction and has written nothing for producer.id.expiration.ms,
// counted from its last batch's timestamp, as Kafka does. An idempotent
// producer left with no partition is gone; a transactional ID keeps its
// producer until transactional.id.expiration.ms.
func (pids *pids) expireProducers() {
	pids.producerCheck = time.Now()
	cutoff := pids.producerCheck.UnixMilli() - int64(pids.c.producerIDExpirationMs())
	for id, pidinf := range pids.ids {
		for t, ps := range pidinf.windows {
			txParts := pidinf.txPartFirstOffsets[t]
			for p, pw := range ps {
				if _, inTx := txParts[p]; pw.lastTimestamp <= cutoff && !inTx {
					pidinf.windows.delp(t, p)
				}
			}
		}
		if pidinf.txid == "" && len(pidinf.windows) == 0 {
			delete(pids.ids, id)
			pids.c.persistPIDEntry(pidLogEntry{Type: "expire", PID: id})
		}
	}
}

func (pids *pids) doInitProducerID(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.InitProducerIDRequest)
	resp := req.ResponseKind().(*kmsg.InitProducerIDResponse)

	if req.TransactionalID != nil {
		txid := *req.TransactionalID
		if txid == "" {
			resp.ErrorCode = kerr.InvalidRequest.Code
			return resp
		}
		coordinator := pids.c.coordinator(txid)
		if creq.cc.b != coordinator {
			resp.ErrorCode = kerr.NotCoordinator.Code
			return resp
		}
		if req.TransactionTimeoutMillis <= 0 || req.TransactionTimeoutMillis > pids.c.transactionMaxTimeoutMs() {
			resp.ErrorCode = kerr.InvalidTransactionTimeout.Code
			return resp
		}
	}

	// A producer ID and epoch come together or not at all.
	if (req.ProducerID == -1) != (req.ProducerEpoch == -1) {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp
	}

	// Idempotent-only, or a transactional ID we do not know (new or
	// expired): a fresh producer ID at epoch 0, whatever producer ID
	// the request carries.
	var pidinf *pidinfo
	if req.TransactionalID != nil {
		pidinf = pids.byTxid[*req.TransactionalID]
	}
	if pidinf == nil {
		pidinf = pids.create(req.TransactionalID, req.TransactionTimeoutMillis)
		pidinf.persist()
		resp.ProducerID = pidinf.id
		resp.ProducerEpoch = pidinf.epoch
		return resp
	}

	// KIP-360 (v3+): the request's producer ID must be the transactional
	// ID's, or the one an epoch overflow rotated away from.
	if req.ProducerID != -1 && req.ProducerID != pidinf.id &&
		(req.ProducerID != pidinf.prevID || req.ProducerEpoch < math.MaxInt16-1) {
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	}

	// Abort any in-flight transaction before deciding on the epoch.
	// Kafka fences the producer and answers CONCURRENT_TRANSACTIONS, and
	// decides the client's retry against the epoch the abort left; we
	// abort immediately and decide in one response. Leaving the old
	// epoch's writes pending lets the next commit swallow them into its
	// own range.
	if pidinf.inTx {
		var ended bool
		if pidinf.ending != nil {
			pidinf, ended = pidinf.endTx(*pidinf.ending, pidinf.endEpoch)
		} else {
			pidinf, ended = pidinf.fenceAbort()
		}
		if !ended {
			resp.ErrorCode = kerr.ConcurrentTransactions.Code
			return resp
		}
		pidinf.persist()
	}

	// A KIP-360 request at our epoch bumps it, and one at our last epoch
	// gets our epoch without a bump. Any other epoch is fenced.
	if req.ProducerID != -1 && req.ProducerEpoch != pidinf.epoch && req.ProducerEpoch != pidinf.lastEpoch {
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	}
	pidinf.prevID = 0
	switch {
	case req.ProducerID == -1:
		pidinf = pids.bumpEpoch(pidinf)
		pidinf.lastEpoch = -1
	case req.ProducerEpoch == pidinf.epoch:
		pidinf = pids.bumpEpoch(pidinf)
	}
	pidinf.txTimeout = req.TransactionTimeoutMillis
	pidinf.txStart = time.Time{}
	pidinf.ended = false
	pidinf.lastActive = time.Now()
	pidinf.persist()
	resp.ProducerID = pidinf.id
	resp.ProducerEpoch = pidinf.epoch
	return resp
}

func (pidinf *pidinfo) maybeStart() {
	if pidinf.inTx {
		return
	}
	pidinf.inTx = true
	pidinf.prevID = 0
	pidinf.txStart = time.Now()
	pidinf.pids.txs[pidinf] = struct{}{}
	// If we crash before EndTxn logs its prepare entry, the restart
	// aborts the transaction.
	pidinf.pids.c.persistPIDEntry(pidLogEntry{Type: "begin", PID: pidinf.id})
}

// logEntry returns the pids.log entry that restores the producer as it
// is now, replacing everything logged before for its producer ID.
func (pidinf *pidinfo) logEntry() pidLogEntry {
	e := pidLogEntry{
		Type:    "init",
		PID:     pidinf.id,
		Epoch:   pidinf.epoch,
		Last:    pidinf.lastEpoch,
		TxID:    pidinf.txid,
		Timeout: pidinf.txTimeout,
		PrevID:  pidinf.prevID,
	}
	if pidinf.ended {
		e.Commit = &pidinf.lastWasCommit
	}
	return e
}

func (pidinf *pidinfo) persist() {
	pidinf.pids.c.persistPIDEntry(pidinf.logEntry())
}

// txnPid returns the transactional ID's producer if it has the producer
// ID, or the error a transaction coordinator answers otherwise.
func (pids *pids) txnPid(txid string, id int64) (*pidinfo, int16) {
	if txid == "" {
		return nil, kerr.InvalidRequest.Code
	}
	pidinf := pids.byTxid[txid]
	if pidinf == nil || pidinf.id != id {
		return nil, kerr.InvalidProducerIDMapping.Code
	}
	return pidinf, 0
}

// doAddPartitions adds, or with VerifyOnly checks, one transaction's
// partitions. fromClient is true for v0-3, which need WRITE on the
// transactional ID and each topic.
func (pids *pids) doAddPartitions(creq *clientReq, t *kmsg.AddPartitionsToTxnRequestTransaction, fromClient bool) kmsg.AddPartitionsToTxnResponseTransaction {
	c := pids.c
	txnID := t.TransactionalID
	st := kmsg.NewAddPartitionsToTxnResponseTransaction()
	st.TransactionalID = txnID

	tidx := make(map[string]int)
	donep := func(topic string, p int32, errCode int16) {
		i, ok := tidx[topic]
		if !ok {
			i = len(st.Topics)
			tidx[topic] = i
			rt := kmsg.NewAddPartitionsToTxnResponseTransactionTopic()
			rt.Topic = topic
			st.Topics = append(st.Topics, rt)
		}
		sp := kmsg.NewAddPartitionsToTxnResponseTransactionTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st.Topics[i].Partitions = append(st.Topics[i].Partitions, sp)
	}
	doneall := func(errCode int16) {
		for _, rt := range t.Topics {
			for _, p := range rt.Partitions {
				donep(rt.Topic, p, errCode)
			}
		}
	}

	// Clients (v0-3) need WRITE on the transactional ID and each topic;
	// brokers (v4+) passed the cluster check. Faults fire only on the
	// transaction coordinator; elsewhere we answer NOT_COORDINATOR below.
	isCoordinator := c.isCoordinator(creq, txnID)
	deny := func(resource string, rt kmsg.ACLResourceType, k faultKey) *kerr.Error {
		k.misrouted = !isCoordinator
		if fromClient {
			return c.deny(creq, resource, rt, kmsg.ACLOperationWrite, k)
		}
		return creq.faults.check(k)
	}
	if e := deny(txnID, kmsg.ACLResourceTypeTransactionalId, faultKey{txnID: txnID}); e != nil {
		doneall(e.Code)
		return st
	}

	// An unauthorized, unknown, or faulted partition fails the whole
	// transaction: those partitions get their error and the rest are
	// not attempted.
	type tpKey struct {
		t string
		p int32
	}
	pdMap := make(map[tpKey]*partData)
	failed := make(map[tpKey]int16)
	for _, rt := range t.Topics {
		ps, _ := c.data.tps.gett(rt.Topic)
		for _, p := range rt.Partitions {
			k := tpKey{rt.Topic, p}
			pd := ps[p]
			if e := deny(rt.Topic, kmsg.ACLResourceTypeTopic, faultKey{txnID: txnID, topic: rt.Topic}.part(p)); e != nil {
				failed[k] = e.Code
				continue
			}
			if pd == nil {
				failed[k] = kerr.UnknownTopicOrPartition.Code
				continue
			}
			pdMap[k] = pd
		}
	}
	if len(failed) > 0 {
		for _, rt := range t.Topics {
			for _, p := range rt.Partitions {
				code, ok := failed[tpKey{rt.Topic, p}]
				if !ok {
					code = kerr.OperationNotAttempted.Code
				}
				donep(rt.Topic, p, code)
			}
		}
		return st
	}

	if !isCoordinator {
		doneall(kerr.NotCoordinator.Code)
		return st
	}

	// A pending end decided at a higher epoch already moved the epoch, as
	// Kafka does when it writes the decision.
	pidinf, code := pids.txnPid(txnID, t.ProducerID)
	if code == 0 && (pidinf.epoch != t.ProducerEpoch || pidinf.ending != nil && pidinf.endEpoch > t.ProducerEpoch) {
		code = kerr.ProducerFenced.Code
	} else if code == 0 && pidinf.ending != nil {
		code = kerr.ConcurrentTransactions.Code
	}
	if code != 0 {
		doneall(code)
		return st
	}

	if t.VerifyOnly {
		for _, rt := range t.Topics {
			for _, p := range rt.Partitions {
				code := kerr.TransactionAbortable.Code
				if _, ok := pidinf.txParts.getp(rt.Topic, p); ok && pidinf.inTx {
					code = 0
				}
				donep(rt.Topic, p, code)
			}
		}
		return st
	}

	for _, rt := range t.Topics {
		for _, p := range rt.Partitions {
			ps := pidinf.txParts.mkt(rt.Topic)
			ps[p] = pdMap[tpKey{rt.Topic, p}]
			donep(rt.Topic, p, 0)
		}
	}
	pidinf.maybeStart()
	return st
}

func (pids *pids) doAddOffsets(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.AddOffsetsToTxnRequest)
	resp := req.ResponseKind().(*kmsg.AddOffsetsToTxnResponse)

	coordinator := pids.c.coordinator(req.TransactionalID)
	if creq.cc.b != coordinator {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp
	}

	// A pending end decided at a higher epoch already moved the epoch, as
	// Kafka does when it writes the decision.
	pidinf, code := pids.txnPid(req.TransactionalID, req.ProducerID)
	if code == 0 && (pidinf.epoch != req.ProducerEpoch || pidinf.ending != nil && pidinf.endEpoch > req.ProducerEpoch) {
		code = kerr.ProducerFenced.Code
	} else if code == 0 && pidinf.ending != nil {
		code = kerr.ConcurrentTransactions.Code
	}
	if code != 0 {
		resp.ErrorCode = code
		return resp
	}

	pidinf.maybeStart()
	if !slices.Contains(pidinf.txGroups, req.Group) {
		pidinf.txGroups = append(pidinf.txGroups, req.Group)
	}
	return resp
}

func (pids *pids) doTxnOffsetCommit(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.TxnOffsetCommitRequest)
	resp := req.ResponseKind().(*kmsg.TxnOffsetCommitResponse)

	doneall := func(errCode int16) {
		for _, rt := range req.Topics {
			st := kmsg.NewTxnOffsetCommitResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = errCode
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
	}

	if req.Group == "" {
		doneall(kerr.InvalidGroupID.Code)
		return resp
	}
	coordinator := pids.c.coordinator(req.Group)
	if creq.cc.b != coordinator {
		doneall(kerr.NotCoordinator.Code)
		return resp
	}

	pidinf := pids.getpid(req.ProducerID)
	if pidinf == nil || pidinf.txid == "" || pidinf.txid != req.TransactionalID {
		doneall(kerr.InvalidProducerIDMapping.Code)
		return resp
	}
	// The group coordinator, not the transaction coordinator, answers
	// a stale epoch, with the produce path's code.
	if pidinf.epoch != req.ProducerEpoch {
		doneall(kerr.InvalidProducerEpoch.Code)
		return resp
	}

	// Like Kafka, a share group takes no offset commits.
	if _, isShare := pids.c.shareGroups.gs[req.Group]; isShare {
		doneall(kerr.GroupIDNotFound.Code)
		return resp
	}

	// KIP-890 Part 2: For v5+ requests, implicitly start the transaction
	// if not already started. For v0-4, require transaction to be active.
	if !pidinf.inTx {
		if req.Version < 5 {
			doneall(kerr.InvalidTxnState.Code)
			return resp
		}
		pidinf.maybeStart()
	}

	// Check if group exists. For generation >= 0, return
	// ILLEGAL_GENERATION if the group doesn't exist. For
	// generation < 0 (admin commits), Kafka creates a simple group
	// when it stores an offset.
	g := pids.c.groups.gs[req.Group]
	if g != nil {
		errCode := g.validateMemberGeneration(req.MemberID, req.InstanceID, req.Generation, req.Topics)
		// Before v6, a stale member epoch was reported as
		// ILLEGAL_GENERATION (KIP-1319).
		if errCode == kerr.StaleMemberEpoch.Code && req.Version < 6 {
			errCode = kerr.IllegalGeneration.Code
		}
		if errCode != 0 {
			doneall(errCode)
			return resp
		}
	} else if req.Generation >= 0 {
		// A group that does not exist is GROUP_ID_NOT_FOUND from v6;
		// older versions mapped it to ILLEGAL_GENERATION (KIP-1319).
		if req.Version >= 6 {
			doneall(kerr.GroupIDNotFound.Code)
		} else {
			doneall(kerr.IllegalGeneration.Code)
		}
		return resp
	}

	groupInTx := slices.Contains(pidinf.txGroups, req.Group)

	// KIP-890: For v5+ requests, implicitly add the group to the transaction
	// if it's not already there. This allows clients to skip AddOffsetsToTxn.
	if !groupInTx {
		if req.Version < 5 {
			doneall(kerr.InvalidTxnState.Code)
			return resp
		}
		pidinf.txGroups = append(pidinf.txGroups, req.Group)
	}

	// Store pending offset commits; will be actually mirrored into
	// the group offsets once the transaction ends with a commit.
	// Validate that each topic/partition exists first.
	if pidinf.txOffsets == nil {
		pidinf.txOffsets = make(map[string]tps[offsetCommit])
	}
	groupOffsets := pidinf.txOffsets[req.Group]
	for _, rt := range req.Topics {
		st := kmsg.NewTxnOffsetCommitResponseTopic()
		st.Topic = rt.Topic
		for _, rp := range rt.Partitions {
			sp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
			sp.Partition = rp.Partition
			e := creq.faults.check(faultKey{txnID: req.TransactionalID, group: req.Group, topic: rt.Topic}.part(rp.Partition))
			if e != nil {
				sp.ErrorCode = e.Code
			}
			switch {
			case e != nil && creq.skipsWork(e): // a timed-out commit is still stored
			case !pids.c.data.tps.checkp(rt.Topic, rp.Partition):
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			case offsetMetadataTooLarge(rp.Metadata):
				sp.ErrorCode = kerr.OffsetMetadataTooLarge.Code
			default:
				if g == nil {
					g = pids.c.groups.newGroup(req.Group)
					pids.c.groups.gs[req.Group] = g
				}
				groupOffsets.set(rt.Topic, rp.Partition, offsetCommit{
					offset:      rp.Offset,
					leaderEpoch: rp.LeaderEpoch,
					metadata:    rp.Metadata,
					lastCommit:  time.Now(),
				})
			}
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
	}
	pidinf.txOffsets[req.Group] = groupOffsets
	return resp
}

func (pids *pids) doEnd(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.EndTxnRequest)
	resp := req.ResponseKind().(*kmsg.EndTxnResponse)

	coordinator := pids.c.coordinator(req.TransactionalID)
	if creq.cc.b != coordinator {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp
	}

	// v5 (KIP-890) bumps the epoch at each end, so a retry of an end
	// that went through carries the epoch below ours, or the last epoch
	// of the producer ID an overflow rotated away from.
	pidinf := pids.byTxid[req.TransactionalID]
	retryOnOverflow := req.Version >= 5 && pidinf != nil && pidinf.prevID == req.ProducerID &&
		req.ProducerEpoch == math.MaxInt16-1 && pidinf.epoch == 0
	if !retryOnOverflow {
		var code int16
		if pidinf, code = pids.txnPid(req.TransactionalID, req.ProducerID); code != 0 {
			resp.ErrorCode = code
			return resp
		}
	}
	retry := req.Version >= 5 && pidinf.ended && !pidinf.inTx &&
		(retryOnOverflow || pidinf.epoch == req.ProducerEpoch+1)

	switch {
	case retry && req.Commit == pidinf.lastWasCommit:
		resp.ProducerID = pidinf.id
		resp.ProducerEpoch = pidinf.epoch
		return resp
	case retry && req.Commit:
		// Our own abort (a timeout) raced the commit, which did not
		// take effect.
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	case retry:
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return resp
	case pidinf.epoch != req.ProducerEpoch:
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	case req.Version < 5 && pidinf.ending != nil && pidinf.endEpoch > req.ProducerEpoch:
		// When Kafka decides our own abort, Kafka moves the epoch,
		// which fences an end below v5 at the old epoch.
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	case pidinf.ending != nil && req.Commit != *pidinf.ending:
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return resp
	case !pidinf.inTx && req.Version < 5:
		// Before KIP-890, only a retry of the last end succeeds.
		if !pidinf.ended || req.Commit != pidinf.lastWasCommit {
			resp.ErrorCode = kerr.InvalidTxnState.Code
		}
		return resp
	case !pidinf.inTx:
		// v5 ends a transaction that added nothing, as an empty
		// commit or abort that bumps the epoch.
		pidinf.txStart = time.Now()
	}

	// A v5 end bumps the epoch, and Kafka writes its markers at the new
	// epoch. A pending end keeps the epoch it was decided at.
	markerEpoch := pidinf.epoch
	if req.Version >= 5 {
		markerEpoch++
	}
	if pidinf.ending != nil {
		markerEpoch = pidinf.endEpoch
	}

	// The decision goes to disk before any marker or offset: a restart
	// finishes an end that a crash cut off, as Kafka does from its
	// prepare record. Kafka answers a failed write NOT_COORDINATOR, and
	// the client retries.
	if err := pids.c.persistPIDEntry(pidLogEntry{Type: "prepare", PID: pidinf.id, Epoch: markerEpoch, ClearLast: pidinf.endClearsLast, Commit: &req.Commit, Offsets: persistTxOffsets(pidinf.txOffsets)}); err != nil {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp
	}

	nBatches, nGroups := pidinf.txBatchCount, len(pidinf.txGroups)
	endTxStart := time.Now()
	pidinf, ended := pidinf.endTx(req.Commit, markerEpoch)
	if elapsed := time.Since(endTxStart); elapsed > 5*time.Millisecond {
		pids.c.cfg.logger.Logf(LogLevelWarn, "txn: EndTxn slow pid=%d epoch=%d elapsed=%dms batches=%d groups=%d",
			req.ProducerID, req.ProducerEpoch, elapsed.Milliseconds(), nBatches, nGroups)
	}
	if !ended { // Kafka's answer while the markers are pending
		resp.ErrorCode = kerr.ConcurrentTransactions.Code
		return resp
	}

	// KIP-890: For v5+ clients, return the bumped ID/epoch.
	if req.Version >= 5 {
		resp.ProducerID = pidinf.id
		resp.ProducerEpoch = pidinf.epoch
	}
	pidinf.persist()
	return resp
}

func (pids *pids) getpid(id int64) *pidinfo {
	return pids.ids[id]
}

// get returns the pidinfo and idempotent-5 window for a producer on a
// topic-partition. If pd is non-nil and the producer is transactional
// but hasn't registered this partition, the partition is implicitly
// added to the transaction (KIP-890 implicit addition for produce
// v12+). If pd is nil, unregistered transactional partitions cause
// (nil, nil) to be returned.
func (pids *pids) get(id int64, t string, p int32, pd *partData) (*pidinfo, *pidwindow) {
	pidinf := pids.ids[id]
	if pidinf == nil {
		return nil, nil
	}
	if pidinf.txid != "" && !pidinf.txParts.checkp(t, p) {
		if pd == nil {
			return nil, nil
		}
		ps := pidinf.txParts.mkt(t)
		ps[p] = pd
		pidinf.maybeStart()
	}
	return pidinf, pidinf.windows.mkpDefault(t, p)
}

// getOrCreateNonTx returns or creates producer state for a non-transactional
// idempotent produce. Apache Kafka and Redpanda implicitly create producer
// state on demand when the first batch arrives for an unknown (PID, epoch).
// See twmb/franz-go#1281. We log a producer we create, so that a crash
// restart keeps the sequence windows it rebuilds for it.
func (pids *pids) getOrCreateNonTx(id int64, epoch int16, t string, p int32) (*pidinfo, *pidwindow) {
	pidinf := pids.ids[id]
	if pidinf == nil {
		pidinf = &pidinfo{
			pids:       pids,
			id:         id,
			epoch:      epoch,
			lastActive: time.Now(),
		}
		pids.ids[id] = pidinf
		pidinf.persist()
	}
	return pidinf, pidinf.windows.mkpDefault(t, p)
}

func (pids *pids) randomID() int64 {
	for {
		id := int64(rand.Uint64()) & math.MaxInt64
		if _, exists := pids.ids[id]; !exists {
			return id
		}
	}
}

// bumpEpoch increments the epoch, and the old epoch becomes the last epoch.
// If the epoch reaches the exhaustion threshold (math.MaxInt16 - 1), a new
// producer ID is allocated, and the caller must have ended any transaction.
func (pids *pids) bumpEpoch(pidinf *pidinfo) *pidinfo {
	if pidinf.epoch >= math.MaxInt16-1 {
		newID := pids.randomID()
		newPidinf := &pidinfo{
			pids:          pids,
			id:            newID,
			epoch:         0,
			lastEpoch:     pidinf.epoch,
			prevID:        pidinf.id,
			txid:          pidinf.txid,
			txTimeout:     pidinf.txTimeout,
			txStart:       pidinf.txStart,
			lastActive:    pidinf.lastActive,
			ended:         pidinf.ended,
			lastWasCommit: pidinf.lastWasCommit,
		}
		pids.ids[newID] = newPidinf
		delete(pids.ids, pidinf.id)
		if pidinf.txid != "" {
			pids.byTxid[pidinf.txid] = newPidinf
		}
		return newPidinf
	}
	pidinf.lastEpoch = pidinf.epoch
	pidinf.epoch++
	return pidinf
}

// create allocates a producer ID at epoch 0, for the transactional ID if
// one is given.
func (pids *pids) create(txidp *string, txTimeout int32) *pidinfo {
	pidinf := &pidinfo{
		pids:       pids,
		id:         pids.randomID(),
		lastEpoch:  -1,
		lastActive: time.Now(),
	}
	if txidp != nil {
		hasher := fnv.New64()
		hasher.Write([]byte(*txidp))
		if id := int64(hasher.Sum64()) & math.MaxInt64; pids.ids[id] == nil {
			pidinf.id = id // otherwise a hash collision, keep the random ID
		}
		pidinf.txid = *txidp
		pidinf.txTimeout = txTimeout
		pids.byTxid[*txidp] = pidinf
	}
	pids.ids[pidinf.id] = pidinf
	return pidinf
}

// expireTransactionalIDs removes idle transactional IDs that haven't been
// used within transactional.id.expiration.ms and are not in an active
// transaction.
func (pids *pids) expireTransactionalIDs() {
	expirationMs := int64(pids.c.txnIDExpirationMs())
	now := time.Now()
	for txid, pidinf := range pids.byTxid {
		if pidinf.inTx {
			continue
		}
		if now.Sub(pidinf.lastActive).Milliseconds() >= expirationMs {
			delete(pids.ids, pidinf.id)
			delete(pids.byTxid, txid)
			pids.c.persistPIDEntry(pidLogEntry{Type: "expire", PID: pidinf.id})
		}
	}
}

// txnMarkerBatch builds the control batch that ends a transaction: one
// control record whose key names the marker type and whose value is the
// EndTxnMarker (version and coordinator epoch), as the transaction
// coordinator writes on a real broker. Returns the batch and its encoded
// size.
func txnMarkerBatch(pid int64, epoch int16, commit bool, coordinatorEpoch int32) (kmsg.RecordBatch, int) {
	key := kmsg.ControlRecordKey{Type: kmsg.ControlRecordKeyTypeAbort}
	if commit {
		key.Type = kmsg.ControlRecordKeyTypeCommit
	}
	marker := kmsg.EndTxnMarker{CoordinatorEpoch: coordinatorEpoch}
	rec := kmsg.Record{
		Key:   key.AppendTo(nil),
		Value: marker.AppendTo(nil),
	}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1) // -1 because length itself is encoded as a varint, and varint_length(record_length) == 1 byte
	now := time.Now().UnixMilli()
	b := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           int16(0b00000000_00110000), // control + transactional
		LastOffsetDelta:      0,
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           pid,
		ProducerEpoch:        epoch,
		FirstSequence:        -1,
		NumRecords:           1,
		Records:              rec.AppendTo(nil),
	}
	benc := b.AppendTo(nil)
	b.Length = int32(len(benc) - 12)
	b.CRC = int32(crc32.Checksum(benc[21:], crc32c))
	return b, len(benc)
}

// endTx writes the transaction's markers at epoch. Once every marker is
// written, it ends the transaction and moves the producer to epoch, and
// returns the producer, which a bump at an exhausted epoch replaces.
// Otherwise, the transaction stays open with the decision, without the
// partitions whose markers were written.
func (pidinf *pidinfo) endTx(commit bool, epoch int16) (*pidinfo, bool) {
	c := pidinf.pids.c
	written := true
	pidinf.txParts.each(func(t string, p int32, _ *partData) {
		// Markers resolve the partition by name at write time, like the
		// coordinator's marker writes on a real broker: if the topic
		// was deleted there is nowhere to write, and if it was deleted
		// and recreated the marker belongs to the CURRENT log at the
		// current log's offsets (where any post-recreation by-name
		// writes of this transaction landed). Writing through the
		// registration-time pointer would corrupt a recreated topic's
		// fresh log on disk: partDir finds the directory by the name's
		// current topic ID.
		pd, ok := c.data.tps.getp(t, p)
		// kfake is its own coordinator and has no coordinator epoch, so
		// the marker carries 0, the same epoch DescribeProducers reports.
		if ok && c.writeTxnMarker(pd, pidinf.id, epoch, commit, 0) < 0 {
			c.cfg.logger.Logf(LogLevelError, "endTx: failed to persist control batch for %s p%d pid %d", t, p, pidinf.id)
			written = false
			return
		}
		pidinf.txParts.delp(t, p)
	})
	if !written {
		pidinf.ending, pidinf.endEpoch, pidinf.lastActive = &commit, epoch, time.Now()
		pidinf.pids.txs[pidinf] = struct{}{}
		return pidinf, false
	}

	// Apply pending offset commits to groups.
	if commit {
		for groupID, groupOffsets := range pidinf.txOffsets {
			if len(groupOffsets) == 0 {
				continue
			}
			g := pidinf.pids.c.groups.gs[groupID]
			if g == nil {
				g = pidinf.pids.c.groups.newGroup(groupID)
				pidinf.pids.c.groups.gs[groupID] = g
			}
			groupOffsets.each(func(t string, p int32, oc *offsetCommit) {
				// The newest commit wins by when we received it, so
				// a regular commit after this TxnOffsetCommit keeps
				// its offset. The commit keeps its TxnOffsetCommit
				// time.
				if prev, ok := g.commits.getp(t, p); ok && prev.lastCommit.After(oc.lastCommit) {
					return
				}
				g.commits.set(t, p, *oc)
				g.c.persistGroupEntry(g.commitEntry(t, p, *oc))
			})
		}
	}

	clearsLast := pidinf.endClearsLast
	pidinf.resetTx(commit)
	if epoch > pidinf.epoch {
		pidinf = pidinf.pids.bumpEpoch(pidinf)
	}
	if clearsLast {
		pidinf.lastEpoch = -1
	}
	return pidinf, true
}

// Aborts an open transaction on our own: after a timeout, an
// InitProducerID, or a crash restart. The abort bumps the epoch, which
// fences the producer, and Kafka writes its markers at the bumped epoch.
// Below transaction version 2, Kafka also clears the last epoch, so that
// a KIP-360 InitProducerID from the aborted producer is fenced as well.
func (pidinf *pidinfo) fenceAbort() (*pidinfo, bool) {
	pidinf.endClearsLast = pidinf.pids.c.featureLevel("transaction.version") < 2
	return pidinf.endTx(false, pidinf.epoch+1)
}

// txExpire returns when the timer aborts the transaction, or writes the
// rest of an end's markers.
func (pidinf *pidinfo) txExpire() time.Time {
	if pidinf.ending != nil {
		return pidinf.lastActive.Add(time.Second)
	}
	return pidinf.txStart.Add(time.Duration(pidinf.txTimeout) * time.Millisecond)
}

func (pidinf *pidinfo) resetTx(wasCommit bool) {
	pidinf.txParts = nil
	pidinf.txBatchCount = 0
	pidinf.txGroups = nil
	pidinf.txOffsets = nil
	pidinf.txPartFirstOffsets = nil
	pidinf.txPartBytes = nil
	pidinf.inTx = false
	pidinf.ending = nil
	pidinf.endClearsLast = false
	pidinf.ended = true
	pidinf.lastWasCommit = wasCommit
	pidinf.prevID = 0
	pidinf.lastActive = time.Now()
	delete(pidinf.pids.txs, pidinf)
}

// nextSeq returns the sequence after a batch. Sequences wrap from
// math.MaxInt32 to 0.
func nextSeq(firstSeq, numRecs int32) int32 {
	return int32((int64(firstSeq) + int64(numRecs)) & math.MaxInt32)
}

// pushAndValidate checks the sequence number against the window and
// returns whether the batch is valid and whether it is a duplicate.
// For duplicates, dupOffset is the base offset from the original push.
// For new (non-dup) batches, baseOffset is stored for future dup
// detection.
//
// Sequence validation uses s.nextSeq (the authoritative next-expected
// sequence), not a position in the 5-entry circular buffer. This
// matches the last sequence Kafka keeps per producer -- nextSeq
// survives buffer rotation, so retries of committed-but-evicted
// batches are correctly identified as dups or gaps rather than
// false-positive OOOSN.
func (s *pidwindow) pushAndValidate(epoch int16, firstSeq, numRecs int32, baseOffset, maxTimestamp int64) (ok, dup bool, dupOffset int64) {
	if s == nil {
		return true, false, 0
	}

	// Epoch change or first-ever batch: client has reset sequences.
	if !s.seen || epoch != s.epoch {
		if s.seen && firstSeq != 0 {
			return false, false, 0
		}
		s.accept(epoch, firstSeq, numRecs, baseOffset, maxTimestamp)
		return true, false, 0
	}

	// Dup check: scan valid entries for a matching (firstSeq, nextSeq) pair.
	next := nextSeq(firstSeq, numRecs)
	for i := range s.count {
		e := s.entries[i]
		if e.firstSeq == firstSeq && e.nextSeq == next {
			return true, true, e.offset
		}
	}

	// Sequence validation: compare against nextSeq, not the
	// circular buffer position. nextSeq is the authoritative
	// next-expected sequence that survives window rotation.
	if firstSeq != s.nextSeq {
		return false, false, 0
	}

	s.accept(epoch, firstSeq, numRecs, baseOffset, maxTimestamp)
	return true, false, 0
}

// marker records a transaction marker at epoch. As in Kafka, a new epoch
// starts the window over, at sequence 0.
func (s *pidwindow) marker(epoch int16, ts int64) {
	if epoch != s.epoch {
		*s = pidwindow{seen: true, epoch: epoch}
	}
	s.lastTimestamp = ts
}

// accept stores a batch in the window and advances nextSeq. A new epoch
// starts the window over.
func (s *pidwindow) accept(epoch int16, firstSeq, numRecs int32, offset, maxTimestamp int64) {
	if !s.seen || epoch != s.epoch {
		*s = pidwindow{seen: true, epoch: epoch}
	}
	s.lastTimestamp = maxTimestamp
	s.nextSeq = nextSeq(firstSeq, numRecs)
	s.entries[s.at] = pidEntry{firstSeq, s.nextSeq, offset}
	s.at = (s.at + 1) % 5
	s.count = min(s.count+1, 5)
}

func (pids *pids) doDescribeTransactions(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.DescribeTransactionsRequest)
	resp := req.ResponseKind().(*kmsg.DescribeTransactionsResponse)

	for _, txnID := range req.TransactionalIDs {
		st := kmsg.NewDescribeTransactionsResponseTransactionState()
		st.TransactionalID = txnID

		isCoordinator := pids.c.isCoordinator(creq, txnID)
		if e := pids.c.deny(creq, txnID, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe, faultKey{txnID: txnID, misrouted: !isCoordinator}); e != nil {
			st.ErrorCode = e.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}
		if !isCoordinator {
			st.ErrorCode = kerr.NotCoordinator.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		pidinf := pids.byTxid[txnID]
		if pidinf == nil {
			st.ErrorCode = kerr.TransactionalIDNotFound.Code
			if txnID == "" {
				st.ErrorCode = kerr.InvalidRequest.Code
			}
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		st.ProducerID = pidinf.id
		st.ProducerEpoch = pidinf.epoch
		st.TimeoutMillis = pidinf.txTimeout
		st.State = pidinf.state()
		st.StartTimestamp = pidinf.startTimestamp()
		if pidinf.inTx {
			pidinf.txParts.each(func(topic string, partition int32, _ *partData) {
				if e := pids.c.deny(creq, topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, faultKey{txnID: txnID, topic: topic}.part(partition)); e != nil {
					return
				}
				var topicEntry *kmsg.DescribeTransactionsResponseTransactionStateTopic
				for i := range st.Topics {
					if st.Topics[i].Topic == topic {
						topicEntry = &st.Topics[i]
						break
					}
				}
				if topicEntry == nil {
					st.Topics = append(st.Topics, kmsg.NewDescribeTransactionsResponseTransactionStateTopic())
					topicEntry = &st.Topics[len(st.Topics)-1]
					topicEntry.Topic = topic
				}
				topicEntry.Partitions = append(topicEntry.Partitions, partition)
			})
		}

		resp.TransactionStates = append(resp.TransactionStates, st)
	}

	return resp
}

// state returns the Kafka name of the transaction state.
func (pidinf *pidinfo) state() string {
	switch {
	case pidinf.ending != nil && *pidinf.ending:
		return "PrepareCommit"
	case pidinf.ending != nil:
		return "PrepareAbort"
	case pidinf.inTx:
		return "Ongoing"
	case !pidinf.ended:
		return "Empty"
	case pidinf.lastWasCommit:
		return "CompleteCommit"
	default:
		return "CompleteAbort"
	}
}

// startTimestamp returns the start of the current or last transaction in
// millis, or -1 if none started since the last InitProducerID.
func (pidinf *pidinfo) startTimestamp() int64 {
	if pidinf.txStart.IsZero() {
		return -1
	}
	return pidinf.txStart.UnixMilli()
}

func (pids *pids) doListTransactions(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.ListTransactionsRequest)
	resp := req.ResponseKind().(*kmsg.ListTransactionsResponse)

	// Build filter sets. A state name Kafka does not know matches
	// nothing, and we echo it back.
	stateFilter := make(map[string]struct{})
	for _, s := range req.StateFilters {
		switch s {
		case "Empty", "Ongoing", "PrepareCommit", "PrepareAbort", "CompleteCommit", "CompleteAbort", "Dead", "PrepareEpochFence":
		default:
			resp.UnknownStateFilters = append(resp.UnknownStateFilters, s)
		}
		stateFilter[s] = struct{}{}
	}
	pidFilter := make(map[int64]struct{})
	for _, pid := range req.ProducerIDFilters {
		pidFilter[pid] = struct{}{}
	}
	var txnIDRegex *regexp.Regexp
	if req.Version >= 2 && req.TransactionalIDPattern != nil && *req.TransactionalIDPattern != "" {
		var err error
		// Kafka matches the whole ID.
		txnIDRegex, err = regexp.Compile("^(?:" + *req.TransactionalIDPattern + ")$")
		if err != nil {
			resp.ErrorCode = kerr.InvalidRegularExpression.Code
			return resp
		}
	}

	for txid, pidinf := range pids.byTxid {
		// Each broker lists only the IDs it coordinates.
		if !pids.c.isCoordinator(creq, txid) {
			continue
		}
		if e := pids.c.deny(creq, txid, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe, faultKey{txnID: txid}); e != nil {
			continue
		}
		state := pidinf.state()
		if len(stateFilter) > 0 {
			if _, ok := stateFilter[state]; !ok {
				continue
			}
		}
		if len(pidFilter) > 0 {
			if _, ok := pidFilter[pidinf.id]; !ok {
				continue
			}
		}
		// The duration filter keeps transactions that started more
		// than that long ago; one with no start passes.
		if req.Version >= 1 && req.DurationFilterMillis >= 0 && creq.at.UnixMilli()-pidinf.startTimestamp() <= req.DurationFilterMillis {
			continue
		}
		if txnIDRegex != nil && !txnIDRegex.MatchString(txid) {
			continue
		}
		ts := kmsg.NewListTransactionsResponseTransactionState()
		ts.TransactionalID = txid
		ts.ProducerID = pidinf.id
		ts.TransactionState = state
		resp.TransactionStates = append(resp.TransactionStates, ts)
	}

	return resp
}

func (pids *pids) doDescribeProducers(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.DescribeProducersRequest)
	resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)

	type partState struct {
		topic     string
		partition int32
		errCode   int16
	}
	var checks []partState
	for _, rt := range req.Topics {
		ps, _ := pids.c.data.tps.gett(rt.Topic)
		for _, p := range rt.Partitions {
			var code int16
			pd, exists := ps[p]
			k := faultKey{topic: rt.Topic, misrouted: exists && !pids.c.hostsReplica(creq.cc.b, rt.Topic, pd)}
			if e := pids.c.deny(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead, k.part(p)); e != nil {
				code = e.Code
			}
			checks = append(checks, partState{rt.Topic, p, code})
		}
	}

	for i := range checks {
		if checks[i].errCode != 0 {
			continue
		}
		t, tok := pids.c.data.tps.gett(checks[i].topic)
		if !tok {
			checks[i].errCode = kerr.UnknownTopicOrPartition.Code
			continue
		}
		pd, pok := t[checks[i].partition]
		if !pok {
			checks[i].errCode = kerr.UnknownTopicOrPartition.Code
			continue
		}
		// Followers answer DescribeProducers too.
		if !pids.c.hostsReplica(creq.cc.b, checks[i].topic, pd) {
			checks[i].errCode = kerr.NotLeaderForPartition.Code
		}
	}

	tidx := make(map[string]int)
	for _, pc := range checks {
		var st *kmsg.DescribeProducersResponseTopic
		if i, ok := tidx[pc.topic]; ok {
			st = &resp.Topics[i]
		} else {
			tidx[pc.topic] = len(resp.Topics)
			resp.Topics = append(resp.Topics, kmsg.NewDescribeProducersResponseTopic())
			st = &resp.Topics[len(resp.Topics)-1]
			st.Topic = pc.topic
		}
		sp := kmsg.NewDescribeProducersResponseTopicPartition()
		sp.Partition = pc.partition
		sp.ErrorCode = pc.errCode
		if pc.errCode == 0 {
			sp.ActiveProducers = pids.txnProducers(pc.topic, pc.partition)
		}
		st.Partitions = append(st.Partitions, sp)
	}

	return resp
}

// txnProducers returns the producers that wrote to a partition,
// idempotent or transactional, with the start of any open transaction.
func (pids *pids) txnProducers(topic string, partition int32) []kmsg.DescribeProducersResponseTopicPartitionActiveProducer {
	var producers []kmsg.DescribeProducersResponseTopicPartitionActiveProducer
	for _, pidinf := range pids.ids {
		pw, ok := pidinf.windows.getp(topic, partition)
		if !ok || !pw.seen {
			continue
		}
		ap := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
		ap.ProducerID = pidinf.id
		ap.ProducerEpoch = int32(pw.epoch)
		ap.LastSequence = (pw.nextSeq - 1) & math.MaxInt32
		if pw.count == 0 { // like Kafka, no batch at this epoch yet
			ap.LastSequence = -1
		}
		ap.LastTimestamp = pw.lastTimestamp
		ap.CoordinatorEpoch = 0
		ap.CurrentTxnStartOffset = -1
		if firstOffset, ok := pidinf.txPartFirstOffsets.getp(topic, partition); ok && *firstOffset >= 0 {
			ap.CurrentTxnStartOffset = *firstOffset
		}
		producers = append(producers, ap)
	}
	return producers
}
