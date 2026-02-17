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
// kfake completes transactions synchronously (no log writes), so
// the PREPARE_COMMIT, PREPARE_ABORT, and pendingTransitionInProgress
// states are not needed. Where a real broker returns
// CONCURRENT_TRANSACTIONS for in-progress transitions, kfake simply
// completes the operation immediately.
//
// State tracking: instead of the full transaction state machine
// (Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit,
// CompleteAbort, Dead, PrepareEpochFence), kfake uses:
//   - inTx (bool): true = Ongoing, false = Empty/Complete
//   - lastWasCommit (bool): whether the last completed transaction
//     was commit or abort, for EndTxn retry detection

type (
	pids struct {
		ids map[int64]*pidinfo

		txs       map[*pidinfo]struct{} // active transactions being tracked for timeout
		reqCh     chan *clientReq       // channel for transaction requests
		controlCh chan func()           // channel for control operations (like produce validation)
		c         *Cluster
	}

	// Seq/txn info for a given individual producer ID.
	pidinfo struct {
		pids *pids

		id      int64
		epoch   int16
		windows tps[pidwindow] // topic/partition 5-window pid sequences

		txid      string
		txTimeout int32                        // millis
		txParts   tps[partData]                // partitions in the transaction, if transactional
		txBatches []*partBatch                 // batches in the transaction
		txGroups  []string                     // consumer groups in the transaction
		txOffsets map[string]tps[offsetCommit] // pending offset commits per group

		// Track per-partition first offset for this transaction.
		// Used for AbortedTransactions in fetch response.
		txPartFirstOffsets tps[int64]

		// Track bytes per partition for this transaction.
		// Used to count committed bytes for readCommitted watchers.
		txPartBytes tps[int]

		txStart time.Time
		inTx    bool

		// Whether the last completed transaction was a commit (true)
		// or abort (false). Used for EndTxn retry detection: if the
		// client retries an EndTxn after the transaction already
		// completed, we return success only if the retry matches
		// (commit after commit, abort after abort).
		lastWasCommit bool
	}

	// Sequence ID window, and where the start is.
	pidwindow struct {
		seq     [5]int32
		offsets [5]int64 // base offsets corresponding to each seq entry, for dup detection
		at      uint8
		epoch   int16 // last seen epoch; when epoch changes, seq 0 is accepted
	}
)

// init initializes the pids management goroutine if needed
func (pids *pids) init() {
	if pids.reqCh == nil {
		pids.txs = make(map[*pidinfo]struct{})
		pids.reqCh = make(chan *clientReq)
		pids.controlCh = make(chan func())
		go pids.manage()
	}
}

// reply sends a response back to the client, similar to groups.go
func (pids *pids) reply(creq *clientReq, kresp kmsg.Response) {
	select {
	case creq.cc.respCh <- clientResp{kresp: kresp, corr: creq.corr, seq: creq.seq}:
	case <-pids.c.die:
	}
}

// waitControl executes fn in the pids management goroutine.
//
// Both the initial send and the completion wait drain adminCh to avoid
// deadlock: the pids manage loop may call c.admin() (e.g. during
// transaction timeout abort) while the cluster goroutine is trying to
// send to controlCh here. Draining adminCh in both phases ensures
// neither goroutine blocks waiting for the other.
func (pids *pids) waitControl(fn func()) {
	wait := make(chan struct{})
	wfn := func() { fn(); close(wait) }
	slowTimer := time.AfterFunc(5*time.Second, func() {
		pids.c.cfg.logger.Logf(LogLevelWarn, "pids.waitControl: blocked >5s")
	})
	defer slowTimer.Stop()
	for {
		select {
		case pids.controlCh <- wfn:
			goto sent
		case admin := <-pids.c.adminCh:
			admin()
		case <-pids.c.die:
			return
		}
	}
sent:
	for {
		select {
		case <-wait:
			return
		case admin := <-pids.c.adminCh:
			admin()
		case <-pids.c.die:
			return
		}
	}
}

func (pids *pids) handleInitProducerID(creq *clientReq) bool {
	pids.init()
	return pids.sendReq(creq)
}

// sendReq dispatches a request to the pids manage loop. It drains
// adminCh while sending to avoid deadlock: if pids.reqCh is blocked
// and pids.manage() is blocked in c.admin() waiting for adminCh,
// we must process adminCh to unblock pids.manage() so it can drain
// reqCh.
func (pids *pids) sendReq(creq *clientReq) bool {
	if pids.reqCh == nil {
		return false
	}
	for {
		select {
		case pids.reqCh <- creq:
			return true
		case admin := <-pids.c.adminCh:
			admin()
		case <-pids.c.die:
			return false
		}
	}
}

func (pids *pids) handleAddPartitionsToTxn(creq *clientReq) bool    { return pids.sendReq(creq) }
func (pids *pids) handleAddOffsetsToTxn(creq *clientReq) bool      { return pids.sendReq(creq) }
func (pids *pids) handleEndTxn(creq *clientReq) bool               { return pids.sendReq(creq) }
func (pids *pids) handleTxnOffsetCommit(creq *clientReq) bool      { return pids.sendReq(creq) }
func (pids *pids) handleDescribeTransactions(creq *clientReq) bool { return pids.sendReq(creq) }
func (pids *pids) handleListTransactions(creq *clientReq) bool     { return pids.sendReq(creq) }
func (pids *pids) handleDescribeProducers(creq *clientReq) bool    { return pids.sendReq(creq) }

// hasUnstableOffsets returns true if any active transaction has pending
// (uncommitted) offset commits for the given group. Used for KIP-447
// RequireStable support in OffsetFetch.
func (pids *pids) hasUnstableOffsets(group string) bool {
	if pids.reqCh == nil {
		return false
	}
	var unstable bool
	pids.waitControl(func() {
		for pidinf := range pids.txs {
			if _, hasGroup := pidinf.txOffsets[group]; hasGroup {
				unstable = true
				return
			}
		}
	})
	return unstable
}

// manage is the central loop for processing transaction requests and timeouts.
// Similar to groups.go's manage loop pattern.
func (pids *pids) manage() {
	t := time.NewTimer(0)
	<-t.C

	// findNextExpiry finds the transaction that will expire soonest
	findNextExpiry := func() (*pidinfo, time.Time) {
		var minPid *pidinfo
		var minExpire time.Time
		for pidinf := range pids.txs {
			timeout := time.Duration(pidinf.txTimeout) * time.Millisecond
			expire := pidinf.txStart.Add(timeout)
			if minPid == nil || expire.Before(minExpire) {
				minPid = pidinf
				minExpire = expire
			}
		}
		return minPid, minExpire
	}

	updateTimer := func() {
		nextPid, nextExpire := findNextExpiry()
		if nextPid != nil {
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(time.Until(nextExpire))
		}
	}

	for {
		select {
		case creq := <-pids.reqCh:
			var kresp kmsg.Response
			switch creq.kreq.(type) {
			case *kmsg.InitProducerIDRequest:
				kresp = pids.doInitProducerID(creq)
			case *kmsg.AddPartitionsToTxnRequest:
				kresp = pids.doAddPartitions(creq)
			case *kmsg.AddOffsetsToTxnRequest:
				kresp = pids.doAddOffsets(creq)
			case *kmsg.TxnOffsetCommitRequest:
				kresp = pids.doTxnOffsetCommit(creq)
			case *kmsg.EndTxnRequest:
				kresp = pids.doEnd(creq)
			case *kmsg.DescribeTransactionsRequest:
				kresp = pids.doDescribeTransactions(creq)
			case *kmsg.ListTransactionsRequest:
				kresp = pids.doListTransactions(creq)
			case *kmsg.DescribeProducersRequest:
				kresp = pids.doDescribeProducers(creq)
			}
			if kresp != nil {
				pids.reply(creq, kresp)
			}
			updateTimer() // update timer in case txs changed

		case fn := <-pids.controlCh:
			fn()

		case <-pids.c.die:
			t.Stop()
			return

		case <-t.C:
			// Timer fired - find and abort the expired transaction.
			nextPid, _ := findNextExpiry()
			if nextPid == nil {
				continue
			}
			// Check if this transaction actually expired.
			timeout := time.Duration(nextPid.txTimeout) * time.Millisecond
			elapsed := time.Since(nextPid.txStart)
			if elapsed >= 30*time.Second && elapsed < timeout {
				pids.c.cfg.logger.Logf(LogLevelWarn,
					"txn long-running: txn_id=%s pid=%d epoch=%d elapsed=%v timeout=%dms batches=%d",
					nextPid.txid, nextPid.id, nextPid.epoch, elapsed, nextPid.txTimeout, len(nextPid.txBatches))
			}
			if elapsed >= timeout {
				pids.c.cfg.logger.Logf(LogLevelDebug,
					"txn timeout abort: txn_id=%s producer_id=%d epoch=%d timeout=%dms elapsed=%v",
					nextPid.txid, nextPid.id, nextPid.epoch, nextPid.txTimeout, time.Since(nextPid.txStart))
				// Bump epoch BEFORE abort so the control record
				// uses the new (fenced) epoch.
				nextPid = pids.bumpEpoch(nextPid)
				nextPid.endTx(false)
			}
			// Reset timer for next expiry
			updateTimer()
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
		if req.TransactionTimeoutMillis <= 0 {
			resp.ErrorCode = kerr.InvalidTransactionTimeout.Code
			return resp
		}
	}

	// Idempotent-only: always allocate fresh.
	if req.TransactionalID == nil {
		id, epoch := pids.create(nil, 0)
		resp.ProducerID = id
		resp.ProducerEpoch = epoch
		pids.c.cfg.logger.Logf(LogLevelDebug, "txn: InitProducerID created pid %d epoch %d txid \"\"", id, epoch)
		return resp
	}

	// KIP-360 (v3+): validate existing producer ID and epoch, bump for recovery.
	if req.ProducerID >= 0 && req.ProducerEpoch >= 0 {
		pidinf := pids.getpid(req.ProducerID)
		if pidinf == nil {
			resp.ErrorCode = kerr.InvalidProducerIDMapping.Code
			return resp
		}
		// Accept current or stale epoch (recovery after timeout
		// bump). Only reject epochs above the server's.
		if req.ProducerEpoch > pidinf.epoch {
			resp.ErrorCode = kerr.ProducerFenced.Code
			return resp
		}
		// Abort any in-flight transaction before bumping. A real
		// broker returns CONCURRENT_TRANSACTIONS and aborts async,
		// but kfake is synchronous so we abort inline.
		if pidinf.inTx {
			pidinf.endTx(false)
		}
		pidinf = pids.bumpEpoch(pidinf)
		resp.ProducerID = pidinf.id
		resp.ProducerEpoch = pidinf.epoch
		return resp
	}

	// New transactional ID or first init.
	id, epoch := pids.create(req.TransactionalID, req.TransactionTimeoutMillis)
	resp.ProducerID = id
	resp.ProducerEpoch = epoch
	pids.c.cfg.logger.Logf(LogLevelDebug, "txn: InitProducerID created pid %d epoch %d txid %q",
		id, epoch, *req.TransactionalID)
	return resp
}

func (pidinf *pidinfo) maybeStart() {
	if pidinf.inTx {
		return
	}
	pidinf.inTx = true
	pidinf.txStart = time.Now()
	pidinf.pids.txs[pidinf] = struct{}{}
	pidinf.pids.c.cfg.logger.Logf(LogLevelDebug, "txn: pid %d epoch %d txid %q started transaction",
		pidinf.id, pidinf.epoch, pidinf.txid)
}

func (pids *pids) doAddPartitions(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.AddPartitionsToTxnRequest)
	resp := req.ResponseKind().(*kmsg.AddPartitionsToTxnResponse)

	tidx := make(map[string]int)
	donep := func(t string, p int32, errCode int16) {
		var st *kmsg.AddPartitionsToTxnResponseTopic
		if i, ok := tidx[t]; ok {
			st = &resp.Topics[i]
		} else {
			tidx[t] = len(resp.Topics)
			resp.Topics = append(resp.Topics, kmsg.NewAddPartitionsToTxnResponseTopic())
			st = &resp.Topics[len(resp.Topics)-1]
			st.Topic = t
		}
		sp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st.Partitions = append(st.Partitions, sp)
	}
	doneall := func(errCode int16) {
		for _, rt := range req.Topics {
			for _, rp := range rt.Partitions {
				donep(rt.Topic, rp, errCode)
			}
		}
	}

	// Check if all topics/partitions exist first.
	var noAttempt bool
out:
	for _, rt := range req.Topics {
		ps, ok := pids.c.data.tps.gett(rt.Topic)
		if !ok {
			noAttempt = true
			break out
		}
		for _, rp := range rt.Partitions {
			if ps[rp] == nil {
				noAttempt = true
				break out
			}
		}
	}
	// If any fail, mark failures as UnknownTopicOrPartition and others as OperationNotAttempted.
	if noAttempt {
		for _, rt := range req.Topics {
			ps, ok := pids.c.data.tps.gett(rt.Topic)
			for _, rp := range rt.Partitions {
				if !ok || ps[rp] == nil {
					donep(rt.Topic, rp, kerr.UnknownTopicOrPartition.Code)
				} else {
					donep(rt.Topic, rp, kerr.OperationNotAttempted.Code)
				}
			}
		}
		return resp
	}

	coordinator := pids.c.coordinator(req.TransactionalID)
	if creq.cc.b != coordinator {
		doneall(kerr.NotCoordinator.Code)
		return resp
	}

	pidinf := pids.getpid(req.ProducerID)
	if pidinf == nil {
		doneall(kerr.InvalidProducerIDMapping.Code)
		return resp
	}
	if pidinf.epoch != req.ProducerEpoch {
		doneall(kerr.ProducerFenced.Code)
		return resp
	}

	for _, rt := range req.Topics {
		for _, partition := range rt.Partitions {
			pd, _ := pids.c.data.tps.getp(rt.Topic, partition)
			ps := pidinf.txParts.mkt(rt.Topic)
			ps[partition] = pd
			donep(rt.Topic, partition, 0)
		}
	}
	pidinf.maybeStart()
	pids.c.cfg.logger.Logf(LogLevelDebug, "txn: AddPartitionsToTxn pid %d epoch %d txid %q added %d topics",
		pidinf.id, pidinf.epoch, pidinf.txid, len(req.Topics))
	return resp
}

func (pids *pids) doAddOffsets(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.AddOffsetsToTxnRequest)
	resp := req.ResponseKind().(*kmsg.AddOffsetsToTxnResponse)

	coordinator := pids.c.coordinator(req.TransactionalID)
	if creq.cc.b != coordinator {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp
	}

	pidinf := pids.getpid(req.ProducerID)
	if pidinf == nil {
		resp.ErrorCode = kerr.InvalidProducerIDMapping.Code
		return resp
	}
	if pidinf.epoch != req.ProducerEpoch {
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	}

	pidinf.maybeStart()
	pidinf.txGroups = append(pidinf.txGroups, req.Group)
	pids.c.cfg.logger.Logf(LogLevelDebug, "txn: AddOffsetsToTxn pid %d epoch %d txid %q added group %q",
		pidinf.id, pidinf.epoch, pidinf.txid, req.Group)
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

	coordinator := pids.c.coordinator(req.Group)
	if creq.cc.b != coordinator {
		doneall(kerr.NotCoordinator.Code)
		return resp
	}

	pidinf := pids.getpid(req.ProducerID)
	if pidinf == nil {
		doneall(kerr.InvalidProducerIDMapping.Code)
		return resp
	}
	if pidinf.epoch != req.ProducerEpoch {
		doneall(kerr.ProducerFenced.Code)
		return resp
	}
	if pidinf.txid == "" || pidinf.txid != req.TransactionalID {
		doneall(kerr.InvalidProducerIDMapping.Code)
		return resp
	}

	// KIP-890 Part 2: For v5+ requests, implicitly start the transaction
	// if not already started. For v0-4, require transaction to be active.
	if !pidinf.inTx {
		if req.Version >= 5 {
			pidinf.maybeStart()
		} else {
			doneall(kerr.InvalidTxnState.Code)
			return resp
		}
	}

	// Check if group exists. For generation >= 0, return
	// ILLEGAL_GENERATION if the group doesn't exist. For
	// generation < 0 (admin commits), allow it through.
	if pids.c.groups.gs != nil {
		if g, ok := pids.c.groups.gs[req.Group]; ok {
			if req.Version >= 3 && (req.MemberID != "" || req.Generation != -1) {
				var errCode int16
				g.waitControl(func() {
					errCode = g.validateMemberGeneration(req.MemberID, req.Generation)
				})
				if errCode != 0 {
					doneall(errCode)
					return resp
				}
			}
		} else if req.Generation >= 0 {
			doneall(kerr.IllegalGeneration.Code)
			return resp
		}
	}

	groupInTx := slices.Contains(pidinf.txGroups, req.Group)

	// KIP-890: For v5+ requests, implicitly add the group to the transaction
	// if it's not already there. This allows clients to skip AddOffsetsToTxn.
	if !groupInTx {
		if req.Version >= 5 {
			pidinf.txGroups = append(pidinf.txGroups, req.Group)
		} else {
			doneall(kerr.InvalidTxnState.Code)
			return resp
		}
	}

	// Store pending offset commits; will be actually mirrored into
	// the group offsets once the transaction ends with a commit.
	if pidinf.txOffsets == nil {
		pidinf.txOffsets = make(map[string]tps[offsetCommit])
	}
	groupOffsets := pidinf.txOffsets[req.Group]
	for _, rt := range req.Topics {
		for _, rp := range rt.Partitions {
			pids.c.cfg.logger.Logf(LogLevelInfo, "TxnOffsetCommit: group=%s topic=%s p=%d offset=%d pid=%d epoch=%d",
				req.Group[:min(16, len(req.Group))], rt.Topic[:min(16, len(rt.Topic))], rp.Partition, rp.Offset, pidinf.id, pidinf.epoch)
			groupOffsets.set(rt.Topic, rp.Partition, offsetCommit{
				offset:      rp.Offset,
				leaderEpoch: rp.LeaderEpoch,
				metadata:    rp.Metadata,
			})
		}
	}
	pidinf.txOffsets[req.Group] = groupOffsets
	doneall(0)
	return resp
}

func (pids *pids) doEnd(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.EndTxnRequest)
	resp := req.ResponseKind().(*kmsg.EndTxnResponse)

	action := "abort"
	if req.Commit {
		action = "commit"
	}
	pids.c.cfg.logger.Logf(LogLevelDebug, "txn: EndTxn received pid %d epoch %d txid %q (%s)",
		req.ProducerID, req.ProducerEpoch, req.TransactionalID, action)

	coordinator := pids.c.coordinator(req.TransactionalID)
	if creq.cc.b != coordinator {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp
	}

	pidinf := pids.getpid(req.ProducerID)
	if pidinf == nil {
		resp.ErrorCode = kerr.InvalidProducerIDMapping.Code
		return resp
	}
	if pidinf.epoch != req.ProducerEpoch {
		// KIP-890 retry detection: if the epoch is exactly one
		// ahead and the producer is not in a transaction, the
		// previous EndTxn already completed and bumped the epoch.
		if req.Version >= 5 && pidinf.epoch == req.ProducerEpoch+1 && !pidinf.inTx {
			resp.ProducerID = pidinf.id
			resp.ProducerEpoch = pidinf.epoch
			return resp
		}
		pids.c.cfg.logger.Logf(LogLevelDebug,
			"EndTxn PRODUCER_FENCED: txn_id=%s producer_id=%d req_epoch=%d server_epoch=%d",
			req.TransactionalID, req.ProducerID, req.ProducerEpoch, pidinf.epoch)
		resp.ErrorCode = kerr.ProducerFenced.Code
		return resp
	}
	if !pidinf.inTx {
		// v5+: allow aborting an empty transaction.
		if req.Version >= 5 && !req.Commit {
			return resp
		}
		// Retry detection: return success if the retry matches the
		// completed action (commit retries commit, abort retries abort).
		if req.Commit == pidinf.lastWasCommit {
			return resp
		}
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return resp
	}

	pidinf.endTx(req.Commit)

	// KIP-890: For v5+ clients, bump epoch and return new ID/epoch.
	if req.Version >= 5 {
		pidinf = pids.bumpEpoch(pidinf)
		resp.ProducerID = pidinf.id
		resp.ProducerEpoch = pidinf.epoch
	}

	return resp
}

func (pids *pids) getpid(id int64) *pidinfo {
	return pids.ids[id]
}

// Returns the pidinfo for this pid, and the idempotent-5 window for this
// specific toppar. If this is transactional and the toppar has not been added
// to the txn, returns nil.
func (pids *pids) get(id int64, t string, p int32) (*pidinfo, *pidwindow) {
	pidinf := pids.ids[id]
	if pidinf == nil {
		return nil, nil
	}
	if pidinf.txid != "" && !pidinf.txParts.checkp(t, p) {
		return nil, nil
	}
	return pidinf, pidinf.windows.mkpDefault(t, p)
}

// getImplicitTxn is like get, but supports KIP-890 implicit partition addition.
// If the producer is transactional and the partition isn't in the transaction,
// this adds it implicitly (rather than returning nil like get does).
// The pd parameter is required to store in txParts for the partition.
func (pids *pids) getImplicitTxn(id int64, t string, p int32, pd *partData) (*pidinfo, *pidwindow) {
	pidinf := pids.ids[id]
	if pidinf == nil {
		return nil, nil
	}
	if pidinf.txid != "" && !pidinf.txParts.checkp(t, p) {
		// KIP-890: Implicitly add partition to transaction
		ps := pidinf.txParts.mkt(t)
		ps[p] = pd
		pidinf.maybeStart()
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

// bumpEpoch increments the epoch. If the epoch reaches the exhaustion
// threshold (math.MaxInt16 - 1), a new producer ID is allocated.
func (pids *pids) bumpEpoch(pidinf *pidinfo) *pidinfo {
	if pidinf.epoch >= math.MaxInt16-1 {
		newID := pids.randomID()
		newPidinf := &pidinfo{
			pids:      pids,
			id:        newID,
			epoch:     0,
			txid:      pidinf.txid,
			txTimeout: pidinf.txTimeout,
		}
		pids.ids[newID] = newPidinf
		delete(pids.ids, pidinf.id)
		return newPidinf
	}
	pidinf.epoch++
	return pidinf
}

func (pids *pids) create(txidp *string, txTimeout int32) (int64, int16) {
	var id int64
	var txid string
	if txidp != nil {
		hasher := fnv.New64()
		hasher.Write([]byte(*txidp))
		id = int64(hasher.Sum64()) & math.MaxInt64
		txid = *txidp
	} else {
		id = pids.randomID()
	}
	pidinf, exists := pids.ids[id]
	if exists {
		pidinf = pids.bumpEpoch(pidinf)
		return pidinf.id, pidinf.epoch
	}
	pidinf = &pidinfo{
		pids:      pids,
		id:        id,
		txid:      txid,
		txTimeout: txTimeout,
	}
	pids.ids[id] = pidinf
	return id, 0
}

func (pidinf *pidinfo) endTx(commit bool) {
	action := "abort"
	if commit {
		action = "commit"
	}
	pidinf.pids.c.cfg.logger.Logf(LogLevelDebug, "txn: pid %d epoch %d txid %q ending transaction (%s), batches=%d groups=%v",
		pidinf.id, pidinf.epoch, pidinf.txid, action, len(pidinf.txBatches), pidinf.txGroups)

	// Control record key format: version (int16=0) + type (int16: 0=abort, 1=commit)
	var controlType byte // abort = 0
	if commit {
		controlType = 1 // commit
	}
	rec := kmsg.Record{Key: []byte{0, 0, 0, controlType}}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1) // -1 because length itself is encoded as a varint, and varint_length(record_length) == 1 byte
	now := time.Now().UnixMilli()
	b := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           int16(0b00000000_00110000),
		LastOffsetDelta:      0,
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           pidinf.id,
		ProducerEpoch:        pidinf.epoch,
		FirstSequence:        -1,
		NumRecords:           1,
		Records:              rec.AppendTo(nil),
	}
	benc := b.AppendTo(nil)
	b.Length = int32(len(benc) - 12)
	b.CRC = int32(crc32.Checksum(benc[21:], crc32c))

	// Execute partition modifications in the cluster loop to avoid races.
	pidinf.pids.c.cfg.logger.Logf(LogLevelDebug, "txn: endTx c.admin() start pid %d epoch %d", pidinf.id, pidinf.epoch)
	adminStart := time.Now()
	pidinf.pids.c.admin(func() {
		for _, batch := range pidinf.txBatches {
			batch.inTx = false
			if !commit {
				batch.aborted = true
			}
		}
		pidinf.txParts.each(func(t string, p int32, pd *partData) {
			pd.pushBatch(len(benc), b, false, 0) // control record is not itself transactional
			oldLSO := pd.lastStableOffset
			pidinf.pids.c.cfg.logger.Logf(LogLevelDebug, "txn: %s %s[%d] LSO %d -> %d, HWM %d",
				action, t, p, oldLSO, pd.lastStableOffset, pd.highWatermark)
			pd.recalculateLSO()
			// Count the now-committed bytes for readCommitted watchers.
			// These bytes were skipped in push() because pd.inTx was true.
			txnBytes, _ := pidinf.txPartBytes.getp(t, p)
			if txnBytes != nil && *txnBytes > 0 {
				for w := range pd.watch {
					if w.readCommitted {
						w.addBytes(pd, *txnBytes)
					}
				}
			}
		})
	})

	if d := time.Since(adminStart); d > time.Second {
		pidinf.pids.c.cfg.logger.Logf(LogLevelWarn, "txn: endTx c.admin() took %v for pid %d", d, pidinf.id)
	}

	// Handle transactional offset commits
	if !commit && len(pidinf.txOffsets) > 0 {
		pidinf.pids.c.cfg.logger.Logf(LogLevelInfo, "TxnOffsetDiscard: pid=%d epoch=%d abort, discarding %d group offsets",
			pidinf.id, pidinf.epoch, len(pidinf.txOffsets))
	}
	if commit && len(pidinf.txOffsets) > 0 {
		// Apply pending offset commits to groups.
		for _, groupID := range pidinf.txGroups {
			groupOffsets, hasOffsets := pidinf.txOffsets[groupID]
			if !hasOffsets || len(groupOffsets) == 0 {
				continue
			}
			if pidinf.pids.c.groups.gs == nil {
				continue
			}
			g, ok := pidinf.pids.c.groups.gs[groupID]
			if !ok {
				continue
			}
			pidinf.pids.c.cfg.logger.Logf(LogLevelDebug, "txn: endTx calling g.waitControl for group %s pid %d", groupID[:min(16, len(groupID))], pidinf.id)
			g.waitControl(func() {
				groupOffsets.each(func(t string, p int32, oc *offsetCommit) {
					pidinf.pids.c.cfg.logger.Logf(LogLevelInfo, "TxnOffsetApply: group=%s topic=%s p=%d offset=%d pid=%d epoch=%d",
						groupID[:min(16, len(groupID))], t[:min(16, len(t))], p, oc.offset, pidinf.id, pidinf.epoch)
					g.commits.set(t, p, *oc)
				})
			})
		}
	}

	pidinf.resetTx(commit)

	pidinf.pids.c.cfg.logger.Logf(LogLevelDebug, "txn: pid %d epoch %d txid %q transaction ended (%s)",
		pidinf.id, pidinf.epoch, pidinf.txid, action)
}

func (pidinf *pidinfo) resetTx(wasCommit bool) {
	pidinf.txParts = nil
	pidinf.txBatches = nil
	pidinf.txGroups = nil
	pidinf.txOffsets = nil
	pidinf.txPartFirstOffsets = nil
	pidinf.txPartBytes = nil
	pidinf.txStart = time.Time{}
	pidinf.inTx = false
	pidinf.lastWasCommit = wasCommit
	delete(pidinf.pids.txs, pidinf)
}

// pushAndValidate checks the sequence number against the window and
// returns whether the batch is valid and whether it is a duplicate.
// For duplicates, dupOffset is the base offset from the original push.
// For new (non-dup) batches, baseOffset is stored for future dup
// detection.
func (s *pidwindow) pushAndValidate(epoch int16, firstSeq, numRecs int32, baseOffset int64) (ok, dup bool, dupOffset int64) {
	if s == nil {
		return true, false, 0
	}

	// If epoch changed, client has reset sequences.
	if epoch != s.epoch {
		if firstSeq != 0 {
			return false, false, 0
		}
		s.epoch = epoch
		s.at = 0
		s.seq = [5]int32{}
		s.offsets = [5]int64{}
		s.seq[0] = 0
		s.seq[1] = numRecs
		s.offsets[0] = baseOffset
		s.at = 1
		return true, false, 0
	}

	var (
		seq    = firstSeq
		seq64  = int64(seq)
		next64 = (seq64 + int64(numRecs)) % math.MaxInt32
		next   = int32(next64)
	)
	for i := range 5 {
		if s.seq[i] == seq && s.seq[(i+1)%5] == next {
			return true, true, s.offsets[i]
		}
	}
	if s.seq[s.at] != seq {
		return false, false, 0
	}
	s.offsets[s.at] = baseOffset
	s.at = (s.at + 1) % 5
	s.seq[s.at] = next
	return true, false, 0
}

func (pids *pids) doDescribeTransactions(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.DescribeTransactionsRequest)
	resp := req.ResponseKind().(*kmsg.DescribeTransactionsResponse)

	for _, txnID := range req.TransactionalIDs {
		st := kmsg.NewDescribeTransactionsResponseTransactionState()
		st.TransactionalID = txnID

		if !pids.c.allowedACL(creq, txnID, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe) {
			st.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		coordinator := pids.c.coordinator(txnID)
		if coordinator != creq.cc.b {
			st.ErrorCode = kerr.NotCoordinator.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		pidinf := pids.findTxnID(txnID)
		if pidinf == nil {
			st.ErrorCode = kerr.TransactionalIDNotFound.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		st.ProducerID = pidinf.id
		st.ProducerEpoch = pidinf.epoch
		st.TimeoutMillis = pidinf.txTimeout

		if pidinf.inTx {
			st.State = "Ongoing"
			st.StartTimestamp = pidinf.txStart.UnixMilli()
			pidinf.txParts.each(func(topic string, partition int32, _ *partData) {
				if !pids.c.allowedACL(creq, topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe) {
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
		} else {
			st.State = "Empty"
			st.StartTimestamp = -1
		}

		resp.TransactionStates = append(resp.TransactionStates, st)
	}

	return resp
}

func (pids *pids) doListTransactions(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.ListTransactionsRequest)
	resp := req.ResponseKind().(*kmsg.ListTransactionsResponse)

	// Build filter sets.
	stateFilter := make(map[string]struct{})
	for _, s := range req.StateFilters {
		stateFilter[s] = struct{}{}
	}
	pidFilter := make(map[int64]struct{})
	for _, pid := range req.ProducerIDFilters {
		pidFilter[pid] = struct{}{}
	}
	var txnIDRegex *regexp.Regexp
	if req.Version >= 2 && req.TransactionalIDPattern != nil && *req.TransactionalIDPattern != "" {
		var err error
		txnIDRegex, err = regexp.Compile(*req.TransactionalIDPattern)
		if err != nil {
			resp.ErrorCode = kerr.InvalidRegularExpression.Code
			return resp
		}
	}

	if pids.ids == nil {
		return resp
	}

	for _, pidinf := range pids.ids {
		if pidinf.txid == "" {
			continue
		}
		if !pids.c.allowedACL(creq, pidinf.txid, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe) {
			continue
		}
		state := "Empty"
		if pidinf.inTx {
			state = "Ongoing"
		}
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
		if req.Version >= 1 && req.DurationFilterMillis >= 0 && pidinf.inTx {
			if creq.at.Sub(pidinf.txStart).Milliseconds() < req.DurationFilterMillis {
				continue
			}
		}
		if txnIDRegex != nil && !txnIDRegex.MatchString(pidinf.txid) {
			continue
		}
		ts := kmsg.NewListTransactionsResponseTransactionState()
		ts.TransactionalID = pidinf.txid
		ts.ProducerID = pidinf.id
		ts.TransactionState = state
		resp.TransactionStates = append(resp.TransactionStates, ts)
	}

	return resp
}

// doDescribeProducers runs in the pids manage loop. It uses a single
// c.admin() call to snapshot partition state (leaders, existence),
// then iterates pids.ids (safe in this goroutine) to find active
// transactional producers.
func (pids *pids) doDescribeProducers(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.DescribeProducersRequest)
	resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)

	// Collect all topic-partitions and their leader/existence state
	// in one c.admin() call.
	type partState struct {
		topic     string
		partition int32
		errCode   int16
	}
	var checks []partState
	for _, rt := range req.Topics {
		if !pids.c.allowedACL(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			for _, p := range rt.Partitions {
				checks = append(checks, partState{rt.Topic, p, kerr.TopicAuthorizationFailed.Code})
			}
			continue
		}
		for _, p := range rt.Partitions {
			checks = append(checks, partState{rt.Topic, p, 0}) // 0 = needs checking
		}
	}

	pids.c.admin(func() {
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
			if pd.leader != creq.cc.b {
				checks[i].errCode = kerr.NotLeaderForPartition.Code
			}
		}
	})

	// Now iterate results and look up producers (safe - we're in pids goroutine).
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

// txnProducers returns active transactional producers for a partition.
// Must be called from the pids manage loop.
func (pids *pids) txnProducers(topic string, partition int32) []kmsg.DescribeProducersResponseTopicPartitionActiveProducer {
	var producers []kmsg.DescribeProducersResponseTopicPartitionActiveProducer
	for _, pidinf := range pids.ids {
		if !pidinf.inTx || !pidinf.txParts.checkp(topic, partition) {
			continue
		}
		ap := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
		ap.ProducerID = pidinf.id
		ap.ProducerEpoch = int32(pidinf.epoch)
		ap.LastTimestamp = pidinf.txStart.UnixMilli()
		ap.CoordinatorEpoch = 0
		ap.CurrentTxnStartOffset = -1
		ap.LastSequence = -1
		if pw, ok := pidinf.windows.getp(topic, partition); ok && pw != nil {
			ap.LastSequence = pw.seq[pw.at] - 1
		}
		if firstOffset, ok := pidinf.txPartFirstOffsets.getp(topic, partition); ok && firstOffset != nil {
			ap.CurrentTxnStartOffset = *firstOffset
		}
		producers = append(producers, ap)
	}
	return producers
}

// findTxnID finds a producer info by transactional ID. Must be called
// from the pids manage loop.
func (pids *pids) findTxnID(txnID string) *pidinfo {
	for _, pidinf := range pids.ids {
		if pidinf.txid == txnID {
			return pidinf
		}
	}
	return nil
}
