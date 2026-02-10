package kfake

import (
	"hash/crc32"
	"hash/fnv"
	"math"
	"math/rand"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// * Add heap of last use, add index to pidwindow, and remove pidwindow as they exhaust max # of pids configured.

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
	}

	// Sequence ID window, and where the start is.
	pidwindow struct {
		seq   [5]int32
		at    uint8
		epoch int16 // last seen epoch; when epoch changes, seq 0 is accepted
	}
)

// init initializes the pids management goroutine if needed
func (pids *pids) init() {
	if pids.reqCh == nil {
		pids.txs = make(map[*pidinfo]struct{})
		pids.reqCh = make(chan *clientReq, 10)
		pids.controlCh = make(chan func(), 10)
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
// While waiting for fn to complete, this function also processes adminCh.
// This is necessary to avoid deadlock: handleProduce (in the cluster loop)
// calls this function to synchronize with the pids loop for sequence
// validation, while endTx (in the pids loop) calls c.admin() to modify
// partition data in the cluster loop. Without processing adminCh here,
// the two loops would deadlock waiting for each other.
func (pids *pids) waitControl(fn func()) {
	wait := make(chan struct{})
	wfn := func() { fn(); close(wait) }
	select {
	case pids.controlCh <- wfn:
	case <-pids.c.die:
		return
	}
	// While waiting for completion, also process adminCh to avoid deadlock
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
	select {
	case pids.reqCh <- creq:
		return true
	case <-pids.c.die:
		return false
	}
}

func (pids *pids) handleAddPartitionsToTxn(creq *clientReq) bool {
	if pids.reqCh == nil {
		return false
	}
	select {
	case pids.reqCh <- creq:
		return true
	case <-pids.c.die:
		return false
	}
}

func (pids *pids) handleAddOffsetsToTxn(creq *clientReq) bool {
	if pids.reqCh == nil {
		return false
	}
	select {
	case pids.reqCh <- creq:
		return true
	case <-pids.c.die:
		return false
	}
}

func (pids *pids) handleEndTxn(creq *clientReq) bool {
	if pids.reqCh == nil {
		return false
	}
	select {
	case pids.reqCh <- creq:
		return true
	case <-pids.c.die:
		return false
	}
}

func (pids *pids) handleTxnOffsetCommit(creq *clientReq) bool {
	if pids.reqCh == nil {
		return false
	}
	select {
	case pids.reqCh <- creq:
		return true
	case <-pids.c.die:
		return false
	}
}

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
			// Timer fired - find and abort the expired transaction
			nextPid, _ := findNextExpiry()
			if nextPid == nil {
				continue
			}
			// Check if this transaction actually expired
			timeout := time.Duration(nextPid.txTimeout) * time.Millisecond
			if time.Since(nextPid.txStart) >= timeout {
				nextPid.endTx(false) // abort (this also removes from pids.txs)
				nextPid.epoch++
				if nextPid.epoch < 0 {
					nextPid.epoch = 0
				}
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
		if req.TransactionTimeoutMillis < 0 { // TODO transaction.max.timeout.ms
			resp.ErrorCode = kerr.InvalidTransactionTimeout.Code
			return resp
		}
	}

	// KIP-360 (v3+): If client provides existing producer ID and epoch,
	// validate them before bumping. This enables recovery from errors.
	if req.ProducerID >= 0 && req.ProducerEpoch >= 0 {
		pidinf := pids.getpid(req.ProducerID)
		if pidinf == nil {
			resp.ErrorCode = kerr.InvalidProducerIDMapping.Code
			return resp
		}
		if pidinf.epoch != req.ProducerEpoch {
			resp.ErrorCode = kerr.InvalidProducerEpoch.Code
			return resp
		}
		// Valid ID and epoch - bump epoch for recovery (may allocate new ID on overflow)
		pidinf = pids.bumpEpoch(pidinf)
		resp.ProducerID = pidinf.id
		resp.ProducerEpoch = pidinf.epoch
		return resp
	}

	id, epoch := pids.create(req.TransactionalID, req.TransactionTimeoutMillis)
	resp.ProducerID = id
	resp.ProducerEpoch = epoch
	return resp
}

func (pidinf *pidinfo) maybeStart() {
	if pidinf.inTx {
		return
	}
	pidinf.inTx = true
	pidinf.txStart = time.Now()
	pidinf.pids.txs[pidinf] = struct{}{}
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
	donet := func(rt kmsg.AddPartitionsToTxnRequestTopic, errCode int16) {
		for _, rp := range rt.Partitions {
			donep(rt.Topic, rp, errCode)
		}
	}
	doneall := func(errCode int16) {
		for _, rt := range req.Topics {
			donet(rt, errCode)
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
		doneall(kerr.InvalidProducerEpoch.Code)
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
		resp.ErrorCode = kerr.InvalidProducerEpoch.Code
		return resp
	}

	if pids.c.groups.gs == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	}
	if _, ok := pids.c.groups.gs[req.Group]; !ok {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	}

	pidinf.maybeStart()
	pidinf.txGroups = append(pidinf.txGroups, req.Group)
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
		doneall(kerr.InvalidProducerEpoch.Code)
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

	// Check if group exists
	if pids.c.groups.gs == nil {
		doneall(kerr.GroupIDNotFound.Code)
		return resp
	}
	g, ok := pids.c.groups.gs[req.Group]
	if !ok {
		doneall(kerr.GroupIDNotFound.Code)
		return resp
	}

	// KIP-447: For v3+ requests, validate GenerationID and MemberID if provided.
	// This allows the broker to fence zombie producers that are no longer part
	// of the consumer group.
	if req.Version >= 3 && (req.MemberID != "" || req.Generation != -1) {
		var errCode int16
		g.waitControl(func() {
			if g.typ == "consumer" {
				// KIP-848: members are in consumerMembers, and
				// generation is per-member (memberEpoch), not
				// the group-level generation.
				if req.MemberID != "" {
					m, exists := g.consumerMembers[req.MemberID]
					if !exists {
						errCode = kerr.UnknownMemberID.Code
						return
					}
					if req.Generation != -1 && req.Generation != m.memberEpoch {
						errCode = kerr.IllegalGeneration.Code
						return
					}
				} else if req.Generation != -1 && req.Generation != g.generation {
					errCode = kerr.IllegalGeneration.Code
					return
				}
			} else {
				if req.MemberID != "" {
					if _, exists := g.members[req.MemberID]; !exists {
						errCode = kerr.UnknownMemberID.Code
						return
					}
				}
				if req.Generation != -1 && req.Generation != g.generation {
					errCode = kerr.IllegalGeneration.Code
					return
				}
			}
		})
		if errCode != 0 {
			doneall(errCode)
			return resp
		}
	}

	// Check if group is part of transaction
	groupInTx := false
	for _, g := range pidinf.txGroups {
		if g == req.Group {
			groupInTx = true
			break
		}
	}

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
		resp.ErrorCode = kerr.InvalidProducerEpoch.Code
		return resp
	}
	if !pidinf.inTx {
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return resp
	}

	pidinf.endTx(req.Commit)

	// KIP-890: For v5+ clients, bump epoch and return new ID/epoch for next transaction.
	// Old clients (v0-4) continue using the same ID/epoch.
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

// bumpEpoch increments the epoch for KIP-890. If the epoch would overflow,
// a new producer ID is allocated. Returns the (possibly new) pidinfo.
// Callers should reset pidinf.windows if the client is expected to reset sequences.
func (pids *pids) bumpEpoch(pidinf *pidinfo) *pidinfo {
	pidinf.epoch++
	if pidinf.epoch >= 0 {
		return pidinf
	}

	// Epoch overflow - allocate a new producer ID
	var newID int64
	for {
		newID = int64(rand.Uint64()) & math.MaxInt64
		if _, exists := pids.ids[newID]; !exists {
			break
		}
	}

	// Create new pidinfo with the new ID
	newPidinf := &pidinfo{
		pids:      pids,
		id:        newID,
		epoch:     0,
		txid:      pidinf.txid,
		txTimeout: pidinf.txTimeout,
	}
	pids.ids[newID] = newPidinf

	// Remove the old ID from tracking (it's fenced now)
	delete(pids.ids, pidinf.id)

	return newPidinf
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
		for {
			id = int64(rand.Uint64()) & math.MaxInt64
			if _, exists := pids.ids[id]; !exists {
				break
			}
		}
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
	pidinf.pids.c.admin(func() {
		for _, batch := range pidinf.txBatches {
			batch.inTx = false
			if !commit {
				batch.aborted = true
			}
		}
		pidinf.txParts.each(func(t string, p int32, pd *partData) {
			pd.pushBatch(len(benc), b, false, 0) // control record is not itself transactional
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

	// Handle transactional offset commits
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
			g.waitControl(func() {
				groupOffsets.each(func(t string, p int32, oc *offsetCommit) {
					g.commits.set(t, p, *oc)
				})
			})
		}
	}

	// Clean up transaction state. We do not delete the pidinf from pids,
	// because a new transaction can begin with this same id/epoch.
	// We just delete all information about this active transaction.
	pidinf.txParts = nil
	pidinf.txBatches = nil
	pidinf.txGroups = nil
	pidinf.txOffsets = nil
	pidinf.txPartFirstOffsets = nil
	pidinf.txPartBytes = nil
	pidinf.txStart = time.Time{}
	pidinf.inTx = false

	// Remove from active transaction tracking
	delete(pidinf.pids.txs, pidinf)
}

func (s *pidwindow) pushAndValidate(epoch int16, firstSeq, numRecs int32) (ok, dup bool) {
	// If there is no pid, we do not do duplicate detection.
	if s == nil {
		return true, false
	}

	// If epoch changed, client has reset sequences. Accept seq 0 and reset window.
	if epoch != s.epoch {
		if firstSeq != 0 {
			return false, false
		}
		s.epoch = epoch
		s.at = 0
		s.seq = [5]int32{}
		s.seq[0] = 0
		s.seq[1] = numRecs
		s.at = 1
		return true, false
	}

	var (
		seq    = firstSeq
		seq64  = int64(seq)
		next64 = (seq64 + int64(numRecs)) % math.MaxInt32
		next   = int32(next64)
	)
	for i := 0; i < 5; i++ {
		if s.seq[i] == seq && s.seq[(i+1)%5] == next {
			return true, true
		}
	}
	if s.seq[s.at] != seq {
		return false, false
	}
	s.at = (s.at + 1) % 5
	s.seq[s.at] = next
	return true, false
}
