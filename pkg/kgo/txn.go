package kgo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TransactionEndTry is simply a named bool.
type TransactionEndTry bool

const (
	// TryAbort attempts to end a transaction with an abort.
	TryAbort TransactionEndTry = false

	// TryCommit attempts to end a transaction with a commit.
	TryCommit TransactionEndTry = true
)

// GroupTransactSession abstracts away the proper way to begin a transaction
// and more importantly how to end a transaction when consuming in a group,
// modifying records, and producing (EOS transaction).
type GroupTransactSession struct {
	cl *Client

	cooperative bool

	revokeMu sync.Mutex
	revoked  bool
}

// AssignGroupTransactSession is exactly the same as AssignGroup, but wraps the
// group consumer's OnRevoke with a function that will ensure a transaction
// session is correctly aborted.
//
// When ETLing in a group in a transaction, if a rebalance happens before the
// transaction is ended, you either (a) must block the rebalance from finishing
// until you are done producing, and then commit before unblocking, or (b)
// allow the rebalance to happen, but abort any work you did.
//
// The problem with (a) is that if your ETL work loop is slow, you run the risk
// of exceeding the rebalance timeout and being kicked from the group. You will
// try to commit, and depending on the Kafka version, the commit may even be
// erroneously successful (pre Kafka 2.5.0). This will lead to duplicates.
//
// Instead, for safety, a GroupTransactSession favors (b). If a rebalance
// occurs at any time before ending a transaction with a commit, this will
// abort the transaction.
//
// This leaves the risk that ending the transaction itself exceeds the
// rebalance timeout, but this is just one request with no cpu logic. With a
// proper rebalance timeout, this single request will not fail and the commit
// will succeed properly.
func (cl *Client) AssignGroupTransactSession(group string, opts ...GroupOpt) *GroupTransactSession {
	cl.AssignGroup(group, opts...)

	s := &GroupTransactSession{
		cl: cl,
	}

	c := &cl.consumer
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.typ != consumerTypeGroup {
		return nil // invalid, but we will let the caller handle this
	}

	g := c.group
	g.mu.Lock()
	defer g.mu.Unlock()

	s.cooperative = g.cooperative

	userRevoked := g.onRevoked
	g.onRevoked = func(ctx context.Context, rev map[string][]int32) {
		s.revokeMu.Lock()
		defer s.revokeMu.Unlock()
		if s.cooperative && len(rev) == 0 && !s.revoked {
			cl.cfg.logger.Log(LogLevelInfo, "transact session in on_revoke with nothing to revoke; allowing next commit")
		} else {
			cl.cfg.logger.Log(LogLevelInfo, "transact session in on_revoke; aborting next commit if we are currently in a transaction")
			s.revoked = true
		}

		if userRevoked != nil {
			userRevoked(ctx, rev)
		}
	}

	return s
}

// Begin begins a transaction, returning an error if the client has no
// transactional id or is already in a transaction.
//
// Begin must be called before producing records in a transaction.
//
// Note that a revoke of any partitions sets the session's revoked state, even
// if the session has not begun. This state is only reset on EndTransaction.
// Thus, it is safe to begin transactions after a poll (but still before you
// produce).
func (s *GroupTransactSession) Begin() error {
	s.cl.cfg.logger.Log(LogLevelInfo, "beginning transact session")
	return s.cl.BeginTransaction()
}

// End ends a transaction, committing if commit is true, if the group did not
// rebalance since the transaction began, and if committing offsets is
// successful. If commit is false, the group has rebalanced, or any partition
// in committing offsets fails, this aborts.
//
// This returns whether the transaction committed or any error that occurred.
func (s *GroupTransactSession) End(ctx context.Context, commit TransactionEndTry) (bool, error) {
	defer func() {
		s.revokeMu.Lock()
		s.revoked = false
		s.revokeMu.Unlock()
	}()
	if err := s.cl.Flush(ctx); err != nil {
		// We do not abort here, since any error is the context
		// closing.
		return false, err
	}

	wantCommit := bool(commit)

	s.revokeMu.Lock()
	revoked := s.revoked
	s.revokeMu.Unlock()

	precommit := s.cl.CommittedOffsets()
	postcommit := s.cl.UncommittedOffsets()

	var oldGeneration bool
	var commitErr error
	if wantCommit && !revoked {
		var commitErrs []string

		committed := make(chan struct{})
		s.cl.commitTransactionOffsets(context.Background(), postcommit,
			func(_ *kmsg.TxnOffsetCommitRequest, resp *kmsg.TxnOffsetCommitResponse, err error) {
				defer close(committed)
				if err != nil {
					commitErrs = append(commitErrs, err.Error())
					return
				}

				for _, t := range resp.Topics {
					for _, p := range t.Partitions {
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							if err == kerr.IllegalGeneration {
								oldGeneration = true
							} else {
								commitErrs = append(commitErrs, fmt.Sprintf("topic %s partition %d: %v", t.Topic, p.Partition, err))
							}
						}
					}
				}
			},
		)
		<-committed

		if len(commitErrs) > 0 {
			commitErr = fmt.Errorf("unable to commit transaction offsets: %s", strings.Join(commitErrs, ", "))
		}
	}

	s.revokeMu.Lock()
	defer s.revokeMu.Unlock()

	tryCommit := !s.revoked && commitErr == nil && !oldGeneration
	willTryCommit := wantCommit && tryCommit

	s.cl.cfg.logger.Log(LogLevelInfo, "transaction session ending",
		"was_revoked", s.revoked,
		"want_commit", wantCommit,
		"can_try_commit", tryCommit,
		"will_try_commit", willTryCommit,
	)

	endTxnErr := s.cl.EndTransaction(ctx, TransactionEndTry(willTryCommit))

	if !willTryCommit || endTxnErr != nil {
		s.cl.cfg.logger.Log(LogLevelInfo, "transact session resetting to prior committed state",
			"tried_commit", willTryCommit,
			"commit_err", endTxnErr,
			"precommit", precommit,
		)
		s.cl.SetOffsets(precommit)
	} else if willTryCommit && endTxnErr == nil {
		s.cl.cfg.logger.Log(LogLevelInfo, "transact session successful, setting to newly committed state",
			"tried_commit", willTryCommit,
			"postcommit", postcommit,
		)
		s.cl.SetOffsets(postcommit)
	}

	switch {
	case commitErr != nil && endTxnErr == nil:
		return false, commitErr

	case commitErr == nil && endTxnErr != nil:
		return false, endTxnErr

	case commitErr != nil && endTxnErr != nil:
		return false, endTxnErr

	default: // both errs nil
		return willTryCommit, nil
	}
}

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
	cl.cfg.logger.Log(LogLevelInfo, "beginning transaction", "transactional_id", *cl.cfg.txnID)
	return nil
}

// AbortBufferedRecords fails all unflushed records with ErrAborted and waits
// for there to be no buffered records.
//
// This accepts a context to quit the wait early, but it is strongly
// recommended to always wait for all records to be flushed. Waits should not
// occur. The only case where this function returns an error is if the context
// is canceled while flushing.
//
// The intent of this function is to provide a way to clear the client's
// production backlog.
//
// For example, before aborting a transaction and beginning a new one, it would
// be erroneous to not wait for the backlog to clear before beginning a new
// transaction. anything not cleared may be a part of the new transaction.
//
// Records produced during or after a call to this function may not be failed,
// thus it is incorrect to concurrently produce with this function.
func (cl *Client) AbortBufferedRecords(ctx context.Context) error {
	atomic.StoreUint32(&cl.producer.aborting, 1)
	defer atomic.StoreUint32(&cl.producer.aborting, 0)
	// At this point, all drain loops that start will immediately stop,
	// thus they will not begin any AddPartitionsToTxn request. We must
	// now wait for any req currently built to be done being issued.

	for _, partitions := range cl.loadTopics() { // a good a time as any to fail all records
		for _, partition := range partitions.load().all {
			partition.records.failAllRecords(ErrAborting)
		}
	}

	cl.unknownTopicsMu.Lock() // we also have to clear anything waiting in unknown topics
	for topic, unknown := range cl.unknownTopics {
		delete(cl.unknownTopics, topic)
		close(unknown.wait)
		for _, pr := range unknown.buffered {
			cl.finishRecordPromise(pr, ErrAborting)
		}
	}
	cl.unknownTopicsMu.Unlock()

	// Now, we wait for any active drain to stop. We must wait for all
	// drains to stop otherwise we could end up with some exceptionally
	// weird scenario where we end a txn and begin a new one before a
	// prior AddPartitionsToTxn request that was built is issued.
	//
	// By waiting for our drains count to hit 0, we know that at that
	// point, no new AddPartitionsToTxn request will be sent.
	quit := false
	done := make(chan struct{})
	go func() {
		cl.producer.notifyMu.Lock()
		defer cl.producer.notifyMu.Unlock()
		defer close(done)

		for !quit && atomic.LoadInt32(&cl.producer.drains) > 0 {
			cl.producer.notifyCond.Wait()
		}
	}()

	select {
	case <-done:
		// All records were failed above, and all drains are stopped.
		// We are safe to return.
		return nil
	case <-ctx.Done():
		cl.producer.notifyMu.Lock()
		quit = true
		cl.producer.notifyMu.Unlock()
		cl.producer.notifyCond.Broadcast()
		return ctx.Err()
	}
}

// EndTransaction ends a transaction and resets the client's internal state to
// not be in a transaction.
//
// Flush and CommitOffsetsForTransaction must be called before this function;
// this function does not flush and does not itself ensure that all buffered
// records are flushed. If no record yet has caused a partition to be added to
// the transaction, this function does nothing and returns nil. Alternatively,
// AbortBufferedRecords should be called before aborting a transaction to
// ensure that any buffered records not yet flushed will not be a part of a new
// transaction.
//
// If records failed with UnknownProducerID and your Kafka version is at least
// 2.5.0, then aborting here will potentially allow the client to recover for
// more production.
func (cl *Client) EndTransaction(ctx context.Context, commit TransactionEndTry) error {
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

	id, epoch, err := cl.producerID()
	if err != nil {
		if commit {
			return ErrCommitWithFatalID
		}

		switch err.(type) {
		case *kerr.Error:
			kip360 := cl.producer.idVersion >= 3 && (err == kerr.UnknownProducerID || err == kerr.InvalidProducerIDMapping)
			kip588 := cl.producer.idVersion >= 4 && (err == kerr.InvalidProducerEpoch || false /* TODO err == kerr.TransactionTimedOut */)

			recoverable := kip360 || kip588
			if !recoverable {
				return err // fatal, unrecoverable
			}

			// At this point, nothing is being produced and the
			// producer ID loads with an error. Before we allow
			// production to continue, we reset all sequence
			// numbers. Storing errReloadProducerID will reset the
			// id / epoch appropriately and everything will work as
			// per KIP-360.
			for _, tp := range cl.loadTopics() {
				for _, tpd := range tp.load().all {
					tpd.records.resetSeq()
				}
			}

			// With UnknownProducerID and v3 init id, we can recover.
			// No sense issuing an abort request, though.
			cl.producer.id.Store(&producerID{
				id:    id,
				epoch: epoch,
				err:   errReloadProducerID,
			})
			return nil

		default:
			// If this is not a kerr.Error, then it was some arbitrary
			// client error. We can try the EndTxnRequest in the hopes
			// that our id / epoch is valid, but the id / epoch may
			// have never loaded (and thus will be -1 / -1).
		}
	}

	// If no partition was added to a transaction, then we have nothing to commit.
	if !anyAdded {
		cl.cfg.logger.Log(LogLevelInfo, "no records were produced during the commit; thus no transaction was began; ending without doing anything")
		return nil
	}

	cl.cfg.logger.Log(LogLevelInfo, "ending transaction",
		"transactional_id", *cl.cfg.txnID,
		"producer_id", id,
		"epoch", epoch,
		"commit", commit,
	)
	resp, err := (&kmsg.EndTxnRequest{
		TransactionalID: *cl.cfg.txnID,
		ProducerID:      id,
		ProducerEpoch:   epoch,
		Commit:          bool(commit),
	}).RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	return kerr.ErrorForCode(resp.ErrorCode)
}

////////////////////////////////////////////////////////////////////////////////////////////
// TRANSACTIONAL COMMITTING                                                               //
// MOSTLY DUPLICATED CODE DUE TO NO GENERICS AND BECAUSE THE TYPES ARE SLIGHTLY DIFFERENT //
////////////////////////////////////////////////////////////////////////////////////////////

// commitTransactionOffsets is exactly like CommitOffsets, but specifically for
// use with transactional consuming and producing.
//
// Since this function is a gigantic footgun if not done properly, we hide this
// and only allow transaction sessions to commit.
//
// Unlike CommitOffsets, we do not update the group's uncommitted map. We leave
// that to the calling code to do properly with SetOffsets depending on whether
// an eventual abort happens or not.
func (cl *Client) commitTransactionOffsets(
	ctx context.Context,
	uncommitted map[string]map[int32]EpochOffset,
	onDone func(*kmsg.TxnOffsetCommitRequest, *kmsg.TxnOffsetCommitResponse, error),
) {
	cl.cfg.logger.Log(LogLevelDebug, "in commitTransactionOffsets", "with", uncommitted)
	defer cl.cfg.logger.Log(LogLevelDebug, "left commitTransactionOffsets")

	if cl.cfg.txnID == nil {
		onDone(nil, nil, ErrNotTransactional)
		return
	}

	// Before committing, ensure we are at least in a transaction. We
	// unlock the producer txnMu before committing to allow EndTransaction
	// to go through, even though that could cut off our commit.
	cl.producer.txnMu.Lock()
	if !cl.producer.inTxn {
		onDone(nil, nil, ErrNotInTransaction)
		cl.producer.txnMu.Unlock()
		return
	}
	cl.consumer.mu.Lock()
	cl.producer.txnMu.Unlock()

	defer cl.consumer.mu.Unlock()
	if cl.consumer.typ != consumerTypeGroup {
		onDone(new(kmsg.TxnOffsetCommitRequest), new(kmsg.TxnOffsetCommitResponse), ErrNotGroup)
		return
	}
	if len(uncommitted) == 0 {
		onDone(new(kmsg.TxnOffsetCommitRequest), new(kmsg.TxnOffsetCommitResponse), nil)
		return
	}

	g := cl.consumer.group
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.offsetsAddedToTxn {
		if err := cl.addOffsetsToTxn(g.ctx, g.id); err != nil {
			if onDone != nil {
				onDone(nil, nil, err)
			}
			return
		}
	}

	g.commitTxn(ctx, uncommitted, onDone)
}

// addOffsetsToTxn ties a transactional producer to a group. Since this
// requires a producer ID, this initializes one if it is not yet initialized.
// This would only be the case if trying to commit before any records have
// been sent.
func (cl *Client) addOffsetsToTxn(ctx context.Context, group string) error {
	id, epoch, err := cl.producerID()
	if err != nil {
		return err
	}

	cl.cfg.logger.Log(LogLevelInfo, "issuing AddOffsetsToTxn",
		"txn", *cl.cfg.txnID,
		"producerID", id,
		"producerEpoch", epoch,
		"group", group,
	)
	resp, err := (&kmsg.AddOffsetsToTxnRequest{
		TransactionalID: *cl.cfg.txnID,
		ProducerID:      id,
		ProducerEpoch:   epoch,
		Group:           group,
	}).RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	return kerr.ErrorForCode(resp.ErrorCode)
}

// commitTxn is ALMOST EXACTLY THE SAME as commit, but changed for txn types
// and we avoid updateCommitted.
func (g *groupConsumer) commitTxn(
	ctx context.Context,
	uncommitted map[string]map[int32]EpochOffset,
	onDone func(*kmsg.TxnOffsetCommitRequest, *kmsg.TxnOffsetCommitResponse, error),
) {
	if onDone == nil { // note we must always call onDone
		onDone = func(_ *kmsg.TxnOffsetCommitRequest, _ *kmsg.TxnOffsetCommitResponse, _ error) {}
	}
	if len(uncommitted) == 0 { // only empty if called thru autocommit / default revoke
		onDone(new(kmsg.TxnOffsetCommitRequest), new(kmsg.TxnOffsetCommitResponse), nil)
		return
	}

	if g.commitCancel != nil {
		g.commitCancel() // cancel any prior commit
	}
	priorCancel := g.commitCancel
	priorDone := g.commitDone

	commitCtx, commitCancel := context.WithCancel(g.ctx) // enable ours to be canceled and waited for
	commitDone := make(chan struct{})

	g.commitCancel = commitCancel
	g.commitDone = commitDone

	// We issue this request even if the producer ID is failed; the request
	// will fail if it is.
	//
	// The id must have been set at least once by this point because of
	// addOffsetsToTxn.
	id, epoch, _ := g.cl.producerID()
	memberID := g.memberID
	req := &kmsg.TxnOffsetCommitRequest{
		TransactionalID: *g.cl.cfg.txnID,
		Group:           g.id,
		ProducerID:      id,
		ProducerEpoch:   epoch,
		Generation:      g.generation,
		MemberID:        memberID,
		InstanceID:      g.instanceID,
	}

	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				commitCancel()
			case <-commitCtx.Done():
			}
		}()
	}

	go func() {
		defer close(commitDone) // allow future commits to continue when we are done
		defer commitCancel()
		if priorDone != nil {
			select {
			case <-priorDone:
			default:
				g.cl.cfg.logger.Log(LogLevelDebug, "canceling prior txn offset commit to issue another")
				priorCancel()
				<-priorDone // wait for any prior request to finish
			}
		}
		g.cl.cfg.logger.Log(LogLevelDebug, "issuing txn offset commit", "uncommitted", uncommitted)

		for topic, partitions := range uncommitted {
			req.Topics = append(req.Topics, kmsg.TxnOffsetCommitRequestTopic{
				Topic: topic,
			})
			reqTopic := &req.Topics[len(req.Topics)-1]
			for partition, eo := range partitions {
				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.TxnOffsetCommitRequestTopicPartition{
					Partition:   partition,
					Offset:      eo.Offset,
					LeaderEpoch: eo.Epoch,
					Metadata:    &memberID,
				})
			}
		}

		var resp *kmsg.TxnOffsetCommitResponse
		var err error
		if len(req.Topics) > 0 {
			resp, err = req.RequestWith(commitCtx, g.cl)
		}
		if err != nil {
			onDone(req, nil, err)
			return
		}
		onDone(req, resp, nil)
	}()
}
