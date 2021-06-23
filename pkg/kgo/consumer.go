package kgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Offset is a message offset in a partition.
type Offset struct {
	at           int64
	relative     int64
	epoch        int32
	currentEpoch int32 // set by us when mapping offsets to brokers
}

// NewOffset creates and returns an offset to use in ConsumePartitions or
// ConsumeResetOffset.
//
// The default offset begins at the end.
func NewOffset() Offset {
	return Offset{
		at:    -1,
		epoch: -1,
	}
}

// AtStart returns a copy of the calling offset, changing the returned offset
// to begin at the beginning of a partition.
func (o Offset) AtStart() Offset {
	o.at = -2
	return o
}

// AtEnd returns a copy of the calling offset, changing the returned offset to
// begin at the end of a partition.
func (o Offset) AtEnd() Offset {
	o.at = -1
	return o
}

// Relative returns a copy of the calling offset, changing the returned offset
// to be n relative to what it currently is. If the offset is beginning at the
// end, Relative(-100) will begin 100 before the end.
func (o Offset) Relative(n int64) Offset {
	o.relative = n
	return o
}

// WithEpoch returns a copy of the calling offset, changing the returned offset
// to use the given epoch. This epoch is used for truncation detection; the
// default of -1 implies no truncation detection.
func (o Offset) WithEpoch(e int32) Offset {
	if e < 0 {
		e = -1
	}
	o.epoch = e
	return o
}

// At returns a copy of the calling offset, changing the returned offset to
// begin at exactly the requested offset.
//
// There are two potential special offsets to use: -2 allows for consuming at
// the start, and -1 allows for consuming at the end. These two offsets are
// equivalent to calling AtStart or AtEnd.
//
// If the offset is less than -2, the client bounds it to -2 to consume at the
// start.
func (o Offset) At(at int64) Offset {
	if at < -2 {
		at = -2
	}
	o.at = at
	return o
}

type consumer struct {
	cl *Client

	// mu is grabbed when
	//  - polling fetches, for quickly draining sources / updating group uncommitted
	//  - calling assignPartitions (group / direct updates)
	mu sync.Mutex
	d  *directConsumer // if non-nil, we are consuming partitions directly
	g  *groupConsumer  // if non-nil, we are consuming as a group member

	// On metadata update, if the consumer is set (direct or group), the
	// client begins a goroutine that updates the consumer kind's
	// assignments.
	//
	// This is done in a goroutine to not block the metadata loop, because
	// the update **could** wait on a group consumer leaving if a
	// concurrent LeaveGroup is called, or if restarting a session takes
	// just a little bit of time.
	//
	// The update realistically should be instantaneous, but if it is slow,
	// some metadata updates could pile up. We loop with our atomic work
	// loop, which collapses repeated updates into one extra update, so we
	// loop as little as necessary.
	outstandingMetadataUpdates workLoop

	// sessionChangeMu is grabbed when a session is stopped and held through
	// when a session can be started again. The sole purpose is to block an
	// assignment change running concurrently with a metadata update.
	sessionChangeMu sync.Mutex

	session atomic.Value // *consumerSession

	usingCursors usedCursors

	sourcesReadyMu          sync.Mutex
	sourcesReadyCond        *sync.Cond
	sourcesReadyForDraining []*source
	fakeReadyForDraining    []Fetch
}

type usedCursors map[*cursor]struct{}

func (u *usedCursors) use(c *cursor) {
	if *u == nil {
		*u = make(map[*cursor]struct{})
	}
	(*u)[c] = struct{}{}
}

func (c *consumer) init(cl *Client) {
	c.cl = cl
	c.sourcesReadyCond = sync.NewCond(&c.sourcesReadyMu)

	if len(cl.cfg.topics) == 0 && len(cl.cfg.partitions) == 0 {
		return // not consuming
	}

	defer cl.triggerUpdateMetadata(true) // we definitely want to trigger a metadata update

	if len(cl.cfg.group) == 0 {
		c.initDirect()
	} else {
		c.initGroup()
	}
}

func (c *consumer) consuming() bool {
	return c.g != nil || c.d != nil
}

// unset, called under the assign mu, transitions the consumer to the unset
// state, invalidating old assignments and leaving a group if it was in one.
//
// If in a group, this waits for the leave group to finish before returning.
func (c *consumer) unset() {
	if !c.consuming() {
		return
	}

	// Unsetting means all old cursors are useless. Before we return, we
	// delete all old cursors. This is especially important in the context
	// of a new assignment, which will create new cursors that may
	// duplicate our old.
	//
	// Note that this is happening outside of the context of a valid
	// consumer session, so we do not need to worry about much else.
	//
	// Since we are blocking metadata, sinksAndSources cannot be modified
	// concurrently and we do not need a lock.
	cl := c.cl
	defer cl.blockingMetadataFn(func() {
		for _, sns := range cl.sinksAndSources {
			s := sns.source
			s.cursorsMu.Lock()
			s.cursors = nil
			s.cursorsStart = 0
			s.cursorsMu.Unlock()
		}
	})

	wait := func() {} // wait AFTER we unlock
	defer func() { wait() }()

	c.mu.Lock() // lock for assign
	defer c.mu.Unlock()

	c.assignPartitions(nil, assignInvalidateAll, noTopicsPartitions)

	if c.g != nil {
		wait = c.g.leave()
	}
}

// addSourceReadyForDraining tracks that a source needs its buffered fetch
// consumed.
func (c *consumer) addSourceReadyForDraining(source *source) {
	c.sourcesReadyMu.Lock()
	c.sourcesReadyForDraining = append(c.sourcesReadyForDraining, source)
	c.sourcesReadyMu.Unlock()
	c.sourcesReadyCond.Broadcast()
}

// addFakeReadyForDraining saves a fake fetch that has important partition
// errors--data loss or auth failures.
func (c *consumer) addFakeReadyForDraining(topic string, partition int32, err error) {
	c.sourcesReadyMu.Lock()
	c.fakeReadyForDraining = append(c.fakeReadyForDraining, Fetch{Topics: []FetchTopic{{
		Topic: topic,
		Partitions: []FetchPartition{{
			Partition: partition,
			Err:       err,
		}},
	}}})
	c.sourcesReadyMu.Unlock()
	c.sourcesReadyCond.Broadcast()
}

// PollFetches waits for fetches to be available, returning as soon as any
// broker returns a fetch. If the ctx quits, this function quits.
//
// It is important to check all partition errors in the returned fetches. If
// any partition has a fatal error and actually had no records, fake fetch will
// be injected with the error.
func (cl *Client) PollFetches(ctx context.Context) Fetches {
	return cl.PollRecords(ctx, 0)
}

// PollRecords waits for records to be available, returning as soon as any
// broker returns a record in a fetch. If the ctx quits, this function quits.
// This returns a maximum of maxPollRecords total across all fetches, or
// returns all buffered records if maxPollRecords is <= 0.
//
// It is important to check all partition errors in the returned fetches. If
// any partition has a fatal error and actually had no records, fake fetch will
// be injected with the error.
//
// If the client is closing or has closed, a fake fetch will be injected that
// has no topic, a partition of 0, and a partition error of ErrClientClosed.
// This can be used to detect if the client is closing and to break out of a
// poll loop.
func (cl *Client) PollRecords(ctx context.Context, maxPollRecords int) Fetches {
	if maxPollRecords == 0 {
		maxPollRecords = -1
	}
	c := &cl.consumer

	var fetches Fetches
	fill := func() {
		// A group can grab the consumer lock then the group mu and
		// assign partitions. The group mu is grabbed to update its
		// uncommitted map. Assigning partitions clears sources ready
		// for draining.
		//
		// We need to grab the consumer mu to ensure proper lock
		// ordering and prevent lock inversion. Polling fetches also
		// updates the group's uncommitted map; if we do not grab the
		// consumer mu at the top, we have a problem: without the lock,
		// we could have grabbed some sources, then a group assigned,
		// and after the assign, we update uncommitted with fetches
		// from the old assignment
		c.mu.Lock()
		defer c.mu.Unlock()

		c.sourcesReadyMu.Lock()
		if maxPollRecords < 0 {
			for _, ready := range c.sourcesReadyForDraining {
				fetches = append(fetches, ready.takeBuffered())
			}
			c.sourcesReadyForDraining = nil
		} else {
			for len(c.sourcesReadyForDraining) > 0 && maxPollRecords > 0 {
				source := c.sourcesReadyForDraining[0]
				fetch, taken, drained := source.takeNBuffered(maxPollRecords)
				if drained {
					c.sourcesReadyForDraining = c.sourcesReadyForDraining[1:]
				}
				maxPollRecords -= taken
				fetches = append(fetches, fetch)
			}
		}

		realFetches := fetches

		fetches = append(fetches, c.fakeReadyForDraining...)
		c.fakeReadyForDraining = nil

		c.sourcesReadyMu.Unlock()

		if len(realFetches) == 0 {
			return
		}

		// Before returning, we want to update our uncommitted. If we
		// updated after, then we could end up with weird interactions
		// with group invalidations where we return a stale fetch after
		// committing in onRevoke.
		//
		// A blocking onRevoke commit, on finish, allows a new group
		// session to start. If we returned stale fetches that did not
		// have their uncommitted offset tracked, then we would allow
		// duplicates.
		if c.g != nil {
			c.g.updateUncommitted(realFetches)
		}
	}

	fill()
	if len(fetches) > 0 {
		return fetches
	}

	done := make(chan struct{})
	quit := false
	go func() {
		c.sourcesReadyMu.Lock()
		defer c.sourcesReadyMu.Unlock()
		defer close(done)

		for !quit && len(c.sourcesReadyForDraining) == 0 {
			c.sourcesReadyCond.Wait()
		}
	}()

	exit := func() {
		c.sourcesReadyMu.Lock()
		quit = true
		c.sourcesReadyMu.Unlock()
		c.sourcesReadyCond.Broadcast()
	}

	select {
	case <-cl.ctx.Done():
		// The client is closed: we inject an error right now, which
		// will be drained immediately in the fill call just below, and
		// then will be returned with our fetches.
		c.addFakeReadyForDraining("", 0, ErrClientClosed)
		exit()
	case <-ctx.Done():
		// The user canceled: no need to inject anything; just return.
		exit()
	case <-done:
	}

	fill()
	return fetches
}

// assignHow controls how assignPartitions operates.
type assignHow int8

const (
	// This option simply assigns new offsets, doing nothing with existing
	// offsets / active fetches / buffered fetches.
	assignWithoutInvalidating assignHow = iota

	// This option invalidates active fetches so they will not buffer and
	// drops all buffered fetches, and then continues to assign the new
	// assignments.
	assignInvalidateAll

	// This option does not assign, but instead invalidates any active
	// fetches for "assigned" (actually lost) partitions. This additionally
	// drops all buffered fetches, because they could contain partitions we
	// lost. Thus, with this option, the actual offset in the map is
	// meaningless / a dummy offset.
	assignInvalidateMatching

	// The counterpart to assignInvalidateMatching, assignSetMatching
	// resets all matching partitions to the specified offset / epoch.
	assignSetMatching
)

func (h assignHow) String() string {
	switch h {
	case assignWithoutInvalidating:
		return "assign without invalidating"
	case assignInvalidateAll:
		return "assign invalidate all"
	case assignInvalidateMatching:
		return "assign invalidate matching"
	case assignSetMatching:
		return "assign set matching"
	}
	return ""
}

// assignPartitions, called under the consumer's mu, is used to set new
// cursors or add to the existing cursors.
func (c *consumer) assignPartitions(assignments map[string]map[int32]Offset, how assignHow, tps *topicsPartitions) {
	var session *consumerSession
	var loadOffsets listOrEpochLoads
	if how == assignInvalidateAll {
		tps = nil
	}
	defer func() {
		if session == nil { // if nil, we stopped the session
			session = c.startNewSession(tps)
		}
		loadOffsets.loadWithSession(session) // odds are this assign came from a metadata update, so no reason to force a refresh with loadWithSessionNow
	}()

	if how == assignWithoutInvalidating {
		// Guarding a session change can actually create a new session
		// if we had no session before, which is why we need to pass in
		// our topicPartitions.
		session = c.guardSessionChange(tps)
		defer c.unguardSessionChange()

	} else {
		loadOffsets, _ = c.stopSession()

		// First, over all cursors currently in use, we unset them or set them
		// directly as appropriate. Anything we do not unset, we keep.

		var keep usedCursors
		for usedCursor := range c.usingCursors {
			shouldKeep := true
			if how == assignInvalidateAll {
				usedCursor.unset()
				shouldKeep = false
			} else { // invalidateMatching or setMatching
				if assignTopic, ok := assignments[usedCursor.topic]; ok {
					if assignPart, ok := assignTopic[usedCursor.partition]; ok {
						if how == assignInvalidateMatching {
							usedCursor.unset()
							shouldKeep = false
						} else { // how == assignSetMatching
							usedCursor.setOffset(cursorOffset{
								offset:            assignPart.at,
								lastConsumedEpoch: assignPart.epoch,
							})
						}
					}
				}
			}
			if shouldKeep {
				keep.use(usedCursor)
			}
		}
		c.usingCursors = keep

		// For any partition that was listing offsets or loading
		// epochs, we want to ensure that if we are keeping those
		// partitions, we re-start the list/load.
		//
		// Note that we do not need to unset cursors here; anything
		// that actually resulted in a cursor is forever tracked in
		// usedCursors. We only do not have used cursors if an
		// assignment went straight to listing / epoch loading, and
		// that list/epoch never finished.
		switch how {
		case assignInvalidateAll:
			loadOffsets = listOrEpochLoads{}
		case assignSetMatching:
			// We had not yet loaded this partition, so there is
			// nothing to set, and we keep everything.
		case assignInvalidateMatching:
			loadOffsets.keepFilter(func(t string, p int32) bool {
				if assignTopic, ok := assignments[t]; ok {
					if _, ok := assignTopic[p]; ok {
						return false
					}
				}
				return true
			})
		}
	}

	// This assignment could contain nothing (for the purposes of
	// invalidating active fetches), so we only do this if needed.
	if len(assignments) == 0 || how == assignInvalidateMatching || how == assignSetMatching {
		return
	}

	c.cl.cfg.logger.Log(LogLevelDebug, "assign requires loading offsets")

	topics := tps.load()
	for topic, partitions := range assignments {
		topicPartitions := topics.loadTopic(topic) // should be non-nil
		if topicPartitions == nil {
			c.cl.cfg.logger.Log(LogLevelError, "BUG! consumer was assigned topic that we did not ask for in ConsumeTopics nor ConsumePartitions, skipping!", "topic", topic)
			continue
		}

		for partition, offset := range partitions {
			// First, if the request is exact, get rid of the relative
			// portion. We are modifying a copy of the offset, i.e. we
			// are appropriately not modfying 'assignments' itself.
			if offset.at >= 0 {
				offset.at = offset.at + offset.relative
				if offset.at < 0 {
					offset.at = 0
				}
				offset.relative = 0
			}

			// If we are requesting an exact offset with an epoch,
			// we do truncation detection and then use the offset.
			//
			// Otherwise, an epoch is specified without an exact
			// request which is useless for us, or a request is
			// specified without a known epoch.
			if offset.at >= 0 && offset.epoch >= 0 {
				loadOffsets.addLoad(topic, partition, loadTypeEpoch, offsetLoad{
					replica: -1,
					Offset:  offset,
				})
				continue
			}

			// If an exact offset is specified and we have loaded
			// the partition, we use it. Without an epoch, if it is
			// out of bounds, we just reset appropriately.
			//
			// If an offset is unspecified or we have not loaded
			// the partition, we list offsets to find out what to
			// use.
			if offset.at >= 0 && partition >= 0 && partition < int32(len(topicPartitions.partitions)) {
				part := topicPartitions.partitions[partition]
				cursor := part.cursor
				cursor.setOffset(cursorOffset{
					offset:            offset.at,
					lastConsumedEpoch: part.leaderEpoch,
				})
				cursor.allowUsable()
				c.usingCursors.use(cursor)
				continue
			}

			loadOffsets.addLoad(topic, partition, loadTypeList, offsetLoad{
				replica: -1,
				Offset:  offset,
			})
		}
	}
}

func (c *consumer) doOnMetadataUpdate() {
	if !c.consuming() {
		return
	}

	// See the comment on the outstandingMetadataUpdates field for why this
	// block below.
	if c.outstandingMetadataUpdates.maybeBegin() {
		doUpdate := func() {
			// We forbid reassignments while we do a quick check for
			// new assignments--for the direct consumer particularly,
			// this prevents TOCTOU.
			c.mu.Lock()
			defer c.mu.Unlock()

			switch {
			case c.d != nil:
				if new := c.d.findNewAssignments(); len(new) > 0 {
					c.assignPartitions(new, assignWithoutInvalidating, c.d.tps)
				}
			case c.g != nil:
				c.g.findNewAssignments()
			}

			go c.loadSession().doOnMetadataUpdate()
		}

		go func() {
			again := true
			for again {
				doUpdate()
				again = c.outstandingMetadataUpdates.maybeFinish(false)
			}
		}()
	}

}

func (s *consumerSession) doOnMetadataUpdate() {
	if s == nil || s == noConsumerSession { // no session started yet
		return
	}

	s.listOrEpochMu.Lock()
	defer s.listOrEpochMu.Unlock()

	if s.listOrEpochMetaCh == nil {
		return // nothing waiting to load epochs / offsets
	}
	select {
	case s.listOrEpochMetaCh <- struct{}{}:
	default:
	}
}

// offsetLoad is effectively an Offset, but also includes a potential replica
// to directly use if a cursor had a preferred replica.
type offsetLoad struct {
	replica int32 // -1 means leader
	Offset
}

type offsetLoadMap map[string]map[int32]offsetLoad

func (o offsetLoadMap) errToLoaded(err error) []loadedOffset {
	var loaded []loadedOffset
	for t, ps := range o {
		for p, o := range ps {
			loaded = append(loaded, loadedOffset{
				topic:     t,
				partition: p,
				err:       err,
				request:   o,
			})
		}
	}
	return loaded
}

// Combines list and epoch loads into one type for simplicity.
type listOrEpochLoads struct {
	list  offsetLoadMap
	epoch offsetLoadMap
}

type listOrEpochLoadType uint8

const (
	loadTypeList listOrEpochLoadType = iota
	loadTypeEpoch
)

// adds an offset to be loaded, ensuring it exists only in the final loadType.
func (l *listOrEpochLoads) addLoad(t string, p int32, loadType listOrEpochLoadType, load offsetLoad) {
	l.removeLoad(t, p)
	dst := &l.list
	if loadType == loadTypeEpoch {
		dst = &l.epoch
	}

	if *dst == nil {
		*dst = make(offsetLoadMap)
	}
	ps := (*dst)[t]
	if ps == nil {
		ps = make(map[int32]offsetLoad)
		(*dst)[t] = ps
	}
	ps[p] = load
}

func (l *listOrEpochLoads) removeLoad(t string, p int32) {
	for _, m := range []offsetLoadMap{
		l.list,
		l.epoch,
	} {
		if m == nil {
			continue
		}
		ps := m[t]
		if ps == nil {
			continue
		}
		delete(ps, p)
		if len(ps) == 0 {
			delete(m, t)
		}
	}
}

func (l listOrEpochLoads) each(fn func(string, int32)) {
	for _, m := range []offsetLoadMap{
		l.list,
		l.epoch,
	} {
		for topic, partitions := range m {
			for partition := range partitions {
				fn(topic, partition)
			}
		}
	}
}

func (l *listOrEpochLoads) keepFilter(keep func(string, int32) bool) {
	for _, m := range []offsetLoadMap{
		l.list,
		l.epoch,
	} {
		for t, ps := range m {
			for p := range ps {
				if !keep(t, p) {
					delete(ps, p)
					if len(ps) == 0 {
						delete(m, t)
					}
				}
			}
		}
	}
}

// Merges loads into the caller; used to coalesce loads while a metadata update
// is happening (see the only use below).
func (dst *listOrEpochLoads) mergeFrom(src listOrEpochLoads) {
	for _, srcs := range []struct {
		m        offsetLoadMap
		loadType listOrEpochLoadType
	}{
		{src.list, loadTypeList},
		{src.epoch, loadTypeEpoch},
	} {
		for t, ps := range srcs.m {
			for p, load := range ps {
				dst.addLoad(t, p, srcs.loadType, load)
			}
		}
	}
}

func (l listOrEpochLoads) isEmpty() bool { return len(l.list) == 0 && len(l.epoch) == 0 }

func (l listOrEpochLoads) loadWithSession(s *consumerSession) {
	if !l.isEmpty() {
		s.incWorker()
		go s.listOrEpoch(l, false)
	}
}

func (l listOrEpochLoads) loadWithSessionNow(s *consumerSession) bool {
	if !l.isEmpty() {
		s.incWorker()
		go s.listOrEpoch(l, true)
		return true
	}
	return false
}

// A consumer session is responsible for an era of fetching records for a set
// of cursors. The set can be added to without killing an active session, but
// it cannot be removed from. Removing any cursor from being consumed kills the
// current consumer session and begins a new one.
type consumerSession struct {
	c *consumer

	ctx    context.Context
	cancel func()

	// tps tracks the topics that were assigned in this session. We use
	// this field to build and handle list offset / load epoch requests.
	tps *topicsPartitions

	// desireFetchCh is sized to the number of concurrent fetches we are
	// configured to be able to send.
	//
	// We receive desires from sources, we reply when they can fetch, and
	// they send back when they are done. Thus, three level chan.
	desireFetchCh       chan chan chan struct{}
	cancelFetchCh       chan chan chan struct{}
	allowedConcurrency  int
	fetchManagerStarted uint32 // atomic, once 1, we start the fetch manager

	// Workers signify the number of fetch and list / epoch goroutines that
	// are currently running within the context of this consumer session.
	// Stopping a session only returns once workers hits zero.
	workersMu   sync.Mutex
	workersCond *sync.Cond
	workers     int

	listOrEpochMu           sync.Mutex
	listOrEpochLoadsWaiting listOrEpochLoads
	listOrEpochMetaCh       chan struct{} // non-nil if Loads is non-nil, signalled on meta update
	listOrEpochLoadsLoading listOrEpochLoads
}

func (c *consumer) newConsumerSession(tps *topicsPartitions) *consumerSession {
	if tps == nil || len(tps.load()) == 0 {
		return noConsumerSession
	}
	ctx, cancel := context.WithCancel(c.cl.ctx)
	session := &consumerSession{
		c: c,

		ctx:    ctx,
		cancel: cancel,

		tps: tps,

		desireFetchCh:      make(chan chan chan struct{}, 8),
		cancelFetchCh:      make(chan chan chan struct{}, 4),
		allowedConcurrency: c.cl.cfg.allowedConcurrentFetches,
	}
	session.workersCond = sync.NewCond(&session.workersMu)
	return session
}

func (c *consumerSession) desireFetch() chan chan chan struct{} {
	if atomic.SwapUint32(&c.fetchManagerStarted, 1) == 0 {
		go c.manageFetchConcurrency()
	}
	return c.desireFetchCh
}

func (c *consumerSession) manageFetchConcurrency() {
	var (
		activeFetches int
		doneFetch     = make(chan struct{}, 20)
		wantFetch     []chan chan struct{}

		ctxCh    = c.ctx.Done()
		wantQuit bool
	)
	for {
		select {
		case register := <-c.desireFetchCh:
			wantFetch = append(wantFetch, register)
		case cancel := <-c.cancelFetchCh:
			var found bool
			for i, want := range wantFetch {
				if want == cancel {
					_ = append(wantFetch[i:], wantFetch[i+1:]...)
					wantFetch = wantFetch[:len(wantFetch)-1]
					found = true
				}
			}
			// If we did not find the channel, then we have already
			// sent to it, removed it from our wantFetch list, and
			// bumped activeFetches.
			if !found {
				activeFetches--
			}

		case <-doneFetch:
			activeFetches--
		case <-ctxCh:
			wantQuit = true
			ctxCh = nil
		}

		if len(wantFetch) > 0 && (activeFetches < c.allowedConcurrency || c.allowedConcurrency == 0) { // 0 means unbounded
			wantFetch[0] <- doneFetch
			wantFetch = wantFetch[1:]
			activeFetches++
			continue
		}

		if wantQuit && activeFetches == 0 {
			return
		}
	}
}

func (c *consumerSession) incWorker() {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()
	c.workers++
}

func (c *consumerSession) decWorker() {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()
	c.workers--
	if c.workers == 0 {
		c.workersCond.Broadcast()
	}
}

// noConsumerSession exists because we cannot store nil into an atomic.Value.
var noConsumerSession = new(consumerSession)

func (c *consumer) loadSession() *consumerSession {
	if session := c.session.Load(); session != nil {
		return session.(*consumerSession)
	}
	return noConsumerSession
}

// Guards against a session being stopped, and must be paired with an unguard.
// This returns a new session if there was no session.
//
// The purpose of this function is when performing additive-only changes to an
// existing session, because additive-only changes can avoid killing a running
// session.
func (c *consumer) guardSessionChange(tps *topicsPartitions) *consumerSession {
	c.sessionChangeMu.Lock()

	session := c.loadSession()
	if session == noConsumerSession {
		// If there is no session, we simply store one. This is fine;
		// sources will be able to begin a fetch loop, but they will
		// have no cursors to consume yet.
		session = c.newConsumerSession(tps)
		c.session.Store(session)
	}

	return session
}
func (c *consumer) unguardSessionChange() {
	c.sessionChangeMu.Unlock()
}

// Stops an active consumer session if there is one, and does not return until
// all fetching, listing, offset for leader epoching is complete. This
// invalidates any buffered fetches for the previous session and returns any
// partitions that were listing offsets or loading epochs.
func (c *consumer) stopSession() (listOrEpochLoads, *topicsPartitions) {
	c.sessionChangeMu.Lock()

	session := c.loadSession()

	if session == noConsumerSession {
		return listOrEpochLoads{}, noTopicsPartitions // we had no session
	}

	// Before storing noConsumerSession, cancel our old. This pairs
	// with the reverse ordering in source, which checks noConsumerSession
	// then checks the session context.
	session.cancel()

	// At this point, any in progress fetches, offset lists, or epoch loads
	// will quickly die.

	c.session.Store(noConsumerSession)

	// At this point, no source can be started, because the session is
	// noConsumerSession.

	session.workersMu.Lock()
	for session.workers > 0 {
		session.workersCond.Wait()
	}
	session.workersMu.Unlock()

	// At this point, all fetches, lists, and loads are dead. We can close
	// our num-fetches manager without worrying about a source trying to
	// register itself.

	c.cl.sinksAndSourcesMu.Lock()
	for _, sns := range c.cl.sinksAndSources {
		sns.source.session.reset()
	}
	c.cl.sinksAndSourcesMu.Unlock()

	// At this point, if we begin fetching anew, then the sources will not
	// be using stale fetch sessions.

	c.sourcesReadyMu.Lock()
	defer c.sourcesReadyMu.Unlock()
	for _, ready := range c.sourcesReadyForDraining {
		ready.discardBuffered()
	}
	c.sourcesReadyForDraining = nil

	// At this point, we have invalidated any buffered data from the prior
	// session. We leave any fake things that were ready so that the user
	// can act on errors. The session is dead.

	session.listOrEpochLoadsWaiting.mergeFrom(session.listOrEpochLoadsLoading)
	return session.listOrEpochLoadsWaiting, session.tps
}

// Starts a new consumer session, allowing fetches to happen.
func (c *consumer) startNewSession(tps *topicsPartitions) *consumerSession {
	session := c.newConsumerSession(tps)
	c.session.Store(session)

	// At this point, sources can start consuming.

	c.sessionChangeMu.Unlock()

	c.cl.sinksAndSourcesMu.Lock()
	for _, sns := range c.cl.sinksAndSources {
		sns.source.maybeConsume()
	}
	c.cl.sinksAndSourcesMu.Unlock()

	// At this point, any source that was not consuming becauase it saw the
	// session was stopped has been notified to potentially start consuming
	// again. The session is alive.

	return session
}

// This function is responsible for issuing ListOffsets or
// OffsetForLeaderEpoch. These requests's responses  are only handled within
// the context of a consumer session.
func (s *consumerSession) listOrEpoch(waiting listOrEpochLoads, immediate bool) {
	defer s.decWorker()

	wait := true
	if immediate {
		s.c.cl.triggerUpdateMetadataNow()
	} else {
		wait = s.c.cl.triggerUpdateMetadata(false) // avoid trigger if within refresh interval
	}

	s.listOrEpochMu.Lock() // collapse any listOrEpochs that occur during meta update into one
	if !s.listOrEpochLoadsWaiting.isEmpty() {
		s.listOrEpochLoadsWaiting.mergeFrom(waiting)
		s.listOrEpochMu.Unlock()
		return
	}
	s.listOrEpochLoadsWaiting = waiting
	s.listOrEpochMetaCh = make(chan struct{}, 1)
	s.listOrEpochMu.Unlock()

	if wait {
		select {
		case <-s.ctx.Done():
			return
		case <-s.listOrEpochMetaCh:
		}
	}

	s.listOrEpochMu.Lock()
	loading := s.listOrEpochLoadsWaiting
	s.listOrEpochLoadsLoading.mergeFrom(loading)
	s.listOrEpochLoadsWaiting = listOrEpochLoads{}
	s.listOrEpochMetaCh = nil
	s.listOrEpochMu.Unlock()

	brokerLoads := s.mapLoadsToBrokers(loading)

	results := make(chan loadedOffsets, 2*len(brokerLoads)) // each broker can receive up to two requests

	var issued, received int
	for broker, brokerLoad := range brokerLoads {
		s.c.cl.cfg.logger.Log(LogLevelDebug, "offsets to load broker", "broker", broker.meta.NodeID, "load", brokerLoad)
		if len(brokerLoad.list) > 0 {
			issued++
			go s.c.cl.listOffsetsForBrokerLoad(s.ctx, broker, brokerLoad.list, s.tps, results)
		}
		if len(brokerLoad.epoch) > 0 {
			issued++
			go s.c.cl.loadEpochsForBrokerLoad(s.ctx, broker, brokerLoad.epoch, s.tps, results)
		}
	}

	var reloads listOrEpochLoads
	defer func() {
		if !reloads.isEmpty() {
			s.incWorker()
			go func() {
				// Before we dec our worker, we must add the
				// reloads back into the session's waiting loads.
				// Doing so allows a concurrent stopSession to
				// track the waiting loads, whereas if we did not
				// add things back to the session, we could abandon
				// loading these offsets and have a stuck cursor.
				defer s.decWorker()
				defer reloads.loadWithSession(s)
				after := time.NewTimer(time.Second)
				defer after.Stop()
				select {
				case <-after.C:
				case <-s.ctx.Done():
					return
				}
			}()
		}
	}()

	for received != issued {
		select {
		case <-s.ctx.Done():
			// If we return early, our session was canceled. We do
			// not move loading list or epoch loads back to
			// waiting; the session stopping manages that.
			return
		case loaded := <-results:
			received++
			reloads.mergeFrom(s.handleListOrEpochResults(loaded))
		}
	}
}

// Called within a consumer session, this function handles results from list
// offsets or epoch loads and returns any loads that should be retried.
//
// To us, all errors are reloadable. We either have request level retriable
// errors (unknown partition, etc) or non-retriable errors (auth), or we have
// request issuing errors (no dial, connection cut repeatedly).
//
// For retriable request errors, we may as well back off a little bit to allow
// Kafka to harmonize if the topic exists / etc.
//
// For non-retriable request errors, we may as well retry to both (a) allow the
// user more signals about a problem that they can maybe fix within Kafka (i.e.
// the auth), and (b) force the user to notice errors.
//
// For request issuing errors, we may as well continue to retry because there
// is not much else we can do. RequestWith already retries, but returns when
// the retry limit is hit. We will backoff 1s and then allow RequestWith to
// continue requesting and backing off.
func (s *consumerSession) handleListOrEpochResults(loaded loadedOffsets) (reloads listOrEpochLoads) {
	// This function can be running twice concurrently, so we need to guard
	// listOrEpochLoadsLoading and usingCursors. For simplicity, we just
	// guard this entire function.
	s.listOrEpochMu.Lock()
	defer s.listOrEpochMu.Unlock()

	for _, load := range loaded.loaded {
		s.listOrEpochLoadsLoading.removeLoad(load.topic, load.partition) // remove the tracking of this load from our session

		use := func() {
			load.cursor.setOffset(cursorOffset{
				offset:            load.offset,
				lastConsumedEpoch: load.leaderEpoch,
			})
			load.cursor.allowUsable()
			s.c.usingCursors.use(load.cursor)
		}

		switch load.err.(type) {
		case *ErrDataLoss:
			s.c.addFakeReadyForDraining(load.topic, load.partition, load.err) // signal we lost data, but set the cursor to what we can
			use()

		case nil:
			use()

		default: // from ErrorCode in a response
			reloads.addLoad(load.topic, load.partition, loaded.loadType, load.request)
			if !kerr.IsRetriable(load.err) && !isRetriableBrokerErr(load.err) { // non-retriable response error; signal such in a response
				s.c.addFakeReadyForDraining(load.topic, load.partition, load.err)
			}
		}
	}

	return reloads
}

// Splits the loads into per-broker loads, mapping each partition to the broker
// that leads that partition.
func (s *consumerSession) mapLoadsToBrokers(loads listOrEpochLoads) map[*broker]listOrEpochLoads {
	brokerLoads := make(map[*broker]listOrEpochLoads)

	s.c.cl.brokersMu.RLock() // hold mu so we can check if partition leaders exist
	defer s.c.cl.brokersMu.RUnlock()

	brokers := s.c.cl.brokers
	seed := brokers[unknownSeedID(0)] // must be non-nil

	topics := s.tps.load()
	for _, loads := range []struct {
		m        offsetLoadMap
		loadType listOrEpochLoadType
	}{
		{loads.list, loadTypeList},
		{loads.epoch, loadTypeEpoch},
	} {
		for topic, partitions := range loads.m {
			topicPartitions := topics.loadTopic(topic) // this must exist, it not existing would be a bug
			for partition, offset := range partitions {

				// We default to the first seed broker if we have no loaded
				// the broker leader for this partition (we should have).
				// Worst case, we get an error for the partition and retry.
				broker := seed
				if partition >= 0 && partition < int32(len(topicPartitions.partitions)) {
					topicPartition := topicPartitions.partitions[partition]
					brokerID := topicPartition.leader
					if offset.replica != -1 {
						// If we are fetching from a follower, we can list
						// offsets against the follower itself. The replica
						// being non-negative signals that.
						brokerID = offset.replica
					}
					if tryBroker := brokers[brokerID]; tryBroker != nil {
						broker = tryBroker
					}
					offset.currentEpoch = topicPartition.leaderEpoch // ensure we set our latest epoch for the partition
				}

				brokerLoad := brokerLoads[broker]
				brokerLoad.addLoad(topic, partition, loads.loadType, offset)
				brokerLoads[broker] = brokerLoad
			}
		}
	}

	return brokerLoads
}

// The result of ListOffsets or OffsetForLeaderEpoch for an individual
// partition.
type loadedOffset struct {
	topic     string
	partition int32

	// The following three are potentially unset if the error is non-nil
	// and not ErrDataLoss; these are what we loaded.
	cursor      *cursor
	offset      int64
	leaderEpoch int32

	// Any error encountered for loading this partition, or for epoch
	// loading, potentially ErrDataLoss. If this error is not retriable, we
	// avoid reloading the offset and instead inject a fake partition for
	// PollFetches containing this error.
	err error

	// The original request.
	request offsetLoad
}

// The results of ListOffsets or OffsetForLeaderEpoch for an individual broker.
type loadedOffsets struct {
	loaded   []loadedOffset
	loadType listOrEpochLoadType
}

func (l *loadedOffsets) add(a loadedOffset) { l.loaded = append(l.loaded, a) }
func (l *loadedOffsets) addAll(as []loadedOffset) loadedOffsets {
	l.loaded = append(l.loaded, as...)
	return *l
}

func (cl *Client) listOffsetsForBrokerLoad(ctx context.Context, broker *broker, load offsetLoadMap, tps *topicsPartitions, results chan<- loadedOffsets) {
	loaded := loadedOffsets{loadType: loadTypeList}

	kresp, err := broker.waitResp(ctx, load.buildListReq(cl.cfg.isolationLevel))
	if err != nil {
		results <- loaded.addAll(load.errToLoaded(err))
		return
	}

	topics := tps.load()
	resp := kresp.(*kmsg.ListOffsetsResponse)
	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		loadParts, ok := load[topic]
		if !ok {
			continue // should not happen: kafka replied with something we did not ask for
		}

		topicPartitions := topics.loadTopic(topic) // must be non-nil at this point
		for _, rPartition := range rTopic.Partitions {
			partition := rPartition.Partition
			loadPart, ok := loadParts[partition]
			if !ok {
				continue // should not happen: kafka replied with something we did not ask for
			}

			if err := kerr.ErrorForCode(rPartition.ErrorCode); err != nil {
				loaded.add(loadedOffset{
					topic:     topic,
					partition: partition,
					err:       err,
					request:   loadPart,
				})
				continue // partition err: handled in results
			}

			if partition < 0 || partition >= int32(len(topicPartitions.partitions)) {
				continue // should not happen: we have not seen this partition from a metadata response
			}
			topicPartition := topicPartitions.partitions[partition]

			delete(loadParts, partition)
			if len(loadParts) == 0 {
				delete(load, topic)
			}

			offset := rPartition.Offset + loadPart.relative
			if len(rPartition.OldStyleOffsets) > 0 { // if we have any, we used list offsets v0
				offset = rPartition.OldStyleOffsets[0] + loadPart.relative
			}
			if loadPart.at >= 0 {
				offset = loadPart.at + loadPart.relative // we obey exact requests, even if they end up past the end
			}
			if offset < 0 {
				offset = 0
			}

			loaded.add(loadedOffset{
				topic:       topic,
				partition:   partition,
				cursor:      topicPartition.cursor,
				offset:      offset,
				leaderEpoch: rPartition.LeaderEpoch,
				request:     loadPart,
			})
		}
	}

	results <- loaded.addAll(load.errToLoaded(kerr.UnknownTopicOrPartition))
}

func (cl *Client) loadEpochsForBrokerLoad(ctx context.Context, broker *broker, load offsetLoadMap, tps *topicsPartitions, results chan<- loadedOffsets) {
	loaded := loadedOffsets{loadType: loadTypeEpoch}

	kresp, err := broker.waitResp(ctx, load.buildEpochReq())
	if err != nil {
		results <- loaded.addAll(load.errToLoaded(err))
		return
	}

	// If the version is < 2, we are speaking to an old broker. We should
	// not have an old version, but we could have spoken to a new broker
	// first then an old broker in the middle of a broker roll. For now, we
	// will just loop retrying until the broker is upgraded.

	topics := tps.load()
	resp := kresp.(*kmsg.OffsetForLeaderEpochResponse)
	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		loadParts, ok := load[topic]
		if !ok {
			continue // should not happen: kafka replied with something we did not ask for
		}

		topicPartitions := topics.loadTopic(topic) // must be non-nil at this point
		for _, rPartition := range rTopic.Partitions {
			partition := rPartition.Partition
			loadPart, ok := loadParts[partition]
			if !ok {
				continue // should not happen: kafka replied with something we did not ask for
			}

			if err := kerr.ErrorForCode(rPartition.ErrorCode); err != nil {
				loaded.add(loadedOffset{
					topic:     topic,
					partition: partition,
					err:       err,
					request:   loadPart,
				})
				continue // partition err: handled in results
			}

			if partition < 0 || partition >= int32(len(topicPartitions.partitions)) {
				continue // should not happen: we have not seen this partition from a metadata response
			}
			topicPartition := topicPartitions.partitions[partition]

			delete(loadParts, partition)
			if len(loadParts) == 0 {
				delete(load, topic)
			}

			offset := loadPart.at
			var err error
			if rPartition.EndOffset < offset {
				offset = rPartition.EndOffset
				err = &ErrDataLoss{topic, partition, offset, rPartition.EndOffset}
			}

			loaded.add(loadedOffset{
				topic:       topic,
				partition:   partition,
				cursor:      topicPartition.cursor,
				offset:      offset,
				leaderEpoch: rPartition.LeaderEpoch,
				err:         err,
				request:     loadPart,
			})
		}
	}

	results <- loaded.addAll(load.errToLoaded(kerr.UnknownTopicOrPartition))
}

func (o offsetLoadMap) buildListReq(isolationLevel int8) *kmsg.ListOffsetsRequest {
	req := &kmsg.ListOffsetsRequest{
		ReplicaID:      -1,
		IsolationLevel: isolationLevel,
		Topics:         make([]kmsg.ListOffsetsRequestTopic, 0, len(o)),
	}
	for topic, partitions := range o {
		parts := make([]kmsg.ListOffsetsRequestTopicPartition, 0, len(partitions))
		for partition, offset := range partitions {
			// If this partition is using an exact offset request,
			// then we are listing for a partition that was not yet
			// loaded by the client (due to metadata). We use -1
			// just to ensure the partition is loaded.
			timestamp := offset.at
			if timestamp >= 0 {
				timestamp = -1
			}
			parts = append(parts, kmsg.ListOffsetsRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: offset.currentEpoch, // KIP-320
				Timestamp:          offset.at,
				MaxNumOffsets:      1,
			})
		}
		req.Topics = append(req.Topics, kmsg.ListOffsetsRequestTopic{
			Topic:      topic,
			Partitions: parts,
		})
	}
	return req
}

func (o offsetLoadMap) buildEpochReq() *kmsg.OffsetForLeaderEpochRequest {
	req := &kmsg.OffsetForLeaderEpochRequest{
		ReplicaID: -1,
		Topics:    make([]kmsg.OffsetForLeaderEpochRequestTopic, 0, len(o)),
	}
	for topic, partitions := range o {
		parts := make([]kmsg.OffsetForLeaderEpochRequestTopicPartition, 0, len(partitions))
		for partition, offset := range partitions {
			parts = append(parts, kmsg.OffsetForLeaderEpochRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: offset.currentEpoch,
				LeaderEpoch:        offset.epoch,
			})
		}
		req.Topics = append(req.Topics, kmsg.OffsetForLeaderEpochRequestTopic{
			Topic:      topic,
			Partitions: parts,
		})
	}
	return req
}
