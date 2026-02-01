package kfake

import (
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Behavior:
//
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

func init() { regKey(1, 4, 18) }

func (c *Cluster) handleFetch(creq *clientReq, w *watchFetch) (kmsg.Response, error) {
	var (
		req  = creq.kreq.(*kmsg.FetchRequest)
		resp = req.ResponseKind().(*kmsg.FetchResponse)
	)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	var (
		readCommitted = req.IsolationLevel == 1
		nbytes        int
		returnEarly   bool
		needp         tps[int]
	)
	if w == nil {
	out:
		for i, rt := range req.Topics {
			if req.Version >= 13 {
				rt.Topic = c.data.id2t[rt.TopicID]
				req.Topics[i].Topic = rt.Topic
			}
			t, ok := c.data.tps.gett(rt.Topic)
			if !ok {
				continue
			}
			for _, rp := range rt.Partitions {
				pd, ok := t[rp.Partition]
				if !ok {
					continue
				}
				if pd.leader != creq.cc.b && !pd.followers.has(creq.cc.b) {
					returnEarly = true // NotLeaderForPartition
					break out
				}
				i, ok, atEnd := pd.searchOffset(rp.FetchOffset)
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
					if pbytes >= int(rp.PartitionMaxBytes) {
						returnEarly = true
						break out
					}
				}
				needp.set(rt.Topic, rp.Partition, int(rp.PartitionMaxBytes)-pbytes)
			}
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
		for _, rt := range req.Topics {
			t, ok := c.data.tps.gett(rt.Topic)
			if !ok {
				continue
			}
			for _, rp := range rt.Partitions {
				pd, ok := t[rp.Partition]
				if !ok {
					continue
				}
				pd.watch[w] = struct{}{}
				w.in = append(w.in, pd)
			}
		}
		w.t = time.AfterFunc(wait, w.cb)
		return nil, nil
	}

	id2t := make(map[uuid]string)
	tidx := make(map[string]int)

	donet := func(t string, id uuid, errCode int16) *kmsg.FetchResponseTopic {
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
		st := donet(t, id, 0)
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
				resp.Brokers = append(resp.Brokers, sb)
			}
		}
	}()

	var batchesAdded int
	nbytes = 0
full:
	for _, rt := range req.Topics {
		for _, rp := range rt.Partitions {
			pd, ok := c.data.tps.getp(rt.Topic, rp.Partition)
			if !ok {
				if req.Version >= 13 {
					donep(rt.Topic, rt.TopicID, rp.Partition, kerr.UnknownTopicID.Code)
				} else {
					donep(rt.Topic, rt.TopicID, rp.Partition, kerr.UnknownTopicOrPartition.Code)
				}
				continue
			}
			if pd.leader != creq.cc.b && !pd.followers.has(creq.cc.b) {
				p := donep(rt.Topic, rt.TopicID, rp.Partition, kerr.NotLeaderForPartition.Code)
				p.CurrentLeader.LeaderID = pd.leader.node
				p.CurrentLeader.LeaderEpoch = pd.epoch
				includeBrokers = true
				continue
			}
			sp := donep(rt.Topic, rt.TopicID, rp.Partition, 0)
			sp.HighWatermark = pd.highWatermark
			sp.LastStableOffset = pd.lastStableOffset
			sp.LogStartOffset = pd.logStartOffset
			i, ok, atEnd := pd.searchOffset(rp.FetchOffset)
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
				if pbytes = pbytes + b.nbytes; pbytes > int(rp.PartitionMaxBytes) && batchesAdded > 0 {
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
	if w.readCommitted && pd.inTx {
		return
	}
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

func (w *watchFetch) cleanup(c *Cluster) {
	w.cleaned = true
	for _, in := range w.in {
		delete(in.watch, w)
	}
	w.t.Stop()
}
