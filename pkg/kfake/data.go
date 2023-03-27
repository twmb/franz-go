package kfake

import (
	"crypto/sha256"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
//
// * Write to disk, if configured.
// * When transactional, wait to send out data until txn committed or aborted.

var noID uuid

type (
	uuid [16]byte

	data struct {
		c   *Cluster
		tps tps[partData]

		id2t      map[uuid]string // topic IDs => topic name
		t2id      map[string]uuid // topic name => topic IDs
		treplicas map[string]int  // topic name => # replicas
	}

	partData struct {
		batches []partBatch

		highWatermark    int64
		lastStableOffset int64
		logStartOffset   int64
		epoch            int32 // current epoch
		maxTimestamp     int64 // current max timestamp in all batches

		// abortedTxns
		rf     int8
		leader *broker

		watch map[*watchFetch]struct{}

		createdAt time.Time
	}

	partBatch struct {
		kmsg.RecordBatch
		nbytes int
		epoch  int32 // epoch when appended

		// For list offsets, we may need to return the first offset
		// after a given requested timestamp. Client provided
		// timestamps gan go forwards and backwards. We answer list
		// offsets with a binary search: even if this batch has a small
		// timestamp, this is produced _after_ a potentially higher
		// timestamp, so it is after it in the list offset response.
		//
		// When we drop the earlier timestamp, we update all following
		// firstMaxTimestamps that match the dropped timestamp.
		maxEarlierTimestamp int64
	}
)

func (d *data) mkt(t string, nparts int, nreplicas int) {
	if d.tps != nil {
		if _, exists := d.tps[t]; exists {
			panic("should have checked existence already")
		}
	}
	var id uuid
	for {
		sha := sha256.Sum256([]byte(strconv.Itoa(int(time.Now().UnixNano()))))
		copy(id[:], sha[:])
		if _, exists := d.id2t[id]; !exists {
			break
		}
	}

	if nparts < 0 {
		nparts = d.c.cfg.defaultNumParts
	}
	if nreplicas < 0 {
		nreplicas = 3 // cluster default
	}
	d.id2t[id] = t
	d.t2id[t] = id
	d.treplicas[t] = nreplicas
	for i := 0; i < nparts; i++ {
		d.tps.mkp(t, int32(i), d.c.newPartData)
	}
}

func (c *Cluster) noLeader() *broker {
	return &broker{
		c:    c,
		node: -1,
	}
}

func (c *Cluster) newPartData() *partData {
	return &partData{
		leader:    c.bs[rand.Intn(len(c.bs))],
		watch:     make(map[*watchFetch]struct{}),
		createdAt: time.Now(),
	}
}

func (pd *partData) pushBatch(nbytes int, b kmsg.RecordBatch) {
	maxEarlierTimestamp := b.FirstTimestamp
	if maxEarlierTimestamp < pd.maxTimestamp {
		maxEarlierTimestamp = pd.maxTimestamp
	} else {
		pd.maxTimestamp = maxEarlierTimestamp
	}
	b.FirstOffset = pd.highWatermark
	b.PartitionLeaderEpoch = pd.epoch
	pd.batches = append(pd.batches, partBatch{b, nbytes, pd.epoch, maxEarlierTimestamp})
	pd.highWatermark += int64(b.NumRecords)
	pd.lastStableOffset += int64(b.NumRecords) // TODO
	for w := range pd.watch {
		w.push(nbytes)
	}
}

func (pd *partData) searchOffset(o int64) (index int, found bool, atEnd bool) {
	if len(pd.batches) == 0 {
		if o == 0 {
			return 0, false, true
		}
	} else {
		lastBatch := pd.batches[len(pd.batches)-1]
		if end := lastBatch.FirstOffset + int64(lastBatch.NumRecords); end == o {
			return 0, false, true
		}
	}

	index, found = sort.Find(len(pd.batches), func(idx int) int {
		b := &pd.batches[idx]
		if o < b.FirstOffset {
			return -1
		}
		if o >= b.FirstOffset+int64(b.NumRecords) {
			return 1
		}
		return 0
	})
	return index, found, false
}
