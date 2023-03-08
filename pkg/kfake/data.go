package kfake

import (
	"crypto/sha256"
	"math/rand"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/exp/slices"
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

		id2t map[uuid]string // topic IDs => topic name
		t2id map[string]uuid // topic name => topic IDs
	}

	partData struct {
		batches []partBatch

		highWatermark    int64
		lastStableOffset int64
		logStartOffset   int64

		// abortedTxns
		rf     int8
		leader *broker

		watch map[*watchFetch]struct{}

		createdAt time.Time
	}

	partBatch struct {
		kmsg.RecordBatch
		nbytes int
	}
)

func (d *data) mkt(t string, nparts int) {
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
	d.id2t[id] = t
	d.t2id[t] = id
	for i := 0; i < nparts; i++ {
		d.tps.mkp(t, int32(i), d.c.newPartData)
	}
}

func (c *Cluster) newPartData() *partData {
	return &partData{
		leader:    c.bs[rand.Uint64()%uint64(len(c.bs))],
		watch:     make(map[*watchFetch]struct{}),
		createdAt: time.Now(),
	}
}

func (pd *partData) pushBatch(nbytes int, b kmsg.RecordBatch) {
	b.FirstOffset = pd.highWatermark
	pd.batches = append(pd.batches, partBatch{b, nbytes})
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

	index, found = slices.BinarySearchFunc(pd.batches, o, func(b partBatch, o int64) int {
		if b.FirstOffset+int64(b.NumRecords) < o {
			return -1
		}
		if b.FirstOffset > o {
			return 1
		}
		return 0
	})
	return index, found, false
}
