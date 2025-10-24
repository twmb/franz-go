package kfake

import (
	"hash/crc32"
	"hash/fnv"
	"math"
	"math/rand"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

/*
* AddPartitionsToTxn
* AddOffsetsToTxn
* EndTxn
* TxnOffsetCommit
 */

// TODO
//
// * Add heap of last use, add index to pidwindow, and remove pidwindow as they
// exhaust max # of pids configured.
//
// * Wrap epochs

type (
	pids struct {
		ids map[int64]*pidinfo

		txs      map[*pidinfo]struct{}
		txNotify chan struct{}
		c        *Cluster
	}

	// Sequence IDs for a given individual producer ID.
	pidinfo struct {
		pids *pids

		id      int64
		epoch   int16
		windows tps[pidwindow] // topic/partition 5-window pid sequences

		txid      string
		txTimeout int32          // millis
		txParts   tps[*partData] // partitions in the transaction, if transactional
		txBatches []*partBatch   // batches in the transaction
		txStart   time.Time

		inTx bool
	}

	// Sequence ID window, and where the start is.
	pidwindow struct {
		seq [5]int32
		at  uint8
	}
)

func (pids *pids) pushTx(pidinf *pidinfo) {
	if pids.txs == nil {
		pids.txs = make(map[*pidinfo]struct{})
		pids.txNotify = make(chan struct{}, 1)
	}
	pids.txs[pidinf] = struct{}{}
	pidinf.inTx = true

	if len(pids.txs) > 1 {
		select {
		case pids.txNotify <- struct{}{}:
		default:
		}
		return
	}

	go func() {
		var exit bool
		for !exit {
			var minPid *pidinfo
			var minExpire time.Time
			pids.c.admin(func() {
				for pidinf := range pids.txs {
					timeout := time.Duration(pidinf.txTimeout) * time.Millisecond
					expire := pidinf.txStart.Add(timeout)
					if minPid == nil || expire.Before(minExpire) {
						minPid = pidinf
						minExpire = expire
					}
				}
				exit = len(pids.txs) == 0
			})
			if exit {
				return
			}

			timeout := time.NewTimer(time.Until(minExpire))
			select {
			case <-pids.c.die:
				timeout.Stop()
				return
			case <-pids.txNotify:
				timeout.Stop()
				continue
			case <-timeout.C:
			}

			pids.c.admin(func() {
				if _, ok := pids.txs[minPid]; !ok {
					return
				}
				minPid.endTx(false)
				minPid.epoch++
				delete(pids.txs, minPid)
				exit = len(pids.txs) == 0
			})
		}
	}()
}

func (pids *pids) getpid(id int64) *pidinfo {
	if pids.ids == nil {
		return nil
	}
	pidinf := pids.ids[id]
	return pidinf
}

// Returns the pidinfo for this pid, and the idempotent-5 window for this
// specific toppar. If this is transactional and the toppar has not been added
// to the txn, returns nil.
func (pids *pids) get(id int64, t string, p int32) (*pidinfo, *pidwindow) {
	if pids.ids == nil {
		return nil, nil
	}
	pidinf := pids.ids[id]
	if pidinf == nil {
		return nil, nil
	}
	if pidinf.txid != "" && !pidinf.txParts.checkp(t, p) {
		return nil, nil
	}
	return pidinf, pidinf.windows.mkpDefault(t, p)
}

func (pids *pids) create(txidp *string, txTimeout int32) (int64, int16) {
	if pids.ids == nil {
		pids.ids = make(map[int64]*pidinfo)
	}
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
		pidinf.epoch++
		return id, pidinf.epoch
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
	defer func() {
		pidinf.txParts = nil
		pidinf.txStart = time.Time{}
		pidinf.inTx = false
		if !commit {
			for _, batch := range pidinf.txBatches {
				batch.inTx = false
				batch.aborted = true
			}
		}
		pidinf.txBatches = nil
		delete(pidinf.pids.txs, pidinf)
		if len(pidinf.pids.txs) == 0 {
			select {
			case pidinf.pids.txNotify <- struct{}{}:
			default:
			}
		}
	}()

	attrs := int16(0x0010)
	if commit {
		attrs |= 0x0020
	}
	now := time.Now().UnixMilli()
	rec := kmsg.Record{
		Key:   []byte{0, 0, 0, 0},
		Value: []byte{0, 0, 0, 0, 0, 0},
	}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1) // -1 because varintlen of this short record is 1 byte
	b := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           attrs,
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

	pidinf.txParts.each(func(t string, p int32, pd **partData) {
		(*pd).pushBatch(len(benc), b, true)
	})
}

func (pidinf *pidinfo) addTxPart(t string, p int32) {
	pidinf.txParts.mkpDefault(t, p)
	if len(pidinf.txParts) > 1 {
		return
	}
	pidinf.txStart = time.Now()
	pidinf.pids.pushTx(pidinf)
}

func (s *pidwindow) pushAndValidate(firstSeq, numRecs int32) (ok, dup bool) {
	// If there is no pid, we do not do duplicate detection.
	if s == nil {
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
