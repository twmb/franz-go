package kfake

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

		id2t           map[uuid]string               // topic IDs => topic name
		t2id           map[string]uuid               // topic name => topic IDs
		treplicas      map[string]int                // topic name => # replicas
		tcfgs          map[string]map[string]*string // topic name => config name => config value
		tnorms map[string]string             // normalized name (. replaced with _) => topic name
	}

	partData struct {
		batches []*partBatch
		t       string
		p       int32
		dir     string

		highWatermark    int64
		lastStableOffset int64
		logStartOffset   int64
		earliestTxOffset int64 // earliest offset of uncommitted transaction, -1 if none
		epoch            int32 // current epoch
		maxTimestamp     int64 // max FirstTimestamp seen (for maxEarlierTimestamp optimization)
		nbytes           int64
		inTx             bool

		// For ListOffsets timestamp -3 (KIP-734): track the batch with max timestamp
		maxTimestampBatchIdx int // index of batch with highest MaxTimestamp, -1 if none

		rf        int8
		leader    *broker
		followers followers

		watch map[*watchFetch]struct{}

		createdAt time.Time
	}

	followers []int32

	partBatch struct {
		kmsg.RecordBatch
		nbytes int
		epoch  int32 // epoch when appended

		// For list offsets, we may need to return the first offset
		// after a given requested timestamp. Client provided
		// timestamps can go forwards and backwards. We answer list
		// offsets with a binary search: even if this batch has a small
		// timestamp, this is produced _after_ a potentially higher
		// timestamp, so it is after it in the list offset response.
		//
		// When we drop the earlier timestamp, we update all following
		// firstMaxTimestamps that match the dropped timestamp.
		maxEarlierTimestamp int64

		inTx bool

		// Filled retroactively, if true, the pid aborted this batch.
		aborted bool

		// For aborted transactions: the first offset of this transaction
		// on this partition. Used for AbortedTransactions in fetch response.
		txnFirstOffset int64
	}
)

func (b *partBatch) pid() (int64, int16) {
	return b.ProducerID, b.ProducerEpoch
}

func (fs followers) has(b *broker) bool {
	for _, f := range fs {
		if f == b.node {
			return true
		}
	}
	return false
}

// normalizeTopicName normalizes a topic name for collision detection.
// Kafka considers topics that differ only in . vs _ as colliding.
func normalizeTopicName(t string) string {
	return strings.ReplaceAll(t, ".", "_")
}

func (d *data) mkt(t string, nparts, nreplicas int, configs map[string]*string) {
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
		if nreplicas > len(d.c.bs) {
			nreplicas = len(d.c.bs)
		}
	}
	d.id2t[id] = t
	d.t2id[t] = id
	d.treplicas[t] = nreplicas
	d.tnorms[normalizeTopicName(t)] = t
	if configs != nil {
		d.tcfgs[t] = configs
	}
	for i := 0; i < nparts; i++ {
		p := int32(i)
		d.tps.mkp(t, p, d.c.newPartData(p))
	}
}

func (c *Cluster) noLeader() *broker {
	return &broker{
		c:    c,
		node: -1,
	}
}

func (c *Cluster) newPartData(p int32) func() *partData {
	return func() *partData {
		return &partData{
			p:                    p,
			dir:                  defLogDir,
			earliestTxOffset:     -1,
			maxTimestampBatchIdx: -1,
			leader:               c.bs[rand.Intn(len(c.bs))],
			watch:                make(map[*watchFetch]struct{}),
			createdAt:            time.Now(),
		}
	}
}

// Returns a pointer to the new batch.
// If transactional, we mark ourselves in a tx.
// Finishing a tx clears the inTx state on pd, but also re-sets it if needed.
// If we are not in a tx, we can bump the stable offset here.
// txnFirstOffset is the first offset of this transaction on this partition (0 if not transactional).
func (pd *partData) pushBatch(nbytes int, b kmsg.RecordBatch, inTx bool, txnFirstOffset int64) *partBatch {
	maxEarlierTimestamp := b.FirstTimestamp
	if maxEarlierTimestamp < pd.maxTimestamp {
		maxEarlierTimestamp = pd.maxTimestamp
	} else {
		pd.maxTimestamp = maxEarlierTimestamp
	}
	b.FirstOffset = pd.highWatermark
	b.PartitionLeaderEpoch = pd.epoch

	// Track max timestamp batch for ListOffsets -3 (KIP-734)
	newIdx := len(pd.batches) // index this batch will have after append
	if pd.maxTimestampBatchIdx < 0 || b.MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
		pd.maxTimestampBatchIdx = newIdx
	}

	pd.batches = append(pd.batches, &partBatch{
		RecordBatch:         b,
		nbytes:              nbytes,
		epoch:               pd.epoch,
		maxEarlierTimestamp: maxEarlierTimestamp,
		inTx:                inTx,
		aborted:             false,
		txnFirstOffset:      txnFirstOffset,
	})
	pd.highWatermark += int64(b.NumRecords)
	if inTx {
		pd.inTx = true
		// Track the earliest uncommitted transaction offset
		if pd.earliestTxOffset == -1 {
			pd.earliestTxOffset = b.FirstOffset
		}
	}
	if !pd.inTx {
		pd.lastStableOffset += int64(b.NumRecords)
	}
	pd.nbytes += int64(nbytes)
	for w := range pd.watch {
		w.push(pd, nbytes)
	}
	return pd.batches[len(pd.batches)-1]
}

// index: the batch index the offset is in, if the offset is found
// found: if the offset was found
// atEnd: if true, the requested offset is one past the HWM - "requesting at the end, wait"
func (pd *partData) searchOffset(o int64) (index int, found bool, atEnd bool) {
	if o < pd.logStartOffset || o > pd.highWatermark {
		return 0, false, false
	}
	if len(pd.batches) == 0 {
		if o == 0 {
			return 0, false, true
		}
	} else {
		lastBatch := pd.batches[len(pd.batches)-1]
		if end := lastBatch.FirstOffset + int64(lastBatch.LastOffsetDelta) + 1; end == o {
			return 0, false, true
		}
	}

	index, found = sort.Find(len(pd.batches), func(idx int) int {
		b := pd.batches[idx]
		if o < b.FirstOffset {
			return -1
		}
		if o >= b.FirstOffset+int64(b.LastOffsetDelta)+1 {
			return 1
		}
		return 0
	})
	return index, found, false
}

func (pd *partData) trimLeft() {
	for len(pd.batches) > 0 {
		b0 := pd.batches[0]
		finRec := b0.FirstOffset + int64(b0.LastOffsetDelta)
		if finRec >= pd.logStartOffset {
			return
		}
		pd.batches = pd.batches[1:]
		pd.nbytes -= int64(b0.nbytes)
	}
}

// recalculateLSO recalculates the last stable offset.
func (pd *partData) recalculateLSO() {
	// Find the earliest uncommitted transaction
	earliestUncommitted := int64(-1)
	for _, b := range pd.batches {
		if b.inTx && !b.aborted {
			earliestUncommitted = b.FirstOffset
			break
		}
	}

	if earliestUncommitted == -1 {
		// No uncommitted transactions, LSO = HWM
		pd.lastStableOffset = pd.highWatermark
		pd.earliestTxOffset = -1
		pd.inTx = false
	} else {
		// LSO is at the start of the earliest uncommitted transaction
		pd.lastStableOffset = earliestUncommitted
		pd.earliestTxOffset = earliestUncommitted
		pd.inTx = true
	}
}

/////////////
// CONFIGS //
/////////////

// TODO support modifying config values changing cluster behavior

// brokerConfigs calls fn for all:
//   - static broker configs (read only)
//   - default configs
//   - dynamic broker configs
func (c *Cluster) brokerConfigs(node int32, fn func(k string, v *string, src kmsg.ConfigSource, sensitive bool)) {
	if node >= 0 {
		for _, b := range c.bs {
			if b.node == node {
				id := strconv.Itoa(int(node))
				fn("broker.id", &id, kmsg.ConfigSourceStaticBrokerConfig, false)
				break
			}
		}
	}
	for _, c := range []struct {
		k    string
		v    string
		sens bool
	}{
		{k: "broker.rack", v: "krack"},
		{k: "sasl.enabled.mechanisms", v: "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512"},
		{k: "super.users", sens: true},
	} {
		v := c.v
		fn(c.k, &v, kmsg.ConfigSourceStaticBrokerConfig, c.sens)
	}

	for k, v := range configDefaults {
		if _, ok := validBrokerConfigs[k]; ok {
			v := v
			fn(k, &v, kmsg.ConfigSourceDefaultConfig, false)
		}
	}

	for k, v := range c.bcfgs {
		fn(k, v, kmsg.ConfigSourceDynamicBrokerConfig, false)
	}
}

// configs calls fn for all
//   - static broker configs (read only)
//   - default configs
//   - dynamic broker configs
//   - dynamic topic configs
//
// This differs from brokerConfigs by also including dynamic topic configs.
func (d *data) configs(t string, fn func(k string, v *string, src kmsg.ConfigSource, sensitive bool)) {
	for k, v := range configDefaults {
		if _, ok := validTopicConfigs[k]; ok {
			v := v
			fn(k, &v, kmsg.ConfigSourceDefaultConfig, false)
		}
	}
	for k, v := range d.c.bcfgs {
		if topicEquiv, ok := validBrokerConfigs[k]; ok && topicEquiv != "" {
			fn(k, v, kmsg.ConfigSourceDynamicBrokerConfig, false)
		}
	}
	for k, v := range d.tcfgs[t] {
		fn(k, v, kmsg.ConfigSourceDynamicTopicConfig, false)
	}
}

// Unlike Kafka, we validate the value before allowing it to be set.
func (c *Cluster) setBrokerConfig(k string, v *string, dry bool) bool {
	if dry {
		return true
	}
	c.bcfgs[k] = v
	return true
}

func (d *data) setTopicConfig(t, k string, v *string, dry bool) bool {
	if dry {
		return true
	}
	if _, ok := d.tcfgs[t]; !ok {
		d.tcfgs[t] = make(map[string]*string)
	}
	d.tcfgs[t][k] = v
	return true
}

// All valid topic configs we support, as well as the equivalent broker
// config if there is one.
var validTopicConfigs = map[string]string{
	"cleanup.policy":         "",
	"compression.type":       "compression.type",
	"max.message.bytes":      "log.message.max.bytes",
	"message.timestamp.type": "log.message.timestamp.type",
	"min.insync.replicas":    "min.insync.replicas",
	"retention.bytes":        "log.retention.bytes",
	"retention.ms":           "log.retention.ms",
}

// All valid broker configs we support, as well as their equivalent
// topic config if there is one.
var validBrokerConfigs = map[string]string{
	"broker.id":                  "",
	"broker.rack":                "",
	"compression.type":           "compression.type",
	"default.replication.factor": "",
	"fetch.max.bytes":            "",
	"log.dir":                    "",
	"log.message.timestamp.type": "message.timestamp.type",
	"log.retention.bytes":        "retention.bytes",
	"log.retention.ms":           "retention.ms",
	"message.max.bytes":          "max.message.bytes",
	"min.insync.replicas":        "min.insync.replicas",
	"sasl.enabled.mechanisms":    "",
	"super.users":                "",
}

// Default topic and broker configs.
var configDefaults = map[string]string{
	"cleanup.policy":         "delete",
	"compression.type":       "producer",
	"max.message.bytes":      "1048588",
	"message.timestamp.type": "CreateTime",
	"min.insync.replicas":    "1",
	"retention.bytes":        "-1",
	"retention.ms":           "604800000",

	"default.replication.factor": "3",
	"fetch.max.bytes":            "57671680",
	"log.dir":                    defLogDir,
	"log.message.timestamp.type": "CreateTime",
	"log.retention.bytes":        "-1",
	"log.retention.ms":           "604800000",
	"message.max.bytes":          "1048588",
}

const defLogDir = "/mem/kfake"

var brokerRack = "krack"

// maxMessageBytes returns the max.message.bytes for a topic, falling back to
// broker config then defaults.
func (d *data) maxMessageBytes(t string) int {
	// Check topic-level config first
	if tcfg, ok := d.tcfgs[t]; ok {
		if v, ok := tcfg["max.message.bytes"]; ok && v != nil {
			if n, err := strconv.Atoi(*v); err == nil {
				return n
			}
		}
	}
	// Check broker-level config (message.max.bytes maps to max.message.bytes)
	if v, ok := d.c.bcfgs["message.max.bytes"]; ok && v != nil {
		if n, err := strconv.Atoi(*v); err == nil {
			return n
		}
	}
	// Fall back to default
	if v, ok := configDefaults["max.message.bytes"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 1048588 // Kafka default
}

func staticConfig(s ...string) func(*string) bool {
	return func(v *string) bool {
		if v == nil {
			return false
		}
		for _, ok := range s {
			if *v == ok {
				return true
			}
		}
		return false
	}
}

func numberConfig(min int, hasMin bool, max int, hasMax bool) func(*string) bool {
	return func(v *string) bool {
		if v == nil {
			return false
		}
		i, err := strconv.Atoi(*v)
		if err != nil {
			return false
		}
		if hasMin && i < min || hasMax && i > max {
			return false
		}
		return true
	}
}

func forEachBatchRecord(batch kmsg.RecordBatch, cb func(kmsg.Record) error) error {
	records, err := kgo.DefaultDecompressor().Decompress(
		batch.Records,
		kgo.CompressionCodecType(batch.Attributes&0x0007),
	)
	if err != nil {
		return err
	}
	for range batch.NumRecords {
		rec := kmsg.NewRecord()
		err := rec.ReadFrom(records)
		if err != nil {
			return fmt.Errorf("corrupt batch: %w", err)
		}
		if err := cb(rec); err != nil {
			return err
		}
		length, amt := binary.Varint(records)
		records = records[length+int64(amt):]
	}
	if len(records) > 0 {
		return fmt.Errorf("corrupt batch, extra left over bytes after parsing batch: %v", len(records))
	}
	return nil
}

// BatchRecord returns the raw kmsg.Record's within a record batch, or an error
// if they could not be processed.
func BatchRecords(b kmsg.RecordBatch) ([]kmsg.Record, error) {
	var rs []kmsg.Record
	err := forEachBatchRecord(b, func(r kmsg.Record) error {
		rs = append(rs, r)
		return nil
	})
	return rs, err
}
