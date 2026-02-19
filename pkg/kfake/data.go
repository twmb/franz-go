package kfake

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
//
// * Write to disk, if configured.

var noID uuid

type (
	uuid [16]byte

	data struct {
		c   *Cluster
		tps tps[partData]

		id2t      map[uuid]string               // topic IDs => topic name
		t2id      map[string]uuid               // topic name => topic IDs
		treplicas map[string]int                // topic name => # replicas
		tcfgs     map[string]map[string]*string // topic name => config name => config value
		tnorms    map[string]string             // normalized name (. replaced with _) => topic name
	}

	abortedTxnEntry struct {
		producerID  int64
		firstOffset int64 // first data offset of this txn on this partition
		lastOffset  int64 // offset of the abort control record
	}

	partData struct {
		batches     []*partBatch
		abortedTxns []abortedTxnEntry // sorted by lastOffset, for read_committed fetch
		t           string
		p           int32
		dir         string

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
func (pd *partData) pushBatch(nbytes int, b kmsg.RecordBatch, inTx bool) *partBatch {
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
	// After compaction, offsets may land in gaps between batches.
	// Skip forward to the next available batch.
	if !found && index < len(pd.batches) {
		found = true
	}
	return index, found, false
}

func (pd *partData) trimLeft() {
	for len(pd.batches) > 0 {
		b0 := pd.batches[0]
		finRec := b0.FirstOffset + int64(b0.LastOffsetDelta)
		if finRec >= pd.logStartOffset {
			break
		}
		pd.batches = pd.batches[1:]
		pd.nbytes -= int64(b0.nbytes)
	}
	// Prune aborted txn entries fully before logStartOffset.
	// Entries are sorted by lastOffset, so binary search for the
	// first entry still relevant.
	i := sort.Search(len(pd.abortedTxns), func(i int) bool {
		return pd.abortedTxns[i].lastOffset >= pd.logStartOffset
	})
	pd.abortedTxns = pd.abortedTxns[i:]
}

// recalculateLSO recalculates the last stable offset.
func (pd *partData) recalculateLSO() {
	// Find the earliest uncommitted transaction
	earliestUncommitted := int64(-1)
	for _, b := range pd.batches {
		if b.inTx {
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

	for k, v := range c.loadBcfgs() {
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
	for k, v := range d.c.loadBcfgs() {
		if topicEquiv, ok := validBrokerConfigs[k]; ok && topicEquiv != "" {
			fn(k, v, kmsg.ConfigSourceDynamicBrokerConfig, false)
		}
	}
	for k, v := range d.tcfgs[t] {
		fn(k, v, kmsg.ConfigSourceDynamicTopicConfig, false)
	}
}

func (c *Cluster) loadBcfgs() map[string]*string {
	return *c.bcfgs.Load()
}

func (c *Cluster) storeBcfgs(m map[string]*string) {
	c.bcfgs.Store(&m)
}

// configListAppend appends val to a comma-separated list config value.
func configListAppend(current, val *string) *string {
	if val == nil {
		return current
	}
	if current == nil || *current == "" {
		return val
	}
	s := *current + "," + *val
	return &s
}

// configListSubtract removes val from a comma-separated list config value.
func configListSubtract(current, val *string) *string {
	if val == nil || current == nil {
		return current
	}
	parts := strings.Split(*current, ",")
	out := parts[:0]
	for _, p := range parts {
		if p != *val {
			out = append(out, p)
		}
	}
	s := strings.Join(out, ",")
	return &s
}

// isListConfig returns whether the named config is a list type.
func isListConfig(name string) bool {
	ct, ok := configTypes[name]
	return ok && ct == kmsg.ConfigTypeList
}

// validateBrokerConfig returns whether v is a valid value for broker config k.
func validateBrokerConfig(k string, v *string) bool {
	if v == nil {
		return true
	}
	if ct, ok := configTypes[k]; ok {
		switch ct {
		case kmsg.ConfigTypeInt, kmsg.ConfigTypeLong:
			if _, err := strconv.ParseInt(*v, 10, 64); err != nil {
				return false
			}
		}
	}
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
	"delete.retention.ms":    "",
	"kfake.is_internal":      "",
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
	"max.incremental.fetch.session.cache.slots": "",
	"group.consumer.heartbeat.interval.ms":      "",
	"group.consumer.session.timeout.ms":         "",
	"group.max.size":                            "",
	"group.min.session.timeout.ms":              "",
	"group.max.session.timeout.ms":              "",
	"log.cleaner.backoff.ms":                    "",
	"log.dir":                                   "",
	"log.message.timestamp.type":                "message.timestamp.type",
	"transaction.max.timeout.ms":                "",
	"transactional.id.expiration.ms":            "",
	"log.retention.bytes":                       "retention.bytes",
	"log.retention.ms":                          "retention.ms",
	"message.max.bytes":                         "max.message.bytes",
	"min.insync.replicas":                       "min.insync.replicas",
	"sasl.enabled.mechanisms":                   "",
	"super.users":                               "",
}

const (
	defLogDir          = "/mem/kfake"
	defMaxMessageBytes = 1048588
)

// defHeartbeatInterval is the default group.consumer.heartbeat.interval.ms.
// Real Kafka defaults to 5s; in test binaries we use 100ms so that
// KIP-848 reconciliation completes quickly.
var defHeartbeatInterval = 5000

// defSessionTimeout is the default group.consumer.session.timeout.ms.
var defSessionTimeout = 45000

func init() {
	if testing.Testing() {
		defHeartbeatInterval = 100
		configDefaults["group.consumer.heartbeat.interval.ms"] = "100"
	}
}

// Default topic and broker configs. Topic/broker pairs that share the same
// underlying setting (e.g. max.message.bytes / message.max.bytes) both
// appear here so that DescribeConfigs returns the correct default for
// whichever name is queried.
var configDefaults = map[string]string{
	"cleanup.policy":         "delete",
	"compression.type":       "producer",
	"delete.retention.ms":    "86400000",
	"max.message.bytes":      strconv.Itoa(defMaxMessageBytes),
	"message.timestamp.type": "CreateTime",
	"min.insync.replicas":    "1",
	"retention.bytes":        "-1",
	"retention.ms":           "604800000",

	"transaction.max.timeout.ms":     "900000",
	"transactional.id.expiration.ms": "604800000",

	"default.replication.factor":                "3",
	"fetch.max.bytes":                           "57671680",
	"log.cleaner.backoff.ms":                    "3600000",
	"max.incremental.fetch.session.cache.slots": strconv.Itoa(defMaxFetchSessionCacheSlots),
	"group.consumer.heartbeat.interval.ms":      strconv.Itoa(defHeartbeatInterval),
	"group.consumer.session.timeout.ms":         strconv.Itoa(defSessionTimeout),
	"group.max.size":                            "2147483647",
	"group.min.session.timeout.ms":              "6000",
	"group.max.session.timeout.ms":              "300000",
	"log.dir":                                   defLogDir,
	"log.message.timestamp.type":                "CreateTime",
	"log.retention.bytes":                       "-1",
	"log.retention.ms":                          "604800000",
	"message.max.bytes":                         strconv.Itoa(defMaxMessageBytes),
}

// configTypes maps config names to their data types for DescribeConfigs v3+.
var configTypes = map[string]kmsg.ConfigType{
	"broker.id":                  kmsg.ConfigTypeInt,
	"broker.rack":                kmsg.ConfigTypeString,
	"cleanup.policy":             kmsg.ConfigTypeList,
	"compression.type":           kmsg.ConfigTypeString,
	"default.replication.factor": kmsg.ConfigTypeInt,
	"delete.retention.ms":        kmsg.ConfigTypeLong,
	"fetch.max.bytes":            kmsg.ConfigTypeInt,
	"max.incremental.fetch.session.cache.slots": kmsg.ConfigTypeInt,
	"group.consumer.heartbeat.interval.ms":      kmsg.ConfigTypeInt,
	"group.consumer.session.timeout.ms":         kmsg.ConfigTypeInt,
	"group.max.size":                            kmsg.ConfigTypeInt,
	"group.min.session.timeout.ms":              kmsg.ConfigTypeInt,
	"group.max.session.timeout.ms":              kmsg.ConfigTypeInt,
	"log.cleaner.backoff.ms":                    kmsg.ConfigTypeLong,
	"log.dir":                                   kmsg.ConfigTypeString,
	"log.message.timestamp.type":                kmsg.ConfigTypeString,
	"log.retention.bytes":                       kmsg.ConfigTypeLong,
	"log.retention.ms":                          kmsg.ConfigTypeLong,
	"max.message.bytes":                         kmsg.ConfigTypeInt,
	"message.max.bytes":                         kmsg.ConfigTypeInt,
	"message.timestamp.type":                    kmsg.ConfigTypeString,
	"min.insync.replicas":                       kmsg.ConfigTypeInt,
	"retention.bytes":                           kmsg.ConfigTypeLong,
	"retention.ms":                              kmsg.ConfigTypeLong,
	"sasl.enabled.mechanisms":                   kmsg.ConfigTypeList,
	"super.users":                               kmsg.ConfigTypeList,
	"transaction.max.timeout.ms":                kmsg.ConfigTypeInt,
	"transactional.id.expiration.ms":            kmsg.ConfigTypeInt,
}

var brokerRack = "krack"

func (c *Cluster) brokerConfigInt(key string, def int) int32 {
	if v, ok := c.loadBcfgs()[key]; ok && v != nil {
		n, _ := strconv.Atoi(*v)
		return int32(n)
	}
	return int32(def)
}

func (c *Cluster) consumerHeartbeatIntervalMs() int32 {
	return c.brokerConfigInt("group.consumer.heartbeat.interval.ms", defHeartbeatInterval)
}

func (c *Cluster) consumerSessionTimeoutMs() int32 {
	return c.brokerConfigInt("group.consumer.session.timeout.ms", defSessionTimeout)
}

func (c *Cluster) groupMaxSize() int32 {
	return c.brokerConfigInt("group.max.size", 2147483647)
}

func (c *Cluster) groupMinSessionTimeoutMs() int32 {
	return c.brokerConfigInt("group.min.session.timeout.ms", int(c.cfg.minSessionTimeout.Milliseconds()))
}

func (c *Cluster) groupMaxSessionTimeoutMs() int32 {
	return c.brokerConfigInt("group.max.session.timeout.ms", int(c.cfg.maxSessionTimeout.Milliseconds()))
}

const defTransactionMaxTimeoutMs = 900000

func (c *Cluster) transactionMaxTimeoutMs() int32 {
	return c.brokerConfigInt("transaction.max.timeout.ms", defTransactionMaxTimeoutMs)
}

const defTxnIDExpirationMs = 604800000 // 7 days

func (c *Cluster) txnIDExpirationMs() int32 {
	return c.brokerConfigInt("transactional.id.expiration.ms", defTxnIDExpirationMs)
}

func (c *Cluster) fetchSessionCacheSlots() int32 {
	return c.brokerConfigInt("max.incremental.fetch.session.cache.slots", defMaxFetchSessionCacheSlots)
}

// maxMessageBytes returns the max.message.bytes for a topic, falling back to
// broker config then defaults.
func (d *data) maxMessageBytes(t string) int {
	if tcfg, ok := d.tcfgs[t]; ok {
		if v, ok := tcfg["max.message.bytes"]; ok && v != nil {
			n, _ := strconv.Atoi(*v)
			return n
		}
	}
	if v, ok := d.c.loadBcfgs()["message.max.bytes"]; ok && v != nil {
		n, _ := strconv.Atoi(*v)
		return n
	}
	return defMaxMessageBytes
}

// retentionMs returns the retention.ms for a topic, falling back to
// broker config then defaults.
func (d *data) retentionMs(t string) int64 {
	if tcfg, ok := d.tcfgs[t]; ok {
		if v, ok := tcfg["retention.ms"]; ok && v != nil {
			n, _ := strconv.ParseInt(*v, 10, 64)
			return n
		}
	}
	if v, ok := d.c.loadBcfgs()["log.retention.ms"]; ok && v != nil {
		n, _ := strconv.ParseInt(*v, 10, 64)
		return n
	}
	return 604800000
}

// retentionBytes returns the retention.bytes for a topic, falling back to
// broker config then defaults. -1 means no limit.
func (d *data) retentionBytes(t string) int64 {
	if tcfg, ok := d.tcfgs[t]; ok {
		if v, ok := tcfg["retention.bytes"]; ok && v != nil {
			n, _ := strconv.ParseInt(*v, 10, 64)
			return n
		}
	}
	if v, ok := d.c.loadBcfgs()["log.retention.bytes"]; ok && v != nil {
		n, _ := strconv.ParseInt(*v, 10, 64)
		return n
	}
	return -1
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

/////////////////
// COMPACTION  //
/////////////////

// hasRetentionConfig returns true if the topic has retention.ms or
// retention.bytes explicitly set in its topic configs.
func (d *data) hasRetentionConfig(t string) bool {
	tcfg, ok := d.tcfgs[t]
	if !ok {
		return false
	}
	_, hasMs := tcfg["retention.ms"]
	_, hasBytes := tcfg["retention.bytes"]
	return hasMs || hasBytes
}

func (d *data) isCompactTopic(t string) bool {
	if tcfg, ok := d.tcfgs[t]; ok {
		if v, ok := tcfg["cleanup.policy"]; ok && v != nil {
			return strings.Contains(*v, "compact")
		}
	}
	return false
}

// isBatchAborted returns true if the batch belongs to an aborted transaction.
// A batch is produced atomically within a transaction, so all its records
// share the same fate. abortedTxns is sorted by lastOffset, so we binary
// search to skip entries that end before this batch.
func (pd *partData) isBatchAborted(batch *partBatch) bool {
	j := sort.Search(len(pd.abortedTxns), func(i int) bool {
		return pd.abortedTxns[i].lastOffset >= batch.FirstOffset
	})
	for ; j < len(pd.abortedTxns); j++ {
		a := pd.abortedTxns[j]
		if a.producerID == batch.ProducerID && batch.FirstOffset >= a.firstOffset {
			return true
		}
	}
	return false
}

// compact removes superseded records from a compactable partition.
// The last batch is treated as the active segment and is never compacted.
func (pd *partData) compact(d *data, topic string) {
	if len(pd.batches) < 2 {
		return
	}

	// Read delete.retention.ms from topic config, falling back to default.
	deleteRetentionMs := int64(86400000)
	if tcfg, ok := d.tcfgs[topic]; ok {
		if v, ok := tcfg["delete.retention.ms"]; ok && v != nil {
			if n, err := strconv.ParseInt(*v, 10, 64); err == nil {
				deleteRetentionMs = n
			}
		}
	}
	now := time.Now().UnixMilli()

	cleanable := pd.batches[:len(pd.batches)-1]

	// Pass 1: build key => highest offset map from cleanable range.
	keyOffsets := make(map[string]int64)
	for _, batch := range cleanable {
		if batch.Attributes&0x0020 != 0 || pd.isBatchAborted(batch) { // skip control batches and aborted txns
			continue
		}
		_ = forEachBatchRecord(batch.RecordBatch, func(rec kmsg.Record) error {
			if rec.Key == nil {
				return nil
			}
			absOffset := batch.FirstOffset + int64(rec.OffsetDelta)
			k := string(rec.Key)
			if prev, ok := keyOffsets[k]; !ok || absOffset > prev {
				keyOffsets[k] = absOffset
			}
			return nil
		})
	}

	// Pass 2: filter batches, keeping only non-superseded records.
	// Also track which PIDs still have surviving data batches.
	survivingPIDs := make(map[int64]bool)
	var kept []*partBatch
	for _, batch := range cleanable {
		if batch.Attributes&0x0020 != 0 {
			// Defer control batch decision until after we know surviving PIDs.
			continue
		}
		if pd.isBatchAborted(batch) {
			continue
		}

		var surviving []kmsg.Record
		_ = forEachBatchRecord(batch.RecordBatch, func(rec kmsg.Record) error {
			absOffset := batch.FirstOffset + int64(rec.OffsetDelta)

			// Drop null-keyed records.
			if rec.Key == nil {
				return nil
			}

			k := string(rec.Key)

			// Drop superseded records (a later record has the same key).
			if keyOffsets[k] > absOffset {
				return nil
			}

			// Drop expired tombstones (nil value).
			if rec.Value == nil {
				recTs := batch.FirstTimestamp + int64(rec.TimestampDelta)
				if now-recTs >= deleteRetentionMs {
					return nil
				}
			}

			surviving = append(surviving, rec)
			return nil
		})

		if len(surviving) == 0 {
			continue
		}

		survivingPIDs[batch.ProducerID] = true
		if len(surviving) == int(batch.NumRecords) {
			// All records survived - keep original batch as-is.
			kept = append(kept, batch)
		} else {
			kept = append(kept, rebuildBatch(batch, surviving))
		}
	}

	// Pass 3: handle control batches - keep only if PID has surviving data.
	// Control batches are functionally inert in kfake (read_committed is
	// driven by pd.abortedTxns and LSO, not control batches), but we
	// match real Kafka's compaction behavior for fidelity.
	for _, batch := range cleanable {
		isControl := batch.Attributes&0x0020 != 0
		if !isControl {
			continue
		}
		if survivingPIDs[batch.ProducerID] {
			kept = append(kept, batch)
		}
	}

	// Sort kept batches by FirstOffset to maintain order (control batches
	// were appended out of order in pass 3).
	sort.Slice(kept, func(i, j int) bool {
		return kept[i].FirstOffset < kept[j].FirstOffset
	})

	// Append the active segment (last batch).
	kept = append(kept, pd.batches[len(pd.batches)-1])

	// Rebuild partition metadata.
	pd.batches = kept
	pd.nbytes = 0
	for _, b := range pd.batches {
		pd.nbytes += int64(b.nbytes)
	}
	if len(pd.batches) > 0 {
		pd.logStartOffset = pd.batches[0].FirstOffset
	}

	// Rebuild maxTimestampBatchIdx.
	pd.maxTimestampBatchIdx = -1
	for i, b := range pd.batches {
		if pd.maxTimestampBatchIdx < 0 || b.MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
			pd.maxTimestampBatchIdx = i
		}
	}

	// Prune aborted txn entries that are now before logStartOffset.
	pd.trimAbortedTxns()
}

// trimAbortedTxns prunes aborted txn entries fully before logStartOffset.
func (pd *partData) trimAbortedTxns() {
	i := sort.Search(len(pd.abortedTxns), func(i int) bool {
		return pd.abortedTxns[i].lastOffset >= pd.logStartOffset
	})
	pd.abortedTxns = pd.abortedTxns[i:]
}

/////////////////
// RETENTION   //
/////////////////

// applyRetention advances logStartOffset past batches that are expired by
// retention.ms or that exceed retention.bytes, then trims them.
func (pd *partData) applyRetention(d *data, topic string) {
	if len(pd.batches) == 0 {
		return
	}

	retMs := d.retentionMs(topic)
	retBytes := d.retentionBytes(topic)
	if retMs < 0 && retBytes < 0 {
		return
	}

	now := time.Now().UnixMilli()
	newLogStart := pd.logStartOffset

	// Time-based retention: drop batches whose max timestamp is older
	// than retention.ms.
	if retMs >= 0 {
		for _, b := range pd.batches {
			if now-b.MaxTimestamp >= retMs {
				end := b.FirstOffset + int64(b.LastOffsetDelta) + 1
				if end > newLogStart {
					newLogStart = end
				}
			}
		}
	}

	// Size-based retention: drop oldest batches until total size is
	// within retention.bytes.
	if retBytes >= 0 && pd.nbytes > retBytes {
		excess := pd.nbytes - retBytes
		for _, b := range pd.batches {
			if excess <= 0 {
				break
			}
			end := b.FirstOffset + int64(b.LastOffsetDelta) + 1
			if end > newLogStart {
				newLogStart = end
			}
			excess -= int64(b.nbytes)
		}
	}

	if newLogStart > pd.logStartOffset {
		pd.logStartOffset = newLogStart
		pd.trimLeft()
	}
}

// rebuildBatch creates a new partBatch with only the kept records,
// re-encoding them uncompressed with updated batch metadata.
func rebuildBatch(orig *partBatch, kept []kmsg.Record) *partBatch {
	// Re-encode records uncompressed.
	var rawRecords []byte
	for _, rec := range kept {
		rawRecords = rec.AppendTo(rawRecords)
	}

	b := kmsg.RecordBatch{
		FirstOffset:          orig.FirstOffset,
		PartitionLeaderEpoch: orig.PartitionLeaderEpoch,
		Magic:                2,
		Attributes:           orig.Attributes &^ 0x0007, // clear compression bits (uncompressed)
		LastOffsetDelta:      kept[len(kept)-1].OffsetDelta,
		FirstTimestamp:       orig.FirstTimestamp,
		MaxTimestamp:         orig.FirstTimestamp,
		ProducerID:           orig.ProducerID,
		ProducerEpoch:        orig.ProducerEpoch,
		FirstSequence:        orig.FirstSequence,
		NumRecords:           int32(len(kept)),
		Records:              rawRecords,
	}

	// Recompute timestamps.
	for _, rec := range kept {
		ts := orig.FirstTimestamp + int64(rec.TimestampDelta)
		if ts > b.MaxTimestamp {
			b.MaxTimestamp = ts
		}
	}

	benc := b.AppendTo(nil)
	b.Length = int32(len(benc) - 12)
	b.CRC = int32(crc32.Checksum(benc[21:], crc32c))

	return &partBatch{
		RecordBatch:         b,
		nbytes:              len(benc),
		epoch:               orig.epoch,
		maxEarlierTimestamp: orig.maxEarlierTimestamp,
	}
}
