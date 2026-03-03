package kfake

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// FORMAT MIGRATION GUIDE
//
// Each file/entry embeds its own version. We support current version
// (currentPersistVersion) and one version back (N-1). On save, we
// always write currentPersistVersion.
//
// Append log entries are version-tagged per-entry, so old entries
// remain readable in-place. No need to rewrite large files on upgrade.
// New entries are appended at the current version.
//
// To evolve the format (bump from version N to N+1):
//
//   1. Bump currentPersistVersion from N to N+1.
//
//   2. For each type whose format changed, copy current type to
//      a versioned name (persistTopics -> persistTopicsVN), then
//      modify persistTopics for the new shape.
//
//   3. Write migration function: migrateTopicsVN(old) -> new
//
//   4. For JSON files: update loader to check version, decode old
//      type if version == N, call migration.
//      For append log entries: update entry decoder to handle both
//      version N and N+1 entries in the same file.
//
//   5. Delete v(N-1) migration code and types. Only keep one old
//      version. Users on v(N-1) must first load+save with v(N)
//      code, then upgrade to v(N+1).
//
//   6. Unchanged types need no migration.

const currentPersistVersion = 1

// entryHeader is the framing for all append log entries.
// Format: [4 bytes length][4 bytes CRC32][2 bytes version][N bytes data]
// All multi-byte integers are little-endian.
const entryHeaderSize = 10 // 4 (length) + 4 (CRC) + 2 (version)

// writeEntry frames data with length+CRC+version and writes it to f.
// If syncWrites is true, fsync is called after the write.
func writeEntry(f file, data []byte, syncWrites bool) error {
	// length = len(version) + len(data)
	length := uint32(2 + len(data))
	var hdr [entryHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], length)
	// CRC covers version + data
	var vbuf [2]byte
	binary.LittleEndian.PutUint16(vbuf[:], currentPersistVersion)
	crcVal := crc32.NewIEEE()
	crcVal.Write(vbuf[:])
	crcVal.Write(data)
	binary.LittleEndian.PutUint32(hdr[4:8], crcVal.Sum32())
	binary.LittleEndian.PutUint16(hdr[8:10], currentPersistVersion)

	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if syncWrites {
		return f.Sync()
	}
	return nil
}

// readEntries reads all valid framed entries from raw bytes.
// It returns the entries and the number of valid bytes consumed.
// Any trailing corrupt or partial entry is ignored (truncation point).
func readEntries(raw []byte) (entries []entryData, validBytes int) {
	pos := 0
	for pos+entryHeaderSize <= len(raw) {
		length := binary.LittleEndian.Uint32(raw[pos : pos+4])
		if length < 2 { // minimum is 2 bytes for version
			break
		}
		if pos+4+4+int(length) > len(raw) {
			break
		}

		storedCRC := binary.LittleEndian.Uint32(raw[pos+4 : pos+8])
		versionAndData := raw[pos+8 : pos+4+4+int(length)]

		// Verify CRC
		if crc32.ChecksumIEEE(versionAndData) != storedCRC {
			break
		}

		version := binary.LittleEndian.Uint16(versionAndData[0:2])
		data := versionAndData[2:]

		entries = append(entries, entryData{
			version: version,
			data:    data,
		})
		pos = pos + 4 + 4 + int(length)
	}
	return entries, pos
}

type entryData struct {
	version uint16
	data    []byte
}

////////////////////
// JSON FILE TYPES
////////////////////

type persistMeta struct {
	Version   int    `json:"version"`
	ClusterID string `json:"cluster_id"`
}

type persistTopics struct {
	Version int            `json:"version"`
	Topics  []persistTopic `json:"topics"`
}

type persistTopic struct {
	Name       string            `json:"name"`
	ID         uuid              `json:"id"`
	Partitions int               `json:"partitions"`
	Replicas   int               `json:"replicas"`
	Configs    map[string]string `json:"configs,omitempty"`
}

type persistACLs struct {
	Version int          `json:"version"`
	ACLs    []persistACL `json:"acls"`
}

type persistACL struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType int8   `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	Pattern      int8   `json:"pattern"`
	Operation    int8   `json:"operation"`
	Permission   int8   `json:"permission"`
}

type persistSASL struct {
	Version  int                     `json:"version"`
	Plain    map[string]string       `json:"plain,omitempty"`
	Scram256 map[string]persistScram `json:"scram256,omitempty"`
	Scram512 map[string]persistScram `json:"scram512,omitempty"`
}

type persistScram struct {
	Mechanism  string `json:"mechanism"`
	Salt       []byte `json:"salt"`
	SaltedPass []byte `json:"salted_pass"`
	Iterations int    `json:"iterations"`
}

type persistBrokerConfigs struct {
	Version int               `json:"version"`
	Configs map[string]string `json:"configs"`
}

type persistQuotas struct {
	Version int            `json:"version"`
	Quotas  []persistQuota `json:"quotas"`
}

type persistQuota struct {
	Entity []persistQuotaEntityComponent `json:"entity"`
	Values map[string]float64            `json:"values"`
}

type persistQuotaEntityComponent struct {
	Type string  `json:"type"`
	Name *string `json:"name"`
}

type persistSeqWindows struct {
	Version int                     `json:"version"`
	Windows []persistSeqWindowEntry `json:"windows"`
}

type persistSeqWindowEntry struct {
	PID     int64    `json:"pid"`
	Topic   string   `json:"topic"`
	Part    int32    `json:"partition"`
	Seq     [5]int32 `json:"seq"`
	Offsets [5]int64 `json:"offsets"`
	At      uint8    `json:"at"`
	Epoch   int16    `json:"epoch"`
}

type persistPartSnapshot struct {
	Version              int                  `json:"version"`
	HighWatermark        int64                `json:"high_watermark"`
	LastStableOffset     int64                `json:"last_stable_offset"`
	LogStartOffset       int64                `json:"log_start_offset"`
	Epoch                int32                `json:"epoch"`
	MaxTimestamp         int64                `json:"max_timestamp"`
	MaxTimestampBatchIdx int                  `json:"max_timestamp_batch_idx"`
	CreatedAt            time.Time            `json:"created_at"`
	AbortedTxns          []persistAbortedTxn  `json:"aborted_txns,omitempty"`
	Segments             []persistSegmentInfo `json:"segments"`
}

type persistAbortedTxn struct {
	ProducerID  int64 `json:"producer_id"`
	FirstOffset int64 `json:"first_offset"`
	LastOffset  int64 `json:"last_offset"`
}

type persistSegmentInfo struct {
	BaseOffset int64 `json:"base_offset"`
	Size       int64 `json:"size"`
}

/////////////////////
// SEGMENT ENTRIES
/////////////////////

// Segment entry data payload (inside the CRC frame):
//   [4 bytes: partition epoch, little-endian]
//   [8 bytes: maxEarlierTimestamp, little-endian]
//   [1 byte: flags (bit 0 = inTx)]
//   [N bytes: wire-format kmsg.RecordBatch via AppendTo]

func encodeSegmentEntry(b *partBatch) []byte {
	batchBytes := b.RecordBatch.AppendTo(nil)
	entry := make([]byte, 4+8+1+len(batchBytes))
	binary.LittleEndian.PutUint32(entry[0:4], uint32(b.epoch))
	binary.LittleEndian.PutUint64(entry[4:12], uint64(b.maxEarlierTimestamp))
	if b.inTx {
		entry[12] = 1
	}
	copy(entry[13:], batchBytes)
	return entry
}

func decodeSegmentEntry(data []byte) (*partBatch, error) {
	if len(data) < 13 {
		return nil, fmt.Errorf("segment entry too short: %d bytes", len(data))
	}
	epoch := int32(binary.LittleEndian.Uint32(data[0:4]))
	maxEarlierTS := int64(binary.LittleEndian.Uint64(data[4:12]))
	inTx := data[12]&1 != 0

	var rb kmsg.RecordBatch
	if err := rb.ReadFrom(data[13:]); err != nil {
		return nil, fmt.Errorf("decoding RecordBatch: %w", err)
	}
	return &partBatch{
		RecordBatch:         rb,
		nbytes:              len(data[13:]),
		epoch:               epoch,
		maxEarlierTimestamp: maxEarlierTS,
		inTx:                inTx,
	}, nil
}

//////////////////////////
// APPEND LOG ENTRIES
//////////////////////////

// Group log entry types
type groupLogEntry struct {
	Type     string  `json:"type"`
	Group    string  `json:"g"`
	Topic    string  `json:"t,omitempty"`
	Part     int32   `json:"p,omitempty"`
	Offset   int64   `json:"o,omitempty"`
	Epoch    int32   `json:"e,omitempty"`
	Metadata *string `json:"m,omitempty"`

	// For meta entries (classic groups)
	GroupType  string `json:"typ,omitempty"`
	ProtoType  string `json:"pt,omitempty"`
	Protocol   string `json:"pr,omitempty"`
	Generation int32  `json:"gen,omitempty"`

	// For meta848 entries (consumer groups)
	Assignor   string `json:"assignor,omitempty"`
	GroupEpoch int32  `json:"epoch848,omitempty"`

	// For static member entries
	InstanceID string `json:"instance,omitempty"`
	MemberID   string `json:"member,omitempty"`
}

// PID log entry types
type pidLogEntry struct {
	Type    string `json:"type"` // init, endtx, timeout
	PID     int64  `json:"pid"`
	Epoch   int16  `json:"epoch"`
	TxID    string `json:"txid,omitempty"`
	Timeout int32  `json:"timeout,omitempty"`
	Commit  *bool  `json:"commit,omitempty"`
}

/////////////////////
// WRITE HELPERS
/////////////////////

// writeJSONFile writes a JSON file atomically via temp+rename.
func writeJSONFile(fsys fs, path string, v any, syncWrites bool) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling %s: %w", path, err)
	}
	tmpPath := path + ".tmp"
	f, err := fsys.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmpPath, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("writing %s: %w", tmpPath, err)
	}
	if syncWrites {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("syncing %s: %w", tmpPath, err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", tmpPath, err)
	}
	return fsys.Rename(tmpPath, path)
}

// appendLogEntry encodes v as JSON and appends it as a framed entry.
func appendLogEntry(f file, v any, syncWrites bool) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeEntry(f, data, syncWrites)
}

// topicDir returns the directory name for a topic, URL-escaping for fs safety.
func topicDir(dataDir, topic string, part int32) string {
	escaped := url.PathEscape(topic)
	return filepath.Join(dataDir, "partitions", fmt.Sprintf("%s-%d", escaped, part))
}

// segmentFileName returns the segment file name for a given base offset.
func segmentFileName(baseOffset int64) string {
	return fmt.Sprintf("%d.dat", baseOffset)
}

//////////////////////
// SAVE (SHUTDOWN)
//////////////////////

// saveToDisk writes all cluster state to disk. Called during orderly shutdown.
func (c *Cluster) saveToDisk() error {
	fsys := c.fs
	dir := c.cfg.dataDir
	syncW := c.cfg.syncWrites

	if err := fsys.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	// meta.json
	if err := writeJSONFile(fsys, filepath.Join(dir, "meta.json"), persistMeta{
		Version:   currentPersistVersion,
		ClusterID: c.cfg.clusterID,
	}, syncW); err != nil {
		return err
	}

	// topics.json
	if err := c.saveTopics(fsys, dir, syncW); err != nil {
		return err
	}

	// acls.json
	if err := c.saveACLs(fsys, dir, syncW); err != nil {
		return err
	}

	// sasl.json
	if err := c.saveSASL(fsys, dir, syncW); err != nil {
		return err
	}

	// broker_configs.json
	if err := c.saveBrokerConfigs(fsys, dir, syncW); err != nil {
		return err
	}

	// quotas.json
	if err := c.saveQuotas(fsys, dir, syncW); err != nil {
		return err
	}

	// partitions - concurrent, one goroutine per partition
	if err := c.savePartitions(fsys, dir, syncW); err != nil {
		return err
	}

	// groups.log - compacted
	if err := c.saveGroupsLog(fsys, dir, syncW); err != nil {
		return err
	}

	// pids.log - compacted
	if err := c.savePIDsLog(fsys, dir, syncW); err != nil {
		return err
	}

	// seq_windows.json
	if err := c.saveSeqWindows(fsys, dir, syncW); err != nil {
		return err
	}

	return nil
}

func (c *Cluster) saveTopics(fsys fs, dir string, syncW bool) error {
	var pts []persistTopic
	for t, id := range c.data.t2id {
		cfgs := make(map[string]string)
		for k, v := range c.data.tcfgs[t] {
			if v != nil {
				cfgs[k] = *v
			}
		}
		nparts := 0
		if ps, ok := c.data.tps[t]; ok {
			nparts = len(ps)
		}
		pts = append(pts, persistTopic{
			Name:       t,
			ID:         id,
			Partitions: nparts,
			Replicas:   c.data.treplicas[t],
			Configs:    cfgs,
		})
	}
	return writeJSONFile(fsys, filepath.Join(dir, "topics.json"), persistTopics{
		Version: currentPersistVersion,
		Topics:  pts,
	}, syncW)
}

func (c *Cluster) saveACLs(fsys fs, dir string, syncW bool) error {
	var pa []persistACL
	for _, a := range c.acls.acls {
		pa = append(pa, persistACL{
			Principal:    a.principal,
			Host:         a.host,
			ResourceType: int8(a.resourceType),
			ResourceName: a.resourceName,
			Pattern:      int8(a.pattern),
			Operation:    int8(a.operation),
			Permission:   int8(a.permission),
		})
	}
	return writeJSONFile(fsys, filepath.Join(dir, "acls.json"), persistACLs{
		Version: currentPersistVersion,
		ACLs:    pa,
	}, syncW)
}

func (c *Cluster) saveSASL(fsys fs, dir string, syncW bool) error {
	ps := persistSASL{Version: currentPersistVersion}
	if len(c.sasls.plain) > 0 {
		ps.Plain = make(map[string]string)
		for k, v := range c.sasls.plain {
			ps.Plain[k] = v
		}
	}
	if len(c.sasls.scram256) > 0 {
		ps.Scram256 = make(map[string]persistScram)
		for k, v := range c.sasls.scram256 {
			ps.Scram256[k] = persistScram{
				Mechanism:  v.mechanism,
				Salt:       v.salt,
				SaltedPass: v.saltedPass,
				Iterations: v.iterations,
			}
		}
	}
	if len(c.sasls.scram512) > 0 {
		ps.Scram512 = make(map[string]persistScram)
		for k, v := range c.sasls.scram512 {
			ps.Scram512[k] = persistScram{
				Mechanism:  v.mechanism,
				Salt:       v.salt,
				SaltedPass: v.saltedPass,
				Iterations: v.iterations,
			}
		}
	}
	return writeJSONFile(fsys, filepath.Join(dir, "sasl.json"), ps, syncW)
}

func (c *Cluster) saveBrokerConfigs(fsys fs, dir string, syncW bool) error {
	cfgs := make(map[string]string)
	for k, v := range c.loadBcfgs() {
		if v != nil {
			cfgs[k] = *v
		}
	}
	return writeJSONFile(fsys, filepath.Join(dir, "broker_configs.json"), persistBrokerConfigs{
		Version: currentPersistVersion,
		Configs: cfgs,
	}, syncW)
}

func (c *Cluster) saveQuotas(fsys fs, dir string, syncW bool) error {
	var pqs []persistQuota
	for _, qe := range c.quotas {
		var entity []persistQuotaEntityComponent
		for _, ec := range qe.entity {
			entity = append(entity, persistQuotaEntityComponent{
				Type: ec.entityType,
				Name: ec.name,
			})
		}
		pqs = append(pqs, persistQuota{
			Entity: entity,
			Values: qe.values,
		})
	}
	return writeJSONFile(fsys, filepath.Join(dir, "quotas.json"), persistQuotas{
		Version: currentPersistVersion,
		Quotas:  pqs,
	}, syncW)
}

// forEachPartition runs fn concurrently on every partition, returning
// the first error encountered.
func (c *Cluster) forEachPartition(fn func(topic string, part int32, pd *partData) error) error {
	type partJob struct {
		topic string
		part  int32
		pd    *partData
	}
	var jobs []partJob
	c.data.tps.each(func(t string, p int32, pd *partData) {
		jobs = append(jobs, partJob{t, p, pd})
	})

	var mu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j partJob) {
			defer wg.Done()
			if err := fn(j.topic, j.part, j.pd); err != nil {
				setErr(fmt.Errorf("partition %s-%d: %w", j.topic, j.part, err))
			}
		}(j)
	}
	wg.Wait()
	return firstErr
}

func (c *Cluster) savePartitions(fsys fs, dir string, syncW bool) error {
	return c.forEachPartition(func(topic string, part int32, pd *partData) error {
		return c.savePartition(fsys, dir, syncW, topic, part, pd)
	})
}

func (c *Cluster) savePartition(fsys fs, dir string, syncW bool, topic string, part int32, pd *partData) error {
	pdir := topicDir(dir, topic, part)
	if err := fsys.MkdirAll(pdir, 0755); err != nil {
		return err
	}

	// Close any open segment file from live sync
	if pd.activeSegFile != nil {
		pd.activeSegFile.Close()
		pd.activeSegFile = nil
	}

	// Write segment files - group batches by segment base offset
	type segInfo struct {
		baseOffset int64
		batches    []*partBatch
		size       int64 // running byte count
	}
	var segments []segInfo

	if len(pd.batches) > 0 {
		if len(pd.segmentBases) > 0 && syncW {
			// Live sync mode - all batches already fsynced on disk; just record
			// existing segment info for the snapshot.
			for _, base := range pd.segmentBases {
				segments = append(segments, segInfo{baseOffset: base})
			}
		} else {
			// Shutdown-only mode (or loaded from prior session without syncWrites) -
			// write all batches into segment files from scratch.
			segMaxBytes := c.segmentBytes(topic)
			for _, b := range pd.batches {
				if len(segments) == 0 || segments[len(segments)-1].size >= segMaxBytes {
					segments = append(segments, segInfo{baseOffset: b.FirstOffset})
				}
				seg := &segments[len(segments)-1]
				seg.batches = append(seg.batches, b)
				seg.size += int64(b.nbytes)
			}
		}
	}

	// Write segment files (only for shutdown-only mode with batches to write)
	var snapSegments []persistSegmentInfo
	for i := range segments {
		seg := &segments[i]
		if len(seg.batches) == 0 {
			// Live sync - segment file already on disk, just record its info
			path := filepath.Join(pdir, segmentFileName(seg.baseOffset))
			info, err := fsys.Stat(path)
			if err != nil {
				continue
			}
			snapSegments = append(snapSegments, persistSegmentInfo{
				BaseOffset: seg.baseOffset,
				Size:       info.Size(),
			})
			continue
		}

		path := filepath.Join(pdir, segmentFileName(seg.baseOffset))
		f, err := fsys.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		var totalSize int64
		for _, b := range seg.batches {
			entryBytes := encodeSegmentEntry(b)
			if err := writeEntry(f, entryBytes, false); err != nil {
				f.Close()
				return err
			}
			totalSize += int64(entryHeaderSize + len(entryBytes))
		}
		if syncW {
			if err := f.Sync(); err != nil {
				f.Close()
				return err
			}
		}
		f.Close()
		snapSegments = append(snapSegments, persistSegmentInfo{
			BaseOffset: seg.baseOffset,
			Size:       totalSize,
		})
	}

	// Write partition snapshot
	var abortedTxns []persistAbortedTxn
	for _, a := range pd.abortedTxns {
		abortedTxns = append(abortedTxns, persistAbortedTxn{
			ProducerID:  a.producerID,
			FirstOffset: a.firstOffset,
			LastOffset:  a.lastOffset,
		})
	}
	snap := persistPartSnapshot{
		Version:              currentPersistVersion,
		HighWatermark:        pd.highWatermark,
		LastStableOffset:     pd.lastStableOffset,
		LogStartOffset:       pd.logStartOffset,
		Epoch:                pd.epoch,
		MaxTimestamp:         pd.maxTimestamp,
		MaxTimestampBatchIdx: pd.maxTimestampBatchIdx,
		CreatedAt:            pd.createdAt,
		AbortedTxns:          abortedTxns,
		Segments:             snapSegments,
	}
	return writeJSONFile(fsys, filepath.Join(pdir, "snapshot.json"), snap, syncW)
}

// segmentBytes returns the max segment size for a topic.
func (c *Cluster) segmentBytes(topic string) int64 {
	if tcfg, ok := c.data.tcfgs[topic]; ok {
		if v, ok := tcfg["segment.bytes"]; ok && v != nil {
			if n, err := strconv.ParseInt(*v, 10, 64); err == nil {
				return n
			}
		}
	}
	if v, ok := c.loadBcfgs()["log.segment.bytes"]; ok && v != nil {
		if n, err := strconv.ParseInt(*v, 10, 64); err == nil {
			return n
		}
	}
	return 1073741824 // 1GB default
}

func (c *Cluster) saveGroupsLog(fsys fs, dir string, syncW bool) error {
	// Phase 1: collect entries via waitControl (no lock - holding
	// groupsLogMu here would deadlock if a group goroutine is
	// concurrently blocked on persistGroupEntry trying to acquire it).
	var allEntries []groupLogEntry
	if c.groups.gs != nil {
		for _, g := range c.groups.gs {
			var entries []groupLogEntry
			g.waitControl(func() {
				entries = c.collectGroupEntries(g)
			})
			allEntries = append(allEntries, entries...)
		}
	}

	// Phase 2: write compacted entries to a temp file, then atomically
	// rename over groups.log. This prevents a crash mid-write from
	// truncating the old file and losing all group state. We hold
	// the lock across the rename to prevent a concurrent
	// persistGroupEntry from writing between close and rename.
	path := filepath.Join(dir, "groups.log")
	tmpPath := path + ".tmp"
	f, err := fsys.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	for _, e := range allEntries {
		if err := appendLogEntry(f, e, false); err != nil {
			f.Close()
			return err
		}
	}
	if syncW {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	c.groupsLogMu.Lock()
	defer c.groupsLogMu.Unlock()
	if c.groupsLogFile != nil {
		c.groupsLogFile.Close()
		c.groupsLogFile = nil
	}
	return fsys.Rename(tmpPath, path)
}

// collectGroupEntries gathers all persistable state from a group.
// This must be called from within the group's manage goroutine via
// waitControl, OR after the group has been stopped (shutdown).
func (c *Cluster) collectGroupEntries(g *group) []groupLogEntry {
	var entries []groupLogEntry

	// Group metadata
	switch g.typ {
	case "consumer":
		entries = append(entries, groupLogEntry{
			Type:       "meta848",
			Group:      g.name,
			GroupType:  g.typ,
			Assignor:   g.assignorName,
			GroupEpoch: g.groupEpoch,
		})
	default:
		entries = append(entries, groupLogEntry{
			Type:       "meta",
			Group:      g.name,
			GroupType:  g.typ,
			ProtoType:  g.protocolType,
			Protocol:   g.protocol,
			Generation: g.generation,
		})
	}

	// Static members
	for instanceID, memberID := range g.staticMembers {
		entries = append(entries, groupLogEntry{
			Type:       "static",
			Group:      g.name,
			InstanceID: instanceID,
			MemberID:   memberID,
		})
	}

	// Committed offsets
	g.commits.each(func(topic string, part int32, oc *offsetCommit) {
		entries = append(entries, groupLogEntry{
			Type:     "commit",
			Group:    g.name,
			Topic:    topic,
			Part:     part,
			Offset:   oc.offset,
			Epoch:    oc.leaderEpoch,
			Metadata: oc.metadata,
		})
	})

	return entries
}

func (c *Cluster) savePIDsLog(fsys fs, dir string, syncW bool) error {
	// Close the live append handle before truncating, matching
	// saveGroupsLog. No race here (pids are single-threaded from
	// run()), but prevents a stale file descriptor.
	if c.pidsLogFile != nil {
		c.pidsLogFile.Close()
		c.pidsLogFile = nil
	}

	path := filepath.Join(dir, "pids.log")
	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, pidinf := range c.pids.ids {
		// Write the latest state for each PID
		entry := pidLogEntry{
			Type:  "init",
			PID:   pidinf.id,
			Epoch: pidinf.epoch,
			TxID:  pidinf.txid,
		}
		if pidinf.txid != "" {
			entry.Timeout = pidinf.txTimeout
			entry.Commit = &pidinf.lastWasCommit
		}
		if err := appendLogEntry(f, entry, false); err != nil {
			return err
		}
	}

	if syncW {
		return f.Sync()
	}
	return nil
}

func (c *Cluster) saveSeqWindows(fsys fs, dir string, syncW bool) error {
	var windows []persistSeqWindowEntry
	for pid, pidinf := range c.pids.ids {
		pidinf.windows.each(func(topic string, part int32, pw *pidwindow) {
			windows = append(windows, persistSeqWindowEntry{
				PID:     pid,
				Topic:   topic,
				Part:    part,
				Seq:     pw.seq,
				Offsets: pw.offsets,
				At:      pw.at,
				Epoch:   pw.epoch,
			})
		})
	}
	return writeJSONFile(fsys, filepath.Join(dir, "seq_windows.json"), persistSeqWindows{
		Version: currentPersistVersion,
		Windows: windows,
	}, syncW)
}

//////////////////////
// LOAD (STARTUP)
//////////////////////

// cleanupTmpFiles removes orphaned .tmp files left by interrupted
// writeJSONFile calls. These are incomplete writes that should be discarded.
// Checks both the top-level dir and partition subdirectories.
func cleanupTmpFiles(fsys fs, dir string) {
	removeTmpIn := func(d string) {
		entries, err := fsys.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range entries {
			if !e.IsDir() && endsWith(e.Name(), ".tmp") {
				fsys.Remove(filepath.Join(d, e.Name()))
			}
		}
	}
	removeTmpIn(dir)
	partsDir := filepath.Join(dir, "partitions")
	pdirs, err := fsys.ReadDir(partsDir)
	if err != nil {
		return
	}
	for _, d := range pdirs {
		if d.IsDir() {
			removeTmpIn(filepath.Join(partsDir, d.Name()))
		}
	}
}

// loadFromDisk loads persisted state into the cluster.
// Returns true if persisted state was found and loaded, false if no data dir exists.
func (c *Cluster) loadFromDisk() (bool, error) {
	fsys := c.fs
	dir := c.cfg.dataDir

	// Check if meta.json exists
	metaBytes, err := fsys.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("reading meta.json: %w", err)
	}

	var meta persistMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return false, fmt.Errorf("parsing meta.json: %w", err)
	}
	if meta.Version > currentPersistVersion {
		return false, fmt.Errorf("meta.json version %d is newer than supported %d", meta.Version, currentPersistVersion)
	}
	c.cfg.clusterID = meta.ClusterID

	// Clean up orphaned .tmp files from interrupted writeJSONFile calls
	cleanupTmpFiles(fsys, dir)

	// Load broker configs (before topics, so segment configs are available)
	if err := c.loadBrokerConfigs(fsys, dir); err != nil {
		return false, err
	}

	// Load topics
	if err := c.loadTopics(fsys, dir); err != nil {
		return false, err
	}

	// Load partitions (concurrent). Full replay may detect in-flight
	// transactions and collect their PIDs for epoch bumping below.
	c.crashAbortedPIDs = make(map[int64]struct{})
	if err := c.loadPartitions(fsys, dir); err != nil {
		return false, err
	}

	// Load pids
	if err := c.loadPIDsLog(fsys, dir); err != nil {
		return false, err
	}

	// Bump epoch for PIDs that had in-flight transactions at crash
	// time. This fences the old producer so it gets
	// PRODUCER_FENCED on reconnect instead of silently continuing.
	for pid := range c.crashAbortedPIDs {
		if pidinf, ok := c.pids.ids[pid]; ok {
			pidinf.epoch++
		}
	}
	c.crashAbortedPIDs = nil

	// Load groups
	if err := c.loadGroupsLog(fsys, dir); err != nil {
		return false, err
	}

	// Load ACLs
	if err := c.loadACLs(fsys, dir); err != nil {
		return false, err
	}

	// Load SASL
	if err := c.loadSASL(fsys, dir); err != nil {
		return false, err
	}

	// Load quotas
	if err := c.loadQuotas(fsys, dir); err != nil {
		return false, err
	}

	// Load sequence windows
	if err := c.loadSeqWindows(fsys, dir); err != nil {
		return false, err
	}

	return true, nil
}

func (c *Cluster) loadBrokerConfigs(fsys fs, dir string) error {
	data, err := fsys.ReadFile(filepath.Join(dir, "broker_configs.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var pbc persistBrokerConfigs
	if err := json.Unmarshal(data, &pbc); err != nil {
		return fmt.Errorf("parsing broker_configs.json: %w", err)
	}
	// Merge: config overrides from NewCluster opts take precedence
	m := make(map[string]*string, len(pbc.Configs))
	for k, v := range pbc.Configs {
		v := v
		m[k] = &v
	}
	// Apply overrides from cfg.brokerConfigs
	for k, v := range c.cfg.brokerConfigs {
		if v == "" {
			m[k] = nil
		} else {
			v := v
			m[k] = &v
		}
	}
	c.storeBcfgs(m)
	return nil
}

func (c *Cluster) loadTopics(fsys fs, dir string) error {
	data, err := fsys.ReadFile(filepath.Join(dir, "topics.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var pt persistTopics
	if err := json.Unmarshal(data, &pt); err != nil {
		return fmt.Errorf("parsing topics.json: %w", err)
	}

	for _, t := range pt.Topics {
		c.data.id2t[t.ID] = t.Name
		c.data.t2id[t.Name] = t.ID
		c.data.treplicas[t.Name] = t.Replicas
		c.data.tnorms[normalizeTopicName(t.Name)] = t.Name

		if len(t.Configs) > 0 {
			cfgs := make(map[string]*string, len(t.Configs))
			for k, v := range t.Configs {
				v := v
				cfgs[k] = &v
			}
			c.data.tcfgs[t.Name] = cfgs
		}

		// Create partition data entries (batches loaded separately)
		for p := range t.Partitions {
			pd := c.data.tps.mkp(t.Name, int32(p), c.newPartData(int32(p)))
			pd.t = t.Name
		}
	}
	return nil
}

func (c *Cluster) loadPartitions(fsys fs, dir string) error {
	return c.forEachPartition(func(topic string, part int32, pd *partData) error {
		return c.loadPartition(fsys, dir, topic, part, pd)
	})
}

func (c *Cluster) loadPartition(fsys fs, dir, topic string, part int32, pd *partData) error {
	pdir := topicDir(dir, topic, part)
	pd.t = topic

	// Try snapshot first
	snapData, err := fsys.ReadFile(filepath.Join(pdir, "snapshot.json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Find segment files
	entries, err := fsys.ReadDir(pdir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no data yet
		}
		return err
	}

	var segFiles []int64
	for _, e := range entries {
		name := e.Name()
		if len(name) > 4 && name[len(name)-4:] == ".dat" {
			base, err := strconv.ParseInt(name[:len(name)-4], 10, 64)
			if err == nil {
				segFiles = append(segFiles, base)
			}
		}
	}
	sort.Slice(segFiles, func(i, j int) bool { return segFiles[i] < segFiles[j] })

	// Try using snapshot for fast startup
	if snapData != nil && len(segFiles) > 0 {
		var snap persistPartSnapshot
		if err := json.Unmarshal(snapData, &snap); err == nil && snap.Version <= currentPersistVersion {
			if snapshotMatchesSegments(snap, segFiles, fsys, pdir) {
				return c.loadPartitionFromSnapshot(pd, snap, segFiles, fsys, pdir)
			}
		}
	}

	// Full replay - scan all segments
	return c.loadPartitionFullReplay(pd, segFiles, fsys, pdir)
}

func snapshotMatchesSegments(snap persistPartSnapshot, segFiles []int64, fsys fs, pdir string) bool {
	if len(snap.Segments) != len(segFiles) {
		return false
	}
	for i, ss := range snap.Segments {
		if ss.BaseOffset != segFiles[i] {
			return false
		}
		info, err := fsys.Stat(filepath.Join(pdir, segmentFileName(ss.BaseOffset)))
		if err != nil || info.Size() != ss.Size {
			return false
		}
	}
	return true
}

func (c *Cluster) loadPartitionFromSnapshot(pd *partData, snap persistPartSnapshot, segFiles []int64, fsys fs, pdir string) error {
	pd.highWatermark = snap.HighWatermark
	pd.lastStableOffset = snap.LastStableOffset
	pd.logStartOffset = snap.LogStartOffset
	pd.epoch = snap.Epoch
	pd.maxTimestamp = snap.MaxTimestamp
	pd.maxTimestampBatchIdx = snap.MaxTimestampBatchIdx
	pd.createdAt = snap.CreatedAt
	pd.segmentBases = segFiles

	for _, a := range snap.AbortedTxns {
		pd.abortedTxns = append(pd.abortedTxns, abortedTxnEntry{
			producerID:  a.ProducerID,
			firstOffset: a.FirstOffset,
			lastOffset:  a.LastOffset,
		})
	}

	// Load actual batch data from segments
	for _, base := range segFiles {
		if err := c.loadSegmentBatches(pd, fsys, pdir, base); err != nil {
			return err
		}
	}

	// Clear stale inTx flags and accumulate nbytes. Segment files are
	// never rewritten after endTx clears inTx in memory, so completed
	// transactions' batches still have inTx=true on disk. The snapshot
	// is written at clean shutdown when no transactions are in flight,
	// so all batches are committed or aborted - inTx=false is correct.
	for _, b := range pd.batches {
		b.inTx = false
		pd.nbytes += int64(b.nbytes)
	}

	// Validate MaxTimestampBatchIdx - a corrupt or truncated segment
	// file could leave fewer batches than the snapshot expected.
	if pd.maxTimestampBatchIdx >= len(pd.batches) {
		pd.maxTimestampBatchIdx = -1
		for i, b := range pd.batches {
			if pd.maxTimestampBatchIdx < 0 || b.MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
				pd.maxTimestampBatchIdx = i
			}
		}
	}

	// Validate HWM, LSO, and logStartOffset against actual loaded
	// batches. A truncated segment could leave fewer batches than the
	// snapshot expected.
	if len(pd.batches) > 0 {
		last := pd.batches[len(pd.batches)-1]
		maxHWM := last.FirstOffset + int64(last.LastOffsetDelta) + 1
		if pd.highWatermark > maxHWM {
			pd.highWatermark = maxHWM
		}
		if pd.lastStableOffset > maxHWM {
			pd.lastStableOffset = maxHWM
		}
	} else {
		pd.highWatermark = pd.logStartOffset
		pd.lastStableOffset = pd.logStartOffset
	}
	if pd.logStartOffset > pd.highWatermark {
		pd.logStartOffset = pd.highWatermark
	}

	// Initialize active segment state so persistBatch appends to
	// the last segment instead of segment 0.
	c.initActiveSegment(pd, fsys, pdir)

	return nil
}

func (c *Cluster) loadPartitionFullReplay(pd *partData, segFiles []int64, fsys fs, pdir string) error {
	pd.segmentBases = segFiles

	for _, base := range segFiles {
		if err := c.loadSegmentBatches(pd, fsys, pdir, base); err != nil {
			return err
		}
	}

	// Reconstruct partition metadata from batches
	if len(pd.batches) > 0 {
		last := pd.batches[len(pd.batches)-1]
		pd.highWatermark = last.FirstOffset + int64(last.LastOffsetDelta) + 1

		// logStartOffset: lossy reconstruction. The true value
		// (set by DeleteRecords) is only in the snapshot. On full
		// replay, the first batch's offset is a conservative lower
		// bound - the true logStartOffset may point into the middle
		// of this batch, so some "deleted" records may reappear.
		// This matches Kafka unclean leader election behavior.
		pd.logStartOffset = pd.batches[0].FirstOffset

		// epoch: use the last batch's epoch (highest seen).
		pd.epoch = pd.batches[len(pd.batches)-1].epoch
	}

	// Rebuild abortedTxns from control batches and clear inTx on
	// completed transactions. On disk, data batches are written with
	// inTx=true at produce time; endTx clears it in memory but not
	// on disk. During full replay we must clear inTx for any
	// transaction that has a corresponding control batch.
	type txnState struct {
		firstOffset int64
	}
	activeTxns := make(map[int64]*txnState) // producerID -> state
	for _, b := range pd.batches {
		isControl := b.Attributes&0x0020 != 0
		if !isControl {
			if b.inTx {
				if _, ok := activeTxns[b.ProducerID]; !ok {
					activeTxns[b.ProducerID] = &txnState{firstOffset: b.FirstOffset}
				}
			}
			continue
		}
		// Control batch: decode the key to determine commit vs abort.
		// Key is 4 bytes: version(2) + type(2). Type 0=ABORT, 1=COMMIT.
		// If the control record is unreadable, treat as abort (fail-safe).
		isAbort := true
		if len(b.Records) >= 4 {
			var rec kmsg.Record
			if err := rec.ReadFrom(b.Records); err == nil && len(rec.Key) >= 4 {
				controlType := int16(binary.BigEndian.Uint16(rec.Key[2:4]))
				isAbort = controlType == 0
			} else {
				c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d: malformed control batch at offset %d, treating as abort",
					pd.t, pd.p, b.FirstOffset)
			}
		} else {
			c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d: empty control batch at offset %d, treating as abort",
				pd.t, pd.p, b.FirstOffset)
		}
		ts := activeTxns[b.ProducerID]
		if isAbort && ts != nil {
			pd.abortedTxns = append(pd.abortedTxns, abortedTxnEntry{
				producerID:  b.ProducerID,
				firstOffset: ts.firstOffset,
				lastOffset:  b.FirstOffset,
			})
		}
		delete(activeTxns, b.ProducerID)
	}

	// Implicitly abort in-flight transactions. Any producer still in
	// activeTxns had a transaction open when the crash happened. We
	// treat these as aborted so the LSO is not stuck forever.
	if len(activeTxns) > 0 {
		c.crashAbortedPIDsMu.Lock()
		for pid, ts := range activeTxns {
			last := ts.firstOffset
			for _, b := range pd.batches {
				if b.ProducerID == pid && b.inTx {
					end := b.FirstOffset + int64(b.LastOffsetDelta)
					if end > last {
						last = end
					}
				}
			}
			pd.abortedTxns = append(pd.abortedTxns, abortedTxnEntry{
				producerID:  pid,
				firstOffset: ts.firstOffset,
				lastOffset:  last,
			})
			c.crashAbortedPIDs[pid] = struct{}{}
		}
		c.crashAbortedPIDsMu.Unlock()
	}

	// abortedTxns must be sorted by lastOffset for binary search in
	// isBatchAborted and trimLeft. Implicit aborts (appended above) can
	// have earlier lastOffset values than control-batch-derived entries,
	// so we re-sort.
	sort.Slice(pd.abortedTxns, func(i, j int) bool {
		return pd.abortedTxns[i].lastOffset < pd.abortedTxns[j].lastOffset
	})

	// Clear inTx on all data batches - completed transactions were
	// handled above, and in-flight ones were just implicitly aborted.
	for _, b := range pd.batches {
		b.inTx = false
	}

	// Rebuild LSO and other metadata
	pd.recalculateLSO()

	// Rebuild maxTimestamp and maxTimestampBatchIdx
	pd.maxTimestampBatchIdx = -1
	for i, b := range pd.batches {
		if b.FirstTimestamp > pd.maxTimestamp {
			pd.maxTimestamp = b.FirstTimestamp
		}
		if pd.maxTimestampBatchIdx < 0 || b.MaxTimestamp >= pd.batches[pd.maxTimestampBatchIdx].MaxTimestamp {
			pd.maxTimestampBatchIdx = i
		}
		pd.nbytes += int64(b.nbytes)
	}

	// Restore createdAt from the first batch. newPartData sets createdAt
	// to time.Now() but on full replay we want the original creation time.
	if len(pd.batches) > 0 {
		pd.createdAt = time.UnixMilli(pd.batches[0].FirstTimestamp)
	}

	// Initialize active segment state so persistBatch appends to
	// the last segment instead of segment 0.
	c.initActiveSegment(pd, fsys, pdir)

	return nil
}

// initActiveSegment sets pd.activeSegBase and pd.activeSegSize to the
// last segment so that persistBatch appends to the right file. Without
// this, activeSegBase stays at 0 and new batches go to segment 0.
func (c *Cluster) initActiveSegment(pd *partData, fsys fs, pdir string) {
	if len(pd.segmentBases) == 0 {
		return
	}
	pd.activeSegBase = pd.segmentBases[len(pd.segmentBases)-1]
	path := filepath.Join(pdir, segmentFileName(pd.activeSegBase))
	if info, err := fsys.Stat(path); err != nil {
		c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d: stat active segment %d: %v", pd.t, pd.p, pd.activeSegBase, err)
	} else {
		pd.activeSegSize = info.Size()
	}
}

func (c *Cluster) loadSegmentBatches(pd *partData, fsys fs, pdir string, base int64) error {
	raw, err := fsys.ReadFile(filepath.Join(pdir, segmentFileName(base)))
	if err != nil {
		return err
	}

	entries, validBytes := readEntries(raw)
	if validBytes < len(raw) {
		// Truncate corrupt tail on crash recovery
		c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d segment %d: truncating %d corrupt bytes",
			pd.t, pd.p, base, len(raw)-validBytes)
		path := filepath.Join(pdir, segmentFileName(base))
		if f, err := fsys.OpenFile(path, os.O_WRONLY, 0644); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d segment %d: open for truncate: %v", pd.t, pd.p, base, err)
		} else {
			if err := f.Truncate(int64(validBytes)); err != nil {
				c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d segment %d: truncate: %v", pd.t, pd.p, base, err)
			}
			if err := f.Close(); err != nil {
				c.cfg.logger.Logf(LogLevelWarn, "partition %s-%d segment %d: close after truncate: %v", pd.t, pd.p, base, err)
			}
		}
	}

	for _, e := range entries {
		batch, err := decodeSegmentEntry(e.data)
		if err != nil {
			return fmt.Errorf("segment %d: %w", base, err)
		}
		pd.batches = append(pd.batches, batch)
	}
	return nil
}

func (c *Cluster) loadPIDsLog(fsys fs, dir string) error {
	raw, err := fsys.ReadFile(filepath.Join(dir, "pids.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	entries, validBytes := readEntries(raw)
	if validBytes < len(raw) {
		c.cfg.logger.Logf(LogLevelWarn, "pids.log: discarding %d corrupt trailing bytes", len(raw)-validBytes)
	}
	for _, e := range entries {
		var entry pidLogEntry
		if err := json.Unmarshal(e.data, &entry); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "pids.log: skipping corrupt entry: %v", err)
			continue
		}
		switch entry.Type {
		case "init":
			pidinf := &pidinfo{
				pids:       &c.pids,
				id:         entry.PID,
				epoch:      entry.Epoch,
				txid:       entry.TxID,
				txTimeout:  entry.Timeout,
				lastActive: time.Now(),
			}
			if entry.Commit != nil {
				pidinf.lastWasCommit = *entry.Commit
			}
			c.pids.ids[entry.PID] = pidinf
			if entry.TxID != "" {
				c.pids.byTxid[entry.TxID] = pidinf
			}
		case "endtx":
			pidinf, ok := c.pids.ids[entry.PID]
			if ok {
				pidinf.epoch = entry.Epoch
				pidinf.inTx = false
				if entry.Commit != nil {
					pidinf.lastWasCommit = *entry.Commit
				}
			}
		case "timeout":
			pidinf, ok := c.pids.ids[entry.PID]
			if ok {
				pidinf.epoch = entry.Epoch
				pidinf.inTx = false
			}
		}
	}
	return nil
}

func (c *Cluster) loadGroupsLog(fsys fs, dir string) error {
	raw, err := fsys.ReadFile(filepath.Join(dir, "groups.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Replay: latest per key wins
	type commitKey struct {
		group, topic string
		part         int32
	}
	commits := make(map[commitKey]groupLogEntry)
	groupMetas := make(map[string]groupLogEntry)
	type staticKey struct {
		group, instance string
	}
	statics := make(map[staticKey]groupLogEntry)

	entries, validBytes := readEntries(raw)
	if validBytes < len(raw) {
		c.cfg.logger.Logf(LogLevelWarn, "groups.log: discarding %d corrupt trailing bytes", len(raw)-validBytes)
	}
	for _, e := range entries {
		var entry groupLogEntry
		if err := json.Unmarshal(e.data, &entry); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "groups.log: skipping corrupt entry: %v", err)
			continue
		}
		switch entry.Type {
		case "commit":
			commits[commitKey{entry.Group, entry.Topic, entry.Part}] = entry
		case "delete":
			delete(commits, commitKey{entry.Group, entry.Topic, entry.Part})
		case "meta", "meta848":
			groupMetas[entry.Group] = entry
		case "static":
			if entry.MemberID == "" {
				delete(statics, staticKey{entry.Group, entry.InstanceID})
			} else {
				statics[staticKey{entry.Group, entry.InstanceID}] = entry
			}
		}
	}

	// Initialize groups from replayed state
	if c.groups.gs == nil {
		c.groups.gs = make(map[string]*group)
	}
	topicSnap := c.snapshotTopicMeta()
	for name, meta := range groupMetas {
		g := c.groups.newGroup(name)
		switch meta.Type {
		case "meta848":
			g.typ = "consumer"
			g.assignorName = meta.Assignor
			g.groupEpoch = meta.GroupEpoch
			g.consumerMembers = make(map[string]*consumerMember)
			g.partitionEpochs = make(map[uuid]map[int32]int32)
			g.lastTopicMeta = topicSnap
		default:
			g.typ = meta.GroupType
			g.protocolType = meta.ProtoType
			g.protocol = meta.Protocol
			g.generation = meta.Generation
		}
		c.groups.gs[name] = g
		go g.manage(func() {})
	}

	// Apply commits
	for ck, entry := range commits {
		g, ok := c.groups.gs[ck.group]
		if !ok {
			g = c.groups.newGroup(ck.group)
			c.groups.gs[ck.group] = g
			go g.manage(func() {})
		}
		oc := offsetCommit{
			offset:      entry.Offset,
			leaderEpoch: entry.Epoch,
			metadata:    entry.Metadata,
		}
		g.waitControl(func() {
			g.commits.set(ck.topic, ck.part, oc)
		})
	}

	// Apply static members
	for sk, entry := range statics {
		g, ok := c.groups.gs[sk.group]
		if !ok {
			continue
		}
		g.waitControl(func() {
			if g.staticMembers == nil {
				g.staticMembers = make(map[string]string)
			}
			g.staticMembers[sk.instance] = entry.MemberID
		})
	}

	return nil
}

func (c *Cluster) loadACLs(fsys fs, dir string) error {
	data, err := fsys.ReadFile(filepath.Join(dir, "acls.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var pa persistACLs
	if err := json.Unmarshal(data, &pa); err != nil {
		return fmt.Errorf("parsing acls.json: %w", err)
	}
	for _, a := range pa.ACLs {
		c.acls.add(acl{
			principal:    a.Principal,
			host:         a.Host,
			resourceType: kmsg.ACLResourceType(a.ResourceType),
			resourceName: a.ResourceName,
			pattern:      kmsg.ACLResourcePatternType(a.Pattern),
			operation:    kmsg.ACLOperation(a.Operation),
			permission:   kmsg.ACLPermissionType(a.Permission),
		})
	}
	return nil
}

func (c *Cluster) loadSASL(fsys fs, dir string) error {
	data, err := fsys.ReadFile(filepath.Join(dir, "sasl.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var ps persistSASL
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("parsing sasl.json: %w", err)
	}
	if len(ps.Plain) > 0 {
		c.sasls.plain = ps.Plain
	}
	if len(ps.Scram256) > 0 {
		c.sasls.scram256 = make(map[string]scramAuth, len(ps.Scram256))
		for k, v := range ps.Scram256 {
			c.sasls.scram256[k] = scramAuth{
				mechanism:  v.Mechanism,
				salt:       v.Salt,
				saltedPass: v.SaltedPass,
				iterations: v.Iterations,
			}
		}
	}
	if len(ps.Scram512) > 0 {
		c.sasls.scram512 = make(map[string]scramAuth, len(ps.Scram512))
		for k, v := range ps.Scram512 {
			c.sasls.scram512[k] = scramAuth{
				mechanism:  v.Mechanism,
				salt:       v.Salt,
				saltedPass: v.SaltedPass,
				iterations: v.Iterations,
			}
		}
	}
	return nil
}

func (c *Cluster) loadQuotas(fsys fs, dir string) error {
	data, err := fsys.ReadFile(filepath.Join(dir, "quotas.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var pq persistQuotas
	if err := json.Unmarshal(data, &pq); err != nil {
		return fmt.Errorf("parsing quotas.json: %w", err)
	}
	for _, q := range pq.Quotas {
		entity := make(quotaEntity, len(q.Entity))
		for i, ec := range q.Entity {
			entity[i] = quotaEntityComponent{
				entityType: ec.Type,
				name:       ec.Name,
			}
		}
		key := entity.key()
		c.quotas[key] = quotaEntry{
			entity: entity,
			values: q.Values,
		}
	}
	return nil
}

func (c *Cluster) loadSeqWindows(fsys fs, dir string) error {
	data, err := fsys.ReadFile(filepath.Join(dir, "seq_windows.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var psw persistSeqWindows
	if err := json.Unmarshal(data, &psw); err != nil {
		return fmt.Errorf("parsing seq_windows.json: %w", err)
	}
	for _, w := range psw.Windows {
		pidinf, ok := c.pids.ids[w.PID]
		if !ok {
			continue
		}
		pw := pidwindow{
			seq:     w.Seq,
			offsets: w.Offsets,
			at:      w.At,
			epoch:   w.Epoch,
		}
		pidinf.windows.set(w.Topic, w.Part, pw)
	}
	return nil
}

///////////////////////////////
// LIVE SYNC PERSIST HELPERS
///////////////////////////////

// persistBatch appends a batch to the active segment file for a partition.
// Called from the produce path when syncWrites is enabled.
func (c *Cluster) persistBatch(pd *partData, b *partBatch) error {
	if c.cfg.dataDir == "" {
		return nil
	}
	fsys := c.fs
	pdir := topicDir(c.cfg.dataDir, pd.t, pd.p)

	// Ensure partition directory exists
	if pd.activeSegFile == nil {
		if err := fsys.MkdirAll(pdir, 0755); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "persist batch mkdir %s: %v", pdir, err)
			return err
		}
		// Open or create active segment
		base := pd.activeSegBase
		if len(pd.segmentBases) == 0 {
			base = b.FirstOffset
			pd.segmentBases = append(pd.segmentBases, base)
			pd.activeSegBase = base
		}
		path := filepath.Join(pdir, segmentFileName(base))
		f, err := fsys.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "persist batch open %s: %v", path, err)
			return err
		}
		pd.activeSegFile = f
	}

	// Check if we need to roll the segment
	segMax := c.segmentBytes(pd.t)
	if pd.activeSegSize >= segMax {
		if err := pd.activeSegFile.Close(); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "persist batch close segment %s-%d: %v", pd.t, pd.p, err)
		}
		pd.activeSegFile = nil
		pd.activeSegBase = b.FirstOffset
		pd.segmentBases = append(pd.segmentBases, pd.activeSegBase)
		pd.activeSegSize = 0
		path := filepath.Join(pdir, segmentFileName(pd.activeSegBase))
		f, err := fsys.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "persist batch open %s: %v", path, err)
			return err
		}
		pd.activeSegFile = f
	}

	entryBytes := encodeSegmentEntry(b)
	if err := writeEntry(pd.activeSegFile, entryBytes, c.cfg.syncWrites); err != nil {
		c.cfg.logger.Logf(LogLevelWarn, "persist batch write %s-%d: %v", pd.t, pd.p, err)
		pd.activeSegFile.Close()
		pd.activeSegFile = nil
		return err
	}
	pd.activeSegSize += int64(entryHeaderSize + len(entryBytes))
	return nil
}

// persistGroupEntry appends a group log entry.
// Called from group handlers when dataDir is set.
// Multiple group manage() goroutines may call this concurrently,
// so writes are serialized with groupsLogMu.
func (c *Cluster) persistGroupEntry(entry groupLogEntry) error {
	if c.cfg.dataDir == "" || c.dead.Load() {
		return nil
	}
	c.groupsLogMu.Lock()
	defer c.groupsLogMu.Unlock()
	if c.groupsLogFile == nil {
		path := filepath.Join(c.cfg.dataDir, "groups.log")
		f, err := c.fs.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "persist group entry open: %v", err)
			return err
		}
		c.groupsLogFile = f
	}
	if err := appendLogEntry(c.groupsLogFile, entry, c.cfg.syncWrites); err != nil {
		c.cfg.logger.Logf(LogLevelWarn, "persist group entry write: %v", err)
		c.groupsLogFile.Close()
		c.groupsLogFile = nil
		return err
	}
	return nil
}

// persistPIDEntry appends a PID log entry.
// Called from txn handlers when dataDir is set.
func (c *Cluster) persistPIDEntry(entry pidLogEntry) error {
	if c.cfg.dataDir == "" {
		return nil
	}
	if c.pidsLogFile == nil {
		path := filepath.Join(c.cfg.dataDir, "pids.log")
		f, err := c.fs.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "persist pid entry open: %v", err)
			return err
		}
		c.pidsLogFile = f
	}
	if err := appendLogEntry(c.pidsLogFile, entry, c.cfg.syncWrites); err != nil {
		c.cfg.logger.Logf(LogLevelWarn, "persist pid entry write: %v", err)
		c.pidsLogFile.Close()
		c.pidsLogFile = nil
		return err
	}
	return nil
}

// persistState is a shared helper for live-sync state file writes.
// It calls the given save function and logs on error.
func (c *Cluster) persistState(name string, fn func(fs, string, bool) error) {
	if c.cfg.dataDir == "" {
		return
	}
	if err := fn(c.fs, c.cfg.dataDir, c.cfg.syncWrites); err != nil {
		c.cfg.logger.Logf(LogLevelWarn, "persist %s: %v", name, err)
	}
}

func (c *Cluster) persistTopicsState()        { c.persistState("topics", c.saveTopics) }
func (c *Cluster) persistACLsState()          { c.persistState("acls", c.saveACLs) }
func (c *Cluster) persistSASLState()          { c.persistState("sasl", c.saveSASL) }
func (c *Cluster) persistBrokerConfigsState() { c.persistState("broker configs", c.saveBrokerConfigs) }
func (c *Cluster) persistQuotasState()        { c.persistState("quotas", c.saveQuotas) }

// closeOpenFiles closes all open file handles for persistence.
func (c *Cluster) closeOpenFiles() {
	c.groupsLogMu.Lock()
	if c.groupsLogFile != nil {
		if err := c.groupsLogFile.Close(); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "closing groups.log: %v", err)
		}
		c.groupsLogFile = nil
	}
	c.groupsLogMu.Unlock()
	if c.pidsLogFile != nil {
		if err := c.pidsLogFile.Close(); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "closing pids.log: %v", err)
		}
		c.pidsLogFile = nil
	}
	c.data.tps.each(func(t string, p int32, pd *partData) {
		if pd.activeSegFile != nil {
			if err := pd.activeSegFile.Close(); err != nil {
				c.cfg.logger.Logf(LogLevelWarn, "closing segment %s-%d: %v", t, p, err)
			}
			pd.activeSegFile = nil
		}
	})
}
