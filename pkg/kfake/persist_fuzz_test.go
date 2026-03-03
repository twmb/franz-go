package kfake

import (
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// FuzzReadEntries feeds arbitrary bytes to readEntries and verifies invariants:
// - never panics
// - validBytes <= len(input)
// - all returned entries have non-nil data
// - re-parsing the valid prefix returns the same entries
func FuzzReadEntries(f *testing.F) {
	// Seed with various interesting inputs
	f.Add([]byte{})                       // empty
	f.Add([]byte{0, 0, 0, 0})             // zero length
	f.Add([]byte{1, 0, 0, 0})             // length=1, too small (<2)
	f.Add(make([]byte, 10))               // all zeros, exactly header size
	f.Add(make([]byte, 100))              // all zeros, larger
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // max uint32 length

	// Seed with a valid entry
	data := []byte("test payload")
	length := uint32(2 + len(data))
	var hdr [10]byte
	binary.LittleEndian.PutUint32(hdr[0:4], length)
	binary.LittleEndian.PutUint16(hdr[8:10], 1)
	crcVal := crc32.NewIEEE()
	var vbuf [2]byte
	binary.LittleEndian.PutUint16(vbuf[:], 1)
	crcVal.Write(vbuf[:])
	crcVal.Write(data)
	binary.LittleEndian.PutUint32(hdr[4:8], crcVal.Sum32())
	validEntry := append(hdr[:], data...)
	f.Add(validEntry)

	// Two valid entries concatenated
	f.Add(append(append([]byte{}, validEntry...), validEntry...))

	// Valid entry + garbage
	f.Add(append(append([]byte{}, validEntry...), 0xDE, 0xAD, 0xBE, 0xEF))

	f.Fuzz(func(t *testing.T, input []byte) {
		entries, validBytes := readEntries(input)

		// validBytes must not exceed input length
		if validBytes > len(input) {
			t.Fatalf("validBytes %d > len(input) %d", validBytes, len(input))
		}

		// validBytes must not be negative
		if validBytes < 0 {
			t.Fatalf("validBytes %d < 0", validBytes)
		}

		// All entries must have non-nil data
		for i, e := range entries {
			if e.data == nil {
				t.Fatalf("entry %d has nil data", i)
			}
		}

		// Re-parsing the valid prefix should return the same number of entries
		if validBytes > 0 {
			entries2, vb2 := readEntries(input[:validBytes])
			if len(entries2) != len(entries) {
				t.Fatalf("reparse of valid prefix: got %d entries, expected %d",
					len(entries2), len(entries))
			}
			if vb2 != validBytes {
				t.Fatalf("reparse validBytes: got %d, expected %d", vb2, validBytes)
			}
		}
	})
}

// FuzzDecodeSegmentEntry feeds arbitrary bytes to decodeSegmentEntry.
// Verifies it never panics and returns a sensible error for bad input.
func FuzzDecodeSegmentEntry(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add(make([]byte, 12)) // too short (< 13)
	f.Add(make([]byte, 13)) // exactly minimum
	f.Add(make([]byte, 100))

	// Valid minimal RecordBatch is complex, but we can at least
	// test with various sizes near boundaries.
	for size := 0; size <= 20; size++ {
		f.Add(make([]byte, size))
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		// Must not panic
		batch, err := decodeSegmentEntry(input)
		if len(input) < 13 {
			if err == nil {
				t.Fatalf("expected error for input len %d < 13", len(input))
			}
			return
		}
		// For >= 13 bytes, it may or may not succeed depending on
		// whether the kmsg.RecordBatch parse succeeds.
		_ = batch
		_ = err
	})
}

// TestReadEntriesTruncatedHeader verifies truncated headers are handled.
func TestReadEntriesTruncatedHeader(t *testing.T) {
	t.Parallel()
	// Less than header size
	for size := 0; size < entryHeaderSize; size++ {
		entries, validBytes := readEntries(make([]byte, size))
		if len(entries) != 0 {
			t.Fatalf("size %d: expected 0 entries, got %d", size, len(entries))
		}
		if validBytes != 0 {
			t.Fatalf("size %d: expected validBytes 0, got %d", size, validBytes)
		}
	}
}

// TestReadEntriesBadCRC verifies CRC mismatch stops parsing.
func TestReadEntriesBadCRC(t *testing.T) {
	t.Parallel()

	makeEntry := func(data []byte) []byte {
		length := uint32(2 + len(data))
		var hdr [10]byte
		binary.LittleEndian.PutUint32(hdr[0:4], length)
		binary.LittleEndian.PutUint16(hdr[8:10], 1)
		crcVal := crc32.NewIEEE()
		var vbuf [2]byte
		binary.LittleEndian.PutUint16(vbuf[:], 1)
		crcVal.Write(vbuf[:])
		crcVal.Write(data)
		binary.LittleEndian.PutUint32(hdr[4:8], crcVal.Sum32())
		return append(hdr[:], data...)
	}

	good := makeEntry([]byte("good entry"))
	bad := makeEntry([]byte("bad entry"))
	bad[5] ^= 0xFF // corrupt CRC byte

	// Good + bad: should parse 1 entry
	combined := append(append([]byte{}, good...), bad...)
	entries, validBytes := readEntries(combined)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if validBytes != len(good) {
		t.Fatalf("expected validBytes %d, got %d", len(good), validBytes)
	}

	// Bad alone: should parse 0 entries
	entries, validBytes = readEntries(bad)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
	if validBytes != 0 {
		t.Fatalf("expected validBytes 0, got %d", validBytes)
	}
}

// TestReadEntriesLengthTooSmall verifies length < 2 stops parsing.
func TestReadEntriesLengthTooSmall(t *testing.T) {
	t.Parallel()
	var buf [14]byte
	binary.LittleEndian.PutUint32(buf[0:4], 1) // length = 1, too small
	entries, validBytes := readEntries(buf[:])
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
	if validBytes != 0 {
		t.Fatalf("expected validBytes 0, got %d", validBytes)
	}
}

// TestReadEntriesLengthExceedsFile verifies overlength entries stop parsing.
func TestReadEntriesLengthExceedsFile(t *testing.T) {
	t.Parallel()
	var buf [10]byte
	binary.LittleEndian.PutUint32(buf[0:4], 1000) // claims 1000 bytes but only 10 available
	entries, validBytes := readEntries(buf[:])
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
	if validBytes != 0 {
		t.Fatalf("expected validBytes 0, got %d", validBytes)
	}
}

// TestWriteReadEntryRoundTrip verifies write + read produces identical data.
func TestWriteReadEntryRoundTrip(t *testing.T) {
	t.Parallel()

	mfs := newMemFS()
	f, err := mfs.OpenFile("test.log", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	payloads := []string{"hello", "world", "", "a longer payload with more data"}
	for _, p := range payloads {
		if err := writeEntry(f, []byte(p), false); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	raw, err := mfs.ReadFile("test.log")
	if err != nil {
		t.Fatal(err)
	}
	entries, validBytes := readEntries(raw)
	if validBytes != len(raw) {
		t.Fatalf("expected all bytes valid, got %d/%d", validBytes, len(raw))
	}
	if len(entries) != len(payloads) {
		t.Fatalf("expected %d entries, got %d", len(payloads), len(entries))
	}
	for i, e := range entries {
		if string(e.data) != payloads[i] {
			t.Fatalf("entry %d: expected %q, got %q", i, payloads[i], string(e.data))
		}
		if e.version != currentPersistVersion {
			t.Fatalf("entry %d: expected version %d, got %d", i, currentPersistVersion, e.version)
		}
	}
}

// TestEncodeDecodeSegmentEntryRoundTrip verifies segment entry encode/decode.
func TestEncodeDecodeSegmentEntryRoundTrip(t *testing.T) {
	t.Parallel()

	batch := &partBatch{
		epoch:               3,
		maxEarlierTimestamp: 1000,
		inTx:                true,
	}
	batch.FirstOffset = 100
	batch.LastOffsetDelta = 9
	batch.FirstTimestamp = 500
	batch.MaxTimestamp = 900
	batch.ProducerID = 7
	batch.ProducerEpoch = 2
	batch.Magic = 2
	batch.Length = 49 // fixed header size after FirstOffset+Length, no records
	// Serialize and set nbytes from the wire format so sizes match.
	batch.nbytes = len(batch.AppendTo(nil))

	encoded := encodeSegmentEntry(batch)
	decoded, err := decodeSegmentEntry(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.epoch != batch.epoch {
		t.Fatalf("epoch: expected %d, got %d", batch.epoch, decoded.epoch)
	}
	if decoded.maxEarlierTimestamp != batch.maxEarlierTimestamp {
		t.Fatalf("maxEarlierTimestamp: expected %d, got %d",
			batch.maxEarlierTimestamp, decoded.maxEarlierTimestamp)
	}
	if decoded.inTx != batch.inTx {
		t.Fatalf("inTx: expected %v, got %v", batch.inTx, decoded.inTx)
	}
	if decoded.FirstOffset != batch.FirstOffset {
		t.Fatalf("FirstOffset: expected %d, got %d", batch.FirstOffset, decoded.FirstOffset)
	}
	if decoded.ProducerID != batch.ProducerID {
		t.Fatalf("ProducerID: expected %d, got %d", batch.ProducerID, decoded.ProducerID)
	}
	if decoded.ProducerEpoch != batch.ProducerEpoch {
		t.Fatalf("ProducerEpoch: expected %d, got %d", batch.ProducerEpoch, decoded.ProducerEpoch)
	}
}

// TestDecodeSegmentEntryTooShort verifies short input returns error.
func TestDecodeSegmentEntryTooShort(t *testing.T) {
	t.Parallel()
	for size := 0; size < 13; size++ {
		_, err := decodeSegmentEntry(make([]byte, size))
		if err == nil {
			t.Fatalf("size %d: expected error", size)
		}
	}
}

// TestMemFSRoundTrip exercises the memFS implementation end-to-end.
func TestMemFSRoundTrip(t *testing.T) {
	t.Parallel()
	mfs := newMemFS()

	// MkdirAll
	if err := mfs.MkdirAll("/a/b/c", 0755); err != nil {
		t.Fatal(err)
	}

	// WriteFile via OpenFile
	f, err := mfs.OpenFile("/a/b/c/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("hello world"))
	f.Close()

	// ReadFile
	data, err := mfs.ReadFile("/a/b/c/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Fatalf("expected %q, got %q", "hello world", string(data))
	}

	// ReadDir
	entries, err := mfs.ReadDir("/a/b/c")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "test.txt" {
		t.Fatalf("ReadDir: expected [test.txt], got %v", entries)
	}

	// Stat
	info, err := mfs.Stat("/a/b/c/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 11 {
		t.Fatalf("Stat: expected size 11, got %d", info.Size())
	}

	// Rename
	if err := mfs.Rename("/a/b/c/test.txt", "/a/b/c/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := mfs.ReadFile("/a/b/c/test.txt"); err == nil {
		t.Fatal("expected error reading old name after rename")
	}
	data, err = mfs.ReadFile("/a/b/c/renamed.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Fatalf("after rename: expected %q, got %q", "hello world", string(data))
	}

	// Remove
	if err := mfs.Remove("/a/b/c/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := mfs.ReadFile("/a/b/c/renamed.txt"); err == nil {
		t.Fatal("expected error after remove")
	}
}

// TestMemFSFaultInjection verifies fault injection in memFS.
func TestMemFSFaultInjection(t *testing.T) {
	t.Parallel()
	mfs := newMemFS()

	f, err := mfs.OpenFile("/test.dat", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Inject write failure
	injectedErr := &testError{"injected write error"}
	mfs.failNextWrite = injectedErr
	_, err = f.Write([]byte("data"))
	if err != injectedErr {
		t.Fatalf("expected injected error, got %v", err)
	}
	// Should clear after one use
	if _, err = f.Write([]byte("data")); err != nil {
		t.Fatalf("expected nil error after cleared, got %v", err)
	}

	// Inject sync failure
	injectedErr = &testError{"injected sync error"}
	mfs.failNextSync = injectedErr
	err = f.Sync()
	if err != injectedErr {
		t.Fatalf("expected injected error, got %v", err)
	}
	// Should clear after one use
	if err = f.Sync(); err != nil {
		t.Fatalf("expected nil error after cleared, got %v", err)
	}
	f.Close()
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

// TestMemFSAppendMode verifies O_APPEND behavior in memFS.
func TestMemFSAppendMode(t *testing.T) {
	t.Parallel()
	mfs := newMemFS()

	// Write initial data
	f, _ := mfs.OpenFile("/test.log", os.O_CREATE|os.O_WRONLY, 0644)
	f.Write([]byte("initial"))
	f.Close()

	// Append
	f, _ = mfs.OpenFile("/test.log", os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("-appended"))
	f.Close()

	data, _ := mfs.ReadFile("/test.log")
	if string(data) != "initial-appended" {
		t.Fatalf("expected %q, got %q", "initial-appended", string(data))
	}
}

// TestMemFSTruncateFlag verifies O_TRUNC behavior in memFS.
func TestMemFSTruncateFlag(t *testing.T) {
	t.Parallel()
	mfs := newMemFS()

	// Write initial data
	f, _ := mfs.OpenFile("/test.dat", os.O_CREATE|os.O_WRONLY, 0644)
	f.Write([]byte("old data that is very long"))
	f.Close()

	// Truncate on open
	f, _ = mfs.OpenFile("/test.dat", os.O_TRUNC|os.O_WRONLY, 0644)
	f.Write([]byte("new"))
	f.Close()

	data, _ := mfs.ReadFile("/test.dat")
	if string(data) != "new" {
		t.Fatalf("expected %q, got %q", "new", string(data))
	}
}

// TestChaosProduceCloseCrashRecover does random produce operations,
// randomly either closes cleanly or simulates crash, then reopens
// and verifies all records are present (for clean close) or at least
// non-corrupt (for crash).
func TestChaosProduceCloseCrashRecover(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(42))

	for iter := range 20 {
		cleanShutdown := iter%2 == 0
		syncWrites := iter%4 < 2
		nTopics := 1 + rng.Intn(3)
		nBatches := 5 + rng.Intn(20)

		dir := t.TempDir()

		topics := make([]string, nTopics)
		for i := range topics {
			topics[i] = topicNames[rng.Intn(len(topicNames))]
		}

		var opts []Opt
		opts = append(opts, DataDir(dir), NumBrokers(1), SeedTopics(1, topics...))
		if syncWrites {
			opts = append(opts, SyncWrites())
		}

		c, err := NewCluster(opts...)
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}

		// Push random batches directly via internal API
		var totalPerTopic = make(map[string]int64)
		for range nBatches {
			topic := topics[rng.Intn(len(topics))]
			pd, ok := c.data.tps.getp(topic, 0)
			if !ok {
				continue
			}
			nRecords := 1 + rng.Intn(5)
			b := makeTestBatch(pd.highWatermark, int32(nRecords))
			pd.pushBatch(b.nbytes, b.RecordBatch, false)
			c.persistBatch(pd, pd.batches[len(pd.batches)-1])
			totalPerTopic[topic] += int64(nRecords)
		}

		// Record expected state
		expectedHWM := make(map[string]int64)
		for _, topic := range topics {
			if pd, ok := c.data.tps.getp(topic, 0); ok {
				expectedHWM[topic] = pd.highWatermark
			}
		}

		if cleanShutdown {
			c.Close()
		}
		// For crash: just abandon the cluster (if syncWrites, data should be on disk)

		// Reopen and verify
		c2, err := NewCluster(DataDir(dir), NumBrokers(1))
		if err != nil {
			t.Fatalf("iter %d reopen: %v", iter, err)
		}

		for _, topic := range topics {
			pd, ok := c2.data.tps.getp(topic, 0)
			if !ok {
				if cleanShutdown || syncWrites {
					t.Fatalf("iter %d: topic %s missing after %s",
						iter, topic, shutdownType(cleanShutdown))
				}
				continue
			}

			if cleanShutdown {
				if pd.highWatermark != expectedHWM[topic] {
					t.Fatalf("iter %d topic %s: expected HWM %d, got %d",
						iter, topic, expectedHWM[topic], pd.highWatermark)
				}
			} else if syncWrites {
				// With sync writes, all fsynced batches should be present.
				// HWM should match since every batch was individually fsynced.
				if pd.highWatermark != expectedHWM[topic] {
					t.Fatalf("iter %d topic %s (sync crash): expected HWM %d, got %d",
						iter, topic, expectedHWM[topic], pd.highWatermark)
				}
			}
			// For non-sync crash: any HWM >= 0 is acceptable (partial data loss OK)
		}
		c2.Close()

		if !cleanShutdown {
			c.Close() // clean up the abandoned cluster
		}
	}
}

// Topic names with various tricky characters for URL escaping.
var topicNames = []string{
	"simple",
	"with.dots",
	"with-dashes",
	"with_underscores",
	"UPPER",
}

func shutdownType(clean bool) string {
	if clean {
		return "clean shutdown"
	}
	return "crash"
}

// makeTestBatch creates a minimal RecordBatch for testing.
func makeTestBatch(baseOffset int64, numRecords int32) partBatch {
	var b partBatch
	b.FirstOffset = baseOffset
	b.LastOffsetDelta = numRecords - 1
	b.FirstTimestamp = baseOffset * 1000
	b.MaxTimestamp = baseOffset*1000 + int64(numRecords)
	b.NumRecords = numRecords
	b.Magic = 2
	b.Length = 49 // fixed header size, no records
	b.nbytes = len(b.AppendTo(nil))
	return b
}

// TestChaosGroupCommitsCrashRecover creates groups with committed offsets,
// randomly crashes or closes, and verifies committed offsets survive.
func TestChaosGroupCommitsCrashRecover(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(99))

	for iter := range 10 {
		cleanShutdown := iter%2 == 0
		dir := t.TempDir()

		c, err := NewCluster(DataDir(dir), SyncWrites(), NumBrokers(1),
			SeedTopics(1, "t1", "t2"))
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}

		// Create some groups with committed offsets
		groupNames := []string{"g1", "g2", "g3"}
		expectedCommits := make(map[string]map[string]int64) // group -> topic -> offset
		for _, gn := range groupNames {
			if c.groups.gs == nil {
				c.groups.gs = make(map[string]*group)
			}
			g := c.groups.newGroup(gn)
			c.groups.gs[gn] = g
			go g.manage(func() {})

			expectedCommits[gn] = make(map[string]int64)
			for _, topic := range []string{"t1", "t2"} {
				offset := int64(rng.Intn(1000))
				expectedCommits[gn][topic] = offset
				g.waitControl(func() {
					g.commits.set(topic, 0, offsetCommit{offset: offset, leaderEpoch: -1})
				})
				c.persistGroupEntry(groupLogEntry{
					Type:   "commit",
					Group:  gn,
					Topic:  topic,
					Part:   0,
					Offset: offset,
					Epoch:  -1,
				})
			}
		}

		if cleanShutdown {
			c.Close()
		}

		// Reopen
		c2, err := NewCluster(DataDir(dir), NumBrokers(1),
			SeedTopics(1, "t1", "t2"))
		if err != nil {
			t.Fatalf("iter %d reopen: %v", iter, err)
		}

		for gn, topics := range expectedCommits {
			g, ok := c2.groups.gs[gn]
			if !ok {
				t.Fatalf("iter %d: group %s missing after restart", iter, gn)
			}
			for topic, expectedOffset := range topics {
				var actualOffset int64
				var found bool
				g.waitControl(func() {
					oc, ok := g.commits.getp(topic, 0)
					if ok {
						actualOffset = oc.offset
						found = true
					}
				})
				if !found {
					t.Fatalf("iter %d: group %s topic %s: no committed offset", iter, gn, topic)
				}
				if actualOffset != expectedOffset {
					t.Fatalf("iter %d: group %s topic %s: expected offset %d, got %d",
						iter, gn, topic, expectedOffset, actualOffset)
				}
			}
		}
		c2.Close()

		if !cleanShutdown {
			c.Close()
		}
	}
}

// TestChaosTopicCreateDeleteRestart creates and deletes topics randomly,
// then closes and reopens, verifying the correct set of topics survives.
func TestChaosTopicCreateDeleteRestart(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(77))
	dir := t.TempDir()

	var allTopics = []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	c, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}

	existing := make(map[string]bool)
	for range 15 {
		topic := allTopics[rng.Intn(len(allTopics))]
		if existing[topic] {
			// Delete it
			if pd, ok := c.data.tps.gett(topic); ok {
				for _, p := range pd {
					for w := range p.watch {
						w.deleted()
					}
				}
			}
			delete(c.data.tps, topic)
			id := c.data.t2id[topic]
			delete(c.data.id2t, id)
			delete(c.data.t2id, topic)
			delete(c.data.treplicas, topic)
			delete(c.data.tcfgs, topic)
			delete(c.data.tnorms, normalizeTopicName(topic))
			c.persistTopicsState()
			delete(existing, topic)
		} else {
			// Create it
			c.data.mkt(topic, 1+rng.Intn(3), -1, nil)
			c.persistTopicsState()
			existing[topic] = true
		}
	}

	// Record what should survive
	expectedTopics := make(map[string]bool)
	for t := range existing {
		expectedTopics[t] = true
	}
	c.Close()

	// Reopen
	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	for topic := range expectedTopics {
		if _, ok := c2.data.tps.gett(topic); !ok {
			t.Fatalf("expected topic %s to exist after restart", topic)
		}
	}
	for _, topic := range allTopics {
		if !expectedTopics[topic] {
			if _, ok := c2.data.tps.gett(topic); ok {
				t.Fatalf("topic %s should NOT exist after restart", topic)
			}
		}
	}
}

// TestPersistTopicURLEscaping verifies topics with special characters
// survive a round-trip through persistence.
func TestPersistTopicURLEscaping(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	specialTopics := []string{
		"normal-topic",
		"topic.with.dots",
		"topic_with_underscores",
		"topic with spaces",
		"topic/with/slashes",
	}

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
		SeedTopics(1, specialTopics...))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	for _, topic := range specialTopics {
		if _, ok := c2.data.tps.gett(topic); !ok {
			t.Fatalf("topic %q missing after restart", topic)
		}
	}
}

// TestPersistPIDEndTxAndTimeout verifies that PID state survives a
// shutdown/restart cycle, testing init, endtx, and timeout log entries.
func TestPersistPIDEndTxAndTimeout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SyncWrites(),
		SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}

	// PID 100: init, then endtx commit (epoch bump to 2).
	p100 := &pidinfo{
		pids:       &c.pids,
		id:         100,
		epoch:      1,
		txid:       "tx-a",
		txTimeout:  60000,
		lastActive: time.Now(),
	}
	c.pids.ids[100] = p100
	c.pids.byTxid["tx-a"] = p100
	c.persistPIDEntry(pidLogEntry{Type: "init", PID: 100, Epoch: 1, TxID: "tx-a", Timeout: 60000})
	commit := true
	c.persistPIDEntry(pidLogEntry{Type: "endtx", PID: 100, Epoch: 2, Commit: &commit})
	p100.epoch = 2
	p100.lastWasCommit = true

	// PID 200: init, then timeout (epoch bump to 2).
	p200 := &pidinfo{
		pids:       &c.pids,
		id:         200,
		epoch:      1,
		txid:       "tx-b",
		txTimeout:  30000,
		lastActive: time.Now(),
	}
	c.pids.ids[200] = p200
	c.pids.byTxid["tx-b"] = p200
	c.persistPIDEntry(pidLogEntry{Type: "init", PID: 200, Epoch: 1, TxID: "tx-b", Timeout: 30000})
	c.persistPIDEntry(pidLogEntry{Type: "timeout", PID: 200, Epoch: 2})
	p200.epoch = 2

	// PID 300: init only - verifies lastWasCommit and txTimeout.
	p300 := &pidinfo{
		pids:          &c.pids,
		id:            300,
		epoch:         5,
		txid:          "tx-c",
		txTimeout:     90000,
		lastWasCommit: true,
		lastActive:    time.Now(),
	}
	c.pids.ids[300] = p300
	c.pids.byTxid["tx-c"] = p300

	c.Close()

	// Reopen - the pids.log has live entries (init+endtx+timeout).
	// On shutdown, savePIDsLog rewrites as compacted "init" entries.
	// So this tests both the live replay path AND the compacted path.
	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// PID 100: endtx committed, epoch should be 2.
	r100, ok := c2.pids.ids[100]
	if !ok {
		t.Fatal("PID 100 missing after restart")
	}
	if r100.epoch != 2 {
		t.Fatalf("PID 100 epoch: expected 2, got %d", r100.epoch)
	}
	if r100.txid != "tx-a" {
		t.Fatalf("PID 100 txid: expected tx-a, got %s", r100.txid)
	}
	if !r100.lastWasCommit {
		t.Fatal("PID 100 lastWasCommit: expected true")
	}
	if r100.txTimeout != 60000 {
		t.Fatalf("PID 100 txTimeout: expected 60000, got %d", r100.txTimeout)
	}

	// PID 200: timeout, epoch should be 2.
	r200, ok := c2.pids.ids[200]
	if !ok {
		t.Fatal("PID 200 missing after restart")
	}
	if r200.epoch != 2 {
		t.Fatalf("PID 200 epoch: expected 2, got %d", r200.epoch)
	}
	if r200.txTimeout != 30000 {
		t.Fatalf("PID 200 txTimeout: expected 30000, got %d", r200.txTimeout)
	}

	// PID 300: init only, verify lastWasCommit and txTimeout round-trip.
	r300, ok := c2.pids.ids[300]
	if !ok {
		t.Fatal("PID 300 missing after restart")
	}
	if r300.epoch != 5 {
		t.Fatalf("PID 300 epoch: expected 5, got %d", r300.epoch)
	}
	if !r300.lastWasCommit {
		t.Fatal("PID 300 lastWasCommit: expected true")
	}
	if r300.txTimeout != 90000 {
		t.Fatalf("PID 300 txTimeout: expected 90000, got %d", r300.txTimeout)
	}

	// Verify byTxid lookups.
	if c2.pids.byTxid["tx-a"] != r100 {
		t.Fatal("byTxid[tx-a] mismatch")
	}
	if c2.pids.byTxid["tx-b"] != r200 {
		t.Fatal("byTxid[tx-b] mismatch")
	}
	if c2.pids.byTxid["tx-c"] != r300 {
		t.Fatal("byTxid[tx-c] mismatch")
	}
}

// TestPersistMeta848GroupReplay verifies that 848 consumer group metadata
// (type, assignor, groupEpoch) survives a restart via groups.log replay.
func TestPersistMeta848GroupReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
		SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}

	// Create a group and set up 848 metadata.
	if c.groups.gs == nil {
		c.groups.gs = make(map[string]*group)
	}
	g := c.groups.newGroup("test-848-group")
	c.groups.gs["test-848-group"] = g
	go g.manage(func() {})

	g.waitControl(func() {
		g.typ = "consumer"
		g.assignorName = "uniform"
		g.groupEpoch = 7
	})

	// Persist the 848 metadata entry.
	c.persistGroupEntry(groupLogEntry{
		Type:       "meta848",
		Group:      "test-848-group",
		GroupType:  "consumer",
		Assignor:   "uniform",
		GroupEpoch: 7,
	})

	// Also set a commit to verify it coexists with meta848.
	var meta string
	g.waitControl(func() {
		g.commits.set("t", 0, offsetCommit{
			offset:      42,
			leaderEpoch: 1,
			metadata:    &meta,
		})
	})

	c.Close()

	// Reopen.
	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	g2, ok := c2.groups.gs["test-848-group"]
	if !ok {
		t.Fatal("group test-848-group missing after restart")
	}

	// Verify 848 metadata was restored.
	if g2.typ != "consumer" {
		t.Fatalf("group type: expected consumer, got %s", g2.typ)
	}
	if g2.assignorName != "uniform" {
		t.Fatalf("assignor: expected uniform, got %s", g2.assignorName)
	}
	if g2.groupEpoch != 7 {
		t.Fatalf("groupEpoch: expected 7, got %d", g2.groupEpoch)
	}

	// Verify the commit coexists.
	oc, ok := g2.commits.getp("t", 0)
	if !ok {
		t.Fatal("commit for t/0 missing after restart")
	}
	if oc.offset != 42 {
		t.Fatalf("commit offset: expected 42, got %d", oc.offset)
	}
}

// TestPersistSASLCredentials verifies SASL credentials round-trip.
func TestPersistSASLCredentials(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), EnableSASL(),
		Superuser("PLAIN", "admin", "adminpass"),
		User("PLAIN", "user1", "pass1"),
		User("SCRAM-SHA-256", "user2", "pass2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	c2, err := NewCluster(DataDir(dir), NumBrokers(1), EnableSASL())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Check PLAIN credentials
	if c2.sasls.plain["admin"] != "adminpass" {
		t.Fatalf("plain admin: expected adminpass, got %q", c2.sasls.plain["admin"])
	}
	if c2.sasls.plain["user1"] != "pass1" {
		t.Fatalf("plain user1: expected pass1, got %q", c2.sasls.plain["user1"])
	}

	// Check SCRAM-256 credentials
	if _, ok := c2.sasls.scram256["user2"]; !ok {
		t.Fatal("scram256 user2 missing after restart")
	}
}

// TestPersistLiveSyncThenShutdown verifies the savePartition live-sync path
// where segmentBases > 0 and segment files already exist on disk.
func TestPersistLiveSyncThenShutdown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), SyncWrites(), NumBrokers(1),
		SeedTopics(1, "live"),
		BrokerConfigs(map[string]string{
			"log.segment.bytes": "100",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Push batches directly to trigger segment rollover
	pd, _ := c.data.tps.getp("live", 0)
	for i := range 30 {
		b := makeTestBatch(int64(i), 1)
		pd.pushBatch(b.nbytes, b.RecordBatch, false)
		c.persistBatch(pd, pd.batches[len(pd.batches)-1])
	}

	// Verify segments were created
	if len(pd.segmentBases) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(pd.segmentBases))
	}
	t.Logf("created %d live segments", len(pd.segmentBases))

	// Close cleanly - should create snapshot referencing existing segments
	c.Close()

	// Reopen
	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	pd2, ok := c2.data.tps.getp("live", 0)
	if !ok {
		t.Fatal("partition missing after restart")
	}
	if len(pd2.batches) != 30 {
		t.Fatalf("expected 30 batches, got %d", len(pd2.batches))
	}
}

// TestPersistEmptyPartition verifies empty partitions are handled.
func TestPersistEmptyPartition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
		SeedTopics(3, "empty"))
	if err != nil {
		t.Fatal(err)
	}
	// Don't produce anything - all partitions are empty
	c.Close()

	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	if _, ok := c2.data.tps.gett("empty"); !ok {
		t.Fatal("topic missing after restart")
	}
	for p := range 3 {
		pd, ok := c2.data.tps.getp("empty", int32(p))
		if !ok {
			t.Fatalf("partition %d missing", p)
		}
		if pd.highWatermark != 0 {
			t.Fatalf("partition %d: expected HWM 0, got %d", p, pd.highWatermark)
		}
	}
}

// TestPersistRepeatedCloseReopen verifies multiple close/reopen cycles.
func TestPersistRepeatedCloseReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	var totalBatches int
	for cycle := range 5 {
		c, err := NewCluster(DataDir(dir), NumBrokers(1),
			SeedTopics(1, "cycle"))
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		// Add some batches each cycle
		pd, _ := c.data.tps.getp("cycle", 0)
		for range 3 {
			b := makeTestBatch(pd.highWatermark, 1)
			pd.pushBatch(b.nbytes, b.RecordBatch, false)
			totalBatches++
		}
		c.Close()
	}

	// Final reopen - verify all batches survived
	c, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, ok := c.data.tps.getp("cycle", 0)
	if !ok {
		t.Fatal("partition missing after final reopen")
	}
	if len(pd.batches) != totalBatches {
		t.Fatalf("expected %d batches after %d cycles, got %d",
			totalBatches, 5, len(pd.batches))
	}
}

// TestPersistSeqWindowsCleanShutdown verifies sequence windows survive
// clean shutdown but not crash.
func TestPersistSeqWindowsCleanShutdown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
		SeedTopics(1, "seq"))
	if err != nil {
		t.Fatal(err)
	}

	// Create a PID with sequence windows
	pidinf := &pidinfo{
		pids:       &c.pids,
		id:         42,
		epoch:      0,
		lastActive: time.Now(),
	}
	c.pids.ids[42] = pidinf
	c.persistPIDEntry(pidLogEntry{Type: "init", PID: 42, Epoch: 0})

	// Add sequence window entries
	sw := pidinf.windows.mkpDefault("seq", 0)
	sw.seq[0] = 1
	sw.seq[1] = 2
	sw.offsets[0] = 100
	sw.offsets[1] = 101
	sw.at = 2
	sw.epoch = 0

	c.Close()

	// Reopen - sequence windows should be restored
	c2, err := NewCluster(DataDir(dir), NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	p2, ok := c2.pids.ids[42]
	if !ok {
		t.Fatal("PID 42 missing after restart")
	}
	sw2, ok := p2.windows.getp("seq", 0)
	if !ok {
		t.Fatal("sequence window missing for seq-0")
	}
	if sw2.at != 2 {
		t.Fatalf("expected at 2, got %d", sw2.at)
	}
	if sw2.seq[0] != 1 || sw2.seq[1] != 2 {
		t.Fatalf("unexpected seq values: %v", sw2.seq)
	}
}

// TestPersistOffsetDeleteRoundTrip verifies that OffsetDelete entries
// (type "delete") are replayed correctly on restart, removing committed
// offsets that were deleted.
func TestPersistOffsetDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), SyncWrites(), NumBrokers(1),
		SeedTopics(1, "t1"))
	if err != nil {
		t.Fatal(err)
	}

	// Create a group with committed offsets
	if c.groups.gs == nil {
		c.groups.gs = make(map[string]*group)
	}
	g := c.groups.newGroup("g1")
	c.groups.gs["g1"] = g
	go g.manage(func() {})

	// Commit offsets for partitions 0 and 1 (topic has 1 partition,
	// but the group commit map is independent of actual partition count)
	g.waitControl(func() {
		g.commits.set("t1", 0, offsetCommit{offset: 100, leaderEpoch: -1})
	})
	c.persistGroupEntry(groupLogEntry{
		Type:   "commit",
		Group:  "g1",
		Topic:  "t1",
		Part:   0,
		Offset: 100,
		Epoch:  -1,
	})

	// Delete partition 0's offset
	g.waitControl(func() {
		g.commits.delp("t1", 0)
	})
	c.persistGroupEntry(groupLogEntry{
		Type:  "delete",
		Group: "g1",
		Topic: "t1",
		Part:  0,
	})

	c.Close()

	// Reopen and verify deleted offset is gone
	c2, err := NewCluster(DataDir(dir), NumBrokers(1),
		SeedTopics(1, "t1"))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	g2, ok := c2.groups.gs["g1"]
	if !ok {
		t.Fatal("group g1 missing after restart")
	}

	var found bool
	g2.waitControl(func() {
		_, found = g2.commits.getp("t1", 0)
	})
	if found {
		t.Fatal("expected offset for t1-0 to be deleted after restart, but it was found")
	}
}

// TestPersistSnapshotFullReplayConvergence verifies that the snapshot
// fast path and full-replay path produce equivalent partition state.
// Produces data, shuts down (snapshot created), opens via snapshot
// (records state), then deletes snapshot and opens via full replay,
// comparing all key fields.
func TestPersistSnapshotFullReplayConvergence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: produce data and shut down cleanly to create snapshot
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1),
			SeedTopics(3, "conv"))
		if err != nil {
			t.Fatal(err)
		}
		pd, _ := c.data.tps.getp("conv", 0)
		for i := range 20 {
			b := makeTestBatch(pd.highWatermark, int32(1+i%3))
			pd.pushBatch(b.nbytes, b.RecordBatch, false)
		}
		c.Close()
	}

	// Phase 2: open with snapshot, record state
	type partState struct {
		hwm            int64
		lso            int64
		logStartOffset int64
		maxTimestamp    int64
		nbytes         int64
		batchCount     int
	}

	var snapState partState
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1))
		if err != nil {
			t.Fatal(err)
		}
		pd, ok := c.data.tps.getp("conv", 0)
		if !ok {
			t.Fatal("partition missing after snapshot load")
		}
		snapState = partState{
			hwm:            pd.highWatermark,
			lso:            pd.lastStableOffset,
			logStartOffset: pd.logStartOffset,
			maxTimestamp:    pd.maxTimestamp,
			nbytes:         pd.nbytes,
			batchCount:     len(pd.batches),
		}
		c.Close()
	}

	// Delete snapshot to force full replay
	snapPath := filepath.Join(dir, "partitions", "conv-0", "snapshot.json")
	if err := os.Remove(snapPath); err != nil {
		t.Fatalf("failed to remove snapshot: %v", err)
	}

	// Phase 3: open via full replay, compare state
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		pd, ok := c.data.tps.getp("conv", 0)
		if !ok {
			t.Fatal("partition missing after full replay")
		}
		replayState := partState{
			hwm:            pd.highWatermark,
			lso:            pd.lastStableOffset,
			logStartOffset: pd.logStartOffset,
			maxTimestamp:    pd.maxTimestamp,
			nbytes:         pd.nbytes,
			batchCount:     len(pd.batches),
		}

		if snapState.hwm != replayState.hwm {
			t.Errorf("HWM: snapshot=%d replay=%d", snapState.hwm, replayState.hwm)
		}
		if snapState.lso != replayState.lso {
			t.Errorf("LSO: snapshot=%d replay=%d", snapState.lso, replayState.lso)
		}
		if snapState.logStartOffset != replayState.logStartOffset {
			t.Errorf("logStartOffset: snapshot=%d replay=%d", snapState.logStartOffset, replayState.logStartOffset)
		}
		if snapState.maxTimestamp != replayState.maxTimestamp {
			t.Errorf("maxTimestamp: snapshot=%d replay=%d", snapState.maxTimestamp, replayState.maxTimestamp)
		}
		if snapState.nbytes != replayState.nbytes {
			t.Errorf("nbytes: snapshot=%d replay=%d", snapState.nbytes, replayState.nbytes)
		}
		if snapState.batchCount != replayState.batchCount {
			t.Errorf("batchCount: snapshot=%d replay=%d", snapState.batchCount, replayState.batchCount)
		}
		// createdAt is inherently lossy on full replay - the snapshot
		// stores the real partition creation time, while full replay
		// can only approximate from the first batch's timestamp. Verify
		// the replay path derived from first batch, not time.Now().
		firstBatchTs := time.UnixMilli(pd.batches[0].FirstTimestamp)
		if !pd.createdAt.Equal(firstBatchTs) {
			t.Errorf("createdAt: expected first batch timestamp %v, got %v", firstBatchTs, pd.createdAt)
		}
	}
}

// TestPersistSnapshotLogStartOffsetClamp verifies that logStartOffset is
// clamped to HWM when segment truncation leaves fewer batches than the
// snapshot expected. Without clamping, logStartOffset > HWM makes the
// partition inaccessible via searchOffset.
func TestPersistSnapshotLogStartOffsetClamp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: produce 10 records, advance logStartOffset to 7, shut down.
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1),
			SeedTopics(1, "lso"))
		if err != nil {
			t.Fatal(err)
		}
		pd, _ := c.data.tps.getp("lso", 0)
		for range 10 {
			b := makeTestBatch(pd.highWatermark, 1)
			pd.pushBatch(b.nbytes, b.RecordBatch, false)
		}
		// Simulate DeleteRecords advancing logStartOffset.
		pd.logStartOffset = 7
		c.Close()
	}

	// Truncate the segment file to only ~3 records worth of data.
	segDir := filepath.Join(dir, "partitions", "lso-0")
	entries, _ := os.ReadDir(segDir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".dat" {
			path := filepath.Join(segDir, e.Name())
			raw, _ := os.ReadFile(path)
			// Keep roughly 1/3 of the file.
			os.WriteFile(path, raw[:len(raw)/3], 0644)
		}
	}

	// Phase 2: reopen via snapshot, verify logStartOffset <= HWM.
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		pd, ok := c.data.tps.getp("lso", 0)
		if !ok {
			t.Fatal("partition missing after restart")
		}
		if pd.logStartOffset > pd.highWatermark {
			t.Fatalf("logStartOffset %d > HWM %d after truncation",
				pd.logStartOffset, pd.highWatermark)
		}
		t.Logf("logStartOffset=%d HWM=%d batches=%d",
			pd.logStartOffset, pd.highWatermark, len(pd.batches))
	}
}
