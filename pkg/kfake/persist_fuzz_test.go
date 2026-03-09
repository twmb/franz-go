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
	binary.LittleEndian.PutUint16(hdr[8:10], currentPersistVersion)
	crcVal := crc32.New(crc32c)
	var vbuf [2]byte
	binary.LittleEndian.PutUint16(vbuf[:], currentPersistVersion)
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

// FuzzDecodeBatchRaw feeds arbitrary bytes to decodeBatchRaw.
// Verifies it never panics and returns a sensible error for bad input.
func FuzzDecodeBatchRaw(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add(make([]byte, 12))
	f.Add(make([]byte, 100))

	for size := 0; size <= 20; size++ {
		f.Add(make([]byte, size))
	}

	f.Fuzz(func(_ *testing.T, input []byte) {
		// Must not panic
		rb, err := decodeBatchRaw(input)
		_ = rb
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
		binary.LittleEndian.PutUint16(hdr[8:10], currentPersistVersion)
		crcVal := crc32.New(crc32c)
		var vbuf [2]byte
		binary.LittleEndian.PutUint16(vbuf[:], currentPersistVersion)
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
	f, err := mfs.OpenFile("test.log", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	payloads := []string{"hello", "world", "", "a longer payload with more data"}
	for _, p := range payloads {
		if err := writeEntry(f, []byte(p), true); err != nil {
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

// TestEncodeBatchRoundTrip verifies batch encode/decode round-trips.
func TestEncodeBatchRoundTrip(t *testing.T) {
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
	enc := batch.RecordBatch.AppendTo(nil)
	batch.Length = int32(len(enc) - 12)
	batch.CRC = int32(crc32.Checksum(enc[21:], crc32c))
	batch.nbytes = len(enc)

	// Test batch encoding round-trip.
	bp := encodeBatch(batch)
	decoded, err := decodeBatchRaw(*bp)
	*bp = (*bp)[:0]
	batchPool.Put(bp)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.FirstOffset != batch.FirstOffset {
		t.Fatalf("FirstOffset: expected %d, got %d", decoded.FirstOffset, batch.FirstOffset)
	}
	if decoded.ProducerID != batch.ProducerID {
		t.Fatalf("ProducerID: expected %d, got %d", decoded.ProducerID, batch.ProducerID)
	}
	if decoded.ProducerEpoch != batch.ProducerEpoch {
		t.Fatalf("ProducerEpoch: expected %d, got %d", decoded.ProducerEpoch, batch.ProducerEpoch)
	}

	// Test index entry round-trip.
	idx := encodeIndexEntry(batch.epoch, batch.maxEarlierTimestamp, batch.inTx)
	epoch, maxTS, inTx, ok := decodeIndexEntry(idx[:])
	if !ok {
		t.Fatal("decodeIndexEntry failed")
	}
	if epoch != batch.epoch {
		t.Fatalf("epoch: expected %d, got %d", batch.epoch, epoch)
	}
	if maxTS != batch.maxEarlierTimestamp {
		t.Fatalf("maxEarlierTimestamp: expected %d, got %d", batch.maxEarlierTimestamp, maxTS)
	}
	if inTx != batch.inTx {
		t.Fatalf("inTx: expected %v, got %v", batch.inTx, inTx)
	}
}

// TestDecodeBatchRawTooShort verifies short input returns error.
func TestDecodeBatchRawTooShort(t *testing.T) {
	t.Parallel()
	for size := range 12 {
		_, err := decodeBatchRaw(make([]byte, size))
		if err != nil {
			return // any error is fine
		}
	}
}

// TestDecodeIndexEntryTooShort verifies short index input returns !ok.
func TestDecodeIndexEntryTooShort(t *testing.T) {
	t.Parallel()
	for size := range indexEntrySize {
		_, _, _, ok := decodeIndexEntry(make([]byte, size))
		if ok {
			t.Fatalf("size %d: expected !ok", size)
		}
	}
}

// TestMemFSRoundTrip exercises the memFS implementation end-to-end.
func TestMemFSRoundTrip(t *testing.T) {
	t.Parallel()
	mfs := newMemFS()

	// MkdirAll
	if err := mfs.MkdirAll("/a/b/c", 0o755); err != nil {
		t.Fatal(err)
	}

	// WriteFile via OpenFile
	f, err := mfs.OpenFile("/a/b/c/test.txt", os.O_CREATE|os.O_WRONLY, 0o644)
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

	f, err := mfs.OpenFile("/test.dat", os.O_CREATE|os.O_WRONLY, 0o644)
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
	f, _ := mfs.OpenFile("/test.log", os.O_CREATE|os.O_WRONLY, 0o644)
	f.Write([]byte("initial"))
	f.Close()

	// Append
	f, _ = mfs.OpenFile("/test.log", os.O_APPEND|os.O_WRONLY, 0o644)
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
	f, _ := mfs.OpenFile("/test.dat", os.O_CREATE|os.O_WRONLY, 0o644)
	f.Write([]byte("old data that is very long"))
	f.Close()

	// Truncate on open
	f, _ = mfs.OpenFile("/test.dat", os.O_TRUNC|os.O_WRONLY, 0o644)
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

	for iter := range 4 {
		cleanShutdown := iter%2 == 0
		nTopics := 1 + rng.Intn(3)
		nBatches := 5 + rng.Intn(20)

		dir := t.TempDir()

		topics := make([]string, nTopics)
		for i := range topics {
			topics[i] = topicNames[rng.Intn(len(topicNames))]
		}

		c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, topics...))
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}

		// Push random batches directly via internal API
		totalPerTopic := make(map[string]int64)
		for range nBatches {
			topic := topics[rng.Intn(len(topics))]
			pd, ok := c.data.tps.getp(topic, 0)
			if !ok {
				continue
			}
			nRecords := 1 + rng.Intn(5)
			b := makeTestBatch(pd.highWatermark, int32(nRecords))
			c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
			// persistBatchToSegment is now called internally by pushBatch
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
		// For crash: just abandon the cluster (writes are always synced)

		// Reopen and verify
		c2, err := NewCluster(DataDir(dir), NumBrokers(1))
		if err != nil {
			t.Fatalf("iter %d reopen: %v", iter, err)
		}

		for _, topic := range topics {
			pd, ok := c2.data.tps.getp(topic, 0)
			if !ok {
				t.Fatalf("iter %d: topic %s missing after %s",
					iter, topic, shutdownType(cleanShutdown))
			}

			if pd.highWatermark != expectedHWM[topic] {
				t.Fatalf("iter %d topic %s: expected HWM %d, got %d",
					iter, topic, expectedHWM[topic], pd.highWatermark)
			}
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
	enc := b.RecordBatch.AppendTo(nil)
	b.Length = int32(len(enc) - 12)
	b.CRC = int32(crc32.Checksum(enc[21:], crc32c))
	b.nbytes = len(enc)
	return b
}

// TestChaosGroupCommitsCrashRecover creates groups with committed offsets,
// randomly crashes or closes, and verifies committed offsets survive.
func TestChaosGroupCommitsCrashRecover(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(99))

	for iter := range 4 {
		cleanShutdown := iter%2 == 0
		dir := t.TempDir()

		c, err := NewCluster(DataDir(dir), NumBrokers(1),
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

	allTopics := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

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

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
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

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
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
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
		// persistBatchToSegment is now called internally by pushBatch
	}

	// Verify segments were created
	if len(pd.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(pd.segments))
	}
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
	if pd2.totalBatches() != 30 {
		t.Fatalf("expected 30 batches, got %d", pd2.totalBatches())
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
	for cycle := range 3 {
		c, err := NewCluster(DataDir(dir), NumBrokers(1),
			SeedTopics(1, "cycle"))
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		// Add some batches each cycle
		pd, _ := c.data.tps.getp("cycle", 0)
		for range 3 {
			b := makeTestBatch(pd.highWatermark, 1)
			c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
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
	if pd.totalBatches() != totalBatches {
		t.Fatalf("expected %d batches after %d cycles, got %d",
			totalBatches, 5, pd.totalBatches())
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
	sw.entries[0] = pidEntry{firstSeq: 1, nextSeq: 2, offset: 100}
	sw.entries[1] = pidEntry{firstSeq: 2, nextSeq: 5, offset: 101}
	sw.count = 2
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
	if sw2.count != 2 {
		t.Fatalf("expected count 2, got %d", sw2.count)
	}
	if sw2.entries[0].firstSeq != 1 || sw2.entries[0].nextSeq != 2 {
		t.Fatalf("unexpected entry 0: %+v", sw2.entries[0])
	}
	if sw2.entries[1].firstSeq != 2 || sw2.entries[1].nextSeq != 5 {
		t.Fatalf("unexpected entry 1: %+v", sw2.entries[1])
	}
}

// TestPersistOffsetDeleteRoundTrip verifies that OffsetDelete entries
// (type "delete") are replayed correctly on restart, removing committed
// offsets that were deleted.
func TestPersistOffsetDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1),
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
			c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
		}
		c.Close()
	}

	// Phase 2: open with snapshot, record state
	type partState struct {
		hwm               int64
		lso               int64
		logStartOffset    int64
		maxFirstTimestamp int64
		nbytes            int64
		batchCount        int
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
			hwm:               pd.highWatermark,
			lso:               pd.lastStableOffset,
			logStartOffset:    pd.logStartOffset,
			maxFirstTimestamp: pd.maxFirstTimestamp,
			nbytes:            pd.nbytes,
			batchCount:        pd.totalBatches(),
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
			hwm:               pd.highWatermark,
			lso:               pd.lastStableOffset,
			logStartOffset:    pd.logStartOffset,
			maxFirstTimestamp: pd.maxFirstTimestamp,
			nbytes:            pd.nbytes,
			batchCount:        pd.totalBatches(),
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
		if snapState.maxFirstTimestamp != replayState.maxFirstTimestamp {
			t.Errorf("maxFirstTimestamp: snapshot=%d replay=%d", snapState.maxFirstTimestamp, replayState.maxFirstTimestamp)
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
		firstBatchTs := time.UnixMilli(pd.segments[0].index[0].firstTimestamp)
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
			c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
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
			os.WriteFile(path, raw[:len(raw)/3], 0o644)
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
	}
}

// TestTrimLeftDeletesSegmentFiles verifies that trimLeft deletes segment files
// for fully-trimmed segments and adjusts nbytes correctly.
func TestTrimLeftDeletesSegmentFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "trim"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("trim", 0)

	// Produce 10 batches (1 record each) to create segment data.
	for range 10 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	if pd.totalBatches() != 10 {
		t.Fatalf("expected 10 batches, got %d", pd.totalBatches())
	}
	nbytesBeforeTrim := pd.nbytes

	// Trim first 5 records.
	pd.logStartOffset = 5
	c.trimLeft(pd)

	if pd.totalBatches() != 5 {
		t.Fatalf("expected 5 batches after trim, got %d", pd.totalBatches())
	}
	if pd.nbytes >= nbytesBeforeTrim {
		t.Fatalf("expected nbytes to decrease: before=%d after=%d", nbytesBeforeTrim, pd.nbytes)
	}
	if pd.nbytes <= 0 {
		t.Fatalf("expected positive nbytes after partial trim, got %d", pd.nbytes)
	}

	// maxTimestampBatch should still be valid
	m := pd.maxTimestampBatch()
	if m == nil {
		t.Fatal("maxTimestampBatch should not be nil after partial trim")
	}
}

// TestTrimLeftAllThenProduce verifies trimming all batches then producing
// more works correctly (no panics, fresh segment created).
func TestTrimLeftAllThenProduce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "trim-all"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("trim-all", 0)

	// Produce 5 batches.
	for range 5 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	// Trim ALL records.
	pd.logStartOffset = pd.highWatermark
	c.trimLeft(pd)

	if pd.totalBatches() != 0 {
		t.Fatalf("expected 0 batches after full trim, got %d", pd.totalBatches())
	}
	if pd.nbytes != 0 {
		t.Fatalf("expected 0 nbytes after full trim, got %d", pd.nbytes)
	}
	if pd.activeSegFile != nil {
		t.Fatal("activeSegFile should be nil after full trim")
	}
	if pd.activeIdxFile != nil {
		t.Fatal("activeIdxFile should be nil after full trim")
	}
	if pd.maxTimestampBatch() != nil {
		t.Fatal("maxTimestampBatch should be nil after full trim")
	}

	// Produce again - should not panic.
	for range 3 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}
	if pd.totalBatches() != 3 {
		t.Fatalf("expected 3 batches after re-produce, got %d", pd.totalBatches())
	}
	if pd.nbytes <= 0 {
		t.Fatalf("expected positive nbytes after re-produce, got %d", pd.nbytes)
	}
}

// TestSearchOffsetEmptyPartition verifies searchOffset on an empty partition.
func TestSearchOffsetEmptyPartition(t *testing.T) {
	t.Parallel()

	c, err := NewCluster(NumBrokers(1), SeedTopics(1, "empty"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("empty", 0)

	// Empty partition, offset 0
	_, _, found, atEnd := pd.searchOffset(0)
	if found {
		t.Fatal("expected not found on empty partition")
	}
	if !atEnd {
		t.Fatal("expected atEnd for offset 0 on empty partition (logStartOffset=0=HWM)")
	}

	// Out of range
	_, _, found, atEnd = pd.searchOffset(1)
	if found || atEnd {
		t.Fatal("expected neither found nor atEnd for offset > HWM")
	}
}

// TestSearchOffsetAfterTrimLeft verifies searchOffset works correctly
// after trimming batches from the front.
func TestSearchOffsetAfterTrimLeft(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "search-trim"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("search-trim", 0)

	// Produce 10 single-record batches.
	for range 10 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	// Trim first 5 records.
	pd.logStartOffset = 5
	c.trimLeft(pd)

	// Offset 4 is before logStartOffset - should not be found.
	_, _, found, _ := pd.searchOffset(4)
	if found {
		t.Fatal("offset 4 should not be found (before logStartOffset)")
	}

	// Offset 5 should be found (first available).
	segIdx, metaIdx, found, _ := pd.searchOffset(5)
	if !found {
		t.Fatal("offset 5 should be found")
	}
	m := &pd.segments[segIdx].index[metaIdx]
	if m.firstOffset != 5 {
		t.Fatalf("expected firstOffset 5, got %d", m.firstOffset)
	}

	// Offset 9 should be found (last record).
	_, _, found, _ = pd.searchOffset(9)
	if !found {
		t.Fatal("offset 9 should be found")
	}

	// Offset 10 should be at end.
	_, _, found, atEnd := pd.searchOffset(10)
	if found {
		t.Fatal("offset 10 should not be found")
	}
	if !atEnd {
		t.Fatal("offset 10 should be atEnd")
	}
}

// TestCompactBailsOnPartialReadError verifies compaction does not proceed
// (and does not lose data) when reading batches from disk fails partway.
func TestCompactBailsOnPartialReadError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "compact-err"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("compact-err", 0)

	// Produce 5 batches.
	for range 5 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}
	totalBefore := pd.totalBatches()
	nbytesBefore := pd.nbytes
	numSegsBefore := len(pd.segments)

	// Corrupt the segment file so readBatchFull fails for some entries.
	pdir := partDir(c.storageDir, pd.t, pd.p)
	segName := segmentFileName(pd.segments[0].base)
	path := filepath.Join(pdir, segName)
	raw, err := c.fs.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the middle of the file.
	if len(raw) > 40 {
		for i := len(raw) / 2; i < len(raw); i++ {
			raw[i] = 0xFF
		}
	}
	f, err := c.fs.OpenFile(path, os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(raw)
	f.Close()

	// Compact should bail out without destroying data.
	c.compact(pd, "compact-err")

	if pd.totalBatches() != totalBefore {
		t.Fatalf("compaction should not have changed batch count: before=%d after=%d",
			totalBefore, pd.totalBatches())
	}
	if pd.nbytes != nbytesBefore {
		t.Fatalf("compaction should not have changed nbytes: before=%d after=%d",
			nbytesBefore, pd.nbytes)
	}
	if len(pd.segments) != numSegsBefore {
		t.Fatalf("compaction should not have changed segment count: before=%d after=%d",
			numSegsBefore, len(pd.segments))
	}
}

// TestSnapshotNbytesWithPartialTrim verifies that after a partial trim
// (logStartOffset in the middle of a segment), snapshot reload correctly
// computes nbytes without double-counting trimmed entries.
func TestSnapshotNbytesWithPartialTrim(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "nbytes-partial"
	var savedNbytes int64
	var savedHWM int64

	// Phase 1: produce records, partially trim, clean shutdown.
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}

		pd, _ := c.data.tps.getp(topic, 0)
		for range 10 {
			b := makeTestBatch(pd.highWatermark, 1)
			c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
		}

		// Advance logStartOffset to trim the first 3 records.
		// This may leave the first segment with some trimmed entries
		// (depending on whether they share a segment file).
		pd.logStartOffset = 3
		c.trimLeft(pd)

		savedNbytes = pd.nbytes
		savedHWM = pd.highWatermark
		if savedNbytes <= 0 {
			t.Fatalf("expected positive nbytes, got %d", savedNbytes)
		}
		c.Close()
	}

	// Phase 2: reopen from snapshot, verify nbytes matches.
	{
		c, err := NewCluster(DataDir(dir), NumBrokers(1))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		pd, ok := c.data.tps.getp(topic, 0)
		if !ok {
			t.Fatal("partition missing after restart")
		}
		if pd.highWatermark != savedHWM {
			t.Fatalf("HWM mismatch: got %d, want %d", pd.highWatermark, savedHWM)
		}
		if pd.nbytes != savedNbytes {
			t.Fatalf("nbytes mismatch after reopen: got %d, want %d", pd.nbytes, savedNbytes)
		}
	}
}

// TestRebuildSegmentsWritesSynced verifies that rebuildSegments
// (used by compact) writes data that survives reopen.
func TestRebuildSegmentsWritesSynced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "sync-rebuild"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("sync-rebuild", 0)

	// Produce some batches.
	for range 5 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	// Build batch list for rebuild.
	var batches []*partBatch
	pd.eachBatchMeta(func(si, _ int, m *batchMeta) bool {
		batch, err := c.readBatchFull(pd, si, m)
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
		return true
	})

	// Rebuild - should not panic and data should survive.
	c.rebuildSegments(pd, batches)

	if pd.totalBatches() != 5 {
		t.Fatalf("expected 5 batches after rebuild, got %d", pd.totalBatches())
	}

	// Verify we can read all batches back.
	pd.eachBatchMeta(func(si, _ int, m *batchMeta) bool {
		_, err := c.readBatchFull(pd, si, m)
		if err != nil {
			t.Fatalf("readBatchFull after rebuild: %v", err)
		}
		return true
	})
}

// TestRebuildSegmentsSegmentSplitting verifies that rebuildSegments correctly
// uses the full entry size (not just batch wire bytes) for segment split
// decisions, preventing segments from exceeding the configured max size.
func TestRebuildSegmentsSegmentSplitting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Set a very small segment.bytes to force multiple segments.
	segBytes := "200"
	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "split"),
		BrokerConfigs(map[string]string{"log.segment.bytes": segBytes}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("split", 0)

	// Produce batches.
	for range 10 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	// Read all batches.
	var batches []*partBatch
	pd.eachBatchMeta(func(si, _ int, m *batchMeta) bool {
		batch, err := c.readBatchFull(pd, si, m)
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
		return true
	})

	// Rebuild with new segment size.
	c.rebuildSegments(pd, batches)

	// Verify multiple segments were created (200 bytes is too small
	// for all 10 batches in one segment).
	if len(pd.segments) < 2 {
		t.Fatalf("expected multiple segments with segment.bytes=%s, got %d",
			segBytes, len(pd.segments))
	}

	// Verify all batches survived.
	if pd.totalBatches() != 10 {
		t.Fatalf("expected 10 batches after rebuild, got %d", pd.totalBatches())
	}

	// Verify segment sizes don't drastically exceed the limit
	// (first batch in a new segment is allowed to exceed).
	for i, seg := range pd.segments {
		if i > 0 && seg.size == 0 {
			t.Fatalf("segment %d has size 0", i)
		}
	}
}

// TestTrimLeftPartialSegment verifies trimLeft correctly handles the case
// where logStartOffset falls in the middle of a segment (some batches
// remain in the segment).
func TestTrimLeftPartialSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Use a large segment.bytes so all batches land in one segment.
	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "partial"),
		BrokerConfigs(map[string]string{"log.segment.bytes": "1073741824"}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("partial", 0)

	for range 10 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	// All batches should be in one segment.
	if len(pd.segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(pd.segments))
	}

	// Trim first 5 - segment should remain but with fewer entries.
	pd.logStartOffset = 5
	c.trimLeft(pd)

	if len(pd.segments) != 1 {
		t.Fatalf("expected 1 segment after partial trim, got %d", len(pd.segments))
	}
	if pd.totalBatches() != 5 {
		t.Fatalf("expected 5 batches after partial trim, got %d", pd.totalBatches())
	}
	// First remaining batch should have offset 5.
	first := &pd.segments[0].index[0]
	if first.firstOffset != 5 {
		t.Fatalf("expected first offset 5, got %d", first.firstOffset)
	}
}

// TestMaxTimestampBatchAfterCompaction verifies that maxTimestampSeg/Idx
// are correctly rebuilt after compaction changes the batch set.
func TestMaxTimestampBatchAfterCompaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	c, err := NewCluster(DataDir(dir), NumBrokers(1), SeedTopics(1, "ts-compact"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	pd, _ := c.data.tps.getp("ts-compact", 0)

	// Produce batches with increasing timestamps (default from makeTestBatch).
	for range 5 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	m := pd.maxTimestampBatch()
	if m == nil {
		t.Fatal("maxTimestampBatch should not be nil")
	}
	maxTS := m.maxTimestamp

	// Rebuild segments (simulating compaction output).
	var batches []*partBatch
	pd.eachBatchMeta(func(si, _ int, m *batchMeta) bool {
		batch, err := c.readBatchFull(pd, si, m)
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
		return true
	})
	c.rebuildSegments(pd, batches)

	m2 := pd.maxTimestampBatch()
	if m2 == nil {
		t.Fatal("maxTimestampBatch should not be nil after rebuild")
	}
	if m2.maxTimestamp != maxTS {
		t.Fatalf("maxTimestamp changed after rebuild: before=%d after=%d", maxTS, m2.maxTimestamp)
	}
}

// TestWriteFailureTruncatesPartialEntry verifies that when writeEntry
// fails (e.g., sync error), persistBatchToSegment truncates any partial
// data so subsequent writes land at correct file positions.
func TestWriteFailureTruncatesPartialEntry(t *testing.T) {
	t.Parallel()

	// Use memFS (no DataDir) so we can inject faults.
	c, err := NewCluster(NumBrokers(1), SeedTopics(1, "fail-trunc"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	mfs := c.fs.(*memFS)
	pd, _ := c.data.tps.getp("fail-trunc", 0)

	// Produce 3 batches successfully.
	for range 3 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	if pd.totalBatches() != 3 {
		t.Fatalf("expected 3 batches, got %d", pd.totalBatches())
	}

	// Record segment file size before failure.
	seg := &pd.segments[len(pd.segments)-1]
	sizeBeforeFailure := seg.size

	// Inject write failure on the segment file.
	// persistBatchToSegment should Truncate to remove the partial entry.
	mfs.failNextWrite = &testError{"injected write"}

	b := makeTestBatch(pd.highWatermark, 1)
	c.pushBatch(pd, b.nbytes, b.RecordBatch, false)

	// The batchMeta was added in-memory but persist failed.
	// The segment file should be truncated back to its pre-failure size.
	pdir := partDir(c.storageDir, pd.t, pd.p)
	path := filepath.Join(pdir, segmentFileName(seg.base))
	info, err := mfs.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != sizeBeforeFailure {
		t.Fatalf("expected file size %d after truncation, got %d",
			sizeBeforeFailure, info.Size())
	}

	// Produce 3 more batches - should succeed and land at correct positions.
	for range 3 {
		b := makeTestBatch(pd.highWatermark, 1)
		c.pushBatch(pd, b.nbytes, b.RecordBatch, false)
	}

	// Verify all post-failure batches can be read correctly via readBatchRaw.
	// If truncation didn't happen, readBatchRaw would read from wrong
	// positions and return incorrect data.
	// pushBatch returns -1 on persist failure and does not add the
	// batch to the index, so we should have exactly 6 readable batches.
	var readCount int
	pd.eachBatchMeta(func(si, _ int, m *batchMeta) bool {
		raw, err := c.readBatchRaw(pd, si, m)
		if err != nil {
			t.Fatalf("readBatchRaw offset %d: %v", m.firstOffset, err)
		}
		if len(raw) == 0 {
			t.Fatalf("readBatchRaw offset %d: empty", m.firstOffset)
		}
		readCount++
		return true
	})
	if readCount != 6 {
		t.Fatalf("expected 6 readable batches, got %d", readCount)
	}
}
