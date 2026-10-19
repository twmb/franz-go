package kmsg

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// TestReadFromBoundsTagCount pins that a flexible message whose tagged-field
// count its remaining bytes cannot hold fails to read instead of looping the
// count down: here an ApiVersions v3 request of two empty compact strings and
// a trailing tag count of 2^32-1, which used to spin ~4.3 billion times.
func TestReadFromBoundsTagCount(t *testing.T) {
	r := NewPtrApiVersionsRequest()
	r.SetVersion(3)
	if err := r.ReadFrom([]byte{0x01, 0x01, 0xff, 0xff, 0xff, 0xff, 0x0f}); err == nil {
		t.Fatal("ReadFrom of a tag count past the bytes left succeeded")
	}

	// Tagged fields the bytes do hold still read: a FindCoordinator v4
	// response, whose structs carry unknown tags, round trips.
	resp := NewPtrFindCoordinatorResponse()
	resp.SetVersion(4)
	c := NewFindCoordinatorResponseCoordinator()
	c.Key, c.Host, c.Port = "group", "broker", 9092
	c.UnknownTags.Set(7, []byte("seven"))
	resp.Coordinators = append(resp.Coordinators, c)
	resp.UnknownTags.Set(3, []byte("three"))
	b := resp.AppendTo(nil)
	got := NewPtrFindCoordinatorResponse()
	got.SetVersion(4)
	if err := got.ReadFrom(b); err != nil {
		t.Fatalf("ReadFrom of a message with tagged fields: %v", err)
	}
	if string(got.AppendTo(nil)) != string(b) {
		t.Fatal("a message with tagged fields does not round trip")
	}
}

// TestReadFromBoundsArrayAllocation pins that an array length the remaining
// bytes cannot hold at the elements' minimum encoded size fails to read
// before the elements are allocated: here a 16 MiB Metadata v12 request
// whose topics array claims an entry per byte, each entry at least 18 bytes
// (a topic ID, a null name and a tag count), which used to allocate 512 MiB.
func TestReadFromBoundsArrayAllocation(t *testing.T) {
	const size = 16 << 20
	b := binary.AppendUvarint(nil, uint64(size-4)+1)
	b = append(b, make([]byte, size-len(b))...)

	r := NewPtrMetadataRequest()
	r.SetVersion(12)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	err := r.ReadFrom(b)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("ReadFrom of an array length past the bytes left succeeded")
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 1<<20 {
		t.Fatalf("ReadFrom allocated %d bytes refusing the array; want under 1 MiB", alloc)
	}

	// The most entries the bytes hold read.
	n := (size - 4 - 3) / 18
	b = binary.AppendUvarint(b[:0], uint64(n)+1)
	b = append(b, make([]byte, size-len(b))...)
	r = NewPtrMetadataRequest()
	r.SetVersion(12)
	if err := r.ReadFrom(b); err != nil || len(r.Topics) != n {
		t.Fatalf("ReadFrom of %d entries the bytes hold: %d read, %v", n, len(r.Topics), err)
	}
}
