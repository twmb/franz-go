package kmsg

import "testing"

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
