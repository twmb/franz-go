package kmsg

import (
	"bytes"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg/internal/kbin"
)

// A tag count or array length past what the input can hold must fail without
// looping through the count or allocating for the length.
func TestReadCountPastInput(t *testing.T) {
	maxCount := []byte{0xff, 0xff, 0xff, 0xff, 0x0f}

	// FetchResponse v16: throttle, error code, and session ID, then 2^20
	// topics in 2^20 bytes.
	fetch := kbin.AppendUvarint(make([]byte, 10), 1<<20+1)
	fetch = append(fetch, make([]byte, 1<<20)...)

	for _, tc := range []struct {
		name string
		read func() bool
	}{
		{"internalReadTags", func() bool {
			r := NewPtrApiVersionsRequest()
			r.Version = 3
			return r.ReadFrom(append([]byte{1, 1}, maxCount...)) == nil
		}},
		{"SkipTags", func() bool {
			b := kbin.Reader{Src: maxCount}
			SkipTags(&b)
			return b.Ok()
		}},
		{"ReadTags", func() bool {
			b := kbin.Reader{Src: maxCount}
			ReadTags(&b)
			return b.Ok()
		}},
		{"array", func() bool {
			r := NewPtrFetchResponse()
			r.Version = 16
			return r.ReadFrom(fetch) == nil
		}},
	} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		ok := tc.read()
		runtime.ReadMemStats(&after)
		if ok {
			t.Errorf("%s: read succeeded", tc.name)
		}
		if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
			t.Errorf("%s: allocated %d bytes", tc.name, n)
		}
	}
}

// AppendTo and AppendRequest return nil for a string over math.MaxInt16
// bytes, in the classic and in the flexible encoding. A string of exactly
// math.MaxInt16 bytes encodes.
func TestAppendTooLong(t *testing.T) {
	over := strings.Repeat("a", math.MaxInt16+1)
	atMax := over[1:]

	offsetDelete := func(group string) Request { // never flexible
		r := NewPtrOffsetDeleteRequest()
		r.Group = group
		return r
	}
	metadata := func(version int16, topic string) Request { // flexible from v9
		r := NewPtrMetadataRequest()
		r.Version = version
		rt := NewMetadataRequestTopic()
		rt.Topic = &topic
		r.Topics = append(r.Topics, rt)
		return r
	}
	for _, tc := range []struct {
		name    string
		r       Request
		wantNil bool
	}{
		{"string in a struct that is never flexible", offsetDelete(over), true},
		{"the same at the max", offsetDelete(atMax), false},
		{"string at a classic version", metadata(8, over), true},
		{"the same at the max", metadata(8, atMax), false},
		{"string at a flexible version", metadata(12, over), true},
		{"the same at the max", metadata(12, atMax), false},
	} {
		if got := tc.r.AppendTo([]byte{0}); (got == nil) != tc.wantNil {
			t.Errorf("%s: AppendTo returned nil %v, want %v", tc.name, got == nil, tc.wantNil)
		}
		if got := NewRequestFormatter().AppendRequest(nil, tc.r, 1); (got == nil) != tc.wantNil {
			t.Errorf("%s: AppendRequest returned nil %v, want %v", tc.name, got == nil, tc.wantNil)
		}
	}

	if got := NewRequestFormatter(FormatterClientID(over)).AppendRequest(nil, offsetDelete("g"), 1); got != nil {
		t.Error("AppendRequest with a client ID over the max returned a request, want nil")
	}
	sticky := StickyMemberMetadata{CurrentAssignment: []StickyMemberMetadataCurrentAssignment{{Topic: over}}}
	if got := sticky.AppendTo([]byte{0}); got != nil {
		t.Error("StickyMemberMetadata with a topic over the max encoded, want nil")
	}
}

// ControlledShutdown v0 has no client ID in its header. The request still
// has its size and its body.
func TestAppendRequest(t *testing.T) {
	r := NewPtrControlledShutdownRequest()
	r.Version = 0
	r.BrokerID = 5
	got := NewRequestFormatter(FormatterClientID("id")).AppendRequest(nil, r, 1)
	// size, key 7, version 0, correlation ID 1, broker ID 5
	want := []byte{0, 0, 0, 12, 0, 7, 0, 0, 0, 0, 0, 1, 0, 0, 0, 5}
	if !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
