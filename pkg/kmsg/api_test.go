package kmsg

import (
	"bytes"
	"runtime"
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
