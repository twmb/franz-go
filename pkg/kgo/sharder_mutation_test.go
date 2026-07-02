package kgo

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// The findCoordinator sharder previously deduplicated the request's keys
// into the CALLER's backing array, rewriting the user's request with
// deduplicated, map-order-shuffled keys.
func TestFindCoordinatorSharderDoesNotMutateRequest(t *testing.T) {
	t.Parallel()
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKeys = []string{"a", "a", "b"}
	if _, _, err := (*findCoordinatorSharder)(nil).shard(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "a", "b"}
	for i, k := range req.CoordinatorKeys {
		if k != want[i] {
			t.Fatalf("caller request keys mutated: %v", req.CoordinatorKeys)
		}
	}
}
