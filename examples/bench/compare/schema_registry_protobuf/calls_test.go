package srprotobench

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

// TestRegistryCallsAreCached checks that neither client talks to the registry
// on the steady state encode path. This is what makes the encode benchmarks
// meaningful: if a client were making an HTTP call per record, its number
// would be measuring srfake rather than the serde.
func TestRegistryCallsAreCached(t *testing.T) {
	const encodes = 50

	newCountedRegistry := func() (string, *atomic.Int64) {
		reg := srfake.New()
		t.Cleanup(reg.Close)
		var calls atomic.Int64
		reg.Intercept(func(http.ResponseWriter, *http.Request) bool {
			calls.Add(1)
			return false // count only; let the real handler respond
		})
		return reg.URL(), &calls
	}

	t.Run("franz-go", func(t *testing.T) {
		ctx := context.Background()
		url, calls := newCountedRegistry()
		cl, err := sr.NewClient(sr.URLs(url))
		if err != nil {
			t.Fatal(err)
		}
		ser, err := protobuf.NewSerializer(cl)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ser.Encode(ctx, topic, msg()); err != nil {
			t.Fatal(err)
		}
		warm := calls.Load()
		for range encodes {
			if _, err := ser.Encode(ctx, topic, msg()); err != nil {
				t.Fatal(err)
			}
		}
		if got := calls.Load() - warm; got != 0 {
			t.Errorf("%d registry calls across %d warm encodes, want 0", got, encodes)
		}
	})

	t.Run("confluent", func(t *testing.T) {
		url, calls := newCountedRegistry()
		ser, _ := confluentSerde(t, url)
		if _, err := ser.Serialize(topic, msg()); err != nil {
			t.Fatal(err)
		}
		warm := calls.Load()
		for range encodes {
			if _, err := ser.Serialize(topic, msg()); err != nil {
				t.Fatal(err)
			}
		}
		if got := calls.Load() - warm; got != 0 {
			t.Errorf("%d registry calls across %d warm encodes, want 0", got, encodes)
		}
	})
}
