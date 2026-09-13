package kfake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestLookbackStartOffset stamps ten records a minute apart across the last
// ten minutes and starts consuming with a five minute lookback. The client
// resolves the timestamp when it lists the partition, so consuming begins
// halfway in rather than at the log start.
func TestLookbackStartOffset(t *testing.T) {
	t.Parallel()

	const (
		topic = "lookback-start"
		nrecs = 10
		want  = 5
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Every record is its own batch. Record 5 is stamped thirty seconds
	// after the five minute boundary and record 4 thirty seconds before it,
	// so the boundary the client computes a moment later still falls
	// between them.
	pcl := newPlainClient(t, c)
	base := time.Now().Add(-10*time.Minute + 30*time.Second)
	for i := range nrecs {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i), Timestamp: base.Add(time.Duration(i) * time.Minute)}
		if err := pcl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().Lookback(5*time.Minute)),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	got := collectRecords(t, cl, nrecs-want, 8*time.Second)
	for i, r := range got {
		if w := int64(want + i); r.Offset != w {
			t.Errorf("record %d is offset %d, want %d", i, r.Offset, w)
		}
	}
}
