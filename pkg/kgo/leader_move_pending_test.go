package kgo

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKip951FetchMovePending(t *testing.T) {
	cl := &Client{ctx: context.Background(), blockingMetadataFnCh: make(chan func())}
	cl.producer.topics = newTopicsPartitions()
	cl.consumer.g = &groupConsumer{tps: newTopicsPartitions()}
	cursor := &cursor{topic: "topic", topicPartitionData: topicPartitionData{leader: 0, leaderEpoch: 1}}
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(16)
	resp.Brokers = []kmsg.FetchResponseBroker{{NodeID: 0, Host: "localhost", Port: 9092}}
	partition := kmsg.NewFetchResponseTopicPartition()
	partition.ErrorCode = kerr.NotLeaderForPartition.Code
	partition.CurrentLeader.LeaderID = 0
	partition.CurrentLeader.LeaderEpoch = 2

	var first kip951move
	if !first.maybeAddFetchPartition(resp, &partition, cursor) || first.empty() {
		t.Fatal("first hint did not stage a move")
	}
	first.maybeBeginMove(cl)
	var apply func()
	select {
	case fn := <-cl.blockingMetadataFnCh:
		var once sync.Once
		apply = func() { once.Do(fn) }
	case <-time.After(time.Second):
		t.Fatal("move did not reach the metadata barrier")
	}
	defer apply()

	for range 100 {
		var duplicate kip951move
		if !duplicate.maybeAddFetchPartition(resp, &partition, cursor) {
			t.Error("pending hint was not handled as an internal retry")
		}
		if !duplicate.empty() {
			t.Fatal("duplicate response staged another move while the first was pending")
		}
	}
	// The topic was purged while the move waited. Even a move that has no
	// live topic to update must release its pending state for future hints.
	apply()
	deadline := time.Now().Add(time.Second)
	for {
		var next kip951move
		next.maybeAddFetchPartition(resp, &partition, cursor)
		if !next.empty() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed move kept rejecting a new hint")
		}
		runtime.Gosched()
	}
}
