package kgo

import (
	"context"
	"testing"
)

func TestKnownLeaderPartitioning(t *testing.T) {
	t.Parallel()

	// Brokers 1 and 2 are in racks a and b, broker 3 has no rack, and
	// broker 4 leads partitions in some tests but is not in our broker
	// list, as when a metadata response names a leader it does not list.
	newCl := func(opts ...Opt) *Client {
		cl, err := NewClient(append([]Opt{
			SeedBrokers("127.0.0.1:1"), // metadata never loads; we store partitions directly
			ManualFlushing(),
			RecordPartitioner(lastPartitioner{}),
		}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		a, b := "a", "b"
		cl.brokersMu.Lock()
		cl.brokers = []*broker{
			{meta: BrokerMetadata{NodeID: 1, Rack: &a}},
			{meta: BrokerMetadata{NodeID: 2, Rack: &b}},
			{meta: BrokerMetadata{NodeID: 3}},
		}
		cl.brokersMu.Unlock()
		cl.producer.topics.storeTopics([]string{"t"})
		return cl
	}

	// store stores topic t with partition i led by leaders[i], as a
	// metadata update would.
	store := func(cl *Client, leaders ...int32) {
		s := cl.newSink(1)
		s.produceVersion.Store(9)
		d := &topicPartitionsData{topic: "t"}
		for i, leader := range leaders {
			r := &recBuf{cl: cl, topic: "t", partition: int32(i), maxRecordBatchBytes: 1 << 20, recBufsIdx: -1, lastAckedOffset: -1, sink: s}
			r.lingerFn = r.unlingerAndManuallyDrain
			tp := &topicPartition{records: r, topicPartitionData: topicPartitionData{leader: leader}}
			d.partitions = append(d.partitions, tp)
			d.writablePartitions = append(d.writablePartitions, tp)
		}
		cl.producer.topics.load()["t"].v.Store(d)
	}

	produce := func(cl *Client, key string, want int32) {
		t.Helper()
		r := &Record{Topic: "t", Value: []byte("v")}
		if key != "" {
			r.Key = []byte(key)
		}
		cl.Produce(context.Background(), r, nil)
		if r.Partition != want {
			t.Errorf("key %q: got partition %d, want %d", key, r.Partition, want)
		}
	}

	cl := newCl()
	store(cl, 1, 2, 4)
	produce(cl, "", 1)  // unkeyed: last of the known-leader partitions 0 and 1
	produce(cl, "k", 2) // keyed: last of all partitions
	store(cl, 4, 3, 4, 1, 4)
	produce(cl, "", 3) // known leaders in the middle: 1 and 3
	store(cl, 4, 4)
	produce(cl, "", 1) // no leader known: all writable partitions
	store(cl, 2, 4)
	produce(cl, "", 0) // the subset follows each metadata update
	store(cl, 4, 2)
	produce(cl, "", 1)

	cl = newCl(Rack("a"), RackAwarePartitioning())
	store(cl, 1, 4, 2)
	produce(cl, "", 0) // known and in our rack
	store(cl, 2, 4)
	produce(cl, "", 0) // no known leader in our rack: the known-leader partitions
}
