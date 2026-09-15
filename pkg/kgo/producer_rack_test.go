package kgo

import (
	"context"
	"testing"
)

// lastPartitioner requires consistency for keyed records and always picks
// the last candidate, so the partition a record lands on reveals which
// candidate set doPartition handed the partitioner.
type lastPartitioner struct{}

func (lastPartitioner) ForTopic(string) TopicPartitioner   { return lastPartitioner{} }
func (lastPartitioner) RequiresConsistency(r *Record) bool { return r.Key != nil }
func (lastPartitioner) Partition(_ *Record, n int) int     { return n - 1 }

func TestRackAwarePartitioning(t *testing.T) {
	t.Parallel()

	// Brokers 1 and 2 are in racks a and b; broker 3 has no rack.
	newCl := func(rack string) *Client {
		cl, err := NewClient(
			SeedBrokers("127.0.0.1:1"), // metadata never loads; we store partitions directly
			ManualFlushing(),
			Rack(rack),
			RackAwarePartitioning(),
			RecordPartitioner(lastPartitioner{}),
		)
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

	cl := newCl("a")
	store(cl, 1, 2, 1, 3)
	produce(cl, "", 2)  // unkeyed: last of the same-rack partitions 0 and 2
	produce(cl, "k", 3) // keyed: last of all partitions
	store(cl, 2, 2, 3, 3)
	produce(cl, "", 3) // no leader in our rack: all partitions
	store(cl, 2, 1, 3, 3)
	produce(cl, "", 1) // the subset follows each metadata update

	cl = newCl("")
	store(cl, 3, 1)
	produce(cl, "", 1) // no rack: a rackless leader is not "in" our empty rack
}
