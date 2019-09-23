package kgo

import "golang.org/x/exp/rand"

// Partitioner creates topic partitioners to determine which partition messages
// should be sent to.
type Partitioner interface {
	forTopic(string) topicPartitioner
}

// topicPartitioner partitions records in an individual topic.
type topicPartitioner interface {
	// onNewBatch is called when producing a record if that record would
	// trigger a new batch on its current partition.
	onNewBatch()
	// requiresConsistency returns true if a record must hash to the same
	// partition even if a partition is down.
	// If true, a record may hash to a partition that cannot be written to
	// and will error until the partition comes back.
	requiresConsistency(*Record) bool
	// partition determines, among a set of n partitions, which index should
	// be chosen to use for the partition for r.
	partition(r *Record, n int) int
}

// StickyPartitioner is the same as StickyKeyPartitioner, but with no logic to
// consistently hash keys. That is, this only partitions according to the
// sticky partition strategy.
func StickyPartitioner() Partitioner {
	return new(stickyPartitioner)
}

type stickyPartitioner struct{}

func (*stickyPartitioner) forTopic(string) topicPartitioner {
	return &stickyTopicPartitioner{
		onPart: -1,
		rng:    rand.New(new(rand.PCGSource)),
	}
}

type stickyTopicPartitioner struct {
	onPart int
	rng    *rand.Rand
}

func (p *stickyTopicPartitioner) onNewBatch()                    { p.onPart = -1 }
func (*stickyTopicPartitioner) requiresConsistency(*Record) bool { return false }
func (p *stickyTopicPartitioner) partition(_ *Record, n int) int {
	if p.onPart == -1 {
		p.onPart = p.rng.Intn(n)
	}
	return p.onPart
}

// StickyKeyPartitioner mirrors the default Java partitioner from Kafka's 2.4.0
// release (see KAFKA-8601).
//
// This is the same "hash the key consistently, if no key, choose random
// partition" strategy that the Java partitioner has always used, but rather
// than always choosing a random key, the partitioner pins a partition to
// produce to until that partition rolls over to a new batch. Only when rolling
// to new batches does this partitioner switch partitions.
//
// The benefit with this pinning is less CPU utilization on Kafka brokers.
// Over time, the random distribution is the same, but the brokers are handling
// on average larger batches.
func StickyKeyPartitioner() Partitioner {
	return new(keyPartitioner)
}

type keyPartitioner struct{}

func (*keyPartitioner) forTopic(string) topicPartitioner {
	return &stickyKeyTopicPartitioner{
		onPart: -1,
		rng:    rand.New(new(rand.PCGSource)),
	}
}

type stickyKeyTopicPartitioner struct {
	onPart int
	rng    *rand.Rand
}

func (p *stickyKeyTopicPartitioner) onNewBatch()                      { p.onPart = -1 }
func (*stickyKeyTopicPartitioner) requiresConsistency(r *Record) bool { return r.Key != nil }
func (p *stickyKeyTopicPartitioner) partition(r *Record, n int) int {
	if r.Key != nil {
		// https://github.com/apache/kafka/blob/d91a94e/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L59
		// https://github.com/apache/kafka/blob/d91a94e/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L865-L867
		// Java just masks the sign bit out to get a positive number,
		// which ultimately was never necessary but here we are.
		return int(murmur2(r.Key)&0x7fffffff) % n
	}
	if p.onPart == -1 {
		p.onPart = p.rng.Intn(n)
	}
	return p.onPart
}

// Straight from the C++ code and from the Java code duplicating it.
// https://github.com/apache/kafka/blob/d91a94e/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L383-L421
// https://github.com/aappleby/smhasher/blob/61a0530f/src/MurmurHash2.cpp#L37-L86
//
// The Java code uses ints but with unsigned shifts, likely due to Java not
// having unsigned ints which is a joke in its own right.
func murmur2(b []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(b))
	for len(b) >= 4 {
		k := uint32(b[3])<<24 + uint32(b[2])<<16 + uint32(b[1])<<8 + uint32(b[0])
		b = b[4:]
		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k
	}
	switch len(b) {
	case 3:
		h ^= uint32(b[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[0])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}
