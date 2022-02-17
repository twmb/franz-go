package kgo

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

// Partitioner creates topic partitioners to determine which partition messages
// should be sent to.
//
// Note that a record struct is unmodified (minus a potential default topic)
// from producing through partitioning, so you can set fields in the record
// struct before producing to aid in partitioning with a custom partitioner.
type Partitioner interface {
	// forTopic returns a partitioner for an individual topic. It is
	// guaranteed that only one record will use the an individual topic's
	// topicPartitioner at a time, meaning partitioning within a topic does
	// not require locks.
	ForTopic(string) TopicPartitioner
}

// TopicPartitioner partitions records in an individual topic.
type TopicPartitioner interface {
	// RequiresConsistency returns true if a record must hash to the same
	// partition even if a partition is down.
	// If true, a record may hash to a partition that cannot be written to
	// and will error until the partition comes back.
	RequiresConsistency(*Record) bool
	// Partition determines, among a set of n partitions, which index should
	// be chosen to use for the partition for r.
	Partition(r *Record, n int) int
}

// TopicPartitionerOnNewBatch is an optional extension interface to
// TopicPartitioner that calls OnNewBatch before any new batch is created. If
// buffering a record would cause a new batch, OnNewBatch is called.
//
// This interface allows for partitioner implementations that effectively pin
// to a partition until a new batch is created, after which the partitioner can
// choose which next partition to use.
type TopicPartitionerOnNewBatch interface {
	// OnNewBatch is called when producing a record if that record would
	// trigger a new batch on its current partition.
	OnNewBatch()
}

// TopicBackupPartitioner is an optional extension interface to
// TopicPartitioner that can partition by the number of records buffered.
//
// If a partitioner implements this interface, the Partition function will
// never be called.
type TopicBackupPartitioner interface {
	TopicPartitioner

	// PartitionByBackup is similar to Partition, but has an additional
	// backupIter. This iterator will return the number of buffered records
	// per partition index. The iterator's Next function can only be called
	// up to n times, calling it any more will panic.
	PartitionByBackup(r *Record, n int, backupIter TopicBackupIter) int
}

// TopicBackupIter is an iterates through partition indices.
type TopicBackupIter interface {
	// Next returns the next partition index and the total buffered records
	// for the partition. If Rem returns 0, calling this function again
	// will panic.
	Next() (int, int64)
	// Rem returns the number of elements left to iterate through.
	Rem() int
}

type leastBackupInput struct {
	mapping []*topicPartition
}

func (i *leastBackupInput) Next() (int, int64) {
	last := len(i.mapping) - 1
	buffered := atomic.LoadInt64(&i.mapping[last].records.buffered)
	i.mapping = i.mapping[:last]
	return last, buffered
}

func (i *leastBackupInput) Rem() int {
	return len(i.mapping)
}

// BasicConsistentPartitioner wraps a single function to provide a Partitioner
// and TopicPartitioner (that function is essentially a combination of
// Partitioner.ForTopic and TopicPartitioner.Partition).
//
// As a minimal example, if you do not care about the topic and you set the
// partition before producing:
//
//     kgo.BasicConsistentPartitioner(func(topic) func(*Record, int) int {
//             return func(r *Record, n int) int {
//                     return int(r.Partition)
//             }
//     })
//
func BasicConsistentPartitioner(partition func(string) func(r *Record, n int) int) Partitioner {
	return &basicPartitioner{partition}
}

type basicPartitioner struct {
	fn func(string) func(*Record, int) int
}

func (b *basicPartitioner) ForTopic(t string) TopicPartitioner {
	return &basicTopicPartitioner{b.fn(t)}
}

type basicTopicPartitioner struct {
	fn func(*Record, int) int
}

func (*basicTopicPartitioner) RequiresConsistency(*Record) bool { return true }
func (b *basicTopicPartitioner) Partition(r *Record, n int) int { return b.fn(r, n) }

// ManualPartitioner is a partitioner that simply returns the Partition field
// that is already set on any record.
//
// Any record with an invalid partition will be immediately failed. This
// partitioner is simply the partitioner that is demonstrated in the
// BasicConsistentPartitioner documentation.
func ManualPartitioner() Partitioner {
	return BasicConsistentPartitioner(func(string) func(*Record, int) int {
		return func(r *Record, _ int) int {
			return int(r.Partition)
		}
	})
}

// RoundRobinPartitioner is a partitioner that round-robin's through all
// available partitions. This algorithm has lower throughput and causes higher
// CPU load on brokers, but can be useful if you want to ensure an even
// distribution of records to partitions.
func RoundRobinPartitioner() Partitioner {
	return new(roundRobinPartitioner)
}

type roundRobinPartitioner struct{}

func (*roundRobinPartitioner) ForTopic(string) TopicPartitioner {
	return new(roundRobinTopicPartitioner)
}

type roundRobinTopicPartitioner struct {
	on int
}

func (*roundRobinTopicPartitioner) RequiresConsistency(*Record) bool { return false }
func (r *roundRobinTopicPartitioner) Partition(_ *Record, n int) int {
	if r.on >= n {
		r.on = 0
	}
	ret := r.on
	r.on++
	return ret
}

// LeastBackupPartitioner prioritizes partitioning by three factors, in order:
//
//  1) pin to the current pick until there is a new batch
//  2) on new batch, choose the least backed up partition (the partition with
//     the fewest amount of buffered records)
//  3) if multiple partitions are equally least-backed-up, choose one at random
//
// This algorithm prioritizes least-backed-up throughput, which may result in
// unequal partitioning. It is likely that this algorithm will talk most to the
// broker that it has the best connection to.
//
// This algorithm is resilient to brokers going down: if a few brokers die, it
// is possible your throughput will be so high that the maximum buffered
// records will be reached in the now-offline partitions before metadata
// responds that the broker is offline. With the standard partitioning
// algorithms, the only recovery is if the partition is remapped or if the
// broker comes back online. With the least backup partitioner, downed
// partitions will see slight backup, but then the other partitions that are
// still accepting writes will get all of the writes and your client will not
// be blocked.
//
// Under ideal scenarios (no broker / connection issues), StickyPartitioner is
// equivalent to LeastBackupPartitioner. This partitioner is only recommended
// if you are a producer consistently dealing with flaky connections or
// problematic brokers and do not mind uneven load on your brokers.
func LeastBackupPartitioner() Partitioner {
	return new(leastBackupPartitioner)
}

type leastBackupPartitioner struct{}

func (*leastBackupPartitioner) ForTopic(string) TopicPartitioner {
	p := newLeastBackupTopicPartitioner()
	return &p
}

func newLeastBackupTopicPartitioner() leastBackupTopicPartitioner {
	return leastBackupTopicPartitioner{
		onPart: -1,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type leastBackupTopicPartitioner struct {
	onPart int
	rng    *rand.Rand
}

func (p *leastBackupTopicPartitioner) OnNewBatch()                    { p.onPart = -1 }
func (*leastBackupTopicPartitioner) RequiresConsistency(*Record) bool { return false }
func (*leastBackupTopicPartitioner) Partition(*Record, int) int       { panic("unreachable") }

func (p *leastBackupTopicPartitioner) PartitionByBackup(_ *Record, n int, backup TopicBackupIter) int {
	if p.onPart == -1 || p.onPart >= n {
		leastBackup := int64(math.MaxInt64)
		npicked := 0
		for ; n > 0; n-- {
			pick, backup := backup.Next()
			if backup < leastBackup {
				leastBackup = backup
				p.onPart = pick
				npicked = 1
			} else {
				npicked++ // reservoir sampling with k = 1
				if p.rng.Intn(npicked) == 0 {
					p.onPart = pick
				}
			}
		}
	}
	return p.onPart
}

// StickyPartitioner is the same as StickyKeyPartitioner, but with no logic to
// consistently hash keys. That is, this only partitions according to the
// sticky partition strategy.
func StickyPartitioner() Partitioner {
	return new(stickyPartitioner)
}

type stickyPartitioner struct{}

func (*stickyPartitioner) ForTopic(string) TopicPartitioner {
	p := newStickyTopicPartitioner()
	return &p
}

func newStickyTopicPartitioner() stickyTopicPartitioner {
	return stickyTopicPartitioner{
		lastPart: -1,
		onPart:   -1,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type stickyTopicPartitioner struct {
	lastPart int
	onPart   int
	rng      *rand.Rand
}

func (p *stickyTopicPartitioner) OnNewBatch()                    { p.lastPart, p.onPart = p.onPart, -1 }
func (*stickyTopicPartitioner) RequiresConsistency(*Record) bool { return false }
func (p *stickyTopicPartitioner) Partition(_ *Record, n int) int {
	if p.onPart == -1 || p.onPart >= n {
		p.onPart = p.rng.Intn(n)
		if p.onPart == p.lastPart {
			p.onPart = (p.onPart + 1) % n
		}
	}
	return p.onPart
}

// StickyKeyPartitioner mirrors the default Java partitioner from Kafka's 2.4.0
// release (see KAFKA-8601).
//
// This is the same "hash the key consistently, if no key, choose random
// partition" strategy that the Java partitioner has always used, but rather
// than always choosing a random partition, the partitioner pins a partition to
// produce to until that partition rolls over to a new batch. Only when rolling
// to new batches does this partitioner switch partitions.
//
// The benefit with this pinning is less CPU utilization on Kafka brokers.
// Over time, the random distribution is the same, but the brokers are handling
// on average larger batches.
//
// overrideHasher is optional; if nil, this will return a partitioner that
// partitions exactly how Kafka does. Specifically, the partitioner will use
// murmur2 to hash keys, will mask out the 32nd bit, and then will mod by the
// number of potential partitions.
func StickyKeyPartitioner(overrideHasher PartitionerHasher) Partitioner {
	if overrideHasher == nil {
		overrideHasher = KafkaHasher(murmur2)
	}
	return &keyPartitioner{overrideHasher}
}

// PartitionerHasher returns a partition to use given the input data and number
// of partitions.
type PartitionerHasher func([]byte, int) int

// KafkaHasher returns a PartitionerHasher using hashFn that mirrors how Kafka
// partitions after hashing data. In Kafka, after hashing into a uint32, the
// hash is converted to an int32 and the high bit is stripped. Kafka by default
// uses murmur2 hashing, and the StickyKeyPartiitoner uses this by default.
// Using this KafkaHasher function is only necessary if you want to change the
// underlying hashing algorithm.
func KafkaHasher(hashFn func([]byte) uint32) PartitionerHasher {
	return func(key []byte, n int) int {
		// https://github.com/apache/kafka/blob/d91a94e/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L59
		// https://github.com/apache/kafka/blob/d91a94e/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L865-L867
		// Masking before or after the int conversion makes no difference.
		return int(hashFn(key)&0x7fffffff) % n
	}
}

// SaramaHasher returns a PartitionerHasher using hashFn that mirrors how
// Sarama partitions after hashing data.
//
// Sarama has two differences from Kafka when partitioning:
//
// 1) In Kafka, when converting the uint32 hash to an int32, Kafka masks the
// high bit. In Sarama, if the high bit is 1 (i.e., the number as an int32 is
// negative), Sarama negates the number.
//
// 2) Kafka by default uses the murmur2 hashing algorithm. Sarama by default
// uses fnv-1a.
//
// Sarama added a NewReferenceHashPartitioner function that attempted to align
// with Kafka, but the reference partitioner only fixed the first difference,
// not the second. Further customization options were added later that made it
// possible to exactly match Kafka when hashing.
//
// In short, to *exactly* match the Sarama defaults, use the following:
//
//     kgo.StickyKeyPartitioner(kgo.SaramaHasher(fnv.New32a()))
//
func SaramaHasher(hashFn func([]byte) uint32) PartitionerHasher {
	return func(key []byte, n int) int {
		p := int(hashFn(key)) % n
		if p < 0 {
			p = -p
		}
		return p
	}
}

type keyPartitioner struct {
	hasher PartitionerHasher
}

func (k *keyPartitioner) ForTopic(string) TopicPartitioner {
	return &stickyKeyTopicPartitioner{k.hasher, newStickyTopicPartitioner()}
}

type stickyKeyTopicPartitioner struct {
	hasher PartitionerHasher
	stickyTopicPartitioner
}

func (*stickyKeyTopicPartitioner) RequiresConsistency(r *Record) bool { return r.Key != nil }
func (p *stickyKeyTopicPartitioner) Partition(r *Record, n int) int {
	if r.Key != nil {
		return p.hasher(r.Key, n)
	}
	return p.stickyTopicPartitioner.Partition(r, n)
}

// Straight from the C++ code and from the Java code duplicating it.
// https://github.com/apache/kafka/blob/d91a94e/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L383-L421
// https://github.com/aappleby/smhasher/blob/61a0530f/src/MurmurHash2.cpp#L37-L86
//
// The Java code uses ints but with unsigned shifts; we do not need to.
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
