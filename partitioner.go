package kgo

import (
	"sync"
	"time"

	"golang.org/x/exp/rand"
)

// Partitioner determines which Kafka partition messages should be sent to.
type Partitioner interface {
	// RequiresConsistency returns true if a record must hash to the same
	// partition even if a partition is down.
	// If true, a record may hash to a partition that cannot be written to
	// and will error until the partition comes back.
	RequiresConsistency(*Record) bool
	// Partition determines, among a set of n partitions, which index should
	// be chosen to use for the partition for r.
	Partition(r *Record, n int) int
}

type randomPartitioner struct {
	rngs sync.Pool
}

// RandomPartitioner randomly chooses partitions for messages.
func RandomPartitioner() Partitioner {
	r := &randomPartitioner{
		rngs: sync.Pool{
			New: func() interface{} {
				rng := rand.New(new(rand.PCGSource))
				rng.Seed(uint64(time.Now().UnixNano()))
				return rng
			}}}
	return r
}

func (*randomPartitioner) RequiresConsistency(*Record) bool { return false }
func (r *randomPartitioner) Partition(_ *Record, n int) int {
	rng := r.rngs.Get().(*rand.Rand)
	ret := rng.Intn(n)
	r.rngs.Put(rng)
	return ret
}
