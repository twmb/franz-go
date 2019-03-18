package kgo

type Partitioner interface {
	RequiresConsistency(*Record) bool
	Partition(*Record, int) int
}
