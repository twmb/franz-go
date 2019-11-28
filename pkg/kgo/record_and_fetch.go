package kgo

import "github.com/twmb/kafka-go/pkg/krec"

type FetchPartition struct {
	Partition        int32
	Err              error
	HighWatermark    int64
	LastStableOffset int64
	Records          []*krec.Rec
}

type FetchTopic struct {
	Topic      string
	Partitions []FetchPartition
}

type Fetch struct {
	Topics []FetchTopic
}

type Fetches []Fetch

func (fs Fetches) RecordIter() *FetchesRecordIter {
	iter := &FetchesRecordIter{fetches: fs}
	iter.prepareNext()
	return iter
}

type FetchesRecordIter struct {
	fetches []Fetch
}

func (i *FetchesRecordIter) Done() bool {
	return len(i.fetches) == 0
}

func (i *FetchesRecordIter) Next() *krec.Rec {
	records := &i.fetches[0].Topics[0].Partitions[0].Records
	next := (*records)[0]
	*records = (*records)[1:]
	i.prepareNext()
	return next
}

func (i *FetchesRecordIter) prepareNext() {
beforeFetch0:
	if len(i.fetches) == 0 {
		return
	}

beforeTopic0:
	fetch0 := &i.fetches[0]
	if len(fetch0.Topics) == 0 {
		i.fetches = i.fetches[1:]
		goto beforeFetch0
	}

beforePartition0:
	topic0 := &fetch0.Topics[0]
	if len(topic0.Partitions) == 0 {
		fetch0.Topics = fetch0.Topics[1:]
		goto beforeTopic0
	}

	partition0 := &topic0.Partitions[0]
	if len(partition0.Records) == 0 {
		topic0.Partitions = topic0.Partitions[1:]
		goto beforePartition0
	}
}
