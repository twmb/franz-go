package kgo

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO KIP-320

type FetchPartition struct {
	Partition        int32
	Err              error
	HighWatermark    int64
	LastStableOffset int64
	Records          []*Record
}

type FetchTopic struct {
	Topic      string
	Partitions []FetchPartition
}

type Fetch struct {
	Topics []FetchTopic
}

type Fetches []Fetch

type recordSource struct {
	broker *broker

	inflightSem chan struct{} // capacity of 1

	mu sync.Mutex

	// consuming tracks topics, partitions, and offsets/epochs that this
	// source owns.
	consuming map[string]map[int32]consumption

	buffered    Fetch
	hasBuffered bool
}

type consumption struct {
	offset int64
	// TODO epoch
}

func (source *recordSource) setConsumption(topic string, partition int32, start consumption) {
	source.mu.Lock()
	defer source.mu.Unlock()

	wasConsuming := len(source.consuming) != 0
	if source.consuming == nil {
		source.consuming = make(map[string]map[int32]consumption, 1)
	}
	topicConsumption := source.consuming[topic]
	if topicConsumption == nil {
		topicConsumption = make(map[int32]consumption, 1)
		source.consuming[topic] = topicConsumption
	}
	topicConsumption[partition] = start

	if !wasConsuming {
		go source.fill()
	}

}

func (source *recourdSource) createRequest() *kmsg.FetchRequest {
	req := &kmsg.FetchRequest{
		ReplicaID: -1,

		MaxWaitTime: 500, // TODO
		MinBytes:    1,
		MaxBytes:    500 << 20, // TODO

		SessionEpoch: -1, // KIP-227, do not create a session

		Topics: make([]kmsg.FetchRequestTopic, 0, len(source.consuming)),
	}

	for topic, partitions := range source.consuming {
		fetchParts := make([]kmsg.FetchRequestTopicPartition, 0, len(partitions))
		for partition, consumption := range partitions {
			fetchParts = append(fetchParts, kmsg.FetchRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: -1, // KIP-320
				FetchOffset:        consumption.offset,
				LogStartOffset:     -1,
				PartitionMaxBytes:  500 << 20, // TODO
			})
		}
		req.Topics = append(req.Topics, kmsg.FetchRequestTopic{
			Topic:      topic,
			Partitions: fetchParts,
		})
	}

	return req
}

func (source *recordSource) fill() {
	time.Sleep(time.Millisecond)

	again := true
	for again {
		source.inflightSem <- struct{}{}

		source.mu.Lock()
		req, again := source.createRequest()

		if len(req.topics) == 0 {
			source.mu.Unlock()
			<-sem
			continue
		}

		source.broker.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				source.handleReqResp(req, resp, err)
			},
		)
		source.mu.Unlock()
	}
}

func (source *recordSource) handleReqResp(req *kmsg.FetchRequest, resp kmsg.Response, err error) {
	source.mu.Lock()
	defer source.mu.Unlock()

	if err != nil {
		// TODO
		// ErrBrokerDead: ok
		return
	}

	r := resp.(*kmsg.FetchResponse)
	newFetch := Fetch{
		Topics: make([]FetchTopic, 0, len(r.Responses)),
	}

	for _, responseTopic := range r.Responses {
		consumedTopic, exists := source.consuming[responseTopic.Topic]
		if !exists {
			// Our consumption changed during the fetch and we are
			// no longer consuming this topic; drop this topic.
			continue
		}

		newFetchTopic := FetchTopic{
			Topic:      responseTopic.Topic,
			Partitions: make([]FetchPartition, 0, len(responseTopic.PartitionResponses)),
		}
		for i := range responseTopic.PartitionResponses {
			responsePartition := &responseTopic.PartitionResponses[i]
			consumedPartition, exists := consumedTopic[responsePartition.Partition]
			if !exists {
				// Our consumption changed during the fetch and we are
				// no longer consuming this partition; drop it.
				continue
			}

			var numResponsePartitionRecords int
			for i := range responsePartition.RecordBatches {
				numResponsePartitionRecords += len(responsePartition.RecordBatches[i].NumRecords)
			}

			newFetchPartition := FetchPartition{
				Partition:        responsePartition.Partition,
				Err:              kerr.ErrorForCode(responsePartition.ErrorCode),
				HighWatermark:    responsePartition.HighWatermark,
				LastStableOffset: responsePartition.LastStableOffset,
				Records:          make([]*Record, 0, numResponsePartitionRecords),
			}

			for i := range responsePartition.RecordBatches {
				if newFetchPartition.Err != nil {
					break
				}
				consumedPartition.processBatch(
					newFetchTopic.Topic,
					newFetchPartition,
					&responsePartition.RecordBatches[i],
				)
			}

			switch newFetchPartition.Err {
			// TODO (needs to be after processBatch cuz can set err)
			// If retriable: send consumption back to consumer to reload
			// metadata
			// If not retriable: delete consumption
			}
		}

		newFetch.Topics = append(newFetch.Topics, newFetchTopic)
	}

	source.buffered = newFetch
	// TODO consumer broadcast
}

/*
	case kerr.UnknownTopicOrPartition,
		kerr.NotLeaderForPartition,
		kerr.ReplicaNotAvailable,
		kerr.KafkaStorageError,
		kerr.UnknownLeaderEpoch,
		kerr.FencedLeaderEpoch:
		// backoff

	default:
		// Fatal:
		// - bad auth
		// - unsupported compression
		// - unsupported message version
		// - out of range offset
		// - unknown error
	}
*/

func (c *consumption) processBatch(topic string, newFetchPartition *FetchPartition, batch *kmsg.RecordBatch) {
	if batch.Length == 0 {
		return // batch had size of zero: there was no batch
	}
	if batch.Magic != 2 {
		newFetchPartition.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return
	}

	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		rawRecords, err = decompress(rawRecords, compression)
		if err != nil {
			newFetchPartition.Err = fmt.Errorf("unable to decompress batch: %v", err)
			return
		}
	}

	records := make([]*Record, 0, batch.NumRecords)
	for i := batch.NumRecords; i > 0; i-- {
		var r kmsg.Record
		var err error
		rawRecords, err = r.ReadFrom(rawRecords)
		if err != nil {
			newFetchPartition.Err = fmt.Errorf("invalid record batch: %v", err)
			return
		}
		record := recordToRecord(topic, newFetchPartition.Partition, batch, &r)
		if record.Offset < c.offset {
			// We asked for offset 5, but that was in the middle of a
			// batch; we got offsets 0 thru 4 that we need to skip.
			continue
		}
		records = append(records, record)
	}

	if len(rawRecords) != 0 {
		newFetchPartition.Err = kmsg.ErrTooMuchData
		return
	}

	newFetchPartition.Records = records
	c.offset += int64(len(records))
}

func (source *recordSource) takeBuffered() Fetch {
	source.mu.Lock()
	var r Fetch
	if source.hasBuffered {
		r = source.buffered
		source.buffered = Fetch{}
		source.hasBuffered = false
		<-source.inflightSem
	}
	source.mu.Unlock()
	return r
}
