package kgo

import (
	"fmt"
	"strings"
)

// This file contains encoding and decoding for messages. Kafka does not really
// document what new fields mean or when they are added, but htere is some (not
// valid) JSON in github that is invaluable:
// github.com/apache/kafka/clients/src/main/resources/common/message
// Also KIPs:
// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals

const messagesMaxKey int16 = 42

type messageRequestKind interface {
	key() int16
	maxVersion() int16
	setVersion(int16)
	appendTo([]byte) []byte
	responseKind() messageResponseKind
}

type messageRequestMinVersioner interface {
	minVersion() int16
}

func appendMessageRequest(
	dst []byte,
	kind messageRequestKind,
	version int16,
	correlationID int32,
	clientID *string,
) []byte {
	dst = append(dst, 0, 0, 0, 0) // reserve length
	dst = appendInt16(dst, kind.key())
	dst = appendInt16(dst, version)
	dst = appendInt32(dst, correlationID)
	dst = appendNullableString(dst, clientID)
	dst = kind.appendTo(dst)
	appendInt32(dst[:0], int32(len(dst[4:])))
	return dst
}

type messageResponseKind interface {
	readFrom([]byte) error
}

// ********** PRODUCE REQUEST (key 0) **********
//   v0, v1, v2 we DO NOT support (old message format).
//   v1 added quota throttling support in response
//   v2 changed how a field in the old message format was used
// Request:
//   - v3 switched to new record format & added transactional id
//   - v7 added zstd compression support for records (KIP-110)
//
// Response:
//   - v4 added KAFKA_STORAGE_ERROR as possible error response
//   - v5 added LogStartOffset to filter some Java client error
//   - v6 adds nothing, but quota violations are responded to before throttling
//     - not even noted in the json...
//     - in github.com/apache/kafka/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.go

// TODO should we support nullable arrays?

type produceRequest struct {
	version       int16
	transactionID *string // v3+
	acks          int16
	timeout       int32
	topicData     []produceRequestTopicData
}

type produceRequestTopicData struct {
	topic string
	sets  []produceRequestTopicDataRecordSets
}

type produceRequestTopicDataRecordSets struct {
	partition int32

	// RECORDS are NULLABLE_BYTES, but special, meaning a record
	// batch is preceeded with a length.
	recordBatchLen int32

	// And now the record batch header (which itself includes a length...)
	baseOffset           int64
	batchLen             int32
	partitionLeaderEpoch int32
	magic                int8
	crc                  int32
	attributes           int16
	lastOffsetDelta      int32
	firstTimestamp       int64
	maxTimestamp         int64
	producerID           int64
	producerEpoch        int16
	baseSequence         int32

	records []produceRequestRecord
}

type produceRequestRecord struct {
	length         int32 // varint
	attributes     int8
	timestampDelta int32  // varint
	offsetDelta    int32  // varint
	key            string // len encoded as varint
	value          string // len encoded as varint
	headers        []produceRequestRecordHeader
}

type produceRequestRecordHeader struct {
	key   string // len encoded as varint
	value string // len encoded as varint
}

var _ messageRequestMinVersioner = new(produceRequest)

func (*produceRequest) key() int16           { return 0 }
func (*produceRequest) maxVersion() int16    { return 7 }
func (*produceRequest) minVersion() int16    { return 3 }
func (p *produceRequest) setVersion(v int16) { p.version = v }
func (p *produceRequest) appendTo(dst []byte) []byte {
	if p.version >= 3 {
		dst = appendNullableString(dst, p.transactionID)
	}
	dst = appendInt16(dst, p.acks)
	dst = appendInt32(dst, p.timeout)
	dst = appendArrayLen(dst, len(p.topicData))
	for _, data := range p.topicData {
		dst = appendString(dst, data.topic)
		dst = appendArrayLen(dst, len(data.sets))
		for _, set := range data.sets {
			dst = appendInt32(dst, set.partition)
			dst = appendInt32(dst, set.recordBatchLen)
			dst = appendInt64(dst, set.baseOffset)
			dst = appendInt32(dst, set.batchLen)
			dst = appendInt32(dst, set.partitionLeaderEpoch)
			dst = appendInt8(dst, set.magic)
			dst = appendInt32(dst, set.crc)
			dst = appendInt16(dst, set.attributes)
			dst = appendInt32(dst, set.lastOffsetDelta)
			dst = appendInt64(dst, set.firstTimestamp)
			dst = appendInt64(dst, set.maxTimestamp)
			dst = appendInt64(dst, set.producerID)
			dst = appendInt16(dst, set.producerEpoch)
			dst = appendInt32(dst, set.baseSequence)
			dst = appendArrayLen(dst, len(set.records))
			for _, record := range set.records {
				dst = appendVarint(dst, record.length)
				dst = appendInt8(dst, record.attributes)
				dst = appendVarint(dst, record.timestampDelta)
				dst = appendVarint(dst, record.offsetDelta)
				dst = appendVarintString(dst, record.key)
				dst = appendVarintString(dst, record.value)
				dst = appendArrayLen(dst, len(record.headers))
				for _, header := range record.headers {
					dst = appendVarintString(dst, header.key)
					dst = appendVarintString(dst, header.value)
				}
			}
		}
	}
	return dst
}
func (p *produceRequest) responseKind() messageResponseKind {
	return &produceResponse{version: p.version}
}

type produceResponse struct {
	version            int16
	responses          []produceResponseTopic
	throttleTimeMillis int32
}
type produceResponseTopic struct {
	topic              string
	partitionResponses []produceResponsePartition
}
type produceResponsePartition struct {
	partition      int32
	errCode        int16
	baseOffset     int64
	logAppendTime  int64
	logStartOffset int64 // v5+
}

func (p *produceResponse) readFrom(src []byte) error {
	b := binreader{src: src}
	for i := b.rarrayLen(); i > 0; i-- {
		responseTopic := produceResponseTopic{
			topic: b.rstring(),
		}
		for j := b.rarrayLen(); j > 0; j-- {
			partition := produceResponsePartition{
				partition:     b.rint32(),
				errCode:       b.rint16(),
				baseOffset:    b.rint64(),
				logAppendTime: b.rint64(),
			}
			if p.version >= 5 {
				partition.logStartOffset = b.rint64()
			}
			responseTopic.partitionResponses = append(responseTopic.partitionResponses, partition)
		}
	}
	p.throttleTimeMillis = b.rint32()
	return b.complete()
}

// ********** METADATA (key 3) **********
// Request:
//   - v0, empty array means request metadata for all topics
//   - v1+, empty array means no topics, nil array means all
//   - v4 adds allowTopicAutocreate
//   - v8 adds requesting authorized operations for cluster/topic (KIP-430)
//
// Response:
//   - v1 adds rack for each broker, controller id, and topic internal state
//   - v2 adds cluster ID
//   - v3 adds throttle time
//   - v5 adds per-partition offline_replicas
//   - v6 adds nothing, but quota violations are responded to before throttling
//   - v7 adds leader epoch to partition metadata
//   - v8 adds responding with authorized operations for topic/cluster

type metadataRequest struct {
	version                int16
	topics                 []string
	allTopics              bool
	allowTopicAutocreate   bool // v4+
	includeClusterAuthOpts bool // v8+
	includeTopicAuthOpts   bool // v8+
}

func (*metadataRequest) key() int16           { return 3 }
func (*metadataRequest) maxVersion() int16    { return 8 }
func (m *metadataRequest) setVersion(v int16) { m.version = v }
func (m *metadataRequest) appendTo(dst []byte) []byte {
	if m.allTopics {
		if m.version == 0 {
			dst = appendArrayLen(dst, 0)
		} else {
			dst = appendArrayLen(dst, -1)
		}
	} else {
		dst = appendArrayLen(dst, len(m.topics))
		for _, topic := range m.topics {
			dst = appendString(dst, topic)
		}
	}
	if m.version >= 4 {
		dst = appendBool(dst, m.allowTopicAutocreate)
	}
	if m.version >= 8 {
		dst = appendBool(dst, m.includeClusterAuthOpts)
		dst = appendBool(dst, m.includeTopicAuthOpts)
	}
	return dst
}
func (m *metadataRequest) responseKind() messageResponseKind {
	return &metadataResponse{version: m.version}
}

type metadataResponse struct {
	version              int16
	throttleTimeMillis   int32 // v3+
	brokers              []metadataRespBroker
	clusterID            *string // v2+
	controllerID         int32   // v1+
	topicMetas           []metadataRespTopicMeta
	authorizedOperations int32 // v8+
}

type metadataRespBroker struct {
	nodeID int32
	host   string
	port   int32
	rack   *string // v1+
}

type metadataRespTopicMeta struct {
	errCode              int16
	topic                string
	isInternal           bool // v1+
	partMetas            []metadataRespPartMeta
	authorizedOperations int32 // v8+
}

type metadataRespPartMeta struct {
	errCode         int16
	partition       int32
	leader          int32
	leaderEpoch     int32 // v7+
	replicas        []int32
	isrs            []int32
	offlineReplicas []int32 // v5+
}

func (m *metadataResponse) readFrom(src []byte) error {
	b := binreader{src: src}

	if m.version >= 3 {
		m.throttleTimeMillis = b.rint32()
	}

	for i := b.rarrayLen(); i > 0; i-- {
		broker := metadataRespBroker{
			nodeID: b.rint32(),
			host:   b.rstring(),
			port:   b.rint32(),
		}
		if m.version >= 1 {
			broker.rack = b.rnullableString()
		}
		m.brokers = append(m.brokers, broker)
	}

	if m.version >= 2 {
		m.clusterID = b.rnullableString()
	}
	if m.version >= 1 {
		m.controllerID = b.rint32()
	}

	for i := b.rarrayLen(); i > 0; i-- {
		topicMeta := metadataRespTopicMeta{
			errCode: b.rint16(),
			topic:   b.rstring(),
		}

		for j := b.rarrayLen(); j > 0; j-- {
			partMeta := metadataRespPartMeta{
				errCode:   b.rint16(),
				partition: b.rint32(),
				leader:    b.rint32(),
			}
			if m.version >= 7 {
				partMeta.leaderEpoch = b.rint32()
			}
			for k := b.rarrayLen(); k > 0; k-- {
				partMeta.replicas = append(partMeta.replicas, b.rint32())
			}
			for k := b.rarrayLen(); k > 0; k-- {
				partMeta.isrs = append(partMeta.isrs, b.rint32())
			}
			if m.version >= 5 {
				for k := b.rarrayLen(); k > 0; k-- {
					partMeta.offlineReplicas = append(partMeta.offlineReplicas, b.rint32())
				}
			}

			topicMeta.partMetas = append(topicMeta.partMetas, partMeta)
		}

		if m.version >= 8 {
			topicMeta.authorizedOperations = b.rint32()
		}
		m.topicMetas = append(m.topicMetas, topicMeta)
	}

	if m.version >= 8 {
		m.authorizedOperations = b.rint32()
	}
	return b.complete()
}

// ********** API VERSIONS (key 18) **********
// Request:
//   - currently always empty
//
// Response:
//   - v1+ added throttle time millis
//   - v2+ adds nothing, but quota violations are responded to before throttling
type apiVersionsRequest struct {
	version int16
}

func (*apiVersionsRequest) key() int16                 { return 18 }
func (*apiVersionsRequest) maxVersion() int16          { return 3 }
func (a *apiVersionsRequest) setVersion(v int16)       { a.version = v }
func (*apiVersionsRequest) appendTo(dst []byte) []byte { return dst }
func (a *apiVersionsRequest) responseKind() messageResponseKind {
	return &apiVersionsResponse{version: a.version}
}

type apiVersionsResponse struct {
	version            int16
	errCode            int16
	keys               apiVersions
	throttleTimeMillis int32 // v1+
}

type apiVersions [messagesMaxKey + 1]int16

func (a *apiVersionsResponse) readFrom(src []byte) error {
	b := binreader{src: src}

	a.errCode = b.rint16()
	nkeys := b.rarrayLen()
	if nkeys < 0 {
		return errInvalidResp
	}

	for i := 0; i < len(a.keys[:]); i++ {
		a.keys[i] = -1 // default unsupported for all keys
	}

	keys := make(map[int16]int16, nkeys)
	for i := int32(0); i < nkeys; i++ {
		key := b.rint16()
		// We discard the lower bound since Kafka is currently entirely
		// backwards compatible.
		b.rint16()
		keys[key] = b.rint16()
	}

	// For all keys both the broker and our client supports, copy the
	// key's max supported version.
	for key, keyMax := range keys {
		if key > messagesMaxKey { // we do not know of this new command
			continue
		}
		a.keys[key] = keyMax
	}

	if a.version >= 1 {
		a.throttleTimeMillis = b.rint32()
	}

	return b.complete()
}

func (a *apiVersionsResponse) String() string {
	var sb strings.Builder
	sb.WriteString("versions =>\n")
	for key := int16(0); key < int16(len(a.keys[:])); key++ {
		fmt.Fprintf(&sb, "  %2d => ..%d\n", key, a.keys[key])
	}
	sb.WriteString("-----------")
	return sb.String()
}
