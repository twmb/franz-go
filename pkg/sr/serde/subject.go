package serde

import (
	"errors"
	"fmt"
	"strings"
)

// Part is whether a subject names a record key or value.
type Part int

const (
	// PartValue derives subjects for record values.
	PartValue Part = iota
	// PartKey derives subjects for record keys.
	PartKey
)

// Strategy selects how a Kafka topic maps to a schema registry subject.
type Strategy int

const (
	// TopicName uses "{topic}-value" or "{topic}-key".
	TopicName Strategy = iota
	// RecordName uses the fully qualified protobuf / Avro record name.
	RecordName
	// TopicRecordName uses "{topic}-{recordName}".
	TopicRecordName
)

// Subject returns the schema registry subject for topic, part, strategy, and
// optional recordName (required for RecordName and TopicRecordName).
func Subject(topic string, part Part, strategy Strategy, recordName string) (string, error) {
	switch strategy {
	case TopicName:
		if part == PartKey {
			return topic + "-key", nil
		}
		return topic + "-value", nil
	case RecordName:
		recordName = strings.TrimSpace(recordName)
		if recordName == "" {
			return "", errors.New("record name is required for RecordName subject strategy")
		}
		return recordName, nil
	case TopicRecordName:
		recordName = strings.TrimSpace(recordName)
		if recordName == "" {
			return "", errors.New("record name is required for TopicRecordName subject strategy")
		}
		return topic + "-" + recordName, nil
	default:
		return "", fmt.Errorf("unknown subject strategy %d", strategy)
	}
}
