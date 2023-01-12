package kotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewRecordCarrier(t *testing.T) {
	tests := []struct {
		name   string
		record *kgo.Record
		want   RecordCarrier
	}{
		{
			name:   "Carrier",
			record: kgo.KeyStringRecord("key", "value"),
			want:   RecordCarrier{kgo.KeyStringRecord("key", "value")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewRecordCarrier(tt.record), "NewRecordCarrier(%v)", tt.record)
		})
	}
}

func TestRecordCarrier_Get(t *testing.T) {
	tests := []struct {
		name   string
		record *kgo.Record
		key    string
		want   string
	}{
		{
			name:   "Exists",
			record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key", Value: []byte("value")}}},
			key:    "key",
			want:   "value",
		},
		{
			name:   "Empty",
			record: &kgo.Record{Headers: []kgo.RecordHeader{{}}},
			key:    "key",
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RecordCarrier{
				record: tt.record,
			}
			assert.Equalf(t, tt.want, c.Get(tt.key), "Get(%v)", tt.key)
		})
	}
}

func TestRecordCarrier_Keys(t *testing.T) {
	tests := []struct {
		name   string
		record *kgo.Record
		want   []string
	}{
		{
			name:   "Empty",
			record: &kgo.Record{},
			want:   []string{},
		},
		{
			name:   "Single",
			record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key", Value: []byte("value")}}},
			want:   []string{"key"},
		},
		{
			name: "Multiple",
			record: &kgo.Record{Headers: []kgo.RecordHeader{
				{Key: "key1", Value: []byte("value1")},
				{Key: "key2", Value: []byte("value2")},
			},
			},
			want: []string{"key1", "key2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RecordCarrier{record: tt.record}
			assert.Equalf(t, tt.want, c.Keys(), "Keys()")
		})
	}
}

func TestRecordCarrier_Set(t *testing.T) {
	type kv struct {
		key string
		val string
	}

	tests := []struct {
		name   string
		record *kgo.Record
		kvs    []kv
		want   []kgo.RecordHeader
	}{
		{
			name:   "Set",
			record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key1", Value: []byte("value1")}}},
			kvs: []kv{
				{key: "key1", val: "updated"},
				{key: "key2", val: "value2"},
				{key: "key2", val: "updated"},
				{key: "key3", val: "value3"},
			},
			want: []kgo.RecordHeader{
				{Key: "key1", Value: []byte("updated")},
				{Key: "key2", Value: []byte("updated")},
				{Key: "key3", Value: []byte("value3")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RecordCarrier{record: tt.record}
			for _, kv := range tt.kvs {
				c.Set(kv.key, kv.val)
			}
			assert.ElementsMatch(t, c.record.Headers, tt.want)
		})
	}
}
