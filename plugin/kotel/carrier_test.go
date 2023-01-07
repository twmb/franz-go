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
		kv1    kv
		kv2    kv
		kv3    kv
		kv4    kv
		want   []kgo.RecordHeader
	}{
		{
			name:   "Set",
			record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key1", Value: []byte("value1")}}},
			kv1:    kv{key: "key1", val: "updated"},
			kv2:    kv{key: "key2", val: "value2"},
			kv3:    kv{key: "key2", val: "updated"},
			kv4:    kv{key: "key3", val: "value3"},
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
			c.Set(tt.kv1.key, tt.kv1.val)
			c.Set(tt.kv2.key, tt.kv2.val)
			c.Set(tt.kv3.key, tt.kv3.val)
			c.Set(tt.kv4.key, tt.kv4.val)
			assert.ElementsMatch(t, c.record.Headers, tt.want)
		})
	}
}
