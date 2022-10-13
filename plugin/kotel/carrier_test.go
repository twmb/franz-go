package kotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewRecordCarrier(t *testing.T) {
	type args struct {
		record *kgo.Record
	}
	tests := []struct {
		name string
		args args
		want RecordCarrier
	}{
		{
			name: "Carrier",
			args: args{record: kgo.KeyStringRecord("key", "value")},
			want: RecordCarrier{kgo.KeyStringRecord("key", "value")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewRecordCarrier(tt.args.record), "NewRecordCarrier(%v)", tt.args.record)
		})
	}
}

func TestRecordCarrier_Get(t *testing.T) {
	type fields struct {
		record *kgo.Record
	}
	type args struct {
		key string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "Exists",
			fields: fields{record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key", Value: []byte("value")}}}},
			args:   args{key: "key"},
			want:   "value",
		},
		{
			name:   "Empty",
			fields: fields{record: &kgo.Record{Headers: []kgo.RecordHeader{{}}}},
			args:   args{key: "key"},
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RecordCarrier{
				record: tt.fields.record,
			}
			assert.Equalf(t, tt.want, c.Get(tt.args.key), "Get(%v)", tt.args.key)
		})
	}
}

func TestRecordCarrier_Keys(t *testing.T) {
	type fields struct {
		record *kgo.Record
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name:   "Empty",
			fields: fields{record: &kgo.Record{}},
			want:   []string{},
		},
		{
			name:   "Single",
			fields: fields{record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key", Value: []byte("value")}}}},
			want:   []string{"key"},
		},
		{
			name: "Multiple",
			fields: fields{
				record: &kgo.Record{Headers: []kgo.RecordHeader{
					{Key: "key1", Value: []byte("value1")},
					{Key: "key2", Value: []byte("value2")},
				}},
			},
			want: []string{"key1", "key2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RecordCarrier{record: tt.fields.record}
			assert.Equalf(t, tt.want, c.Keys(), "Keys()")
		})
	}
}

func TestRecordCarrier_Set(t *testing.T) {
	type fields struct {
		record *kgo.Record
	}
	type args struct {
		key1 string
		val1 string
		key2 string
		val2 string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []kgo.RecordHeader
	}{
		{
			name:   "Set",
			fields: fields{record: &kgo.Record{Headers: []kgo.RecordHeader{{Key: "key1", Value: []byte("value1")}}}},
			args:   args{key1: "key1", val1: "value1", key2: "key2", val2: "value2"},
			want: []kgo.RecordHeader{
				{Key: "key1", Value: []byte("value1")},
				{Key: "key2", Value: []byte("value2")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RecordCarrier{record: tt.fields.record}
			c.Set(tt.args.key1, tt.args.val1)
			c.Set(tt.args.key2, tt.args.val2)

			assert.ElementsMatch(t, c.record.Headers, tt.want)
		})
	}
}
