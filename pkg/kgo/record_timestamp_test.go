package kgo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"
)

func TestRecordTimestampMilliseconds(t *testing.T) {
	for _, test := range []struct {
		name      string
		timestamp time.Time
		millis    int64
	}{
		{"before nanosecond range", time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), -11676096000000},
		{"epoch", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{"modern", time.Date(2024, 1, 1, 0, 0, 0, 123000000, time.UTC), 1704067200123},
		{"after nanosecond range", time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC), 16725225600000},
	} {
		for _, layout := range []string{"%d", "%d{ascii}", "%d{big64}", "%d{little64}"} {
			t.Run(test.name+"/"+layout, func(t *testing.T) {
				want := []byte(strconv.FormatInt(test.millis, 10))
				switch layout {
				case "%d{big64}":
					want = binary.BigEndian.AppendUint64(nil, uint64(test.millis))
				case "%d{little64}":
					want = binary.LittleEndian.AppendUint64(nil, uint64(test.millis))
				}
				formatter, err := NewRecordFormatter(layout)
				if err != nil {
					t.Fatal(err)
				}
				if got := formatter.AppendRecord(nil, &Record{Timestamp: test.timestamp}); !bytes.Equal(got, want) {
					t.Errorf("formatted timestamp = %q, want %q", got, want)
				}

				// The ASCII reader accepts unsigned numbers; negative
				// timestamps are read through the binary formats.
				if test.millis < 0 && (layout == "%d" || layout == "%d{ascii}") {
					return
				}
				reader, err := NewRecordReader(bytes.NewReader(want), layout)
				if err != nil {
					t.Fatal(err)
				}
				record, err := reader.ReadRecord()
				if err != nil {
					t.Fatal(err)
				}
				if !record.Timestamp.Equal(test.timestamp) {
					t.Errorf("read timestamp = %s, want %s", record.Timestamp.UTC(), test.timestamp)
				}
				if _, err := reader.ReadRecord(); !errors.Is(err, io.EOF) {
					t.Errorf("reading past the timestamp: got %v, want EOF", err)
				}
			})
		}
	}
}
