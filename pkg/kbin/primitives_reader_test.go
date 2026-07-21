package kbin

import (
	"bytes"
	"testing"
)

// TestReaderBytesNullAndEmpty pins the byte-array readers' handling of the null
// (-1) length and the empty (0) length, including the intentional, non-spec
// EventHubs workaround where the non-nullable Bytes/CompactBytes readers return
// an empty (never nil) slice for a -1 length instead of erroring. See the
// comment on Reader.Bytes.
func TestReaderBytesNullAndEmpty(t *testing.T) {
	for _, tc := range []struct {
		name    string
		src     []byte
		read    func(*Reader) []byte
		want    []byte
		wantNil bool
	}{
		{
			name: "Bytes null (-1) is empty and never nil",
			src:  AppendInt32(nil, -1),
			read: (*Reader).Bytes,
			want: []byte{},
		},
		{
			name: "Bytes normal",
			src:  append(AppendInt32(nil, 3), "abc"...),
			read: (*Reader).Bytes,
			want: []byte("abc"),
		},
		{
			name:    "NullableBytes null (-1) is nil",
			src:     AppendInt32(nil, -1),
			read:    (*Reader).NullableBytes,
			wantNil: true,
		},
		{
			name: "NullableBytes normal",
			src:  append(AppendInt32(nil, 2), "hi"...),
			read: (*Reader).NullableBytes,
			want: []byte("hi"),
		},
		{
			name: "CompactBytes null (uvarint 0 => -1) is empty and never nil",
			src:  AppendUvarint(nil, 0),
			read: (*Reader).CompactBytes,
			want: []byte{},
		},
		{
			name: "CompactBytes empty (uvarint 1 => len 0) is empty and never nil",
			src:  AppendUvarint(nil, 1),
			read: (*Reader).CompactBytes,
			want: []byte{},
		},
		{
			name:    "CompactNullableBytes null (uvarint 0 => -1) is nil",
			src:     AppendUvarint(nil, 0),
			read:    (*Reader).CompactNullableBytes,
			wantNil: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Reader{Src: tc.src}
			got := tc.read(r)
			if err := r.Complete(); err != nil {
				t.Fatalf("Complete() = %v, want nil", err)
			}
			if tc.wantNil {
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("got nil, want %q (this reader must never return nil)", tc.want)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestReaderBytesShortRead ensures a length that exceeds the remaining source
// marks the reader bad (Complete returns ErrNotEnoughData) instead of reading
// past the buffer.
func TestReaderBytesShortRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(*Reader) []byte
	}{
		{"Bytes overruns src", (*Reader).Bytes},
		{"NullableBytes overruns src", (*Reader).NullableBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A length of 5 with no payload following must not be readable.
			r := &Reader{Src: AppendInt32(nil, 5)}
			_ = tc.read(r)
			if r.Ok() {
				t.Fatal("Ok() = true, want false after reading past the buffer")
			}
			if err := r.Complete(); err != ErrNotEnoughData {
				t.Fatalf("Complete() = %v, want ErrNotEnoughData", err)
			}
		})
	}
}
