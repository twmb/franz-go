package kgo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func TestRecordFormatter(t *testing.T) {
	r := &Record{
		Key:   []byte("key"),
		Value: []byte("value"),
		Headers: []RecordHeader{
			{"H1", []byte("V1")},
			{"h2", []byte("v2")},
		},
		Timestamp:     time.Unix(17, 0),
		Topic:         "topictopictopictopictopict",
		Partition:     3,
		Attrs:         RecordAttrs{attrs: 0b1011_0011}, // no timestamp type, control, txnal, lz4
		ProducerEpoch: 1,
		ProducerID:    791,
		LeaderEpoch:   -1,
		Offset:        343,
	}

	// Give the record share group context so %D and %A produce values.
	// shareAckFromCtx requires st.cursor to be non-nil to treat the
	// record as a share group record.
	slab := &shareAckSlab{
		states:               []shareAckState{{deliveryCount: 3}},
		records0:             r,
		cursor:               new(shareCursor),
		acqLockDeadlineNanos: time.Unix(42, 0).UnixNano(),
	}
	r.Context = context.WithValue(context.Background(), shareAckKey, slab)
	p := &FetchPartition{
		HighWatermark:    999,
		LastStableOffset: 666,
		LogStartOffset:   333,
	}

	for _, test := range []struct {
		layout string
		rec    *Record // defaults to r if nil
		expR   string
		expP   string // defaults to expR if empty
	}{
		{
			layout: "%v",
			expR:   "value",
		},
		{
			layout: "%v{}",
			expR:   "value",
		},

		{
			layout: "%T{hex16}%t %V{ascii} %v %V{little16} %k %K{big32} %o",
			expR:   "001atopictopictopictopictopict 5 value \x05\x00 key \x00\x00\x00\x03 343",
		},

		{
			layout: "%[ %| %]",
			expR:   "<nil> <nil> <nil>",
			expP:   "333 666 999",
		},

		{
			layout: "%d{strftime## %a ##} %d %d{ascii}",
			expR:   " Thu  17000 17000",
		},

		{
			layout: "%T{ascii} %T{hex64} %T{hex32} %T{hex16} %T{hex8} %T{hex4} %T{hex}",
			expR:   "26 000000000000001a 0000001a 001a 1a a 1a",
		},

		{
			layout: "%K{big64} %K{big32} %K{big16} %K{big8}",
			expR:   "\x00\x00\x00\x00\x00\x00\x00\x03 \x00\x00\x00\x03 \x00\x03 \x03",
		},

		{
			layout: "%K{little64} %K{little32} %K{little16} %K{little8}",
			expR:   "\x03\x00\x00\x00\x00\x00\x00\x00 \x03\x00\x00\x00 \x03\x00 \x03",
		},

		{
			layout: `\t\r\n\\\x00 %{%}%%`,
			expR:   "\t\r\n\\\x00 {}%",
		},

		{
			// \xNN names the full [0,255] byte range; high bytes
			// (0x80-0xff) were previously rejected by the parser.
			layout: `\x00\x7f\x80\xfe\xff`,
			expR:   "\x00\x7f\x80\xfe\xff",
		},

		{
			layout: "%T %K %V %H %p %o %e %i %x %y",
			expR:   "26 3 5 2 3 343 -1 1 791 1",
			expP:   "26 3 5 2 3 343 -1 2 791 1",
		},

		{
			layout: "%k{base64} %k{hex}",
			expR:   "a2V5 6b6579",
		},

		{
			layout: "%v{base64} %v{base64raw}",
			expR:   "dmFsdWU= dmFsdWU",
		},

		{
			layout: "%H %h{ %K{ascii} %k %v %V } %k %v",
			expR:   "2  2 H1 V1 2  2 h2 v2 2  key value",
		},

		{
			layout: "%a{compression}",
			expR:   "lz4",
		},

		{
			layout: "%a{compression;number} %a{transactional-bit;bool} %a{control-bit;hex8} %a{timestamp-type;hex8}",
			expR:   "3 true 01 ff",
		},

		{
			layout: "%a{compression} %a{transactional-bit} %a{control-bit} %a{timestamp-type}",
			expR:   "lz4 1 1 -1",
		},

		// %D and %A use the share group context set up on r.
		{layout: "%D", expR: "3"},
		{layout: "%D{hex8}", expR: "03"},
		{layout: "%A", expR: "42000"},
		{layout: "%A{strftime[%F]}", expR: "1970-01-01"},

		{layout: "%i %i", expR: "1 1", expP: "2 2"},
		{layout: "%h{%i}", expR: "11", expP: "22"},
		{layout: "%h{{%k}}", expR: "{H1}{h2}"},

		// Timestamps outside the UnixNano range.
		{
			layout: "%d %d{big64} %d{little64}",
			rec:    &Record{Timestamp: time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC)},
			expR:   "-11676096000000 \xff\xff\xf5arr\x10\x00 \x00\x10rra\xf5\xff\xff",
		},
		{
			layout: "%d %d{big64} %d{little64}",
			rec:    &Record{Timestamp: time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)},
			expR:   "16725225600000 \x00\x00\x0f6%!t\x00 \x00t!%6\x0f\x00\x00",
		},

		// strftime's 12-hour clock runs 1 through 12; it has no hour 0.
		{
			layout: "%d{strftime[%I %l %r]}",
			rec:    &Record{Timestamp: time.Date(2024, 1, 2, 0, 5, 6, 0, time.UTC)},
			expR:   "12 12 12:05:06 AM",
		},
		{
			layout: "%d{strftime[%I %l %r]}",
			rec:    &Record{Timestamp: time.Date(2024, 1, 2, 12, 5, 6, 0, time.UTC)},
			expR:   "12 12 12:05:06 PM",
		},
		{
			layout: "%d{strftime[%I %l %r]}",
			rec:    &Record{Timestamp: time.Date(2024, 1, 2, 1, 5, 6, 0, time.UTC)},
			expR:   "01  1 01:05:06 AM",
		},
		{
			layout: "%d{strftime[%I %l %r]}",
			rec:    &Record{Timestamp: time.Date(2024, 1, 2, 13, 5, 6, 0, time.UTC)},
			expR:   "01  1 01:05:06 PM",
		},

		// Days before the year's first Sunday (%U) or Monday (%W) are
		// in week 00.
		{
			layout: "%d{strftime[%U %W]}",
			rec:    &Record{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			expR:   "00 01",
		},
		{
			layout: "%d{strftime[%U %W]}",
			rec:    &Record{Timestamp: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)},
			expR:   "00 00",
		},

		// Flags and field widths.
		{
			layout: "%d{strftime[%-d %_m %^a %#b %#Z %5Y %-5d %012F %^c]}",
			rec:    &Record{Timestamp: time.Date(2024, 3, 5, 13, 4, 5, 0, time.UTC)},
			expR:   "5  3 TUE MAR utc 02024     5 002024-03-05 TUE MAR  5 13:04:05 2024",
		},

		// A size is the raw length unless it asks for the encoded one.
		{
			layout: "%V{encoded} %v{base64} %K{ascii;decoded} %k{hex} %V %v{hex}",
			expR:   "8 dmFsdWU= 3 6b6579 5 76616c7565",
		},
		{
			layout: "%V{encoded} %V{hex8;encoded} %v{hex}",
			expR:   "10 0a 76616c7565",
		},

		// %E and %O change nothing in the C locale, and an invalid pair
		// such as %Ed prints as written. %v is BSD's, and %x is %m/%d/%y.
		{
			layout: "%d{strftime[%Ey %Od %Ed %v %x]}",
			rec:    &Record{Timestamp: time.Date(2024, 3, 5, 13, 4, 5, 0, time.UTC)},
			expR:   "24 05 %Ed  5-Mar-2024 03/05/24",
		},
		// Years are not zero padded, and %C rounds down so that
		// 100*%C + %y is the year.
		{
			layout: "%d{strftime[%Y %C %y %F]}",
			rec:    &Record{Timestamp: time.Date(-101, 1, 1, 13, 4, 5, 0, time.UTC)},
			expR:   "-101 -2 99 -101-01-01",
		},

		//
	} {
		f, err := NewRecordFormatter(test.layout)
		if err != nil {
			t.Errorf("%s: unexpected err: %v", test.layout, err)
			continue
		}

		rec := r
		if test.rec != nil {
			rec = test.rec
		}
		gotR := string(f.AppendRecord(nil, rec))
		gotP := string(f.AppendPartitionRecord(nil, p, rec))

		if gotR != test.expR {
			t.Errorf("R[%s]: got %s != exp %s", test.layout, gotR, test.expR)
		}

		// Partition formatting defaults to the record format if the
		// expectation is empty.
		expP := test.expP
		if expP == "" {
			expP = test.expR
		}
		if gotP != expP {
			t.Errorf("P[%s]: got %s != exp %s", test.layout, gotP, expP)
		}
	}

	f, _ := NewRecordFormatter("%h{%k}")
	if got := string(f.AppendRecord(nil, nil)); got != "<nil>" {
		t.Errorf("nil record: got %s != exp <nil>", got)
	}
	for _, layout := range []string{
		"%d{", "%v{", "%T{", "%v%",
		"%v{hex} %V{encoded}",  // no %v after the ;encoded size
		"%h{%p}", "%h{%h{%k}}", // a header has only a key and value
	} {
		if _, err := NewRecordFormatter(layout); err == nil {
			t.Errorf("%s: got nil error, want an error", layout)
		}
	}
}

func TestRecordFormatterUnpack(t *testing.T) {
	for _, test := range []struct {
		layout string
		in     string
		exp    string
	}{
		{
			layout: "%v{unpack#c#}",
			in:     "foo",
			exp:    "f",
		},

		{
			layout: "%v{unpack#cbs#}",
			in:     "foo",
			exp:    "f111o",
		},

		{
			layout: "%v{unpack#cBs#}",
			in:     "foo",
			exp:    "f111o",
		},

		{
			layout: "%v{unpack[ch]}",
			in:     "f\xff\xff",
			exp:    "f-1",
		},
		{
			layout: "%v{unpack[cH]}",
			in:     "f\xff\xff",
			exp:    "f65535",
		},

		{
			layout: "%v{unpack[ci]}",
			in:     "f\xff\xff\xff\xff\xff\xff\xff\xff",
			exp:    "f-1",
		},
		{
			layout: "%v{unpack[cI]}",
			in:     "f\xff\xff\xff\xff\xff\xff\xff\xff",
			exp:    "f4294967295",
		},

		{
			layout: "%v{unpack{{{cq}}}}",
			in:     "f\xff\xff\xff\xff\xff\xff\xff\xff",
			exp:    "f-1",
		},
		{
			layout: "%v{unpack((cQ))}",
			in:     "f\xff\xff\xff\xff\xff\xff\xff\xff",
			exp:    "f18446744073709551615",
		},

		{
			layout: "%v{unpack[x<xH.xx>Hxx.xxHxx.xx<xxHxx$]}",
			in:     "\x00\x01 \x00\x01 \x00\x01 \x00\x01",
			exp:    "256 1 1 256",
		},

		{
			layout: "%v{unpack[bB]}",
			in:     "\xff\xff",
			exp:    "-1255",
		},

		//
	} {
		f, err := NewRecordFormatter(test.layout)
		if err != nil {
			t.Errorf("%s: unexpected err: %v", test.layout, err)
			continue
		}
		got := string(f.AppendRecord(nil, &Record{Value: []byte(test.in)}))
		if got != test.exp {
			t.Errorf("%q: got %s != exp %s", test.in, got, test.exp)
		}
	}
}

func TestRecordReader(t *testing.T) {
	for _, test := range []struct {
		layout string
		in     string
		exp    []*Record
		expErr bool
	}{
		{
			layout: "%v",
			in:     "foo bar biz\nbaz",
			exp:    []*Record{StringRecord("foo bar biz\nbaz")},
		},
		{
			layout: "%v{}",
			in:     "foo bar biz\nbaz",
			exp:    []*Record{StringRecord("foo bar biz\nbaz")},
		},

		{
			layout: "%k %v",
			in:     "foo bar biz\nbaz",
			exp:    []*Record{KeyStringRecord("foo", "bar biz\nbaz")},
		},

		{
			layout: "%k %v\n",
			in:     "foo bar biz\nbaz \n biz\n",
			exp: []*Record{
				KeyStringRecord("foo", "bar biz"),
				KeyStringRecord("baz", ""),
				KeyStringRecord("", "biz"),
			},
		},

		{
			layout: "%t %k %v",
			in:     "foo bar biz",
			exp: []*Record{
				{Topic: "foo", Key: []byte("bar"), Value: []byte("biz")},
			},
		},

		{
			layout: "%T%t %K%k %V{byte}%v",
			in:     "3foo 3bar \x03biz",
			exp: []*Record{
				{Topic: "foo", Key: []byte("bar"), Value: []byte("biz")},
			},
		},

		{
			layout: "%T%to %k %v",
			in:     "3fooo bar biz",
			exp: []*Record{
				{Topic: "foo", Key: []byte("bar"), Value: []byte("biz")},
			},
		},

		{
			layout: "%K{ascii}%k",
			in:     "3foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K%k",
			in:     "3foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{hex64}%k",
			in:     "0000000000000003foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{hex32}%k",
			in:     "00000003foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{hex16}%k",
			in:     "0003foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{hex8}%k",
			in:     "03foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{hex4}%k",
			in:     "3foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{big64}%k",
			in:     "\x00\x00\x00\x00\x00\x00\x00\x03foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{big32}%k",
			in:     "\x00\x00\x00\x03foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{big16}%k",
			in:     "\x00\x03foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{big8}%k",
			in:     "\x03foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{little64}%k",
			in:     "\x03\x00\x00\x00\x00\x00\x00\x00foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{little32}%k",
			in:     "\x03\x00\x00\x00foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{little16}%k",
			in:     "\x03\x00foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{little8}%k",
			in:     "\x03foo",
			exp:    []*Record{KeyStringRecord("foo", "")},
		},
		{
			layout: "%K{3}%kgap%V{3}%v",
			in:     "foogapbar",
			exp:    []*Record{KeyStringRecord("foo", "bar")},
		},

		{
			layout: `\t\r\n\\\x00 %{%}%% %v`,
			in:     "\t\r\n\\\x00 {}% foo",
			exp:    []*Record{StringRecord("foo")},
		},

		{
			layout: "%H{2}%V{ascii}%v%h{%V%v%K%k}",
			in:     "3foo1v1k2vv2kk",
			exp: []*Record{
				{
					Value: []byte("foo"),
					Headers: []RecordHeader{
						{"k", []byte("v")},
						{"kk", []byte("vv")},
					},
				},
			},
		},

		{
			layout: "%V{3}%v bar",
			in:     "foo bar",
			exp:    []*Record{StringRecord("foo")},
		},

		{
			layout: "%V{bool}%v",
			in:     "truee",
			exp:    []*Record{StringRecord("e")},
		},

		{
			layout: "%V{bool}%ve",
			in:     "trueee",
			exp:    []*Record{StringRecord("e")},
		},

		{
			layout: "%V{bool}%v",
			in:     "false",
			exp:    []*Record{StringRecord("")},
		},

		{
			layout: "%V{bool}%v %K{bool}%k",
			in:     "false true1",
			exp:    []*Record{KeyStringRecord("1", "")},
		},

		{
			layout: "%V{bool}%v %K{ascii}%k",
			in:     "false 0",
			exp:    []*Record{KeyStringRecord("", "")},
		},

		{
			layout: "%V %v{hex}asdf",
			in:     "6 6b6579asdf",
			exp:    []*Record{StringRecord("key")},
		},

		{
			layout: "%V %v{base64}asdf",
			in:     "4 a2V5asdf",
			exp:    []*Record{StringRecord("key")},
		},
		{
			layout: "%V %v{base64raw} %k{base64raw}\n",
			in:     "3 Zm8 YmFy\n",
			exp:    []*Record{KeyStringRecord("bar", "fo")},
		},
		{
			layout: "%V{decoded} %v{base64} %K{ascii;encoded} %k{hex}\n",
			in:     "3 Zm9v 6 6b6579\n",
			exp:    []*Record{KeyStringRecord("key", "foo")},
		},

		{
			layout: "%p{hex} %o{hex} %d{go#2006-01-02T15:04:05.000Z07:00#}\n",
			in:     "ff ffffffffffffffff 2024-03-05T13:04:05.123Z\n",
			exp:    []*Record{{Partition: 255, Offset: -1, Timestamp: time.Date(2024, 3, 5, 13, 4, 5, 123e6, time.UTC)}},
		},
		{
			layout: "%d{go#DateTime#} %v\n",
			in:     "2024-03-05 13:04:05 a b\n",
			exp:    []*Record{{Value: []byte("a b"), Timestamp: time.Date(2024, 3, 5, 13, 4, 5, 0, time.UTC)}},
		},
		{
			layout: "%d{go#ANSIC#} %v\n",
			in:     "Tue Mar  5 13:04:05 2024 x\nFri Mar 15 13:04:05 2024 y\n",
			exp: []*Record{
				{Value: []byte("x"), Timestamp: time.Date(2024, 3, 5, 13, 4, 5, 0, time.UTC)},
				{Value: []byte("y"), Timestamp: time.Date(2024, 3, 15, 13, 4, 5, 0, time.UTC)},
			},
		},
		{
			layout: "%d{go#2006#}8%v\n",
			in:     "20188x\n",
			exp:    []*Record{{Value: []byte("x"), Timestamp: time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)}},
		},
		// The shortest timestamp wins, though day 11 with value x writes
		// this too.
		{
			layout: "%d{go#2006-1-2#}1%v\n",
			in:     "2024-3-111x\n",
			exp:    []*Record{{Value: []byte("1x"), Timestamp: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)}},
		},
		// Parse accepts any number of fraction digits, but we look at
		// most 80 bytes ahead.
		{
			layout: "%d{go#15:04:05#} %v\n",
			in:     "13:04:05." + strings.Repeat("0", 80) + " x\n",
			expErr: true,
		},
		{
			layout: "%d{go#DateTime#} %v\n",
			in:     "2024-03-05",
			expErr: true,
		},
		// Less input than the text after the first field.
		{
			layout: "%k, %v\n",
			in:     "a",
			expErr: true,
		},

		{
			layout: "%p %o %e %d %x %y{hex8}",
			in:     "3 4 5 17000 6 0a",
			exp: []*Record{{
				Partition:     3,
				Offset:        4,
				LeaderEpoch:   5,
				Timestamp:     time.Unix(17, 0),
				ProducerID:    6,
				ProducerEpoch: 10,
			}},
		},

		{
			layout: "%e %o %d %p",
			in:     "-1 -2 -3 4",
			exp:    []*Record{{LeaderEpoch: -1, Offset: -2, Timestamp: time.UnixMilli(-3), Partition: 4}},
		},
		{
			layout: "%K %k",
			in:     "-1 x",
			expErr: true,
		},
		{
			layout: "%K %k",
			in:     "3 ",
			expErr: true,
		},
		{
			layout: "%K{3}%k\n",
			in:     "abc",
			expErr: true,
		},
		{
			layout: "%H{big64}%h{%k{re[a*]}%v{re[b*]}}x",
			in:     "\xff\xff\xff\xff\xff\xff\xff\xffx",
			expErr: true,
		},

		// Timestamps outside the UnixNano range.
		{
			layout: "%d{big64}",
			in:     "\xff\xff\xf5arr\x10\x00",
			exp:    []*Record{{Timestamp: time.UnixMilli(-11676096000000)}},
		},
		{
			layout: "%d",
			in:     "16725225600000",
			exp:    []*Record{{Timestamp: time.UnixMilli(16725225600000)}},
		},
		{
			layout: "%d{little64}",
			in:     "\x00t!%6\x0f\x00\x00",
			exp:    []*Record{{Timestamp: time.UnixMilli(16725225600000)}},
		},

		{
			layout: `%v{re[\d{2}]}`,
			in:     "2345",
			exp: []*Record{
				StringRecord("23"),
				StringRecord("45"),
			},
		},

		{
			layout: `%v{re[(\d{2}|asdf)]}`,
			in:     "23asdf45",
			exp: []*Record{
				StringRecord("23"),
				StringRecord("asdf"),
				StringRecord("45"),
			},
		},

		{
			layout: `%v{re[(\d{2}|asdf)]}`,
			in:     "",
			exp:    []*Record{},
		},
		{
			layout: `%v{re[a*]}`,
			in:     "",
			exp:    []*Record{},
		},
		{
			layout: `%v{re[a+]}\n`,
			in:     "aaa\naa\n",
			exp: []*Record{
				StringRecord("aaa"),
				StringRecord("aa"),
			},
		},
		{
			layout: `%k{re[a*]}%v{re[b+]}`,
			in:     "bbbaab",
			exp: []*Record{
				KeyStringRecord("", "bbb"),
				KeyStringRecord("aa", "b"),
			},
		},

		{
			layout: `%K{3}%v{re[.*?\d]}%k`,
			in:     "abcdefg[1aaad2bbb",
			exp: []*Record{
				KeyStringRecord("aaa", "abcdefg[1"),
				KeyStringRecord("bbb", "d2"),
			},
		},

		{
			layout: `%v`,
			in:     "foo\n",
			exp: []*Record{
				StringRecord("foo\n"),
			},
		},

		{
			layout: `%v{re#...#}`,
			in:     "abc123",
			exp: []*Record{
				StringRecord("abc"),
				StringRecord("123"),
			},
		},

		{
			layout: `%v{re#...?#}\n`,
			in:     "abc\n",
			exp: []*Record{
				StringRecord("abc"),
			},
		},

		{
			layout: `%v{re#...?#}\n`,
			in:     "ab\n",
			exp: []*Record{
				StringRecord("ab"),
			},
		},

		// A regexp that matches the empty string consumes nothing from
		// input it does not match. That is an error, not an empty record
		// forever.
		{
			layout: `%v{re[a*]}`,
			in:     "bbb",
			expErr: true,
		},
		{
			layout: `%k{re[a*]}%v{re[b*]}`,
			in:     "abc",
			exp:    []*Record{KeyStringRecord("a", "b")},
			expErr: true,
		},

		{
			layout: `%v{re#....#}`,
			in:     "abc123",
			exp: []*Record{
				StringRecord("abc1"),
			},
			expErr: true,
		},

		{
			layout: `%v{re#....#}`,
			in:     "abc",
			exp:    []*Record{},
			expErr: true,
		},

		{
			layout: `%v\n`,
			in:     "foo",
			expErr: true,
		},

		{
			layout: `%V{4}%v`,
			in:     "fo",
			expErr: true,
		},

		{
			layout: `asdf%vasdf`,
			in:     "asdf123addd",
			expErr: true,
		},

		{
			layout: `asdf%v`,
			in:     "asd",
			expErr: true,
		},

		{
			layout: `asdf%v`,
			in:     "asddd",
			expErr: true,
		},

		{
			layout: `asdf%v`,
			in:     "asddd",
			expErr: true,
		},

		{
			layout: `a%V{bool}%v`,
			in:     "abc",
			expErr: true,
		},

		{
			layout: `a%V{bool}%v`,
			in:     "atruze",
			expErr: true,
		},

		{
			layout: `a%V{bool}%v`,
			in:     "afalsn",
			expErr: true,
		},

		{
			layout: `%o{hex8}`,
			in:     "az",
			expErr: true,
		},

		// some json tests -- more below
		{
			layout: `%v{json} %k{json}`,
			in:     `{"foo": "bar"} true`,
			exp: []*Record{
				KeyStringRecord("true", `{"foo":"bar"}`),
			},
		},
		{
			layout: `%v{json}%k{json}`,
			in:     `3null`,
			exp: []*Record{
				KeyStringRecord("null", "3"),
			},
		},
		{
			layout: `%k{json}\t%v{json}`,
			in:     `true	{ "foo" : "bar", "biz": ["\n", 4.4 , 3,4] }`,
			exp: []*Record{
				KeyStringRecord("true", `{"foo":"bar","biz":["\n",4.4,3,4]}`),
			},
		},

		// Regexps match runes, not bytes. Invalid UTF-8 decodes as U+FFFD
		// with width one, the same as bufio.Reader.ReadRune.
		{
			layout: `%v{re[.]}\n`,
			in:     "\u00e9\n",
			exp:    []*Record{StringRecord("\u00e9")},
		},
		{
			layout: "%v{re[\u00e9+]}\\n",
			in:     "\u00e9\u00e9\n",
			exp:    []*Record{StringRecord("\u00e9\u00e9")},
		},
		{
			layout: `%v{re[\p{L}+]}\n`,
			in:     "a\u00e9\u4e2d\n",
			exp:    []*Record{StringRecord("a\u00e9\u4e2d")},
		},
		{
			layout: `%k{re[.]}%v{re[.]}\n`,
			in:     "\u00e9\u4e2d\n",
			exp:    []*Record{KeyStringRecord("\u00e9", "\u4e2d")},
		},
		{
			layout: `%v{re[.]}`,
			in:     "a\u00e9\u4e2d\U0001f642",
			exp: []*Record{
				StringRecord("a"),
				StringRecord("\u00e9"),
				StringRecord("\u4e2d"),
				StringRecord("\U0001f642"),
			},
		},
		{
			layout: `%v{re[\x{FFFD}+]}\n`,
			in:     "\xff\x80\n",
			exp:    []*Record{StringRecord("\xff\x80")},
		},
		{
			layout: `%v{re[\x{FFFD}+]}`,
			in:     "\xf0\x9f\x99",
			exp:    []*Record{StringRecord("\xf0\x9f\x99")},
		},
		// The default bufio buffer is 4096 bytes: the emoji straddles its
		// end, and the value field must still decode it whole.
		{
			layout: `%k{re[a+]}%v{re[.]}`,
			in:     strings.Repeat("a", 4094) + "\U0001f642",
			exp:    []*Record{KeyStringRecord(strings.Repeat("a", 4094), "\U0001f642")},
		},
		// A match can be longer than that buffer, and the records after it
		// still read.
		{
			layout: `%v{re[a+]}\n`,
			in:     strings.Repeat("a", 5000) + "\naa\n",
			exp:    []*Record{StringRecord(strings.Repeat("a", 5000)), StringRecord("aa")},
		},
		// Every alternative is anchored at the start of the field.
		{
			layout: `%v{re[a|b]}\n`,
			in:     "zzzb\n",
			expErr: true,
		},
		{
			layout: `%v{re[\Q1+1]}\n`, // \Q quotes to the end
			in:     "1+1\n",
			exp:    []*Record{StringRecord("1+1")},
		},

		// A record cut off before a header %H counted is an error, not a
		// record with fewer headers or the end of the input.
		{
			layout: "%H{2}%h{%k=%v;}\n",
			in:     "a=b;c=d;\na=b;",
			exp:    []*Record{{Headers: []RecordHeader{{"a", []byte("b")}, {"c", []byte("d")}}}},
			expErr: true,
		},

		{
			layout: "%p\n",
			in:     "4294967295\n",
			expErr: true,
		},
		{
			layout: "%p{hex}\n",
			in:     "100000001\n",
			expErr: true,
		},
		{
			layout: "%k%p{-1} %v\n",
			in:     "key value\n",
			exp:    []*Record{{Key: []byte("key"), Value: []byte("value"), Partition: -1}},
		},

		// Whitespace after the last JSON value ends the input cleanly.
		{
			layout: `%v{json}`,
			in:     "{\"a\":1}\n",
			exp:    []*Record{StringRecord(`{"a":1}`)},
		},
		{
			layout: `%v{json}%k`,
			in:     "1",
			exp:    []*Record{StringRecord("1")},
		},
		{
			layout: `%V %v{json}`,
			in:     `7 {"a":1}`,
			exp:    []*Record{StringRecord(`{"a":1}`)},
		},
		{
			layout: `%H{1}%h{%v{json}}`,
			in:     "{\"a\":1}\n",
			exp:    []*Record{{Headers: []RecordHeader{{"", []byte(`{"a":1}`)}}}},
		},

		//
	} {
		t.Run(test.layout, func(t *testing.T) {
			r, err := NewRecordReader(strings.NewReader(test.in), test.layout)
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				return
			}
			for i, exp := range test.exp {
				rec, err := r.ReadRecord()
				if err != nil {
					t.Errorf("%d: unable to read record: %v", i, err)
					return
				}
				if !reflect.DeepEqual(rec, exp) {
					t.Errorf("%d:\ngot %#v\nexp %#v", i, rec, exp)
				}
			}

			_, err = r.ReadRecord()
			// If we are expecting an error, we expect this final read to
			// error with something other than io.EOF.
			if test.expErr {
				if err == nil || errors.Is(err, io.EOF) {
					t.Errorf("was expecting an error, got %v", err)
				}
				return
			} else if !errors.Is(err, io.EOF) {
				t.Errorf("got err %v != io.EOF after exhausting records", err)
			}
		})
	}
}

func TestRecordReaderRegexpMismatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		layout string
		in     string
	}{
		{"nonmatch", `%v{re[a+]}`, "bbb"},
		{"nonmatch with delimiter", `%v{re[a+]}\n`, "bbb\n"},
		{"nonmatch non-ASCII", `%v{re[a+]}`, "\u00e9\u00e9\u00e9"},
		{"partial match", `%v{re[a{3}]}`, "aab"},
		{"truncated match", `%v{re[a{3}]}`, "aa"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, err := NewRecordReader(strings.NewReader(test.in), test.layout)
			if err != nil {
				t.Fatal(err)
			}
			// A failed match must not repeatedly return empty records
			// with nil errors while leaving the input untouched.
			for attempt := 0; attempt < 3; attempt++ {
				if _, err := r.ReadRecord(); err == nil || !strings.Contains(err.Error(), "regexp text mismatch") {
					t.Errorf("read %d: got %v, want regexp text mismatch", attempt, err)
				}
			}
		})
	}
}

func TestRecordReaderRegexpReadError(t *testing.T) {
	errRead := errors.New("read failed")
	for _, in := range []string{"", "aa"} {
		t.Run(in, func(t *testing.T) {
			r, err := NewRecordReader(io.MultiReader(strings.NewReader(in), iotest.ErrReader(errRead)), `%v{re[a{3}]}`)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.ReadRecord(); !errors.Is(err, errRead) {
				t.Errorf("got %v, want %v", err, errRead)
			}
		})
	}
}

// Returns data, at most chunk bytes per read if chunk is positive, and err
// with the last of it, once, then io.EOF.
type dataErrReader struct {
	data  string
	err   error
	chunk int
}

func (r *dataErrReader) Read(p []byte) (int, error) {
	if r.chunk > 0 && len(p) > r.chunk {
		p = p[:r.chunk]
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) > 0 {
		return n, nil
	}
	err := r.err
	r.err = io.EOF
	return n, err
}

func TestRecordReaderJson(t *testing.T) {
	tests := []string{
		"",
		"   ",
		" z",
		" 1  ",
		"   {}",
		"   []",
		"   true",
		"   null",
		"   \"n\"",

		// string
		"\"\xe2",       // begin line-sep but invalid finish
		"\"\xe2\x79\"", // begin line-sep but not actually line sep
		`"foo"`,
		"\"\xe2\x80\xa8\xe2\x80\xa9\"", // line-sep and paragraph-sep
		` "\uaaaa" `,
		` "\uaaaa\uaaaa" `,
		` "\`,
		` "\z`,
		" \"f\x00o\"",
		` "foo`,
		` "\uazaa" `,

		// number
		"1",
		"  0 ",
		" 0e1 ",
		" 0.1 ",
		"1.",
		"1",
		" 0e+0 ",
		" -0e+0 ",
		"-0",
		"1e6",
		"1e+6",
		"-1e+6",
		"-0e+6",
		" -103e+1 ",
		"-0.01e+006",
		"-z",
		"-",
		"1e",
		"1e+",
		" 0.3e+1 ",
		" 1e.1 ",
		" 0 ",
		"1.e3",
		"0.1e+6",
		"-0.01e+06",
		"0e+01",

		// object
		"{}",
		`{"foo": 3}`,
		` {}    `,
		strings.Repeat(`{"f":`, 1000) + "{}" + strings.Repeat("}", 1000),
		`{"f":{}}`,
		`{"":3,"":2}`,
		`{"foo": [{"":3, "4": "3"}, 4, {}], "t_wo": 1}`,
		` {"foo": 2,"fudge}`,
		`{{"foo": }}`,
		`{"foo": true, f "a": true}`,
		`{{"foo": [{"":3, 4: "3"}, 4, "5": {4}]}, "t_wo": 1}`,
		`{"\uaaaa\uaaaa" : true}`,
		"{\"\xe2\x80\xa8\xe2\x80\xa9\": true}", // line-sep and paragraph-sep
		"{",
		`{"foo"`,
		`{"foo",f}`,
		`{"foo",`,
		`{"foo"f`,
		"{}",
		`{"":[4.4]}`,
		`{"":[4.4e4]}`,
		`{"":d}`,
		`{"":{}d}`,
		`{"a":true,"b":1e+5}`,
		`[false,1e5]`,
		`[null,2E-1]`,

		// array
		`[]`,
		`[[]]`,
		`[ ]`,
		`[ 1, {}]`,
		strings.Repeat("[", 1000) + strings.Repeat("]", 1000),
		`[1, 2, 3, 4, {}]`,
		`[`,
		`[1,`,
		`[1a`,
		`[1a]`,
		`[]`,

		// boolean
		"true",
		"   true ",
		"tru",
		"false",
		"  true ",
		"fals",
		"false",

		// null
		"null ",
		" null ",
		"nul",
		" null ",
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r, err := NewRecordReader(strings.NewReader(test), "%v{json}")
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				return
			}

			in := []byte(test)
			valid := json.Valid(in)
			expErr := !valid
			rec, err := r.ReadRecord()
			gotErr := err != nil

			if expErr != gotErr {
				t.Errorf("got err? %v, exp err? %v", gotErr, expErr)
				return
			}
			if expErr {
				return
			}

			var exp bytes.Buffer
			json.Compact(&exp, in)
			if !bytes.Equal(rec.Value, exp.Bytes()) {
				t.Errorf("got %q, exp %q", rec.Value, exp.Bytes())
			}
		})
	}
}

// The fields of a record that RecordReader can read.
type recordFields struct {
	t, k, v, h, d string
	p, e          int32
	o, x          int64
	y             int16
}

func fieldsOf(r *Record) recordFields {
	var h []byte
	for _, hdr := range r.Headers {
		h = strconv.AppendQuote(h, hdr.Key)
		h = strconv.AppendQuote(h, string(hdr.Value))
	}
	return recordFields{r.Topic, string(r.Key), string(r.Value), string(h), r.Timestamp.UTC().Format(time.RFC3339Nano), r.Partition, r.LeaderEpoch, r.Offset, r.ProducerID, r.ProducerEpoch}
}

// Ensure RecordReader reads back what RecordFormatter writes with the same
// layout. Every layout carries every field the reader can read.
func FuzzRecordRoundTrip(f *testing.F) {
	const nums = "%p %o %e %d %x %y"
	layouts := []string{
		"%T{big32}%t%K{big32}%k%V{big32}%v%H{big32}%h{%K{big32}%k%V{big32}%v}%p{big32}%o{big64}%e{big32}%d{big64}%x{big64}%y{big16}",
		"%T{little32}%t%K{little64}%k%V{little32}%v%H{little16}%h{%K{little32}%k%V{little32}%v}%p{little32}%o{little64}%e{little32}%d{little64}%x{little64}%y{little16}",
		"%T{hex32}%t%K{hex64}%k%V{hex64}%v%H{hex16}%h{%K{hex32}%k%V{hex32}%v}%p{hex32}%o{hex64}%e{hex32}%d{hex64}%x{hex64}%y{hex16}",
		"%T{decoded} %t{base64} %K{encoded} %k{hex} %V{big32;decoded}%v{base64raw} %H %h{%K{decoded} %k{base64raw} %V{hex32;encoded} %v{hex} } " + nums + "\n",
		"%t{hex} %k{hex} %v{base64} %H %h{%k{base64}:%v{hex};} " + nums + "\n",
		"%T %t %K %k %V %v %H %h{%K %k %V %v} %p{hex} %o{hex} %e{hex} %x{hex} %y{hex} %d{go#2006-01-02T15:04:05.000Z07:00#}\n",
		"%d{go#RFC3339Nano#} %T %t %K %k %V %v %H %h{%K %k %V %v} %p %o %e %x %y %d{go#Mon Jan _2 15:04:05.000 MST 2006#}\n",
	}
	for i := range layouts {
		f.Add(uint8(i), "topic", []byte("key"), []byte("value"), "hk", []byte("hv"), uint8(2), int32(-3), int64(4), int32(-1), int64(1700000000123), int64(-1), int16(-1))
		f.Add(uint8(i), "", []byte{}, []byte{}, "", []byte{}, uint8(0), int32(0), int64(0), int32(0), int64(0), int64(0), int16(0))
	}

	f.Fuzz(func(t *testing.T, li uint8, topic string, key, value []byte, hk string, hv []byte, nh uint8, p int32, o int64, e int32, ts, x int64, y int16) {
		layout := layouts[int(li)%len(layouts)]
		if strings.Contains(layout, "%d{go") {
			const yearTenThousand = 253402300800000 // in millis; Go's Parse wants a four digit year
			ts = (ts%yearTenThousand + yearTenThousand) % yearTenThousand
		}
		rec := &Record{Topic: topic, Key: key, Value: value, Partition: p, Offset: o, LeaderEpoch: e, Timestamp: time.UnixMilli(ts), ProducerID: x, ProducerEpoch: y}
		for range nh % 4 {
			rec.Headers = append(rec.Headers, RecordHeader{hk, hv})
		}

		fm, err := NewRecordFormatter(layout)
		if err != nil {
			t.Fatalf("%q: %v", layout, err)
		}
		out := fm.AppendRecord(nil, rec)
		r, err := NewRecordReader(bytes.NewReader(out), layout)
		if err != nil {
			t.Fatalf("%q: %v", layout, err)
		}
		got, err := r.ReadRecord()
		if err != nil {
			t.Fatalf("%q: reading %q: %v", layout, out, err)
		}
		if g, w := fieldsOf(got), fieldsOf(rec); g != w {
			t.Fatalf("%q: reading %q:\ngot  %+v\nwant %+v", layout, out, g, w)
		}
		if _, err := r.ReadRecord(); !errors.Is(err, io.EOF) {
			t.Fatalf("%q: reading %q: second read got %v, want io.EOF", layout, out, err)
		}
	})
}

// Ensure RecordReader reads the same records from any input, whether the input
// comes in one read or in reads of any one size, and returns an error from the
// input rather than io.EOF. FuzzRecordRoundTrip cannot check this: the
// formatter writes only complete records and no regular expressions.
func FuzzRecordReader(f *testing.F) {
	errInput := errors.New("input failed")
	inputs := []struct{ layout, seed string }{
		{`%v{re[a+]}\n`, "aa\naaa\n"},
		{`%k{re[a*]}%v{re[b+]}`, "abbaab"},
		{`%K{3}%v{re[.*?\d]}%k`, "ab1key"},
		{`%v{re[\p{L}+]}\n`, "h\u00e9llo\nw\u00f6rld\n"},
		{`%H{big64}%h{%k{re[a*]}%v{re[b*]}}x`, "\x00\x00\x00\x00\x00\x00\x00\x01aabx"},
		{`%d{go#DateTime#} %v\n`, "2024-03-05 13:04:05 a b\n"},
		{`%d{go#2006-1-2#}1%v\n`, "2024-3-111x\n"},
		{`%T %t %K %k %V %v %H %h{%K %k %V %v} %p %o %e %d %x %y\n`, "5 topic 3 key 5 value 1 2 hk 2 hv 1 2 3 4 5 6\n"},
		{`%K{big16}%k%V{little32}%v`, "\x00\x03key\x05\x00\x00\x00value"},
		{`%V{decoded} %v{base64} %K{ascii;encoded} %k{hex}\n`, "5 dmFsdWU= 6 6b6579\n"},
		{`%p %o %e %d %x %y{hex8}`, "1 2 3 4 5 0a"},
		{`%v{json} %k{json}`, `{"a":[1,"x"]} "k" 2 null`},
		{`%k %v`, "key a value"},
		{`%v{re[a+]}\n`, strings.Repeat("a", 5000) + "\naa"},                // a match larger than the buffer
		{`%V{little32}%v`, "\x10\x27\x00\x00" + strings.Repeat("a", 10000)}, // a value larger than the buffer
		{`%k{re[a*]}%v{re[b+]}`, "bab"},                                     // the regexp reads past its match into the end
	}
	for i, in := range inputs {
		f.Add(uint8(i), []byte(in.seed), uint8(0), false)
		f.Add(uint8(i), []byte(in.seed), uint8(1), false)
		f.Add(uint8(i), []byte(in.seed), uint8(0), true)
	}

	f.Fuzz(func(t *testing.T, li uint8, in []byte, chunk uint8, inject bool) {
		layout := inputs[int(li)%len(inputs)].layout
		read := func(src io.Reader) ([]recordFields, error) {
			r, err := NewRecordReader(src, layout)
			if err != nil {
				t.Fatalf("%q: %v", layout, err)
			}
			var recs []recordFields
			for range len(in) + 1 { // every record reads at least one byte
				rec, err := r.ReadRecord()
				if err != nil {
					return recs, err
				}
				recs = append(recs, fieldsOf(rec))
			}
			t.Fatalf("%q: reading %q: more records than bytes", layout, in)
			return nil, nil
		}
		want, wantErr := read(bytes.NewReader(in))
		srcErr := io.EOF
		if inject {
			srcErr = errInput
		}
		got, gotErr := read(&dataErrReader{string(in), srcErr, int(chunk)})

		ok := slices.Equal(got, want) && gotErr.Error() == wantErr.Error()
		if inject {
			// The last record can need io.EOF to end, as a last %v does.
			short := len(got) == len(want)-1 && slices.Equal(got, want[:len(got)])
			ended := errors.Is(wantErr, io.EOF) || errors.Is(wantErr, io.ErrUnexpectedEOF)
			ok = (slices.Equal(got, want) || short) &&
				(errors.Is(gotErr, errInput) || !short && !ended && gotErr.Error() == wantErr.Error())
		}
		if !ok {
			t.Fatalf("%q: reading %q, chunk %d, inject %v:\ngot  %+v, %v\nwant %+v, %v", layout, in, chunk, inject, got, gotErr, want, wantErr)
		}
	})
}

func BenchmarkFormatter(b *testing.B) {
	buf := make([]byte, 1024)
	r := &Record{
		Key:   []byte(strings.Repeat("key", 10)),
		Value: []byte(strings.Repeat("foo", 40)),
	}
	for _, fmt := range []string{
		"%v\n",
		"%k %v",
		"%k %v %o",
	} {
		b.Run(fmt, func(b *testing.B) {
			f, _ := NewRecordFormatter(fmt)
			for i := 0; i < b.N; i++ {
				buf = f.AppendRecord(buf[:0], r)
			}
		})
	}
}

func BenchmarkStrftime(b *testing.B) {
	t := time.Date(2024, 3, 5, 13, 4, 5, 0, time.UTC)
	buf := make([]byte, 0, 256)
	for _, f := range []string{
		"%F %T",
		"%Y-%m-%dT%H:%M:%S%z",
		"%c",
		"%a %b %e %H:%M:%S %Y",
		"%s",
	} {
		b.Run(f, func(b *testing.B) {
			for b.Loop() {
				buf = strftimeAppendFormat(buf[:0], f, t)
			}
		})
	}
}

func BenchmarkRecordReader(b *testing.B) {
	into := new(Record)
	for _, bench := range []struct {
		fmt   string
		input string
	}{
		{"%v\n", "foo bar biz baz\n"},
		{"%V{16}%v", "0123456789abcdef"},
		{"%k %o %v\n", "key 30 " + strings.Repeat("val", 40) + "\n"},
		{"%k %p{2}%o{hex8} %v\n", "key 30 " + strings.Repeat("val", 40) + "\n"},
	} {
		b.Run(bench.fmt, func(b *testing.B) {
			s := strings.NewReader(bench.input)
			r, _ := NewRecordReader(s, bench.fmt)
			for i := 0; i < b.N; i++ {
				s.Reset(bench.input)
				if err := r.ReadRecordInto(into); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// An invalid layout must yield an error, never a panic. The reader's unknown
// escape arm sliced layout[:1] after layout was advanced past the verb, so an
// unknown verb at the end of the layout (e.g. "%q") indexed an empty string.
// The size-after-value arms (e.g. "%t%T") relied on a bits flag that a typo
// (bit.set(bit) instead of bits.set(bit)) never set, so those layouts were
// silently accepted.
func TestNewRecordReaderRejectsBadLayouts(t *testing.T) {
	for _, layout := range []string{
		"%q",         // unknown verb at end of layout
		"%qx",        // unknown verb mid-layout
		"%t%T",       // topic size spec after topic value spec
		"%v%V",       // value size spec after value value spec
		"%k%K",       // key size spec after key value spec
		"%p{3}",      // only a fixed-number verb: reads nothing, would loop forever
		"%T{3}",      // only a fixed-size spec with no value verb: reads nothing
		"%p{3}%o{4}", // all fixed-number verbs: reads nothing
		"%v{",        // unclosed brace at end of layout
		"%k%v",       // %k would read the input that %v needs
		"%H%h{%t}",   // a header spec setting the topic

		"%V{decoded} %v{json}", // json has no decoded size
		"%v{re[a)|(b]}",        // escapes the anchoring group
		"%H %h{%k=%v}\n",       // the first header's value reads the rest
		"%d{go{}} %v",          // an empty Go layout

		"%d{go#" + strings.Repeat("2006", 21) + "#} %v", // can format to more than 80 bytes
	} {
		t.Run(layout, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("NewRecordReader(%q) panicked: %v", layout, p)
				}
			}()
			if _, err := NewRecordReader(strings.NewReader(""), layout); err == nil {
				t.Errorf("NewRecordReader(%q): got nil error, want an error", layout)
			}
		})
	}
}

func TestRecordReaderReuse(t *testing.T) {
	r, _ := NewRecordReader(strings.NewReader("1 a=b;\n1 c=d;\n"), "%H %h{%k=%v;}\n")
	var rec Record
	for range 2 {
		if err := r.ReadRecordInto(&rec); err != nil {
			t.Fatal(err)
		}
	}
	if exp := []RecordHeader{{"c", []byte("d")}}; !reflect.DeepEqual(rec.Headers, exp) {
		t.Errorf("got headers %v != exp %v", rec.Headers, exp)
	}

	// A read that EOFs partway through a bool must not leave state for
	// the next reader.
	r, _ = NewRecordReader(strings.NewReader("tr"), "%p{bool}")
	if _, err := r.ReadRecord(); err == nil {
		t.Error("tr: got nil error, want an error")
	}
	r.SetReader(strings.NewReader("true"))
	if rec, err := r.ReadRecord(); err != nil || rec.Partition != 1 {
		t.Errorf("true: got partition %d, err %v, want 1, nil", rec.Partition, err)
	}
}

// A size verb whose value overflows int (here math.MaxUint64) must produce a
// read error, not a panic in make([]byte, n) with a wrapped-negative length.
func TestRecordReaderOverflowingSizeNoPanic(t *testing.T) {
	r, err := NewRecordReader(strings.NewReader("18446744073709551615x"), "%V%v")
	if err != nil {
		t.Fatalf("unexpected layout parse error: %v", err)
	}
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("ReadRecord panicked on an overflowing size verb: %v", p)
		}
	}()
	if _, err := r.ReadRecord(); err == nil {
		t.Error("got nil error reading an overflowing size verb, want an error")
	}
}

// A record whose input ends at its last field, after an earlier field consumed
// bytes, must surface as io.ErrUnexpectedEOF (the documented mid-record EOF)
// rather than panicking in the binary decoder on the empty buffer that the
// io.EOF fall-through in next() hands it, failing to parse the empty field, or
// returning the record without it.
func TestRecordReaderTruncatedLastField(t *testing.T) {
	for _, test := range []struct {
		name   string
		layout string
		in     string
	}{
		{"big64", "%p{big32}%o{big64}", "\x00\x00\x00\x01"},                 // partition read, 8-byte offset truncated to 0 bytes
		{"little64", "%p{big32}%o{little64}", "\x00\x00\x00\x01"},           // same, little endian
		{"big32", "%o{big64}%p{big32}", "\x00\x00\x00\x00\x00\x00\x00\x01"}, // offset read, 4-byte partition truncated
		{"big16", "%k:%e{big16}", "key:"},                                   // key read via delim, 2-byte epoch truncated
		{"byte", "%k:%p{byte}", "key:"},                                     // key read via delim, 1-byte partition truncated
		{"ascii", "%k %o", "k "},                                            // ascii offset truncated to 0 bytes
		{"re", "%k %v{re[a+]}", "k "},                                       // value regexp truncated to 0 bytes
		{"headers", "%H %h{%k=%v;}", "1 "},                                  // one header counted, none present
		{"time", "%k %d{go#DateTime#} ", "k 2024-01-02 15:0"},               // timestamp truncated after its own space
	} {
		t.Run(test.name, func(t *testing.T) {
			r, err := NewRecordReader(strings.NewReader(test.in), test.layout)
			if err != nil {
				t.Fatalf("unexpected layout parse error: %v", err)
			}
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("ReadRecord panicked on a truncated read: %v", p)
				}
			}()
			if _, err := r.ReadRecord(); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("got err %v, want io.ErrUnexpectedEOF", err)
			}
			if _, err := r.ReadRecord(); err != io.EOF {
				t.Errorf("second read got err %v, want io.EOF", err)
			}
		})
	}
}

// reReader must decode runes the way strings.Reader does: whole UTF-8
// sequences, and one U+FFFD per byte of anything invalid or truncated. It
// must also leave the input in place for readRe to discard.
func TestReReaderReadRune(t *testing.T) {
	readErr := errors.New("read error")
	for _, in := range []string{
		"", "ascii", "a\u00e9\u4e2d\U0001f642\ufffdz",
		"\xff\x80", "\xc0\xaf", "\xed\xa0\x80", "\xf4\x90\x80\x80",
		"\xc3", "\xe4\xb8", "\xf0\x9f\x99", "\xe4a", "\u00e9\xf0\x9f\x99",
	} {
		for _, terminal := range []error{io.EOF, readErr} {
			t.Run(strconv.Quote(in)+"/"+terminal.Error(), func(t *testing.T) {
				r, err := NewRecordReader(io.MultiReader(strings.NewReader(in), iotest.ErrReader(terminal)), "%v")
				if err != nil {
					t.Fatal(err)
				}
				re := reReader{r: r}
				want := strings.NewReader(in)
				for {
					expRune, expSize, expErr := want.ReadRune()
					gotRune, gotSize, err := re.ReadRune()
					if expErr != nil {
						if err != terminal || gotSize != 0 {
							t.Fatalf("after input: got (%U, %d, %v), want error %v", gotRune, gotSize, err, terminal)
						}
						break
					}
					if gotRune != expRune || gotSize != expSize || err != nil {
						t.Fatalf("got (%U, %d, %v), want (%U, %d, nil)", gotRune, gotSize, err, expRune, expSize)
					}
				}
				if peek, err := r.r.Peek(len(in)); err != nil || string(peek) != in {
					t.Errorf("input consumed: got (%q, %v), want (%q, nil)", peek, err, in)
				}
			})
		}
	}
}
