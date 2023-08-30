package kgo

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
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
	p := &FetchPartition{
		HighWatermark:    999,
		LastStableOffset: 666,
		LogStartOffset:   333,
	}

	for _, test := range []struct {
		layout string
		expR   string
		expP   string // defaults to expR if empty
	}{
		{
			layout: "%v",
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

		//
	} {
		f, err := NewRecordFormatter(test.layout)
		if err != nil {
			t.Errorf("%s: unexpected err: %v", test.layout, err)
			continue
		}

		gotR := string(f.AppendRecord(nil, r))
		gotP := string(f.AppendPartitionRecord(nil, p, r))

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
			// not be io.EOF.
			if test.expErr {
				if errors.Is(err, io.EOF) {
					t.Error("was expecting an error, got io.EOF")
				}
				return
			} else if !errors.Is(err, io.EOF) {
				t.Errorf("got err %v != io.EOF after exhausting records", err)
			}
		})
	}
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
			if expErr || !gotErr {
				return
			}

			if !bytes.Equal(rec.Value, in) {
				t.Errorf("got %q, exp %q", rec.Value, in)
			}
		})
	}
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
