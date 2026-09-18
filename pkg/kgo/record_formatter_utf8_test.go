package kgo

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func TestRecordReaderRegexpUTF8(t *testing.T) {
	for _, test := range []struct {
		name   string
		layout string
		in     string
		exp    []*Record
	}{
		{"ascii", `%v{re[.]}\n`, "a\n", []*Record{StringRecord("a")}},
		{"two bytes", `%v{re[.]}\n`, "\u00e9\n", []*Record{StringRecord("\u00e9")}},
		{"three bytes", `%v{re[.]}\n`, "\u4e2d\n", []*Record{StringRecord("\u4e2d")}},
		{"four bytes", `%v{re[.]}\n`, "\U0001f642\n", []*Record{StringRecord("\U0001f642")}},
		{"literal", "%v{re[\u00e9+]}\\n", "\u00e9\u00e9\n", []*Record{StringRecord("\u00e9\u00e9")}},
		{"class", `%v{re[\p{L}+]}\n`, "a\u00e9\u4e2d\n", []*Record{StringRecord("a\u00e9\u4e2d")}},
		{"mixed", `%v{re[.+]}\n`, "a\u00e9\u4e2d\U0001f642z\n", []*Record{StringRecord("a\u00e9\u4e2d\U0001f642z")}},
		{"unicode delimiter", "%v{re[a+]}\U0001f642", "aa\U0001f642", []*Record{StringRecord("aa")}},
		{"adjacent fields", `%k{re[.]}%v{re[.]}\n`, "\u00e9\u4e2d\n", []*Record{KeyStringRecord("\u00e9", "\u4e2d")}},
		{"successive records", `%v{re[.]}`, "a\u00e9\u4e2d\U0001f642", []*Record{StringRecord("a"), StringRecord("\u00e9"), StringRecord("\u4e2d"), StringRecord("\U0001f642")}},
		{"eof", `%v{re[.+]}`, "\u00e9\u4e2d\U0001f642", []*Record{StringRecord("\u00e9\u4e2d\U0001f642")}},
		{"empty", `%v{re[.]}`, "", nil},
		{"invalid", `%v{re[\x{FFFD}+]}\n`, "\xff\x80\n", []*Record{StringRecord("\xff\x80")}},
		{"replacement rune", `%v{re[.]}\n`, "\ufffd\n", []*Record{StringRecord("\ufffd")}},
		{"truncated eof", `%v{re[\x{FFFD}+]}`, "\xf0\x9f\x99", []*Record{StringRecord("\xf0\x9f\x99")}},
		{"truncated lookahead", `%k{re[.]}%v{re[\x{FFFD}+]}`, "\u00e9\xf0\x9f\x99", []*Record{KeyStringRecord("\u00e9", "\xf0\x9f\x99")}},
	} {
		for _, source := range []struct {
			name string
			wrap func(io.Reader) io.Reader
		}{
			{"normal", func(r io.Reader) io.Reader { return r }},
			{"one byte", iotest.OneByteReader},
			{"data and EOF", iotest.DataErrReader},
		} {
			t.Run(test.name+"/"+source.name, func(t *testing.T) {
				r, err := NewRecordReader(source.wrap(strings.NewReader(test.in)), test.layout)
				if err != nil {
					t.Fatal(err)
				}
				for i, exp := range test.exp {
					rec, err := r.ReadRecord()
					if err != nil {
						t.Fatalf("record %d: %v", i, err)
					}
					if !reflect.DeepEqual(rec, exp) {
						t.Errorf("record %d: got %#v, want %#v", i, rec, exp)
					}
				}
				if _, err := r.ReadRecord(); err != io.EOF {
					t.Errorf("after last record: got %v, want EOF", err)
				}
			})
		}
	}
}

func TestReReaderReadRune(t *testing.T) {
	readErr := errors.New("read error")
	for _, in := range []string{
		"", "ascii", "a\u00e9\u4e2d\U0001f642\ufffdz",
		"\xff\x80", "\xc0\xaf", "\xed\xa0\x80", "\xf4\x90\x80\x80",
		"\xc3", "\xe4\xb8", "\xf0\x9f\x99", "\xe4a", "\u00e9\xf0\x9f\x99",
	} {
		for _, terminal := range []error{io.EOF, readErr} {
			t.Run(fmt.Sprintf("%x/%v", in, terminal), func(t *testing.T) {
				source := io.MultiReader(iotest.OneByteReader(strings.NewReader(in)), iotest.ErrReader(terminal))
				r := reReader{r: &RecordReader{r: bufio.NewReader(source)}}
				want := bufio.NewReader(strings.NewReader(in))
				for {
					expRune, expSize, expErr := want.ReadRune()
					gotRune, gotSize, err := r.ReadRune()
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
				if peek, err := r.r.r.Peek(len(in)); err != nil || string(peek) != in {
					t.Errorf("input consumed: got (%q, %v), want (%q, nil)", peek, err, in)
				}
			})
		}
	}
}

func TestReReaderReadRuneBufferFull(t *testing.T) {
	in := strings.Repeat("a", 14) + "\U0001f642"
	r := reReader{r: &RecordReader{r: bufio.NewReaderSize(strings.NewReader(in), 16)}}
	for i := 0; i < 14; i++ {
		if got, size, err := r.ReadRune(); got != 'a' || size != 1 || err != nil {
			t.Fatalf("rune %d: got (%U, %d, %v), want ('a', 1, nil)", i, got, size, err)
		}
	}
	if got, size, err := r.ReadRune(); size != 0 || err != bufio.ErrBufferFull {
		t.Errorf("incomplete rune at buffer limit: got (%U, %d, %v), want ErrBufferFull", got, size, err)
	}
}
