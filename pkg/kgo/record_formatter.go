package kgo

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"
)

////////////
// WRITER //
////////////

// RecordFormatter formats records.
type RecordFormatter struct {
	fns   []func([]byte, *FetchPartition, *Record) []byte
	calls int64
}

// AppendRecord appends a record to b given the parsed format and returns the
// updated slice.
func (f *RecordFormatter) AppendRecord(b []byte, r *Record) []byte {
	for _, fn := range f.fns {
		b = fn(b, nil, r)
	}
	return b
}

// AppendPartitionRecord appends a record and partition to b given the parsed
// format and returns the updated slice.
func (f *RecordFormatter) AppendPartitionRecord(b []byte, p *FetchPartition, r *Record) []byte {
	for _, fn := range f.fns {
		b = fn(b, p, r)
	}
	return b
}

// NewRecordFormatter returns a formatter for the given layout, or an error if
// the layout is invalid.
//
// The formatter is very powerful, as such there is a lot to describe. This
// documentation attempts to be as succinct as possible.
//
// Similar to the fmt package, record formatting is based off of slash escapes
// and percent "verbs" (copying fmt package lingo). Slashes are used for common
// escapes,
//
//     \t \n \r \\ \xNN
//
// printing tabs, newlines, carriage returns, slashes, and hex encoded
// characters.
//
// Percent encoding opts in to printing aspects of either a record or a fetch
// partition:
//
//     %t    topic
//     %T    topic length
//     %k    key
//     %K    key length
//     %v    value
//     %V    value length
//     %h    begin the header specification
//     %H    number of headers
//     %p    partition
//     %o    offset
//     %e    leader epoch
//     %d    timestamp (date, formatting described below)
//     %x    producer id
//     %y    producer epoch
//
// For AppendPartitionRecord, the formatter also undersands the following three
// formatting options:
//
//     %[    partition log start offset
//     %|    partition last stable offset
//     %]    partition high watermark
//
// The formatter internally tracks the number of times AppendRecord or
// AppendPartitionRecord have been called. The special option %i prints the
// iteration / call count:
//
//     %i    format iteration number (starts at 1)
//
// Lastly, there are three escapes to print raw characters that are usually
// used for formatting options:
//
//     %%    percent sign
//     %{    left brace (required if a brace is after another format option)
//     %}    right brace
//
// Header specification
//
// Specifying headers is essentially a primitive nested format option,
// accepting the key and value escapes above:
//
//     %K    header key length
//     %k    header key
//     %V    header value length
//     %v    header value
//
// For example, "%H %h{%k %v }" will print the number of headers, and then each
// header key and value with a space after each.
//
// Verb modifiers
//
// Most of the previous verb specifications can be modified by adding braces
// with a given modifier, e.g., "%V{ascii}". All modifiers are described below.
//
// Numbers
//
// All number verbs accept braces that control how the number is printed:
//
//     %v{ascii}       the default, print the number as ascii
//
//     %v{hex64}       print 16 hex characters for the number
//     %v{hex32}       print 8 hex characters for the number
//     %v{hex16}       print 4 hex characters for the number
//     %v{hex8}        print 2 hex characters for the number
//     %v{hex4}        print 1 hex characters for the number
//     %v{hex}         print as many hex characters as necessary for the number
//
//     %v{big64}       print the number in big endian uint64 format
//     %v{big32}       print the number in big endian uint32 format
//     %v{big16}       print the number in big endian uint16 format
//     %v{big8}        alias for byte
//
//     %v{little64}    print the number in little endian uint64 format
//     %v{little32}    print the number in little endian uint32 format
//     %v{little16}    print the number in little endian uint16 format
//     %v{little8}     alias for byte
//
//     %v{byte}        print the number as a single byte
//
// All numbers are truncated as necessary per each given format.
//
// Timestamps
//
// Timestamps can be specified in two formats: native Go timestamp formatting,
// or strftime formatting. Both format options, specified within braces, have
// internal format options:
//
//     %d{go##2006-01-02T15:04:05Z07:00##}
//     %d{strftime[%F]}
//
// An arbitrary amount of pound symbols, braces, and brackets are understood
// before beginning the actual timestamp formatting. For Go formatting, the
// format is simply passed to the time package's AppendFormat function. For
// strftime, all "man strftime" options are supported.
//
// Text
//
// Topics, keys, and values have "base64" and "hex" formatting options:
//
//     %t{hex}
//     %v{base64}
//
func NewRecordFormatter(layout string) (*RecordFormatter, error) {
	var f RecordFormatter

	var literal []byte // non-formatted raw text to output
	var i int
	for len(layout) > 0 {
		i++
		c, size := utf8.DecodeRuneInString(layout)
		rawc := layout[:size]
		layout = layout[size:]
		switch c {
		default:
			literal = append(literal, rawc...)
			continue

		case '\\':
			c, n, err := parseLayoutSlash(layout)
			if err != nil {
				return nil, err
			}
			layout = layout[n:]
			literal = append(literal, c)
			continue

		case '%':
		}

		if len(layout) == 0 {
			return nil, errors.New("invalid escape sequence at end of layout string")
		}

		cNext, size := utf8.DecodeRuneInString(layout)
		if cNext == '%' || cNext == '{' || cNext == '}' {
			literal = append(literal, byte(cNext))
			layout = layout[size:]
			continue
		}

		var (
			isOpenBrace  = len(layout) > 2 && layout[1] == '{'
			handledBrace = false
			escaped      = layout[0]
		)
		layout = layout[1:]

		// We are entering a format string. If we have any built
		// literal before, this is now raw text that we will append.
		if len(literal) > 0 {
			l := literal
			literal = nil
			f.fns = append(f.fns, func(b []byte, _ *FetchPartition, _ *Record) []byte { return append(b, l...) })
		}

		if isOpenBrace { // opening a brace: layout continues after
			layout = layout[1:]
		}

		switch escaped {
		default:
			return nil, fmt.Errorf("unknown escape sequence %%%s", string(escaped))

		case 'T', 'K', 'V', 'H', 'p', 'o', 'e', 'i', 'x', 'y', '[', '|', ']':
			// Numbers default to ascii, but we support a bunch of
			// formatting options. We parse the format here, and
			// then below is switching on which field to print.
			var numfn func([]byte, int64) []byte
			if handledBrace = isOpenBrace; handledBrace {
				numfn2, n, err := parseNumWriteLayout(layout)
				if err != nil {
					return nil, err
				}
				layout = layout[n:]
				numfn = numfn2
			} else {
				numfn = writeNumAscii
			}
			switch escaped {
			case 'T':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(len(r.Topic))) })
				})
			case 'K':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(len(r.Key))) })
				})
			case 'V':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(len(r.Value))) })
				})
			case 'H':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(len(r.Headers))) })
				})
			case 'p':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(r.Partition)) })
				})
			case 'o':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, r.Offset) })
				})
			case 'e':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(r.LeaderEpoch)) })
				})
			case 'i':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, _ *Record) []byte {
					return numfn(b, atomic.AddInt64(&f.calls, 1))
				})
			case 'x':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, r.ProducerID) })
				})
			case 'y':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(r.ProducerEpoch)) })
				})
			case '[':
				f.fns = append(f.fns, func(b []byte, p *FetchPartition, _ *Record) []byte {
					return writeP(b, p, func(b []byte, p *FetchPartition) []byte { return numfn(b, p.LogStartOffset) })
				})
			case '|':
				f.fns = append(f.fns, func(b []byte, p *FetchPartition, _ *Record) []byte {
					return writeP(b, p, func(b []byte, p *FetchPartition) []byte { return numfn(b, p.LastStableOffset) })
				})
			case ']':
				f.fns = append(f.fns, func(b []byte, p *FetchPartition, _ *Record) []byte {
					return writeP(b, p, func(b []byte, p *FetchPartition) []byte { return numfn(b, p.HighWatermark) })
				})
			}

		case 't', 'k', 'v':
			var appendFn func([]byte, []byte) []byte
			if handledBrace = isOpenBrace; handledBrace {
				switch {
				case strings.HasPrefix(layout, "base64}"):
					appendFn = appendBase64
					layout = layout[len("base64}"):]
				case strings.HasPrefix(layout, "hex}"):
					appendFn = appendHex
					layout = layout[len("hex}"):]
				default:
					return nil, fmt.Errorf("unknown %%%s{ escape", string(escaped))
				}
			} else {
				appendFn = appendPlain
			}
			switch escaped {
			case 't':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return appendFn(b, []byte(r.Topic)) })
				})
			case 'k':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return appendFn(b, []byte(r.Key)) })
				})
			case 'v':
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return appendFn(b, []byte(r.Value)) })
				})

			}

		case 'h':
			if !isOpenBrace {
				return nil, errors.New("missing open brace sequence on %h signifying how headers are written")
			}
			handledBrace = true
			// Headers can have their own internal braces, so we
			// must look for a matching end brace.
			braces := 1
			at := 0
			for braces != 0 && len(layout[at:]) > 0 {
				switch layout[at] {
				case '{':
					if at > 0 && layout[at-1] != '%' {
						braces++
					}
				case '}':
					if at > 0 && layout[at-1] != '%' {
						braces--
					}
				}
				at++
			}
			if braces > 0 {
				return nil, fmt.Errorf("invalid header specification: missing closing brace in %q", layout)
			}

			spec := layout[:at-1]
			layout = layout[at:]
			inf, err := NewRecordFormatter(spec)
			if err != nil {
				return nil, fmt.Errorf("invalid header specification %q: %v", spec, err)
			}

			f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
				reuse := new(Record)
				for _, header := range r.Headers {
					reuse.Key = []byte(header.Key)
					reuse.Value = header.Value
					b = inf.AppendRecord(b, reuse)
				}
				return b
			})

		case 'd':
			// For datetime parsing, we support plain millis in any
			// number format, strftime, or go formatting. We
			// default to plain ascii millis.
			handledBrace = isOpenBrace
			if !handledBrace {
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return strconv.AppendInt(b, r.Timestamp.UnixNano()/1e6, 10) })
				})
				continue
			}

			switch {
			case strings.HasPrefix(layout, "strftime"):
				tfmt, rem, err := nomOpenClose(layout[len("strftime"):])
				if err != nil {
					return nil, fmt.Errorf("strftime parse err: %v", err)
				}
				if len(rem) == 0 || rem[0] != '}' {
					return nil, fmt.Errorf("%%d{strftime missing closing } in %q", layout)
				}
				layout = rem[1:]
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return strftimeAppendFormat(b, tfmt, r.Timestamp) })
				})

			case strings.HasPrefix(layout, "go"):
				tfmt, rem, err := nomOpenClose(layout[len("go"):])
				if err != nil {
					return nil, fmt.Errorf("go parse err: %v", err)
				}
				if len(rem) == 0 || rem[0] != '}' {
					return nil, fmt.Errorf("%%d{go missing closing } in %q", layout)
				}
				layout = rem[1:]
				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return r.Timestamp.AppendFormat(b, tfmt) })
				})

			default:
				numfn, n, err := parseNumWriteLayout(layout)
				if err != nil {
					return nil, fmt.Errorf("unknown %%d{ time specification in %q", layout)
				}
				layout = layout[n:]

				f.fns = append(f.fns, func(b []byte, _ *FetchPartition, r *Record) []byte {
					return writeR(b, r, func(b []byte, r *Record) []byte { return numfn(b, int64(r.Timestamp.UnixNano())/1e6) })
				})
			}
		}

		// If we opened a brace, we require a closing brace.
		if isOpenBrace && !handledBrace {
			return nil, fmt.Errorf("unhandled open brace %q", layout)
		}
	}

	// Ensure we print any trailing text.
	if len(literal) > 0 {
		f.fns = append(f.fns, func(b []byte, _ *FetchPartition, _ *Record) []byte { return append(b, literal...) })
	}

	return &f, nil
}

func appendPlain(dst, src []byte) []byte {
	return append(dst, src...)
}

func appendBase64(dst, src []byte) []byte {
	fin := append(dst, make([]byte, base64.RawStdEncoding.EncodedLen(len(src)))...)
	base64.RawStdEncoding.Encode(fin[len(dst):], src)
	return fin
}

func appendHex(dst, src []byte) []byte {
	fin := append(dst, make([]byte, hex.EncodedLen(len(src)))...)
	hex.Encode(fin[len(dst):], src)
	return fin
}

// nomOpenClose extracts a middle section from a string beginning with repeated
// delimiters and returns it as with remaining (past end delimiters) string.
func nomOpenClose(src string) (string, string, error) {
	if len(src) == 0 {
		return "", "", errors.New("empty layout")
	}
	delim := src[0]
	openers := 1
	for openers < len(src) && src[openers] == delim {
		openers++
	}
	switch delim {
	case '{':
		delim = '}'
	case '[':
		delim = ']'
	case '(':
		delim = ')'
	}
	src = src[openers:]
	end := strings.Repeat(string(delim), openers)
	idx := strings.Index(src, end)
	if idx < 0 {
		return "", "", fmt.Errorf("missing end delim %q", end)
	}
	middle := src[:idx]
	return middle, src[idx+len(end):], nil
}

func parseNumWriteLayout(layout string) (func([]byte, int64) []byte, int, error) {
	braceEnd := strings.IndexByte(layout, '}')
	if braceEnd == -1 {
		return nil, 0, errors.New("missing brace end } to close number format specification")
	}
	end := braceEnd + 1
	switch layout = layout[:braceEnd]; layout {
	case "ascii":
		return writeNumAscii, end, nil
	case "hex64":
		return writeNumHex64, end, nil
	case "hex32":
		return writeNumHex32, end, nil
	case "hex16":
		return writeNumHex16, end, nil
	case "hex8":
		return writeNumHex8, end, nil
	case "hex4":
		return writeNumHex4, end, nil
	case "hex":
		return writeNumHex, end, nil
	case "big64":
		return writeNumBig64, end, nil
	case "big32":
		return writeNumBig32, end, nil
	case "big16":
		return writeNumBig16, end, nil
	case "byte", "big8", "little8":
		return writeNumByte, end, nil
	case "little64":
		return writeNumLittle64, end, nil
	case "little32":
		return writeNumLittle32, end, nil
	case "little16":
		return writeNumLittle16, end, nil
	default:
		return nil, 0, fmt.Errorf("invalid output number layout %q", layout)
	}
}

func writeR(b []byte, r *Record, fn func([]byte, *Record) []byte) []byte {
	if r == nil {
		return append(b, "<nil>"...)
	}
	return fn(b, r)
}

func writeP(b []byte, p *FetchPartition, fn func([]byte, *FetchPartition) []byte) []byte {
	if p == nil {
		return append(b, "<nil>"...)
	}
	return fn(b, p)
}
func writeNumAscii(b []byte, n int64) []byte { return strconv.AppendInt(b, n, 10) }

const hexc = "0123456789abcdef"

func writeNumHex64(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b,
		hexc[(u>>60)&0xf],
		hexc[(u>>56)&0xf],
		hexc[(u>>52)&0xf],
		hexc[(u>>48)&0xf],
		hexc[(u>>44)&0xf],
		hexc[(u>>40)&0xf],
		hexc[(u>>36)&0xf],
		hexc[(u>>32)&0xf],
		hexc[(u>>28)&0xf],
		hexc[(u>>24)&0xf],
		hexc[(u>>20)&0xf],
		hexc[(u>>16)&0xf],
		hexc[(u>>12)&0xf],
		hexc[(u>>8)&0xf],
		hexc[(u>>4)&0xf],
		hexc[u&0xf],
	)
}

func writeNumHex32(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b,
		hexc[(u>>28)&0xf],
		hexc[(u>>24)&0xf],
		hexc[(u>>20)&0xf],
		hexc[(u>>16)&0xf],
		hexc[(u>>12)&0xf],
		hexc[(u>>8)&0xf],
		hexc[(u>>4)&0xf],
		hexc[u&0xf],
	)
}

func writeNumHex16(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b,
		hexc[(u>>12)&0xf],
		hexc[(u>>8)&0xf],
		hexc[(u>>4)&0xf],
		hexc[u&0xf],
	)
}

func writeNumHex8(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b,
		hexc[(u>>4)&0xf],
		hexc[u&0xf],
	)
}

func writeNumHex4(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b,
		hexc[u&0xf],
	)
}

func writeNumHex(b []byte, n int64) []byte {
	return strconv.AppendUint(b, uint64(n), 16)
}

func writeNumBig64(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func writeNumLittle64(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b, byte(u), byte(u>>8), byte(u>>16), byte(u>>24), byte(u>>32), byte(u>>40), byte(u>>48), byte(u>>56))
}

func writeNumBig32(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func writeNumLittle32(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b, byte(u), byte(u>>8), byte(u>>16), byte(u>>24))
}
func writeNumBig16(b []byte, n int64) []byte { u := uint64(n); return append(b, byte(u>>8), byte(u)) }
func writeNumLittle16(b []byte, n int64) []byte {
	u := uint64(n)
	return append(b, byte(u), byte(u>>8))
}
func writeNumByte(b []byte, n int64) []byte { u := uint64(n); return append(b, byte(u)) }

////////////
// READER //
////////////

// RecordReader reads records from an io.Reader.
type RecordReader struct {
	r *bufio.Reader

	buf []byte
	fns []readParse

	maxRead int
}

// NewRecordReader returns a record reader for the given layout, or an error if
// the layout is invalid. The maxRead option specifies the maximum size of any
// parsing option. Using -1 opts in to the internal default size, which is
// currently bufio.MaxScanTokenSize. Any read more than maxRead returns a parse
// error.
//
// Similar to the RecordFormatter, the RecordReader parsing is quite powerful.
// There is a bit less to describe in comparison to RecordFormatter, but still,
// this documentation attempts to be as succinct as possible.
//
// Similar to the fmt package, record parsing is based off of slash escapes and
// percent "verbs" (copying fmt package lingo). Slashes are used for common
// escapes,
//
//     \t \n \r \\ \xNN
//
// reading tabs, newlines, carriage returns, slashes, and hex encoded
// characters.
//
// Percent encoding reads into specific values of a Record:
//
//     %t    topic
//     %T    topic length
//     %k    key
//     %K    key length
//     %v    value
//     %V    value length
//     %h    begin the header specification
//     %H    number of headers
//     %p    partition
//     %o    offset
//     %e    leader epoch
//     %x    producer id
//     %y    producer epoch
//
// If using length / number verbs (i.e., "sized" verbs), they must occur before
// what they are sizing.
//
// There are three escapes to parse raw characters, rather than opting into
// some formatting option:
//
//     %%    percent sign
//     %{    left brace
//     %}    right brace
//
// Numbers
//
// All size numbers can be parsed in the following ways:
//
//     %v{ascii}       parse numeric digits until a non-numeric
//
//     %v{hex64}       read 16 hex characters for the number
//     %v{hex32}       read 8 hex characters for the number
//     %v{hex16}       read 4 hex characters for the number
//     %v{hex8}        read 2 hex characters for the number
//     %v{hex4}        read 1 hex characters for the number
//
//     %v{big64}       read the number as big endian uint64 format
//     %v{big32}       read the number as big endian uint32 format
//     %v{big16}       read the number as big endian uint16 format
//     %v{big8}        alias for byte
//
//     %v{little64}    read the number as little endian uint64 format
//     %v{little32}    read the number as little endian uint32 format
//     %v{little16}    read the number as little endian uint16 format
//     %v{little8}     read the number as a byte
//
//     %v{byte}        read the number as a byte
//
//     %v{3}           read 3 characters (any number)
//
// Header specification
//
// Similar to number formatting, headers are parsed using a nested primitive
// format option, accpeting the key and value escapes previously mentioned.
//
func NewRecordReader(reader io.Reader, maxRead int, layout string) (*RecordReader, error) {
	switch {
	case maxRead < 0:
		maxRead = bufio.MaxScanTokenSize
	case maxRead == 0:
		return nil, fmt.Errorf("invalid max read size %d", maxRead)
	}
	r := &RecordReader{r: bufio.NewReader(reader), maxRead: maxRead}
	if err := r.parseReadLayout(layout); err != nil {
		return nil, err
	}
	return r, nil
}

// ReadRecord reads the next record in the reader and returns it, or returns a
// parsing error.
func (r *RecordReader) ReadRecord() (*Record, error) {
	rec := new(Record)
	return rec, r.ReadRecordInto(rec)
}

// ReadRecord reads the next record into the given record and returns any
// parsing error
func (r *RecordReader) ReadRecordInto(rec *Record) error {
	return r.next(rec)
}

// SetReader replaces the underlying reader with the given reader.
func (r *RecordReader) SetReader(reader io.Reader) {
	r.r = bufio.NewReader(reader)
}

const (
	parsesTopic parseRecordBits = 1 << iota
	parsesTopicSize
	parsesKey
	parsesKeySize
	parsesValue
	parsesValueSize
	parsesHeaders
	parsesHeadersNum
)

// The record reading format must be either entirely sized or entirely unsized.
// This type helps us track what's what.
type parseRecordBits uint8

func (p *parseRecordBits) set(r parseRecordBits)     { *p = *p | r }
func (p parseRecordBits) has(r parseRecordBits) bool { return p&r != 0 }

func (p parseRecordBits) hasSized() bool {
	return p&(parsesTopicSize|parsesKeySize|parsesValueSize|parsesHeadersNum) != 0
}

func (r *RecordReader) parseReadLayout(layout string) error {
	var (
		// If we are reading by size, we parse the layout size into one
		// of these variables. When reading, we use the captured
		// variable's value.
		topicSize  uint64
		keySize    uint64
		valueSize  uint64
		headersNum uint64

		bits parseRecordBits

		literal    []byte // raw literal we are currently working on
		addLiteral = func() {
			if len(r.fns) > 0 && r.fns[len(r.fns)-1].read.empty() {
				r.fns[len(r.fns)-1].read.delim = literal
			} else if len(literal) > 0 {
				r.fns = append(r.fns, readParse{
					read: readKind{exact: literal},
				})
			}
			literal = nil
		}
	)

	for len(layout) > 0 {
		c, size := utf8.DecodeRuneInString(layout)
		rawc := layout[:size]
		layout = layout[size:]
		switch c {
		default:
			literal = append(literal, rawc...)
			continue

		case '\\':
			c, n, err := parseLayoutSlash(layout)
			if err != nil {
				return err
			}
			layout = layout[n:]
			literal = append(literal, c)
			continue

		case '%':
		}

		if len(layout) == 0 {
			return errors.New("invalid escape sequence at end of layout string")
		}

		cNext, size := utf8.DecodeRuneInString(layout)
		if cNext == '%' || cNext == '{' || cNext == '}' {
			literal = append(literal, byte(cNext))
			layout = layout[size:]
			continue
		}

		var (
			isOpenBrace  = len(layout) > 2 && layout[1] == '{'
			handledBrace = false
			escaped      = layout[0]
		)
		layout = layout[1:]
		addLiteral()

		if isOpenBrace { // opening a brace: layout continues after
			layout = layout[1:]
		}

		switch escaped {
		default:
			return fmt.Errorf("unknown percent escape sequence %q", layout[:1])

		case 'T', 'K', 'V', 'H':
			var dst *uint64
			var bit parseRecordBits
			switch escaped {
			case 'T':
				dst, bit = &topicSize, parsesTopicSize
			case 'K':
				dst, bit = &keySize, parsesKeySize
			case 'V':
				dst, bit = &valueSize, parsesValueSize
			case 'H':
				dst, bit = &headersNum, parsesHeadersNum
			}
			if bits.has(bit) {
				return fmt.Errorf("%%%s is doubly specified", string(escaped))
			}
			if bits.has(bit >> 1) {
				return fmt.Errorf("size specification %%%s cannot come after value specification %%%s", string(escaped), strings.ToLower(string(escaped)))
			}
			bits.set(bit)
			fn, n, err := r.parseReadSize("ascii", dst, false)
			if handledBrace = isOpenBrace; handledBrace {
				fn, n, err = r.parseReadSize(layout, dst, true)
			}
			if err != nil {
				return fmt.Errorf("unable to parse %%%s: %s", string(escaped), err)
			}
			layout = layout[n:]
			r.fns = append(r.fns, fn)

		case 'p', 'o', 'e', 'x', 'y':
			dst := new(uint64)
			fn, n, err := r.parseReadSize("ascii", dst, false)
			if handledBrace = isOpenBrace; handledBrace {
				fn, n, err = r.parseReadSize(layout, dst, true)
			}
			if err != nil {
				return fmt.Errorf("unable to parse %%%s: %s", string(escaped), err)
			}
			layout = layout[n:]
			numParse := fn.parse
			switch escaped {
			case 'p':
				fn.parse = func(b []byte, rec *Record) error {
					if err := numParse(b, nil); err != nil {
						return err
					}
					rec.Partition = int32(*dst)
					return nil
				}
			case 'o':
				fn.parse = func(b []byte, rec *Record) error {
					if err := numParse(b, nil); err != nil {
						return err
					}
					rec.Offset = int64(*dst)
					return nil
				}
			case 'e':
				fn.parse = func(b []byte, rec *Record) error {
					if err := numParse(b, nil); err != nil {
						return err
					}
					rec.LeaderEpoch = int32(*dst)
					return nil
				}
			case 'x':
				fn.parse = func(b []byte, rec *Record) error {
					if err := numParse(b, nil); err != nil {
						return err
					}
					rec.ProducerID = int64(*dst)
					return nil
				}
			case 'y':
				fn.parse = func(b []byte, rec *Record) error {
					if err := numParse(b, nil); err != nil {
						return err
					}
					rec.ProducerEpoch = int16(*dst)
					return nil
				}
			}
			r.fns = append(r.fns, fn)

		case 't':
			bits.set(parsesTopic)
			fn := readParse{parse: func(b []byte, r *Record) error { r.Topic = string(b); return nil }}
			if bits.has(parsesTopicSize) {
				fn.read = readKind{sizefn: func() int { return int(topicSize) }}
			}
			r.fns = append(r.fns, fn)

		case 'k':
			bits.set(parsesKey)
			fn := readParse{parse: func(b []byte, r *Record) error { r.Key = dupslice(b); return nil }}
			if bits.has(parsesKeySize) {
				fn.read = readKind{sizefn: func() int { return int(keySize) }}
			}
			r.fns = append(r.fns, fn)

		case 'v':
			bits.set(parsesValue)
			fn := readParse{parse: func(b []byte, r *Record) error { r.Value = dupslice(b); return nil }}
			if bits.has(parsesValueSize) {
				fn.read = readKind{sizefn: func() int { return int(valueSize) }}
			}
			r.fns = append(r.fns, fn)

		case 'h':
			bits.set(parsesHeaders)
			if !bits.has(parsesHeadersNum) {
				return errors.New("missing header count specification %H before header specification %h")
			}
			if !isOpenBrace {
				return errors.New("missing open brace sequence on %h signifying how headers are encoded")
			}
			handledBrace = true
			// Similar to above, headers can have their own
			// internal braces, so we look for a matching end.
			braces := 1
			at := 0
			for braces != 0 && len(layout[at:]) > 0 {
				switch layout[at] {
				case '{':
					if at > 0 && layout[at-1] != '%' {
						braces++
					}
				case '}':
					if at > 0 && layout[at-1] != '%' {
						braces--
					}
				}
				at++
			}
			if braces > 0 {
				return fmt.Errorf("invalid header specification: missing closing brace in %q", layout)
			}

			// We parse the header specification recursively, but
			// we require that it is sized and contains only keys
			// and values. Checking the delimiter checks sizing.
			var inr RecordReader
			if err := inr.parseReadLayout(layout[:at-1]); err != nil {
				return fmt.Errorf("invalid header specification: %v", err)
			}
			layout = layout[at:]

			// To parse headers, we save the inner reader's parsing
			// function stash the current record's key/value before
			// parsing, and then capture the key/value as a header.
			r.fns = append(r.fns, readParse{read: readKind{handoff: func(r *RecordReader, rec *Record) error {
				k, v := rec.Key, rec.Value
				defer func() { rec.Key, rec.Value = k, v }()
				inr.r = r.r
				for i := uint64(0); i < headersNum; i++ {
					rec.Key, rec.Value = nil, nil
					if err := inr.next(rec); err != nil {
						return err
					}
					rec.Headers = append(rec.Headers, RecordHeader{Key: string(rec.Key), Value: rec.Value})
				}
				return nil
			}}})

		}

		if isOpenBrace && !handledBrace {
			return fmt.Errorf("unhandled open brace %q", layout)
		}
	}

	addLiteral()
	return nil
}

// Returns a function that parses a number from the internal reader into dst.
//
// If needBrace is true, the user is specifying how to read the number,
// otherwise we default to ascii. Reading ascii requires us to peek at bytes
// until we get to a non-number byte.
func (r *RecordReader) parseReadSize(layout string, dst *uint64, needBrace bool) (readParse, int, error) {
	var end int
	if needBrace {
		braceEnd := strings.IndexByte(layout, '}')
		if braceEnd == -1 {
			return readParse{}, 0, errors.New("missing brace end } to close number size specification")
		}
		layout = layout[:braceEnd]
		end = braceEnd + 1
	}

	switch layout {
	default:
		num, err := strconv.Atoi(layout)
		if err != nil {
			return readParse{}, 0, fmt.Errorf("unrecognized number reading layout %q: %v", layout, err)
		}
		if num <= 0 {
			return readParse{}, 0, fmt.Errorf("invalid zero or negative number %q when parsing read size", layout)
		}
		return readParse{
			readKind{noread: true},
			func([]byte, *Record) error { *dst = uint64(num); return nil },
		}, end, nil

	case "ascii":
		return readParse{
			readKind{condition: func(b byte) bool { return b < '0' || b > '9' }},
			func(b []byte, _ *Record) (err error) { *dst, err = strconv.ParseUint(string(b), 10, 64); return err },
		}, end, nil

	case "big64":
		return readParse{
			readKind{size: 8},
			func(b []byte, _ *Record) error { *dst = binary.BigEndian.Uint64(b); return nil },
		}, end, nil
	case "big32":
		return readParse{
			readKind{size: 4},
			func(b []byte, _ *Record) error { *dst = uint64(binary.BigEndian.Uint32(b)); return nil },
		}, end, nil
	case "big16":
		return readParse{
			readKind{size: 2},
			func(b []byte, _ *Record) error { *dst = uint64(binary.BigEndian.Uint16(b)); return nil },
		}, end, nil

	case "little64":
		return readParse{
			readKind{size: 8},
			func(b []byte, _ *Record) error { *dst = binary.LittleEndian.Uint64(b); return nil },
		}, end, nil
	case "little32":
		return readParse{
			readKind{size: 4},
			func(b []byte, _ *Record) error { *dst = uint64(binary.LittleEndian.Uint32(b)); return nil },
		}, end, nil
	case "little16":
		return readParse{
			readKind{size: 2},
			func(b []byte, _ *Record) error { *dst = uint64(binary.LittleEndian.Uint16(b)); return nil },
		}, end, nil

	case "byte", "big8", "little8":
		return readParse{
			readKind{size: 1},
			func(b []byte, _ *Record) error { *dst = uint64(b[0]); return nil },
		}, end, nil

	case "hex64":
		return readParse{
			readKind{size: 16},
			func(b []byte, _ *Record) (err error) { *dst, err = strconv.ParseUint(string(b), 16, 64); return err },
		}, end, nil
	case "hex32":
		return readParse{
			readKind{size: 8},
			func(b []byte, _ *Record) (err error) { *dst, err = strconv.ParseUint(string(b), 16, 64); return err },
		}, end, nil
	case "hex16":
		return readParse{
			readKind{size: 4},
			func(b []byte, _ *Record) (err error) { *dst, err = strconv.ParseUint(string(b), 16, 64); return err },
		}, end, nil
	case "hex8":
		return readParse{
			readKind{size: 2},
			func(b []byte, _ *Record) (err error) { *dst, err = strconv.ParseUint(string(b), 16, 64); return err },
		}, end, nil
	case "hex4":
		return readParse{
			readKind{size: 1},
			func(b []byte, _ *Record) (err error) { *dst, err = strconv.ParseUint(string(b), 16, 64); return err },
		}, end, nil
	}
}

type readKind struct {
	noread    bool
	exact     []byte
	condition func(byte) bool
	size      int
	sizefn    func() int
	handoff   func(*RecordReader, *Record) error
	delim     []byte
}

func (r *readKind) empty() bool {
	return r.noread == false &&
		r.exact == nil &&
		r.condition == nil &&
		r.size == 0 &&
		r.sizefn == nil &&
		r.handoff == nil &&
		r.delim == nil
}

type readParse struct {
	read  readKind
	parse func([]byte, *Record) error
}

func dupslice(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	dup := make([]byte, len(b))
	copy(dup, b)
	return dup
}

// TODO maxRead
func (r *RecordReader) next(rec *Record) error {
	for i, fn := range r.fns {
		var err error
		switch {
		case fn.read.noread:
			// do nothing
		case fn.read.exact != nil:
			err = r.readExact(fn.read.exact)
		case fn.read.condition != nil:
			err = r.readCondition(fn.read.condition)
		case fn.read.size > 0:
			err = r.readSize(fn.read.size)
		case fn.read.sizefn != nil:
			err = r.readSize(fn.read.sizefn())
		case fn.read.handoff != nil:
			err = fn.read.handoff(r, rec)
		default:
			err = r.readDelim(fn.read.delim) // we *always* fall back to delim parsing
		}
		if err != nil {
			if err == io.EOF && i != 0 {
				err = io.ErrUnexpectedEOF
			}
			return err
		}

		if fn.parse == nil {
			continue
		}

		if err = fn.parse(r.buf, rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *RecordReader) readCondition(fn func(byte) bool) error {
	// Conditions are essentially "read numerals until a non-numeral",
	// which is short. We start with a buf of ~4.
	r.buf = append(r.buf[:0], make([]byte, 4)...)[:0]
	for {
		peek, err := r.r.Peek(1)
		if err != nil {
			return err
		}
		c := peek[0]
		if fn(c) {
			return nil
		}
		r.r.Discard(1)
		r.buf = append(r.buf, c)
	}
}

func (r *RecordReader) readSize(n int) error {
	r.buf = append(r.buf[:0], make([]byte, n)...)
	_, err := io.ReadFull(r.r, r.buf)
	return err
}

func (r *RecordReader) readExact(d []byte) error {
	r.buf = append(r.buf[:0], make([]byte, len(d))...)
	_, err := io.ReadFull(r.r, r.buf)
	if err != nil {
		return err
	}
	if !bytes.Equal(d, r.buf) {
		return fmt.Errorf("exact text mismatch, read %q when expecting %q", r.buf, d)
	}
	return nil
}

func (r *RecordReader) readDelim(d []byte) error {
	// Empty delimiters opt in to reading the rest of the text.
	if len(d) == 0 {
		b, err := io.ReadAll(r.r)
		r.buf = b
		return err
	}

	// We use the simple inefficient search algorithm, which can be O(nm),
	// but we aren't expecting huge search spaces. Long term we could
	// convert to a two-way search.
	r.buf = r.buf[:0]
	for {
		peek, err := r.r.Peek(len(d))
		if err != nil {
			return err
		}
		if !bytes.Equal(peek, d) {
			r.buf = append(r.buf, peek[0])
			r.r.Discard(1)
			continue
		}
		r.r.Discard(len(d))
		return nil
	}
}

////////////
// COMMON //
////////////

func parseLayoutSlash(layout string) (byte, int, error) {
	if len(layout) == 0 {
		return 0, 0, errors.New("invalid slash escape at end of delim string")
	}
	switch layout[0] {
	case 't':
		return '\t', 1, nil
	case 'n':
		return '\n', 1, nil
	case 'r':
		return '\r', 1, nil
	case '\\':
		return '\\', 1, nil
	case 'x':
		if len(layout) < 3 { // on x, need two more
			return 0, 0, errors.New("invalid non-terminated hex escape sequence at end of delim string")
		}
		hex := layout[1:3]
		n, err := strconv.ParseInt(hex, 16, 8)
		if err != nil {
			return 0, 0, fmt.Errorf("unable to parse hex escape sequence %q: %v", hex, err)
		}
		return byte(n), 3, nil
	default:
		return 0, 0, fmt.Errorf("unknown slash escape sequence %q", layout[:1])
	}
}
