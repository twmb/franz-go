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
//     %v{big8}        print the number truncated to a byte
//     %v{byte}        same as big8
//
//     %v{little64}    print the number in little endian uint64 format
//     %v{little32}    print the number in little endian uint32 format
//     %v{little16}    print the number in little endian uint16 format
//     %v{little8}     print the number truncated to a byte
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
	r       io.Reader
	scanner *bufio.Scanner

	fn        func(*RecordReader, *Record) error
	delimiter *parseDelims

	maxRead int
	parseRecordBits
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
//
// If using length / number verbs (i.e., "sized" verbs), then all verbs must be
// sized. Using sizes allows the format to read a size before reading the
// value, rather than using delimiter based parsing.
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
//     %v{big8}        read the number as a byte
//     %v{byte}        same as big8
//
//     %v{little64}    read the number as little endian uint64 format
//     %v{little32}    read the number as little endian uint32 format
//     %v{little16}    read the number as little endian uint16 format
//     %v{little8}     read the number as a byte
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
	r := &RecordReader{r: reader, maxRead: maxRead}
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
	return r.fn(r, rec)
}

// SetReader replaces the underlying reader with the given reader.
func (r *RecordReader) SetReader(reader io.Reader) {
	r.r = reader
	if r.delimiter != nil {
		r.scanner = bufio.NewScanner(r.r)
		r.scanner.Buffer(nil, r.maxRead)
		r.scanner.Split(r.delimiter.split)
	}
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

		// Until the end, we build both sized fns and delim fns.
		sizeFns  []func(*RecordReader, *Record) error
		delimFns []func([]byte, *Record)

		// literals contains spans of raw text to read. We always have
		// one literal per sizeFn/delimFn to make indexing easy, even
		// if a given literal is empty.
		literals [][]byte
		literal  []byte // raw literal we are currently working on
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

		literals = append(literals, literal) // always cut literal at new format
		literal = nil

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
			if r.has(bit) {
				return fmt.Errorf("%%%s is doubly specified", string(escaped))
			}
			if r.has(bit - 1) {
				return fmt.Errorf("size specification %%%s cannot come after value specification %%%s", string(escaped), strings.ToLower(string(escaped)))
			}
			r.set(bit)
			fn, n, err := r.parseReadSize("ascii", dst, false)
			if handledBrace = isOpenBrace; handledBrace {
				fn, n, err = r.parseReadSize(layout, dst, true)
			}
			if err != nil {
				return fmt.Errorf("unable to parse %%%s: %s", string(escaped), err)
			}
			layout = layout[n:]
			sizeFns = append(sizeFns, fn)

		case 't':
			r.set(parsesTopic)
			delimFns = append(delimFns, func(in []byte, r *Record) { r.Topic = string(in) })
			sizeFns = append(sizeFns, func(r *RecordReader, rec *Record) error {
				if topicSize > uint64(r.maxRead) {
					return fmt.Errorf("topic size %d larger than allowed %d", topicSize, r.maxRead)
				}
				if topicSize == 0 {
					return nil
				}
				buf := make([]byte, topicSize)
				_, err := io.ReadFull(r.r, buf)
				rec.Topic = string(buf)
				return err
			})

		case 'k':
			r.set(parsesKey)
			delimFns = append(delimFns, func(in []byte, r *Record) {
				if len(in) > 0 {
					r.Key = in
				}
			})
			sizeFns = append(sizeFns, func(r *RecordReader, rec *Record) error {
				if keySize > uint64(r.maxRead) {
					return fmt.Errorf("key size %d larger than allowed %d", keySize, r.maxRead)
				}
				if keySize == 0 {
					return nil
				}
				rec.Key = make([]byte, keySize)
				_, err := io.ReadFull(r.r, rec.Key)
				return err
			})

		case 'v':
			r.set(parsesValue)
			delimFns = append(delimFns, func(in []byte, r *Record) {
				if len(in) > 0 {
					r.Value = in
				}
			})
			sizeFns = append(sizeFns, func(r *RecordReader, rec *Record) error {
				if valueSize > uint64(r.maxRead) {
					return fmt.Errorf("value size %d larger than allowed %d", valueSize, r.maxRead)
				}
				if valueSize == 0 {
					return nil
				}
				rec.Value = make([]byte, valueSize)
				_, err := io.ReadFull(r.r, rec.Value)
				return err
			})

		case 'h':
			r.set(parsesHeaders)
			if !r.has(parsesHeadersNum) {
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
			if inr.delimiter != nil {
				return errors.New("invalid header specification: does not use sized fields")
			}
			if inr.has(parsesTopic | parsesHeaders) {
				return errors.New("invalid header specification: only keys and values can be specified")
			}

			// To parse headers, we save the inner reader's parsing
			// function stash the current record's key/value before
			// parsing, and then capture the key/value as a header.
			fnInner := inr.fn
			sizeFns = append(sizeFns, func(r *RecordReader, rec *Record) error {
				k, v := rec.Key, rec.Value
				defer func() { rec.Key, rec.Value = k, v }()
				for i := uint64(0); i < headersNum; i++ {
					rec.Key, rec.Value = nil, nil
					if err := fnInner(r, rec); err != nil {
						return err
					}
					rec.Headers = append(rec.Headers, RecordHeader{Key: string(rec.Key), Value: rec.Value})
				}
				return nil
			})
		}

		if isOpenBrace && !handledBrace {
			return fmt.Errorf("unhandled open brace %q", layout)
		}
	}

	if r.hasSized() {
		if r.has(parsesTopic) && !r.has(parsesTopicSize) ||
			r.has(parsesKey) && !r.has(parsesKeySize) ||
			r.has(parsesValue) && !r.has(parsesValueSize) ||
			r.has(parsesHeaders) && !r.has(parsesHeadersNum) {
			return errors.New("invalid mix of sized fields and unsized fields for reading")
		}
		if r.has(parsesTopicSize) && !r.has(parsesTopic) ||
			r.has(parsesKeySize) && !r.has(parsesKey) ||
			r.has(parsesValueSize) && !r.has(parsesValue) ||
			r.has(parsesHeadersNum) && !r.has(parsesHeaders) {
			return errors.New("field size specified without corresponding field specification")
		}

		if len(literal) > 0 {
			literals = append(literals, literal)
			sizeFns = append(sizeFns, func(*RecordReader, *Record) error { return nil })
		}

		var largestLiteral []byte
		for _, literal := range literals {
			if len(literal) > len(largestLiteral) {
				largestLiteral = literal
			}
		}

		// For exact size reading, we read a literal (if non-empty), and then
		// we read the size fn.
		scratchLiteral := make([]byte, len(largestLiteral))
		r.fn = func(r *RecordReader, rec *Record) error {
			for i, literal := range literals {
				if len(literal) > 0 {
					if _, err := io.ReadFull(r.r, scratchLiteral); err != nil {
						return fmt.Errorf("unable to read literal: %v", err)
					}
					if !bytes.Equal(scratchLiteral, literal) {
						return fmt.Errorf("read literal piece %q != expected %q", scratchLiteral, literal)
					}
				}
				if err := sizeFns[i](r, rec); err != nil {
					return err
				}
			}
			return nil
		}
		return nil
	}

	var leadingDelim bool
	if len(literals) > 0 {
		if len(literals[0]) != 0 {
			leadingDelim = true
			delimFns = append([]func([]byte, *Record){nil}, delimFns...)
		} else {
			literals = literals[1:]
		}
	}
	literals = append(literals, literal)
	d := &parseDelims{delims: literals}

	r.scanner = bufio.NewScanner(r.r)
	r.scanner.Split(d.split)
	r.delimiter = d
	r.fn = func(r *RecordReader, rec *Record) error {
		var scanned int
		for r.scanner.Scan() {
			if scanned == 0 && leadingDelim {
				if len(r.scanner.Bytes()) > 0 {
					return fmt.Errorf("invalid content %q before leading delimeter", r.scanner.Bytes())
				}
			} else {
				val := make([]byte, len(r.scanner.Bytes()))
				copy(val, r.scanner.Bytes())
				delimFns[scanned](val, rec)
			}
			scanned++
			if scanned == len(d.delims) {
				return nil
			}
		}
		if r.scanner.Err() != nil {
			return r.scanner.Err()
		}
		return io.EOF
	}

	return nil
}

type parseDelims struct {
	delims  [][]byte
	atDelim int
}

func (d *parseDelims) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	delim := d.delims[d.atDelim]
	// If the delim is empty, we consume to the end.
	if len(delim) == 0 && d.atDelim+1 == len(d.delims) {
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	}

	// We look for our delimiter. If we find it, our token is up *to* the
	// delimiter, and we advance past the token *and* the delimiter.
	if i := bytes.Index(data, delim); i >= 0 {
		d.atDelim++
		if d.atDelim == len(d.delims) {
			d.atDelim = 0
		}
		return i + len(delim), data[0:i], nil
	}
	if atEOF {
		return 0, nil, fmt.Errorf("unfinished delim %q", delim)
	}
	return 0, nil, nil
}

// Returns a function that parses a number from the internal reader into dst.
//
// If needBrace is true, the user is specifying how to read the number,
// otherwise we default to ascii. Reading ascii requires us to peek at bytes
// until we get to a non-number byte.
func (r *RecordReader) parseReadSize(layout string, dst *uint64, needBrace bool) (func(*RecordReader, *Record) error, int, error) {
	var end int
	if needBrace {
		braceEnd := strings.IndexByte(layout, '}')
		if braceEnd == -1 {
			return nil, 0, errors.New("missing brace end } to close number size specification")
		}
		layout = layout[:braceEnd]
		end = braceEnd + 1
	}

	switch layout {
	default:
		num, err := strconv.Atoi(layout)
		if err != nil {
			return nil, 0, fmt.Errorf("unrecognized number reading layout %q: %v", layout, err)
		}
		if num <= 0 {
			return nil, 0, fmt.Errorf("invalid zero or negative number %q when parsing read size", layout)
		}
		return func(r *RecordReader, _ *Record) error { *dst = uint64(num); return nil }, end, nil

	case "big64":
		return func(r *RecordReader, _ *Record) error {
			var buf [8]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = binary.BigEndian.Uint64(buf[:])
			return nil
		}, end, nil

	case "big32":
		return func(r *RecordReader, _ *Record) error {
			var buf [4]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = uint64(binary.BigEndian.Uint32(buf[:]))
			return nil
		}, end, nil

	case "big16":
		return func(r *RecordReader, _ *Record) error {
			var buf [2]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = uint64(binary.BigEndian.Uint16(buf[:]))
			return nil
		}, end, nil

	case "byte", "big8", "little8":
		return func(r *RecordReader, _ *Record) error {
			var buf [1]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = uint64(buf[0])
			return nil
		}, end, nil

	case "little64":
		return func(r *RecordReader, _ *Record) error {
			var buf [8]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = binary.LittleEndian.Uint64(buf[:])
			return nil
		}, end, nil

	case "little32":
		return func(r *RecordReader, _ *Record) error {
			var buf [4]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = uint64(binary.LittleEndian.Uint32(buf[:]))
			return nil
		}, end, nil

	case "little16":
		return func(r *RecordReader, _ *Record) error {
			var buf [2]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			*dst = uint64(binary.LittleEndian.Uint16(buf[:]))
			return nil
		}, end, nil

	case "hex64":
		return func(r *RecordReader, _ *Record) error {
			var buf [16]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			du64, err := strconv.ParseUint(string(buf[:]), 16, 64)
			if err != nil {
				return err
			}
			*dst = du64
			return nil
		}, end, nil
	case "hex32":
		return func(r *RecordReader, _ *Record) error {
			var buf [8]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			du64, err := strconv.ParseUint(string(buf[:]), 16, 64)
			if err != nil {
				return err
			}
			*dst = du64
			return nil
		}, end, nil
	case "hex16":
		return func(r *RecordReader, _ *Record) error {
			var buf [4]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			du64, err := strconv.ParseUint(string(buf[:]), 16, 64)
			if err != nil {
				return err
			}
			*dst = du64
			return nil
		}, end, nil

	case "hex8":
		return func(r *RecordReader, _ *Record) error {
			var buf [2]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			du64, err := strconv.ParseUint(string(buf[:]), 16, 64)
			if err != nil {
				return err
			}
			*dst = du64
			return nil
		}, end, nil

	case "hex4":
		return func(r *RecordReader, _ *Record) error {
			var buf [1]byte
			if _, err := io.ReadFull(r.r, buf[:]); err != nil {
				return err
			}
			du64, err := strconv.ParseUint(string(buf[:]), 16, 64)
			if err != nil {
				return err
			}
			*dst = du64
			return nil
		}, end, nil

	case "ascii":
		return func(r *RecordReader, _ *Record) error {
			// We could read many ascii characters in one go.  On
			// the first read, we wrap our reader in a byte peeker.
			// All future reads will use this single peeker.
			peeker, ok := r.r.(bytePeeker)
			if !ok {
				r.r = &bytePeekWrapper{r: r.r}
				peeker = r.r.(bytePeeker)
			}
			rawNum := make([]byte, 0, 20)
			for i := 0; i < 21; i++ {
				next, err := peeker.Peek()
				if err != nil || next < '0' || next > '9' {
					if err != nil && len(rawNum) == 0 {
						return err
					}
					break
				}
				if i == 20 {
					return fmt.Errorf("still parsing ascii number in %s past max uint64 possible length of 20", rawNum)
				}
				rawNum = append(rawNum, next)
				peeker.SkipPeek()
			}
			parsed, err := strconv.ParseUint(string(rawNum), 10, 64)
			if err != nil {
				return err
			}
			*dst = parsed
			return nil
		}, end, nil
	}
}

// bytePeeker is our little reader helper time for when we need to read ascii
// numbers. While reading the number, we peek at the next byte and continue
// processing as a number until wee see a non-numeric ascii char.
type bytePeeker interface {
	Peek() (byte, error)
	SkipPeek()
	io.Reader
}

type bytePeekWrapper struct {
	haspeek bool
	peek    byte
	r       io.Reader
}

func (b *bytePeekWrapper) Peek() (byte, error) {
	if b.haspeek {
		return b.peek, nil
	}
	var peek [1]byte
	if _, err := io.ReadFull(b.r, peek[:]); err != nil {
		return 0, err
	}
	b.haspeek = true
	b.peek = peek[0]
	return b.peek, nil
}

func (b *bytePeekWrapper) SkipPeek() {
	b.haspeek = false
}

func (b *bytePeekWrapper) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.haspeek {
		b.haspeek = false
		p[n] = b.peek
		n++
	}
	nn, err := b.r.Read(p[n:])
	return n + nn, err
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
