package main

import (
	"bytes"
	"strconv"
	"strings"
)

// If you are looking here, yes this is a shoddy parser, but it does the job.

// newStructs are top level structs that we print at the end.
// We keep these in order to generate types in order.
var newStructs []Struct

var types = map[string]Type{
	"bool":            Bool{},
	"int8":            Int8{},
	"int16":           Int16{},
	"int32":           Int32{},
	"int64":           Int64{},
	"uint32":          Uint32{},
	"varint":          Varint{},
	"varlong":         Varlong{},
	"string":          String{},
	"nullable-string": NullableString{},
	"bytes":           Bytes{},
	"nullable-bytes":  NullableBytes{},
	"varint-string":   VarintString{},
	"varint-bytes":    VarintBytes{},
}

// LineScanner is a really shoddy scanner that allows us to peek an entire
// line.
type LineScanner struct {
	buf []byte
}

func (l *LineScanner) HasLine() bool {
	return bytes.IndexByte(l.buf, '\n') >= 0
}

func (l *LineScanner) Line() string {
	idx := bytes.IndexByte(l.buf, '\n')
	r := string(l.buf[:idx])
	return r
}

func (l *LineScanner) KeepLine() {
	idx := bytes.IndexByte(l.buf, '\n')
	l.buf = l.buf[idx+1:]
}

// BuildFrom parses a struct, building it from the current scanner. Level
// signifies how nested this struct is. Top level structs have a level of 0.
//
// If a blank line is ever encountered, the struct is done being built and
// the done status is bubbled up through all recursion levels.
func (s *Struct) BuildFrom(scanner *LineScanner, level int) (unprocessed string, done bool) {
	fieldSpaces := strings.Repeat(" ", 2*(level+1))

	var nextComment string

	for !done && scanner.HasLine() { // check done before scanning the new line
		line := scanner.Line()
		if len(line) == 0 { // empty lines mean done
			scanner.KeepLine()
			return "", true
		}
		if !strings.HasPrefix(line, fieldSpaces) {
			return line, false
		}
		// we will be keeping this line
		scanner.KeepLine()
		line = line[len(fieldSpaces):]

		if strings.HasPrefix(line, "//") { // buffer comments
			if nextComment != "" {
				nextComment += "\n"
			}
			nextComment += line
			continue
		}

		// Fields are name on left, type on right.
		fields := strings.Split(line, ": ")
		if len(fields) != 2 || len(fields[0]) == 0 || len(fields[1]) == 0 {
			die("improper struct field format on line %q", line)
		}

		f := StructField{
			Comment:   nextComment,
			FieldName: fields[0],
		}
		nextComment = ""

		typ := fields[1]

		// Parse the version from the type on the right. Currently all
		// versions are right open ended, but v3+ looks better than v3.
		if idx := strings.Index(typ, " // v"); idx > 0 {
			var err error
			f.MinVersion, err = strconv.Atoi(strings.TrimSuffix(typ[idx+5:], "+"))
			if err != nil {
				die("unable to parse min version on line %q: %v", line, err)
			}
			typ = typ[:idx]
		}

		isArray := false
		isVarintArray := false
		if strings.HasPrefix(typ, "varint[") {
			isArray = true
			isVarintArray = true
			typ = typ[len("varint[") : len(typ)-1]
		}
		if typ[0] == '[' {
			isArray = true
			typ = typ[1 : len(typ)-1]
		}

		if typ == "=>" {
			var newS Struct
			newS.Name = s.Name + f.FieldName
			unprocessed, done = newS.BuildFrom(scanner, level+1)
			f.Type = newS
			newStructs = append(newStructs, newS)
		} else {
			if types[typ] == nil {
				die("unknown type %q on line %q", typ, line)
			}
			f.Type = types[typ]
		}

		if isArray {
			f.Type = Array{
				Inner:         f.Type,
				IsVarintArray: isVarintArray,
			}
		}

		s.Fields = append(s.Fields, f)
	}

	// we have no unprocessed line here.
	return "", done
}

// Parse parses the raw contents of a messages file and adds all newly
// parsed structs to newStructs.
func Parse(raw []byte) {
	scanner := &LineScanner{buf: raw}

	var nextComment string

	for scanner.HasLine() {
		line := scanner.Line()
		scanner.KeepLine()
		if len(line) == 0 {
			continue
		}

		// if this is a comment line, keep it and continue.
		if strings.HasPrefix(line, "//") {
			if len(nextComment) > 0 {
				nextComment += "\n"
			}
			nextComment += line
			continue
		}

		s := Struct{
			Comment: nextComment,
		}
		nextComment = ""

		orig := line
		topLevel := true

		name := strings.TrimSuffix(line, " => not top level")
		save := func() {
			s.Name = name
			s.TopLevel = topLevel
			s.BuildFrom(scanner, 0)
			types[name] = s
			newStructs = append(newStructs, s)
		}

		// A top level struct can say it is not top level to avoid
		// having encode/decode/etc funcs generated. If we trimmed not
		// top level, save and continue.
		if orig != name {
			topLevel = false
			save()
			continue
		}

		// This is a top level struct, so we parse a few more
		// things.
		delim := strings.Index(name, " =>")
		if delim == -1 {
			die("missing struct delimiter on line %q", line)
		}
		rem := name[delim+3:]
		name = name[:delim]

		if strings.HasPrefix(rem, " from ") {
			last := rem[6:]
			prior := &newStructs[len(newStructs)-1]
			if prior.Name != last {
				die("from %q does not refer to last message defn on line %q", last, line)
			}
			prior.ResponseKind = name
			save()
			continue
		}

		// key, max, min (optional), admin (optional])
		const adminStr = ", admin"
		if idx := strings.Index(rem, adminStr); idx > 0 {
			s.Admin = true
			rem = rem[:idx]
		}
		const minStr = ", min version "
		if idx := strings.Index(rem, minStr); idx > 0 {
			min, err := strconv.Atoi(rem[idx+len(minStr):])
			if err != nil {
				die("min version on line %q parse err: %v", line, err)
			}
			s.MinVersion = min
			rem = rem[:idx]
		}
		const maxStr = ", max version "
		if idx := strings.Index(rem, maxStr); idx == -1 {
			die("missing max version on line %q", line)
		} else {
			max, err := strconv.Atoi(rem[idx+len(maxStr):])
			if err != nil {
				die("max version on line %q parse err: %v", line, err)
			}
			s.MaxVersion = max
			rem = rem[:idx]
		}
		const keyStr = " key "
		if idx := strings.Index(rem, keyStr); idx == -1 {
			die("missing key on line %q", line)
		} else {
			key, err := strconv.Atoi(rem[idx+len(keyStr):])
			if err != nil {
				die("key on line %q pare err: %v", line, err)
			}
			s.Key = key
			if key > maxKey {
				maxKey = key
			}
		}

		save()
	}
}
