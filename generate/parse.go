package main

import (
	"bytes"
	"fmt"
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
			Comment:    nextComment,
			FieldName:  fields[0],
			MaxVersion: -1,
		}
		nextComment = ""

		typ := fields[1]

		// Parse the version from the type on the right.
		if idx := strings.Index(typ, " // v"); idx > 0 {
			var err error
			f.MinVersion, f.MaxVersion, err = parseVersion(typ[idx+4:]) // start after ` // ` but include v
			if err != nil {
				die("unable to parse version on line %q: %v", line, err)
			}
			typ = typ[:idx]
		}

		// We count the array depth here, check if it is encoded
		// specially, and remove any array decoration.
		//
		// This does not support special modifiers at all levels.
		isArray := false
		isVarintArray := false
		isNullableArray := false
		arrayLevel := strings.Count(typ, "[")
		if arrayLevel > 0 {
			if strings.HasPrefix(typ, "varint[") {
				isVarintArray = true
				typ = typ[len("varint"):]
			} else if strings.HasPrefix(typ, "nullable[") {
				isNullableArray = true
				typ = typ[len("nullable"):]
			}
			typ = typ[arrayLevel : len(typ)-arrayLevel]
			isArray = true
		}

		if strings.HasPrefix(typ, "=>") {
			var newS Struct
			newS.Name = s.Name + f.FieldName
			if isArray {
				if rename := typ[2:]; rename != "" {
					newS.Name = s.Name + rename
				} else {
					newS.Name = strings.TrimSuffix(newS.Name, "s") // make plural singular
				}
			}
			unprocessed, done = newS.BuildFrom(scanner, level+1)
			f.Type = newS
			newStructs = append(newStructs, newS)
		} else if strings.HasPrefix(typ, "length-field-minus => ") {
			typ = strings.TrimPrefix(typ, "length-field-minus => ")
			from, minus, err := parseFieldLength(typ)
			if err != nil {
				die("unable to parse field-length-bytes in %q: %v", typ, err)
			}
			f.Type = FieldLengthMinusBytes{
				Field:       from,
				LengthMinus: minus,
			}

		} else {
			if types[typ] == nil {
				die("unknown type %q on line %q", typ, line)
			}
			f.Type = types[typ]
		}

		if isArray {
			for arrayLevel > 1 {
				f.Type = Array{Inner: f.Type}
				arrayLevel--
			}
			f.Type = Array{
				Inner:           f.Type,
				IsVarintArray:   isVarintArray,
				IsNullableArray: isNullableArray,
			}
		}

		s.Fields = append(s.Fields, f)
	}

	// we have no unprocessed line here.
	return "", done
}

func parseVersion(in string) (int, int, error) {
	max := -1
	if strings.IndexByte(in, '-') == -1 {
		if !strings.HasSuffix(in, "+") {
			return 0, 0, fmt.Errorf("open ended version %q missing + suffix", in)
		}
		if !strings.HasPrefix(in, "v") {
			return 0, 0, fmt.Errorf("open ended version %q mising v prefix", in)
		}
		min, err := strconv.Atoi(in[1 : len(in)-1])
		return min, max, err
	}

	min := 0
	span := strings.Split(in, "-")
	if len(span) != 2 {
		return 0, 0, fmt.Errorf("version %q invalid multiple ranges", in)
	}
	for _, part := range []struct {
		src string
		dst *int
	}{
		{span[0], &min},
		{span[1], &max},
	} {
		if !strings.HasPrefix(part.src, "v") {
			return 0, 0, fmt.Errorf("version number in range %q missing v prefix", in)
		}
		var err error
		if *part.dst, err = strconv.Atoi(part.src[1:]); err != nil {
			return 0, 0, fmt.Errorf("version number in range %q atoi err %v", in, err)
		}
	}
	if max < min {
		return 0, 0, fmt.Errorf("min %d > max %d on line %q", min, max, in)
	}
	return min, max, nil
}

func parseFieldLength(in string) (string, int, error) {
	lr := strings.Split(in, " - ")
	if len(lr) != 2 {
		return "", 0, fmt.Errorf("expected only two fields around ' = ', saw %d", len(lr))
	}
	length, err := strconv.Atoi(lr[1])
	if err != nil {
		return "", 0, fmt.Errorf("unable to parse length sub in %q", lr[1])
	}
	return lr[0], length, nil
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
