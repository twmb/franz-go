package main

import (
	"fmt"
	"regexp"
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

// LineScanner is a shoddy scanner that allows us to peek an entire line.
type LineScanner struct {
	lineno int
	buf    string
	nlat   int
}

func (l *LineScanner) Ok() bool {
	if l.nlat >= 0 {
		return true
	}
	l.nlat = strings.IndexByte(l.buf, '\n')
	return l.nlat >= 0
}

func (l *LineScanner) Peek() string {
	return l.buf[:l.nlat]
}

func (l *LineScanner) Next() {
	l.lineno++
	l.buf = l.buf[l.nlat+1:]
	l.nlat = -1
}

// BuildFrom parses a struct, building it from the current scanner. Level
// signifies how nested this struct is. Top level structs have a level of 0.
//
// If a blank line is ever encountered, the struct is done being built and
// the done status is bubbled up through all recursion levels.
func (s *Struct) BuildFrom(scanner *LineScanner, level int) (done bool) {
	fieldSpaces := strings.Repeat(" ", 2*(level+1))

	var nextComment string

	for !done && scanner.Ok() {
		line := scanner.Peek()
		if len(line) == 0 { // struct we were working on is done
			scanner.Next()
			return true
		}
		if !strings.HasPrefix(line, fieldSpaces) {
			return false // inner level struct done
		}

		scanner.Next() // we will be keeping this line, so skip the scanner to the next now

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
			die("improper struct field format on line %q (%d)", line, scanner.lineno)
		}

		f := StructField{
			Comment:    nextComment,
			FieldName:  fields[0],
			MaxVersion: -1,
		}
		nextComment = ""

		typ := fields[1]

		if idx := strings.Index(typ, " // v"); idx > 0 { // first, check version bounds
			var err error
			f.MinVersion, f.MaxVersion, err = parseVersion(typ[idx+4:]) // start at first v
			if err != nil {
				die("unable to parse version on line %q: %v", line, err)
			}
			typ = typ[:idx]
		}

		// First, some array processing. Arrays can be nested.
		// We count the array depth here, check if it is encoded
		// specially, and remove any array decoration.
		//
		// This does not support special modifiers at any level but
		// the outside level.
		isArray := false
		isVarintArray := false
		isNullableArray := false
		nullableArrayVersion := 0
		arrayLevel := strings.Count(typ, "[")
		if arrayLevel > 0 {
			if strings.HasPrefix(typ, "varint[") {
				isVarintArray = true
				typ = typ[len("varint"):]
			} else if strings.HasPrefix(typ, "nullable") {
				isNullableArray = true
				typ = typ[len("nullable"):]
				if strings.HasPrefix(typ, "-v") {
					typ = typ[len("-v"):]
					vend := strings.IndexByte(typ, '[')
					if vend < 2 {
						die("empty nullable array version number")
					}
					if typ[vend-1] != '+' {
						die("max version number bound is unhandled in arrays")
					}
					var err error
					nullableArrayVersion, err = strconv.Atoi(typ[:vend-1])
					if err != nil {
						die("improper nullable array version number %q: %v", typ[:vend-1], err)
					}
					typ = typ[vend:]
				}
			}
			typ = typ[arrayLevel : len(typ)-arrayLevel]
			isArray = true
		}

		switch {
		case strings.HasPrefix(typ, "=>"): // nested struct; recurse
			var newS Struct
			newS.Name = s.Name + f.FieldName
			newS.Anonymous = true
			if isArray {
				if rename := typ[2:]; rename != "" { // allow rename hint after `=>`; braces were stripped above
					newS.Name = s.Name + rename
				} else {
					newS.Name = strings.TrimSuffix(newS.Name, "s") // make plural singular
				}
			}
			done = newS.BuildFrom(scanner, level+1)
			f.Type = newS
			newStructs = append(newStructs, newS)

		case strings.HasPrefix(typ, "length-field-minus => "): // special bytes referencing another field
			typ = strings.TrimPrefix(typ, "length-field-minus => ")
			from, minus, err := parseFieldLength(typ)
			if err != nil {
				die("unable to parse field-length-bytes in %q: %v", typ, err)
			}
			f.Type = FieldLengthMinusBytes{
				Field:       from,
				LengthMinus: minus,
			}

		default: // type is known, lookup and set
			if types[typ] == nil {
				die("unknown type %q on line %q", typ, line)
			}
			f.Type = types[typ]
		}

		// Finally, this field is an array, wrap what we parsed in an
		// array (perhaps multilevel).
		if isArray {
			for arrayLevel > 1 {
				f.Type = Array{Inner: f.Type}
				arrayLevel--
			}
			f.Type = Array{
				Inner:           f.Type,
				IsVarintArray:   isVarintArray,
				IsNullableArray: isNullableArray,
				NullableVersion: nullableArrayVersion,
			}
		}

		s.Fields = append(s.Fields, f)
	}

	return done
}

var versionRe = regexp.MustCompile(`^v(\d+)(?:\+|\-v(\d+))$`)

func parseVersion(in string) (int, int, error) {
	match := versionRe.FindStringSubmatch(in)
	if len(match) == 0 {
		return 0, 0, fmt.Errorf("invalid version %q", in)
	}
	min, _ := strconv.Atoi(match[1])
	max, _ := strconv.Atoi(match[2])
	if match[2] == "" {
		max = -1
	} else if max < min {
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
	scanner := &LineScanner{
		buf:  string(raw),
		nlat: -1,
	}

	var nextComment strings.Builder
	resetComment := func() {
		l := nextComment.Len()
		nextComment.Reset()
		nextComment.Grow(l)
	}

	for scanner.Ok() {
		line := scanner.Peek()
		scanner.Next()
		if len(line) == 0 { // allow for arbitrary comments
			resetComment()
			continue
		}

		if strings.HasPrefix(line, "//") { // comment? keep and continue
			if nextComment.Len() > 0 {
				nextComment.WriteByte('\n')
			}
			nextComment.WriteString(line)
			continue
		}

		s := Struct{
			Comment: nextComment.String(),
		}
		resetComment()

		topLevel := true

		name := strings.TrimSuffix(line, " => not top level")
		save := func() {
			s.Name = name
			s.TopLevel = topLevel
			s.BuildFrom(scanner, 0)
			types[name] = s
			newStructs = append(newStructs, s)
		}

		if line != name { // if we trimmed, this is not top level
			topLevel = false
			save()
			continue
		}

		if strings.HasSuffix(name, "Response =>") { // response following a request?
			last := strings.Replace(name, "Response =>", "Request", 1)
			prior := &newStructs[len(newStructs)-1]
			if prior.Name != last {
				die("from %q does not refer to last message defn on line %q", last, line)
			}
			name = strings.TrimSuffix(name, " =>")
			prior.ResponseKind = name
			save()
			continue
		}

		// At this point, we are dealing with a top level request.
		// The order, l to r, is key, version, admin/group/txn.
		// We strip and process r to l.
		delim := strings.Index(name, " =>")
		if delim == -1 {
			die("missing struct delimiter on line %q", line)
		}
		rem := name[delim+3:]
		name = name[:delim]

		if idx := strings.Index(rem, ", admin"); idx > 0 {
			s.Admin = true
			rem = rem[:idx]
		} else if idx := strings.Index(rem, ", group coordinator"); idx > 0 {
			s.GroupCoordinator = true
			rem = rem[:idx]
		} else if idx := strings.Index(rem, ", txn coordinator"); idx > 0 {
			s.TxnCoordinator = true
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
