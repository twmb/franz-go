package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// If you are looking here, yes this is a shoddy parser, but it does the job.

// newStructs and newEnums are top level structs that we print at the end.
var (
	newStructs []Struct
	newEnums   []Enum
)

var enums = make(map[string]Enum)

var types = map[string]Type{
	"bool":            Bool{},
	"int8":            Int8{},
	"int16":           Int16{},
	"uint16":          Uint16{},
	"int32":           Int32{},
	"int64":           Int64{},
	"float64":         Float64{},
	"uint32":          Uint32{},
	"varint":          Varint{},
	"uuid":            Uuid{},
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
func (s *Struct) BuildFrom(scanner *LineScanner, key, level int) (done bool) {
	fieldSpaces := strings.Repeat(" ", 2*(level+1))

	var nextComment string
	var err error

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

		// ThrottleMillis is a special field:
		// - we die if there is preceding documentation
		// - there can only be a minimum version, no max
		// - no tags
		if strings.Contains(line, "ThrottleMillis") {
			if nextComment != "" {
				die("unexpected comment on ThrottleMillis: %s", nextComment)
			}
			s.Fields = append(s.Fields, parseThrottleMillis(line))
			continue
		}

		// TimeoutMillis can be a special field, or it can be standard
		// (for misc).
		if strings.Contains(line, "TimeoutMillis") && !strings.Contains(line, ":") {
			if nextComment != "" {
				die("unexpected comment on TimeoutMillis: %s", nextComment)
			}
			s.Fields = append(s.Fields, parseTimeoutMillis(line))
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
			Tag:        -1,
		}
		nextComment = ""

		typ := fields[1]

		// We parse field comments first; this is everything following
		// a // after the field.
		if idx := strings.Index(typ, " // "); idx >= 0 {
			f.MinVersion, f.MaxVersion, f.Tag, err = parseFieldComment(typ[idx:])
			if err != nil {
				die("unable to parse field comment on line %q: %v", line, err)
			}
			typ = typ[:idx]
		}

		// Now we do some array processing. Arrays can be nested
		// (although they are not nested in our definitions since
		// the flexible tag support).
		// We count the array depth here, check if it is encoded
		// specially, and remove any array decoration.
		//
		// This does not support special modifiers at any level but
		// the outside level.
		var isArray,
			isVarintArray,
			isNullableArray bool
		nullableVersion := 0
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
					if nullableVersion, err = strconv.Atoi(typ[:vend-1]); err != nil {
						die("improper nullable array version number %q: %v", typ[:vend-1], err)
					}
					typ = typ[vend:]
				}
			}
			typ = typ[arrayLevel : len(typ)-arrayLevel]
			isArray = true
		}

		// Now we check for defaults.
		var hasDefault bool
		var def string
		if start := strings.IndexByte(typ, '('); start >= 0 {
			end := strings.IndexByte(typ[start:], ')')
			if end <= 0 {
				die("invalid default: start %d, end %d", start, end)
			}
			hasDefault = true
			def = typ[start+1 : start+end]
			typ = typ[:start]
			if len(f.Comment) > 0 {
				f.Comment += "\n//\n"
			}
			f.Comment += "// This field has a default of " + def + "."
		}

		switch {
		case strings.HasPrefix(typ, "=>"): // nested struct; recurse
			newS := Struct{FromFlexible: s.FromFlexible, FlexibleAt: s.FlexibleAt}
			newS.Name = s.Name + f.FieldName
			newS.Key = key // for kmsg generating ordering purposes
			newS.Anonymous = true
			if isArray {
				if rename := typ[2:]; rename != "" { // allow rename hint after `=>`; braces were stripped above
					newS.Name = s.Name + rename
				} else {
					newS.Name = strings.TrimSuffix(newS.Name, "s") // make plural singular
				}
			}
			done = newS.BuildFrom(scanner, key, level+1)
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

		case strings.HasPrefix(typ, "nullable-string-v"):
			if typ[len(typ)-1] != '+' {
				die("invalid missing + at end of nullable-string-v; nullable-strings cannot become nullable and then become non-nullable")
			}
			if nullableVersion, err = strconv.Atoi(typ[len("nullable-string-v") : len(typ)-1]); err != nil {
				die("improper nullable string version number in %q: %v", typ, err)
			}
			f.Type = NullableString{
				FromFlexible:    s.FromFlexible,
				NullableVersion: nullableVersion,
			}

		case strings.HasPrefix(typ, "enum-"):
			typ = strings.TrimPrefix(typ, "enum-")
			if _, ok := enums[typ]; !ok {
				die("unknown enum %q on line %q", typ, line)
			}
			f.Type = enums[typ]

			if hasDefault {
				f.Type = f.Type.(Defaulter).SetDefault(def)
			}

		default: // type is known, lookup and set
			got := types[typ]
			if got == nil {
				die("unknown type %q on line %q", typ, line)
			}
			if s, ok := got.(Struct); ok {
				// If this field's struct type specified no encoding, then it
				// is not anonymous, but it is tied to a request and should be
				// ordered by that request when generating code.
				//
				// The default key is -1, so if we still have they key, we fix
				// it and also fix the key in the newStructs slice.
				if s.WithNoEncoding && s.Key == -1 {
					for i := range newStructs {
						if newStructs[i].Name == s.Name {
							newStructs[i].Key = key
						}
					}
					s.Key = key
					types[typ] = s
				}
			}
			f.Type = got

			if hasDefault {
				f.Type = f.Type.(Defaulter).SetDefault(def)
			}

			if s.FromFlexible {
				if setter, ok := f.Type.(FlexibleSetter); ok {
					f.Type = setter.AsFromFlexible()
				}
			}
		}

		// Finally, this field is an array, wrap what we parsed in an
		// array (perhaps multilevel).
		if isArray {
			for arrayLevel > 1 {
				f.Type = Array{Inner: f.Type, FromFlexible: s.FromFlexible}
				arrayLevel--
			}
			f.Type = Array{
				Inner:           f.Type,
				IsVarintArray:   isVarintArray,
				IsNullableArray: isNullableArray,
				NullableVersion: nullableVersion,
				FromFlexible:    s.FromFlexible,
			}
		}

		s.Fields = append(s.Fields, f)
	}

	return done
}

// 0: entire line
// 1: min version, if versioned
// 2: max version, if versioned, if exists
// 3: tag, if versioned, if exists
// 4: tag, if not versioned
var fieldRe = regexp.MustCompile(`^ // (?:v(\d+)(?:\+|\-v(\d+))(?:, tag (\d+))?|tag (\d+))$`)

func parseFieldComment(in string) (min, max, tag int, err error) {
	match := fieldRe.FindStringSubmatch(in)
	if len(match) == 0 {
		return 0, 0, 0, fmt.Errorf("invalid field comment %q", in)
	}

	if match[4] != "" { // not versioned
		tag, _ := strconv.Atoi(match[4])
		return -1, -1, tag, nil
	}

	min, _ = strconv.Atoi(match[1])
	max, _ = strconv.Atoi(match[2])
	tag, _ = strconv.Atoi(match[3])
	if match[2] == "" {
		max = -1
	} else if max < min {
		return 0, 0, 0, fmt.Errorf("min %d > max %d on line %q", min, max, in)
	}
	if match[3] == "" {
		tag = -1
	}
	return min, max, tag, nil
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

// 0: entire thing
// 1: optional version switched to post-reply throttling
// 2: optional version introduced
var throttleRe = regexp.MustCompile(`^ThrottleMillis(?:\((\d+)\))?(?: // v(\d+)\+)?$`)

func parseThrottleMillis(in string) StructField {
	match := throttleRe.FindStringSubmatch(in)
	if len(match) == 0 {
		die("throttle line does not match: %s", in)
	}

	typ := Throttle{}
	typ.Switchup, _ = strconv.Atoi(match[1])

	s := StructField{
		MaxVersion: -1,
		Tag:        -1,
		FieldName:  "ThrottleMillis",
		Type:       typ,
	}
	s.MinVersion, _ = strconv.Atoi(match[2])

	const switchupFmt = `// ThrottleMillis is how long of a throttle Kafka will apply to the client
// after this request.
// For Kafka < 2.0.0, the throttle is applied before issuing a response.
// For Kafka >= 2.0.0, the throttle is applied after issuing a response.
//
// This request switched at version %d.`

	const static = `// ThrottleMillis is how long of a throttle Kafka will apply to the client
// after responding to this request.`

	s.Comment = static
	if typ.Switchup > 0 {
		s.Comment = fmt.Sprintf(switchupFmt, typ.Switchup)
	}

	return s
}

// 0: entire thing
// 1: optional default
// 2: optional version introduced
var timeoutRe = regexp.MustCompile(`^TimeoutMillis(?:\((\d+)\))?(?: // v(\d+)\+)?$`)

func parseTimeoutMillis(in string) StructField {
	match := timeoutRe.FindStringSubmatch(in)
	if len(match) == 0 {
		die("timeout line does not match: %s", in)
	}

	s := StructField{
		Comment: `// TimeoutMillis is how long Kafka can wait before responding to this request.
// This field has no effect on Kafka's processing of the request; the request
// will continue to be processed if the timeout is reached. If the timeout is
// reached, Kafka will reply with a REQUEST_TIMED_OUT error.`,
		MaxVersion: -1,
		Tag:        -1,
		FieldName:  "TimeoutMillis",
		Type:       Timeout{},
	}
	s.MinVersion, _ = strconv.Atoi(match[2])
	def := "15000" // default to 15s for all timeouts
	if match[1] != "" {
		def = match[1]
	}
	s.Comment += "\n//\n// This field has a default of " + def + "."
	s.Type = s.Type.(Defaulter).SetDefault(def)

	return s
}

// 0: entire thing
// 1: name
// 2: "no encoding" if present
// 3: "with version field" if present
// 4: flexible version if present
var notTopLevelRe = regexp.MustCompile(`^([A-Za-z0-9]+) => not top level(?:, (?:(no encoding)|(with version field))(?:, flexible v(\d+)\+)?)?$`)

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

			FlexibleAt: -1, // by default, not flexible
		}
		resetComment()

		topLevel := true
		withVersionField, withNoEncoding := false, false

		flexibleAt := -1
		fromFlexible := false

		nameMatch := notTopLevelRe.FindStringSubmatch(line)
		name := line
		parseNoEncodingFlexible := func() {
			name = nameMatch[1]
			if nameMatch[4] != "" {
				flexible, err := strconv.Atoi(nameMatch[4])
				if err != nil || flexible < 0 {
					die("flexible version on line %q parse err: %v", line, err)
				}
				flexibleAt = flexible
				fromFlexible = true
			}
		}
		switch {
		case len(nameMatch) == 0:
		case nameMatch[2] != "":
			withNoEncoding = true
			parseNoEncodingFlexible()
		case nameMatch[3] != "":
			withVersionField = true
			parseNoEncodingFlexible()
		default: // simply "not top level"
			name = nameMatch[1]
		}

		key := -1
		save := func() {
			s.Name = name
			s.TopLevel = topLevel
			s.WithVersionField = withVersionField
			s.WithNoEncoding = withNoEncoding
			s.Key = key
			if !topLevel && fromFlexible {
				s.FromFlexible = fromFlexible
				s.FlexibleAt = flexibleAt
			}

			s.BuildFrom(scanner, key, 0)
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
			s.RequestKind = prior.Name
			if prior.FromFlexible {
				s.FlexibleAt = prior.FlexibleAt
				s.FromFlexible = true
			}
			key = prior.Key
			s.MaxVersion = prior.MaxVersion
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
			if rem[idx:] != ", admin" {
				die("unknown content after \"admin\" in %q", rem[idx:])
			}
			rem = rem[:idx]
		} else if idx := strings.Index(rem, ", group coordinator"); idx > 0 {
			s.GroupCoordinator = true
			if rem[idx:] != ", group coordinator" {
				die("unknown content after \"group coordinator\" in %q", rem[idx:])
			}
			rem = rem[:idx]
		} else if idx := strings.Index(rem, ", txn coordinator"); idx > 0 {
			s.TxnCoordinator = true
			if rem[idx:] != ", txn coordinator" {
				die("unknown content q after \"txn coordinator\" in %q", rem[idx:])
			}
			rem = rem[:idx]
		}

		if strings.HasSuffix(rem, "+") {
			const flexibleStr = ", flexible v"
			if idx := strings.Index(rem, flexibleStr); idx == -1 {
				die("missing flexible text on string ending with + in %q", rem)
			} else {
				flexible, err := strconv.Atoi(rem[idx+len(flexibleStr) : len(rem)-1])
				if err != nil || flexible < 0 {
					die("flexible version on line %q parse err: %v", line, err)
				}
				s.FlexibleAt = flexible
				s.FromFlexible = true
				rem = rem[:idx]
			}
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
			var err error
			if key, err = strconv.Atoi(rem[idx+len(keyStr):]); err != nil {
				die("key on line %q pare err: %v", line, err)
			}
			if key > maxKey {
				maxKey = key
			}
		}

		save()
	}
}

func ParseEnums(raw []byte) {
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

	writeComment := func(line string) {
		if nextComment.Len() > 0 {
			nextComment.WriteByte('\n')
		}
		nextComment.WriteString(line)
	}

	getComment := func() string {
		r := nextComment.String()
		resetComment()
		return r
	}

	// 1: name
	// 2: type
	// 3: if camel case (optional)
	enumNameRe := regexp.MustCompile(`^([A-Za-z]+) ([^ ]+) (camelcase )?\($`)
	// 1: value (number)
	// 2: word (meaning)
	enumFieldRe := regexp.MustCompile(`^ {2}(\d+): ([A-Z_a-z]+)$`)

	for scanner.Ok() {
		line := scanner.Peek()
		scanner.Next()
		if len(line) == 0 { // allow for arbitrary empty lines
			resetComment()
			continue
		}

		if strings.HasPrefix(line, "//") { // comment? keep and continue
			writeComment(line)
			continue
		}
		nameMatch := enumNameRe.FindStringSubmatch(line)
		if len(nameMatch) == 0 {
			die("invalid enum name, unable to match `Name type (`")
		}

		e := Enum{
			Comment: getComment(),

			Name:      nameMatch[1],
			Type:      types[nameMatch[2]],
			CamelCase: nameMatch[3] != "",
		}

		var ev EnumValue
		canStop := true
		saveValue := func() {
			ev.Comment = getComment()
			e.Values = append(e.Values, ev)
			if ev.Value == 0 {
				e.HasZero = true
			}
			ev = EnumValue{}
			canStop = true
		}

	out:
		for scanner.Ok() {
			line := scanner.Peek()
			scanner.Next()

			fieldMatch := enumFieldRe.FindStringSubmatch(line)

			switch {
			default:
				die("unable to determine line %s", line)
			case strings.HasPrefix(line, "  //"):
				canStop = false
				writeComment(line)
			case len(fieldMatch) > 0:
				num, err := strconv.Atoi(fieldMatch[1])
				if err != nil {
					die("unable to convert to number on line %s", line)
				}
				ev.Value = num
				ev.Word = fieldMatch[2]

				saveValue()

			case line == ")":
				break out
			}
		}
		if !canStop {
			die("invalid enum ending with a comment")
		}

		enums[e.Name] = e
		newEnums = append(newEnums, e)
	}
}
