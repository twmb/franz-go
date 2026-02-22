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
	"varlong":         Varlong{},
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
	file   string
	lineno int
	buf    string
	nlat   int
}

func (l *LineScanner) dief(why string, args ...any) {
	die("%s:%d: "+why, append([]any{l.file, l.lineno}, args...)...)
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

		// ThrottleMillis and TimeoutMillis are special fields with
		// their own syntax (no ": " separator). TimeoutMillis can
		// also appear as a regular field (with ":"), so we only
		// treat it specially when there is no colon.
		if sf, ok := parseSpecialField(scanner, line); ok {
			if nextComment != "" {
				scanner.dief("unexpected comment on %s: %s", sf.FieldName, nextComment)
			}
			nextComment = ""
			s.Fields = append(s.Fields, sf)
			continue
		}

		// Fields are name on left, type on right.
		fields := strings.Split(line, ": ")
		if len(fields) != 2 || len(fields[0]) == 0 || len(fields[1]) == 0 {
			scanner.dief("improper struct field format on line %q", line)
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
				scanner.dief("unable to parse field comment on line %q: %v", line, err)
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
						scanner.dief("empty nullable array version number")
					}
					if typ[vend-1] != '+' {
						scanner.dief("max version number bound is unhandled in arrays")
					}
					if nullableVersion, err = strconv.Atoi(typ[:vend-1]); err != nil {
						scanner.dief("improper nullable array version number %q: %v", typ[:vend-1], err)
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
				scanner.dief("invalid default: start %d, end %d", start, end)
			}
			hasDefault = true
			def = typ[start+1 : start+end]
			typ = typ[:start]
			if len(f.Comment) > 0 {
				f.Comment += "\n//\n"
			}
			f.Comment += "// This field has a default of " + def + "."
		}

		if strings.HasPrefix(typ, "=>") || strings.HasPrefix(typ, "nullable=>") {
			// Nested struct; recurse.
			newS := Struct{
				FromFlexible: s.FromFlexible,
				FlexibleAt:   s.FlexibleAt,
				Nullable:     strings.HasPrefix(typ, "nullable"),
			}
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
		} else {
			f.Type = resolveType(scanner, s, typ, key, line)
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

// resolveType resolves a type name string to a Type, handling special type
// prefixes (length-field-minus, nullable-string-v, enum-) and named type
// lookup. The caller handles defaults and flexible setter application.
func resolveType(scanner *LineScanner, s *Struct, typ string, key int, line string) Type {
	switch {
	case strings.HasPrefix(typ, "length-field-minus => "):
		typ = strings.TrimPrefix(typ, "length-field-minus => ")
		from, minus, err := parseFieldLength(typ)
		if err != nil {
			scanner.dief("unable to parse field-length-bytes in %q: %v", typ, err)
		}
		return FieldLengthMinusBytes{
			Field:       from,
			LengthMinus: minus,
		}

	case strings.HasPrefix(typ, "nullable-string-v"):
		if typ[len(typ)-1] != '+' {
			scanner.dief("invalid missing + at end of nullable-string-v; nullable-strings cannot become nullable and then become non-nullable")
		}
		nullableVersion, err := strconv.Atoi(typ[len("nullable-string-v") : len(typ)-1])
		if err != nil {
			scanner.dief("improper nullable string version number in %q: %v", typ, err)
		}
		return NullableString{
			FromFlexible:    s.FromFlexible,
			NullableVersion: nullableVersion,
		}

	case strings.HasPrefix(typ, "enum-"):
		typ = strings.TrimPrefix(typ, "enum-")
		if _, ok := enums[typ]; !ok {
			scanner.dief("unknown enum %q on line %q", typ, line)
		}
		return enums[typ]

	default:
		got := types[typ]
		if got == nil {
			scanner.dief("unknown type %q on line %q", typ, line)
		}
		if s, ok := got.(Struct); ok {
			// If this field's struct type specified no encoding, then it
			// is not anonymous, but it is tied to a request and should be
			// ordered by that request when generating code.
			//
			// The default key is -1, so if we still have the key, we fix
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
		return got
	}
}

// validateStructs validates all parsed structs. This centralizes checks that
// were previously scattered across code generation, catching errors earlier
// with clearer messages.
func validateStructs() {
	for _, s := range newStructs {
		validateStructFields(s.Name, s.Fields, s.FromFlexible)
	}
}

func validateStructFields(structName string, fields []StructField, fromFlexible bool) {
	tags := make(map[int]StructField)
	for _, f := range fields {
		if f.MinVersion == -1 && f.MaxVersion > 0 {
			die("struct %s field %s: unexpected negative min version %d while max version %d", structName, f.FieldName, f.MinVersion, f.MaxVersion)
		}
		if f.MinVersion == -1 && f.Tag < 0 {
			die("struct %s field %s: min version -1 with no tag", structName, f.FieldName)
		}
		if f.Tag >= 0 {
			if prev, exists := tags[f.Tag]; exists {
				die("struct %s: duplicate tag %d on fields %s and %s", structName, f.Tag, prev.FieldName, f.FieldName)
			}
			tags[f.Tag] = f
		}
	}
	if fromFlexible {
		for i := range len(tags) {
			f, exists := tags[i]
			if !exists {
				die("struct %s: saw %d tags, but did not see tag %d; expected monotonically increasing", structName, len(tags), i)
			}
			if _, canDefault := f.Type.(Defaulter); !canDefault {
				die("struct %s field %s: tagged field has no Defaulter", structName, f.FieldName)
			}
		}
	}
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

func parseThrottleMillis(scanner *LineScanner, in string) StructField {
	match := throttleRe.FindStringSubmatch(in)
	if len(match) == 0 {
		scanner.dief("throttle line does not match: %s", in)
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

func parseTimeoutMillis(scanner *LineScanner, in string) StructField {
	match := timeoutRe.FindStringSubmatch(in)
	if len(match) == 0 {
		scanner.dief("timeout line does not match: %s", in)
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

// parseSpecialField checks if a line is a special field (ThrottleMillis or
// TimeoutMillis without a colon separator) and returns the parsed field.
func parseSpecialField(scanner *LineScanner, line string) (StructField, bool) {
	if strings.Contains(line, "ThrottleMillis") {
		return parseThrottleMillis(scanner, line), true
	}
	if strings.Contains(line, "TimeoutMillis") && !strings.Contains(line, ":") {
		return parseTimeoutMillis(scanner, line), true
	}
	return StructField{}, false
}

const notTopLevelSuffix = " => not top level"

// Parse parses the raw contents of a messages file and adds all newly
// parsed structs to newStructs.
func Parse(file string, raw []byte) {
	scanner := &LineScanner{
		file: file,
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

		name := line
		if idx := strings.Index(line, notTopLevelSuffix); idx > 0 {
			name = line[:idx]
			rem := line[idx+len(notTopLevelSuffix):]
			if rem != "" {
				for _, part := range strings.Split(strings.TrimPrefix(rem, ", "), ", ") {
					switch {
					case part == "no encoding":
						withNoEncoding = true
					case part == "with version field":
						withVersionField = true
					case strings.HasPrefix(part, "flexible v"):
						vstr := strings.TrimSuffix(part[len("flexible v"):], "+")
						flexible, err := strconv.Atoi(vstr)
						if err != nil || flexible < 0 {
							scanner.dief("flexible version on line %q parse err: %v", line, err)
						}
						flexibleAt = flexible
						fromFlexible = true
					default:
						scanner.dief("unknown not-top-level modifier %q on line %q", part, line)
					}
				}
			}
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
				scanner.dief("from %q does not refer to last message defn on line %q", last, line)
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
		// Format: Name => key N, max version N[, admin|group coordinator|txn coordinator][, flexible vN+]
		delim := strings.Index(name, " => ")
		if delim == -1 {
			scanner.dief("missing struct delimiter on line %q", line)
		}
		modifiers := name[delim+4:]
		name = name[:delim]

		for _, part := range strings.Split(modifiers, ", ") {
			switch {
			case strings.HasPrefix(part, "key "):
				var err error
				if key, err = strconv.Atoi(part[len("key "):]); err != nil {
					scanner.dief("key on line %q parse err: %v", line, err)
				}
				if key > maxKey {
					maxKey = key
				}
			case strings.HasPrefix(part, "max version "):
				max, err := strconv.Atoi(part[len("max version "):])
				if err != nil {
					scanner.dief("max version on line %q parse err: %v", line, err)
				}
				s.MaxVersion = max
			case part == "admin":
				s.Admin = true
			case part == "group coordinator":
				s.GroupCoordinator = true
			case part == "txn coordinator":
				s.TxnCoordinator = true
			case strings.HasPrefix(part, "flexible v"):
				vstr := strings.TrimSuffix(part[len("flexible v"):], "+")
				flexible, err := strconv.Atoi(vstr)
				if err != nil || flexible < 0 {
					scanner.dief("flexible version on line %q parse err: %v", line, err)
				}
				s.FlexibleAt = flexible
				s.FromFlexible = true
			default:
				scanner.dief("unknown request modifier %q on line %q", part, line)
			}
		}

		save()
	}
}

func ParseEnums(file string, raw []byte) {
	scanner := &LineScanner{
		file: file,
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
			scanner.dief("invalid enum name, unable to match `Name type (`")
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
				scanner.dief("unable to determine line %s", line)
			case strings.HasPrefix(line, "  //"):
				canStop = false
				writeComment(line)
			case len(fieldMatch) > 0:
				num, err := strconv.Atoi(fieldMatch[1])
				if err != nil {
					scanner.dief("unable to convert to number on line %s", line)
				}
				ev.Value = num
				ev.Word = fieldMatch[2]

				saveValue()

			case line == ")":
				break out
			}
		}
		if !canStop {
			scanner.dief("invalid enum ending with a comment")
		}

		enums[e.Name] = e
		newEnums = append(newEnums, e)
	}
}
