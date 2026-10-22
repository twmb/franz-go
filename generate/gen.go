package main

import (
	"fmt"
	"strconv"
	"strings"
)

func (Bool) TypeName() string                  { return "bool" }
func (Int8) TypeName() string                  { return "int8" }
func (Int16) TypeName() string                 { return "int16" }
func (Uint16) TypeName() string                { return "uint16" }
func (Int32) TypeName() string                 { return "int32" }
func (Int64) TypeName() string                 { return "int64" }
func (Float64) TypeName() string               { return "float64" }
func (Uint32) TypeName() string                { return "uint32" }
func (Varint) TypeName() string                { return "int32" }
func (Varlong) TypeName() string               { return "int64" }
func (Uuid) TypeName() string                  { return "[16]byte" }
func (String) TypeName() string                { return "string" }
func (NullableString) TypeName() string        { return "*string" }
func (Bytes) TypeName() string                 { return "[]byte" }
func (NullableBytes) TypeName() string         { return "[]byte" }
func (VarintString) TypeName() string          { return "string" }
func (VarintBytes) TypeName() string           { return "[]byte" }
func (a Array) TypeName() string               { return "[]" + a.Inner.TypeName() }
func (Throttle) TypeName() string              { return "int32" }
func (FieldLengthMinusBytes) TypeName() string { return "[]byte" }

func (s Struct) TypeName() string {
	if s.Nullable {
		return "*" + s.Name
	}
	return s.Name
}

func (e Enum) TypeName() string { return e.Name }

// writeAppendValue appends val, a value of t, for any t but a struct or an
// array: every other type is appended with one call, mostly to a function in
// codec.go.
func writeAppendValue(l *LineWriter, t Type, val string) {
	switch t := t.(type) {
	case Bool:
		l.Write("dst = appendBool(dst, %s)", val)
	case Int8:
		l.Write("dst = appendInt8(dst, %s)", val)
	case Int16:
		l.Write("dst = appendInt16(dst, %s)", val)
	case Uint16:
		l.Write("dst = appendUint16(dst, %s)", val)
	case Int32, Throttle, Timeout:
		l.Write("dst = appendInt32(dst, %s)", val)
	case Uint32:
		l.Write("dst = kbin.AppendUint32(dst, %s)", val)
	case Int64:
		l.Write("dst = appendInt64(dst, %s)", val)
	case Uuid:
		l.Write("dst = appendUuid(dst, %s)", val)
	case Float64:
		l.Write("dst = kbin.AppendFloat64(dst, %s)", val)
	case Varint:
		l.Write("dst = kbin.AppendVarint(dst, %s)", val)
	case Varlong:
		l.Write("dst = kbin.AppendVarlong(dst, %s)", val)
	case VarintString:
		l.Write("dst = kbin.AppendVarintString(dst, %s)", val)
	case VarintBytes:
		l.Write("dst = kbin.AppendVarintBytes(dst, %s)", val)
	case String:
		compactAppend(t.FromFlexible, "String", val, l)
	case Bytes:
		compactAppend(t.FromFlexible, "Bytes", val, l)
	case NullableBytes:
		compactAppend(t.FromFlexible, "NullableBytes", val, l)
	case NullableString:
		if t.NullableVersion > 0 {
			l.Write("if version < %d {", t.NullableVersion)
			l.Write("var vv string")
			l.Write("if %s != nil {", val)
			l.Write("vv = *%s", val)
			l.Write("}")
			compactAppend(t.FromFlexible, "String", "vv", l)
			l.Write("} else {")
			defer l.Write("}")
		}
		compactAppend(t.FromFlexible, "NullableString", val, l)
	case FieldLengthMinusBytes:
		l.Write("dst = append(dst, %s...)", val)
	case Enum:
		writeAppendValue(l, t.Type, t.Type.TypeName()+"("+val+")")
	default:
		die("no append for %T", t)
	}
}

func compactAppend(fromFlexible bool, name, val string, l *LineWriter) {
	if fromFlexible {
		l.Write("dst = append%s(dst, %s, isFlexible)", name, val) // see codec.go
		return
	}
	l.Write("dst = kbin.Append%s(dst, %s)", name, val)
}

// writeAppendV appends v, a value of t that the caller declared.
func writeAppendV(l *LineWriter, t Type) {
	switch t := t.(type) {
	case Struct:
		t.WriteAppend(l)
	case Array:
		t.WriteAppend(l)
	default:
		writeAppendValue(l, t, "v")
	}
}

func (a Array) WriteAppend(l *LineWriter) {
	writeNullable := func() {
		if a.FromFlexible {
			l.Write("dst = appendNullableArrayLen(dst, len(v), v == nil, isFlexible)") // see codec.go
		} else {
			l.Write("dst = kbin.AppendNullableArrayLen(dst, len(v), v == nil)")
		}
	}

	writeNormal := func() {
		if a.FromFlexible {
			l.Write("dst = appendArrayLen(dst, len(v), isFlexible)") // see codec.go
		} else {
			l.Write("dst = kbin.AppendArrayLen(dst, len(v))")
		}
	}

	switch {
	case a.IsVarintArray:
		l.Write("dst = kbin.AppendVarint(dst, int32(len(v)))")
	case a.IsNullableArray:
		if a.NullableVersion > 0 {
			l.Write("if version >= %d {", a.NullableVersion)
			writeNullable()
			l.Write("} else {")
			writeNormal()
			l.Write("}")
		} else {
			writeNullable()
		}
	default:
		writeNormal()
	}
	l.Write("for i := range v {")
	switch t := a.Inner.(type) {
	case Struct:
		// If the array elements are structs, we avoid copying the
		// struct out and instead grab a pointer to the element.
		if t.Nullable {
			l.Write("v := v[i]")
		} else {
			l.Write("v := &v[i]")
		}
		t.WriteAppend(l)
	case Array:
		l.Write("v := v[i]")
		t.WriteAppend(l)
	default:
		writeAppendValue(l, t, "v[i]")
	}
	l.Write("}")
}

func (s Struct) WriteAppend(l *LineWriter) {
	tags := make(map[int]StructField)
	if s.Nullable {
		l.Write("if v == nil {")
		l.Write("dst = append(dst, 255)") // -1
		l.Write("} else {")
		l.Write("dst = append(dst, 1)")
		defer l.Write("}")
	}
	for _, f := range s.Fields {
		if onlyTag := f.writeBeginAndTag(l, tags); onlyTag {
			continue
		}
		switch t := f.Type.(type) {
		case Struct:
			// If the struct field is a struct itself, we avoid copying it
			// and instead grab a pointer.
			if t.Nullable {
				l.Write("v := v.%s", f.FieldName)
			} else {
				l.Write("v := &v.%s", f.FieldName)
			}
			t.WriteAppend(l)
		case Array:
			l.Write("v := v.%s", f.FieldName)
			t.WriteAppend(l)
		default:
			writeAppendValue(l, t, "v."+f.FieldName)
		}
		l.Write("}")
	}

	if !s.FromFlexible {
		return
	}

	l.Write("if isFlexible {")
	defer l.Write("}")

	if len(tags) > 0 {
		defer l.Write("dst = v.UnknownTags.AppendEach(dst)")

		l.Write("var toEncode []uint32")
		for i := range len(tags) {
			f := tags[i]
			canDefault := false
			if d, ok := f.Type.(Defaulter); ok {
				canDefault = true
				def, has := d.GetDefault()
				if !has {
					def = d.GetTypeDefault()
				}
				switch t := f.Type.(type) {
				case Struct:
					l.Write("if %s {", strings.Join(notDefault("v."+f.FieldName, t), " || "))

				case Array:
					if t.IsNullableArray {
						l.Write("if version < %[1]d && len(v.%[2]s) > 0 || version >= %[1]d && v.%[2]s != nil {", t.NullableVersion, f.FieldName)
					} else {
						l.Write("if len(v.%s) > 0 {", f.FieldName)
					}

				default:
					l.Write("if v.%s != %v {", f.FieldName, def)
				}
			}
			l.Write("toEncode = append(toEncode, %d)", i)
			if canDefault {
				l.Write("}")
			}
		}

		l.Write("dst = kbin.AppendUvarint(dst, uint32(len(toEncode) + v.UnknownTags.Len()))")
		l.Write("for _, tag := range toEncode {")
		l.Write("switch tag {")
		defer l.Write("}")
		defer l.Write("}")
	} else {
		// No defined tags, we add unknown only.
		l.Write("dst = v.UnknownTags.appendWithCount(dst)")
	}

	for i := range len(tags) {
		l.Write("case %d:", i)
		f := tags[i]

		l.Write("{")
		l.Write("v := v.%s", f.FieldName)
		l.Write("dst = kbin.AppendUvarint(dst, %d)", i) // tag num
		switch f.Type.(type) {
		case Bool, Int8:
			l.Write("dst = kbin.AppendUvarint(dst, 1)") // size
			writeAppendV(l, f.Type)
		case Int16, Uint16:
			l.Write("dst = kbin.AppendUvarint(dst, 2)")
			writeAppendV(l, f.Type)
		case Int32, Uint32:
			l.Write("dst = kbin.AppendUvarint(dst, 4)")
			writeAppendV(l, f.Type)
		case Int64, Float64:
			l.Write("dst = kbin.AppendUvarint(dst, 8)")
			writeAppendV(l, f.Type)
		case Varint:
			l.Write("dst = kbin.AppendUvarint(dst, kbin.VarintLen(v))")
			writeAppendV(l, f.Type)
		case Varlong:
			l.Write("dst = kbin.AppendUvarint(dst, kbin.VarlongLen(v))")
			writeAppendV(l, f.Type)
		case Uuid:
			l.Write("dst = kbin.AppendUvarint(dst, 16)")
			writeAppendV(l, f.Type)
		case Array, Struct, String, NullableString, Bytes, NullableBytes:
			l.Write("sized := false")
			l.Write("lenAt := len(dst)")
			l.Write("f%s:", f.FieldName)
			writeAppendV(l, f.Type)
			l.Write("if !sized {")
			l.Write("dst = kbin.AppendUvarint(dst[:lenAt], uint32(len(dst[lenAt:])))")
			l.Write("sized = true")
			l.Write("goto f%s", f.FieldName)
			l.Write("}")
		default:
			die("tag type %v unsupported in append! fix this!", f.Type.TypeName())
		}
		l.Write("}")
	}
}

// notDefault returns the conditions under which expr, a value of type t,
// differs from its default; a tagged field is encoded if any is true.
func notDefault(expr string, t Type) []string {
	switch t := t.(type) {
	case Struct:
		if t.Nullable {
			return []string{expr + " != nil"}
		}
		var conds []string
		for _, f := range t.Fields {
			conds = append(conds, notDefault(expr+"."+f.FieldName, f.Type)...)
		}
		if t.FromFlexible {
			conds = append(conds, expr+".UnknownTags.Len() > 0")
		}
		return conds
	case Array:
		if t.IsNullableArray {
			return []string{expr + " != nil"}
		}
		return []string{"len(" + expr + ") > 0"}
	case Bytes, VarintBytes:
		return []string{"len(" + expr + ") > 0"}
	case String, VarintString:
		return []string{expr + ` != ""`}
	case Defaulter:
		def, has := t.GetDefault()
		if !has {
			def = t.GetTypeDefault()
		}
		return []string{fmt.Sprintf("%s != %v", expr, def)}
	}
	die("no default comparison for %T", t)
	return nil
}

// writeBeginAndTag begins a struct field encode/decode and adds the field to
// the tags map if necessary. If this field is only tagged, this returns true.
// Validation of field versions and tags is done in validateStructFields.
func (f StructField) writeBeginAndTag(l *LineWriter, tags map[int]StructField) (onlyTag bool) {
	if f.Tag >= 0 {
		tags[f.Tag] = f
	}
	switch {
	case f.MaxVersion > -1:
		l.Write("if version >= %d && version <= %d {", f.MinVersion, f.MaxVersion)
	case f.MinVersion > 0:
		l.Write("if version >= %d {", f.MinVersion)
	case f.MinVersion == -1:
		return true
	default:
		l.Write("{")
	}
	return false
}

// writeDecodeInto decodes into dst for any t but a struct or an array: every
// other type is decoded with one call, mostly to a function in codec.go. The
// functions for strings and bytes store into dst themselves, so that the store
// and its write barrier are not repeated at every field.
func writeDecodeInto(l *LineWriter, t Type, dst string) {
	flex := func(fromFlexible bool) string {
		if fromFlexible {
			return "isFlexible"
		}
		return "false"
	}
	switch t := t.(type) {
	case String:
		l.Write("readString(&b, &%s, %s, unsafe)", dst, flex(t.FromFlexible))
	case Bytes:
		l.Write("readBytes(&b, &%s, %s)", dst, flex(t.FromFlexible))
	case NullableBytes:
		l.Write("readNullableBytes(&b, &%s, %s)", dst, flex(t.FromFlexible))
	case VarintString:
		l.Write("readVarintString(&b, &%s, unsafe)", dst)
	case VarintBytes:
		l.Write("readVarintBytes(&b, &%s)", dst)
	case NullableString:
		// If there is a nullable version, we write a "read string, then
		// set pointer" block.
		if t.NullableVersion > 0 {
			l.Write("if version < %d {", t.NullableVersion)
			l.Write("v := new(string)")
			l.Write("readString(&b, v, %s, unsafe)", flex(t.FromFlexible))
			l.Write("%s = v", dst)
			l.Write("} else {")
			defer l.Write("}")
		}
		l.Write("readNullableString(&b, &%s, %s, unsafe)", dst, flex(t.FromFlexible))
	case FieldLengthMinusBytes:
		l.Write("%s = b.Span(int(s.%s) - %d)", dst, t.Field, t.LengthMinus)
	default:
		l.Write("%s = %s", dst, readExpr(t))
	}
}

// readExpr returns the expression reading a fixed size or varint t.
func readExpr(t Type) string {
	switch t := t.(type) {
	case Bool:
		return "readBool(&b)"
	case Int8:
		return "readInt8(&b)"
	case Int16:
		return "readInt16(&b)"
	case Uint16:
		return "readUint16(&b)"
	case Int32, Throttle, Timeout:
		return "readInt32(&b)"
	case Uint32:
		return "b.Uint32()"
	case Int64:
		return "readInt64(&b)"
	case Uuid:
		return "readUuid(&b)"
	case Float64:
		return "b.Float64()"
	case Varint:
		return "b.Varint()"
	case Varlong:
		return "b.Varlong()"
	case Enum:
		return t.Name + "(" + readExpr(t.Type) + ")"
	}
	die("no read for %T", t)
	return ""
}

func (a Array) WriteDecode(l *LineWriter) {
	// For decoding arrays, we copy our "v" variable to our own "a"
	// variable so that the scope opened just below can use its own
	// v variable. At the end, we reset v with any updates to a.
	l.Write("a := v")
	l.Write("var l int32")

	if a.IsVarintArray {
		l.Write("l = b.VarintArrayLen()")
	} else {
		if a.FromFlexible {
			l.Write("l = readArrayLen(&b, isFlexible)") // see codec.go
		} else {
			l.Write("l = b.ArrayLen()")
		}
		if a.IsNullableArray {
			l.Write("if version < %d || l == 0 {", a.NullableVersion)
			l.Write("a = %s{}", a.TypeName())
			l.Write("}")
		}
	}

	a.writeLenBound(l)

	l.Write("a = a[:0]")

	l.Write("if l > 0 {")
	l.Write("a = append(a, make(%s, l)...)", a.TypeName())
	l.Write("}")

	l.Write("for i := range a {")
	switch t := a.Inner.(type) {
	case Struct:
		if t.Nullable {
			l.Write("if present := b.Int8(); present != -1 && b.Ok() {")
			defer l.Write("}")
		}
		l.Write("v := &a[i]")
		l.Write("v.Default()") // set defaults first
		t.WriteDecode(l)
	case Array:
		// With nested arrays, we declare a new v and introduce scope
		// so that the next level will not collide with our current "a".
		l.Write("v := a[i]")
		l.Write("{")
		t.WriteDecode(l)
		l.Write("}")
		l.Write("a[i] = v")
	default:
		writeDecodeInto(l, t, "a[i]")
	}

	l.Write("}") // close the for loop

	l.Write("v = a")
}

// writeLenBound fails a decode if reading l failed, or if l elements, each at
// least its fewest encoded bytes, cannot fit in the bytes left. The ArrayLen
// functions allow a byte per element, but we allocate every element before
// reading any, and an element can take hundreds of bytes in memory.
//
// The fewest bytes can change with version; we check the size of the latest
// versions, and the smallest size of every version before them. We do not
// know here which versions are flexible, so a size is the smaller of the
// flexible and non-flexible sizes.
func (a Array) writeLenBound(l *LineWriter) {
	size := func(v int) int {
		return min(minSize(a.Inner, v, false), minSize(a.Inner, v, true))
	}
	last := lastVersionChange(a.Inner)
	latest := size(last)
	since, least := last, latest
	for v := last - 1; v >= -1; v-- { // -1 is every negative version
		n := size(v)
		if n == latest && since == v+1 {
			since = v
		}
		least = min(least, n)
	}
	conds := []string{"!b.Ok()"}
	if least > 1 {
		conds = append(conds, fmt.Sprintf("int64(l)*%d > int64(len(b.Src))", least))
	}
	if latest > least {
		conds = append(conds, fmt.Sprintf("version >= %d && int64(l)*%d > int64(len(b.Src))", since, latest))
	}
	l.Write("if %s {", strings.Join(conds, " || "))
	l.Write("return kbin.ErrNotEnoughData")
	l.Write("}")
}

// minSize returns the fewest bytes t can be encoded in at version, if the
// version is flexible or not.
func minSize(t Type, version int, flexible bool) int {
	switch t := t.(type) {
	case Bool, Int8, Varint, Varlong, VarintString, VarintBytes:
		return 1
	case Int16, Uint16:
		return 2
	case Int32, Uint32, Throttle, Timeout:
		return 4
	case Int64, Float64:
		return 8
	case Uuid:
		return 16
	case FieldLengthMinusBytes:
		return 0
	case Enum:
		return minSize(t.Type, version, flexible)
	case String:
		return lenSize(t.FromFlexible && flexible, 2)
	case NullableString:
		return lenSize(t.FromFlexible && flexible, 2)
	case Bytes:
		return lenSize(t.FromFlexible && flexible, 4)
	case NullableBytes:
		return lenSize(t.FromFlexible && flexible, 4)
	case Array:
		return lenSize(t.FromFlexible && flexible || t.IsVarintArray, 4)
	case Struct:
		if t.Nullable {
			return 1
		}
		fields := t.Fields
		if t.WithVersionField {
			fields = fields[1:] // see Struct.WriteDecode
		}
		var n int
		for _, f := range fields {
			if f.inBody(version) {
				n += minSize(f.Type, version, flexible)
			}
		}
		if t.FromFlexible && flexible {
			n++ // the tag count
		}
		return n
	}
	die("unknown type %T for minSize", t)
	return 0
}

// lenSize returns the fewest bytes of a length: a one byte uvarint if compact,
// or the classic size.
func lenSize(compact bool, classic int) int {
	if compact {
		return 1
	}
	return classic
}

// inBody returns whether f is decoded outside of tags at version, matching
// writeBeginAndTag.
func (f StructField) inBody(version int) bool {
	switch {
	case f.MaxVersion > -1:
		return version >= f.MinVersion && version <= f.MaxVersion
	case f.MinVersion > 0:
		return version >= f.MinVersion
	case f.MinVersion == -1:
		return false
	default:
		return true
	}
}

// lastVersionChange returns the version from which minSize(t) stops changing.
func lastVersionChange(t Type) int {
	s, ok := t.(Struct)
	if !ok || s.Nullable {
		return 0
	}
	var last int
	for _, f := range s.Fields {
		last = max(last, f.MinVersion, f.MaxVersion+1, lastVersionChange(f.Type))
	}
	return last
}

func (f StructField) WriteDecode(l *LineWriter) {
	switch t := f.Type.(type) {
	case Struct:
		// For decoding a nested struct, we copy a pointer out.
		// The nested version will then set the fields directly.
		if t.Nullable {
			l.Write("if present := b.Int8(); present != -1 && b.Ok() {")
			l.Write("s.%s = new(%s)", f.FieldName, t.Name)
			l.Write("v := s.%s", f.FieldName)
			defer l.Write("}")
		} else {
			l.Write("v := &s.%s", f.FieldName)
		}
		l.Write("v.Default()")
		t.WriteDecode(l)
	case Array:
		// For arrays, we need to copy the array into a v
		// field so that the array function can use it, and then
		// copy the changes back.
		l.Write("v := s.%s", f.FieldName)
		t.WriteDecode(l)
		l.Write("s.%s = v", f.FieldName)
	default:
		writeDecodeInto(l, t, "s."+f.FieldName)
	}
}

func (s Struct) WriteDecode(l *LineWriter) {
	if len(s.Fields) == 0 {
		return
	}
	rangeFrom := s.Fields
	if s.WithVersionField {
		f := s.Fields[0]
		if f.FieldName != "Version" {
			die("expected first field in 'with version field' type to be version, is %s", f.FieldName)
		}
		if f.Type != (Int16{}) {
			die("expected field version type to be int16, was %v", f.Type)
		}
		rangeFrom = s.Fields[1:]
	}
	l.Write("s := v")

	tags := make(map[int]StructField)

	for _, f := range rangeFrom {
		if onlyTag := f.writeBeginAndTag(l, tags); onlyTag {
			continue
		}
		f.WriteDecode(l)
		l.Write("}")
	}

	if !s.FromFlexible {
		return
	}

	l.Write("if isFlexible {")
	if len(tags) == 0 {
		l.Write("internalReadTags(&b, &s.UnknownTags)")
		l.Write("}")
		return
	}
	defer l.Write("}")

	l.Write("for i := b.Uvarint(); i > 0 && b.Ok(); i-- {")
	defer l.Write("}")

	l.Write("switch key := b.Uvarint(); key {")
	defer l.Write("}")

	l.Write("default:")
	l.Write("s.UnknownTags.Set(key, b.Span(int(b.Uvarint())))")

	for i := range len(tags) {
		l.Write("case %d:", i)
		l.Write("b := kbin.Reader{Src: b.Span(int(b.Uvarint()))}")
		tags[i].WriteDecode(l)
		l.Write("if err := b.Complete(); err != nil {")
		l.Write("return err")
		l.Write("}")
	}
}

func (s Struct) WriteDefault(l *LineWriter) {
	if len(s.Fields) == 0 || s.Nullable {
		return
	}

	// Like decoding above, we skip the version field.
	rangeFrom := s.Fields
	if s.WithVersionField {
		f := s.Fields[0]
		if f.FieldName != "Version" {
			die("expected first field in 'with version field' type to be version, is %s", f.FieldName)
		}
		if f.Type != (Int16{}) {
			die("expected field version type to be int16, was %v", f.Type)
		}
		rangeFrom = s.Fields[1:]
	}

	for _, f := range rangeFrom {
		switch inner := f.Type.(type) {
		case Struct:
			l.Write("{")
			l.Write("v := &v.%s", f.FieldName)
			l.Write("_ = v")
			inner.WriteDefault(l)
			l.Write("}")
		default:
			if d, ok := f.Type.(Defaulter); ok {
				def, has := d.GetDefault()
				if has {
					l.Write("v.%s = %v", f.FieldName, def)
				}
			}
		}
	}
}

func (s Struct) WriteDefn(l *LineWriter) {
	if s.Comment != "" {
		l.Write("%s", s.Comment)
	}
	l.Write("type %s struct {", s.Name)
	if s.TopLevel {
		// Top level messages always have a Version field.
		l.Write("// Version is the version of this message used with a Kafka broker.")
		l.Write("Version int16")
		l.Write("")
	}
	for i, f := range s.Fields {
		if f.Comment != "" {
			l.Write("%s", f.Comment)
		}
		versionTag := ""
		switch {
		case f.MinVersion > 0 && f.MaxVersion > 0:
			versionTag = fmt.Sprintf(" // v%d-v%d", f.MinVersion, f.MaxVersion)
		case f.MinVersion > 0:
			versionTag = fmt.Sprintf(" // v%d+", f.MinVersion)
		case f.MaxVersion > 0:
			versionTag = fmt.Sprintf(" // v0-v%d", f.MaxVersion)
		}
		if f.Tag >= 0 {
			if versionTag == "" {
				versionTag += " // tag "
			} else {
				versionTag += ", tag "
			}
			versionTag += strconv.Itoa(f.Tag)
		}
		l.Write("%s %s%s", f.FieldName, f.Type.TypeName(), versionTag)
		if i < len(s.Fields)-1 {
			l.Write("") // blank between fields
		}
	}
	if s.FlexibleAt >= 0 {
		l.Write("")
		l.Write("// UnknownTags are tags Kafka sent that we do not know the purpose of.")
		if s.FlexibleAt == 0 {
			l.Write("UnknownTags Tags")
		} else {
			l.Write("UnknownTags Tags // v%d+", s.FlexibleAt)
		}
		l.Write("")
	}
	l.Write("}")
}

func (s Struct) WriteKeyFunc(l *LineWriter) {
	l.Write("func (*%s) Key() int16 { return %d }", s.Name, s.Key)
}

func (s Struct) WriteMaxVersionFunc(l *LineWriter) {
	l.Write("func (*%s) MaxVersion() int16 { return %d }", s.Name, s.MaxVersion)
}

func (s Struct) WriteGetVersionFunc(l *LineWriter) {
	l.Write("func (v *%s) GetVersion() int16 { return v.Version }", s.Name)
}

func (s Struct) WriteSetVersionFunc(l *LineWriter) {
	l.Write("func (v *%s) SetVersion(version int16) { v.Version = version }", s.Name)
}

func (s Struct) WriteAdminFunc(l *LineWriter) {
	l.Write("func (v *%s) IsAdminRequest() {}", s.Name)
}

func (s Struct) WriteGroupCoordinatorFunc(l *LineWriter) {
	l.Write("func (v *%s) IsGroupCoordinatorRequest() {}", s.Name)
}

func (s Struct) WriteTxnCoordinatorFunc(l *LineWriter) {
	l.Write("func (v *%s) IsTxnCoordinatorRequest() {}", s.Name)
}

func (s Struct) WriteShareCoordinatorFunc(l *LineWriter) {
	l.Write("func (v *%s) IsShareCoordinatorRequest() {}", s.Name)
}

func (s Struct) WriteResponseKindFunc(l *LineWriter) {
	l.Write("func (v *%s) ResponseKind() Response {", s.Name)
	l.Write("r := &%s{Version: v.Version }", s.ResponseKind)
	l.Write("r.Default()")
	l.Write("return r")
	l.Write("}")
}

func (s Struct) WriteRequestKindFunc(l *LineWriter) {
	l.Write("func (v *%s) RequestKind() Request { return &%s{Version: v.Version }}", s.Name, s.RequestKind)
}

func (s Struct) WriteIsFlexibleFunc(l *LineWriter) {
	if s.FlexibleAt >= 0 {
		l.Write("func (v *%s) IsFlexible() bool { return v.Version >= %d }", s.Name, s.FlexibleAt)
	} else {
		l.Write("func (v *%s) IsFlexible() bool { return false }", s.Name)
	}
}

func (s Struct) WriteThrottleMillisFunc(f StructField, l *LineWriter) {
	t := f.Type.(Throttle)
	l.Write("func (v *%s) Throttle() (int32, bool) { return v.ThrottleMillis, v.Version >= %d }", s.Name, t.Switchup)
}

func (s Struct) WriteSetThrottleMillisFunc(l *LineWriter) {
	l.Write("func (v *%s) SetThrottle(throttleMillis int32) { v.ThrottleMillis = throttleMillis}", s.Name)
}

func (s Struct) WriteTimeoutMillisFunc(l *LineWriter) {
	l.Write("func (v *%s) Timeout() int32 { return v.TimeoutMillis }", s.Name)
}

func (s Struct) WriteSetTimeoutMillisFunc(l *LineWriter) {
	l.Write("func (v *%s) SetTimeout(timeoutMillis int32) { v.TimeoutMillis = timeoutMillis }", s.Name)
}

func (s Struct) WriteAppendFunc(l *LineWriter) {
	l.Write("func (v *%s) AppendTo(dst []byte) []byte {", s.Name)
	if s.TopLevel || s.WithVersionField {
		l.Write("version := v.Version")
		l.Write("_ = version")
	}
	if s.FlexibleAt >= 0 {
		l.Write("isFlexible := version >= %d", s.FlexibleAt)
		l.Write("_ = isFlexible")
	}
	s.WriteAppend(l)
	l.Write("return dst")
	l.Write("}")
}

func (s Struct) WriteDecodeFunc(l *LineWriter) {
	l.Write("func (v *%s) ReadFrom(src []byte) error {", s.Name)
	l.Write("return v.readFrom(src, false)")
	l.Write("}")

	l.Write("func (v *%s) UnsafeReadFrom(src []byte) error {", s.Name)
	l.Write("return v.readFrom(src, true)")
	l.Write("}")

	l.Write("func (v *%s) readFrom(src []byte, unsafe bool) error {", s.Name)
	l.Write("v.Default()")
	l.Write("b := kbin.Reader{Src: src}")
	if s.WithVersionField {
		l.Write("v.Version = b.Int16()")
	}
	if s.TopLevel || s.WithVersionField {
		l.Write("version := v.Version")
		l.Write("_ = version")
	}
	if s.FlexibleAt >= 0 {
		l.Write("isFlexible := version >= %d", s.FlexibleAt)
		l.Write("_ = isFlexible")
	}
	s.WriteDecode(l)
	l.Write("return b.Complete()")
	l.Write("}")
}

func (s Struct) WriteRequestWithFunc(l *LineWriter) {
	l.Write("// RequestWith is requests v on r and returns the response or an error.")
	l.Write("// For sharded requests, the response may be merged and still return an error.")
	l.Write("// It is better to rely on client.RequestSharded than to rely on proper merging behavior.")
	l.Write("func (v *%s) RequestWith(ctx context.Context, r Requestor) (*%s, error) {", s.Name, s.ResponseKind)
	l.Write("kresp, err := r.Request(ctx, v)")
	l.Write("resp, _ := kresp.(*%s)", s.ResponseKind)
	l.Write("return resp, err")
	l.Write("}")
}

func (s Struct) WriteDefaultFunc(l *LineWriter) {
	l.Write("// Default sets any default fields. Calling this allows for future compatibility")
	l.Write("// if new fields are added to %s.", s.Name)
	l.Write("func (v *%s) Default() {", s.Name)
	s.WriteDefault(l)
	l.Write("}")
}

func (s Struct) WriteNewFunc(l *LineWriter) {
	l.Write("// New%[1]s returns a default %[1]s", s.Name)
	l.Write("// This is a shortcut for creating a struct and calling Default yourself.")
	l.Write("func New%[1]s() %[1]s {", s.Name)
	l.Write("var v %s", s.Name)
	l.Write("v.Default()")
	l.Write("return v")
	l.Write("}")
}

func (s Struct) WriteNewPtrFunc(l *LineWriter) {
	l.Write("// NewPtr%[1]s returns a pointer to a default %[1]s", s.Name)
	l.Write("// This is a shortcut for creating a new(struct) and calling Default yourself.")
	l.Write("func NewPtr%[1]s() *%[1]s {", s.Name)
	l.Write("var v %s", s.Name)
	l.Write("v.Default()")
	l.Write("return &v")
	l.Write("}")
}

func (e Enum) WriteDefn(l *LineWriter) {
	if e.Comment != "" {
		l.Write("%s", e.Comment)
		l.Write("// ")
	}
	l.Write("// Possible values and their meanings:")
	l.Write("// ")
	for _, v := range e.Values {
		l.Write("// * %d (%s)", v.Value, v.Word)
		if len(v.Comment) > 0 {
			l.Write("%s", v.Comment)
		}
		l.Write("//")
	}
	l.Write("type %s %s", e.Name, e.Type.TypeName())
}

func (e Enum) WriteStringFunc(l *LineWriter) {
	l.Write("func (v %s) String() string {", e.Name)
	l.Write("switch v {")
	l.Write("default:")
	if e.CamelCase {
		l.Write(`return "Unknown"`)
	} else {
		l.Write(`return "UNKNOWN"`)
	}
	for _, v := range e.Values {
		l.Write("case %d:", v.Value)
		l.Write(`return "%s"`, v.Word)
	}
	l.Write("}")
	l.Write("}")
}

func (e Enum) WriteStringsFunc(l *LineWriter) {
	l.Write("func %sStrings() []string {", e.Name)
	l.Write("return []string{")
	for _, v := range e.Values {
		l.Write(`"%s",`, v.Word)
	}
	l.Write("}")
	l.Write("}")
}

func (e Enum) WriteParseFunc(l *LineWriter) {
	l.Write("// Parse%s normalizes the input s and returns", e.Name)
	l.Write("// the value represented by the string.")
	l.Write("//")
	l.Write("// Normalizing works by stripping all dots, underscores, and dashes,")
	l.Write("// trimming spaces, and lowercasing.")
	l.Write("func Parse%[1]s(s string) (%[1]s, error) {", e.Name)
	l.Write("switch strnorm(s) {")
	for _, v := range e.Values {
		l.Write(`case "%s":`, strnorm(v.Word))
		l.Write("return %d, nil", v.Value)
	}
	l.Write("default:")
	l.Write(`return 0, fmt.Errorf("%s: unable to parse %%q", s)`, e.Name)
	l.Write("}")
	l.Write("}")
}

func (e Enum) WriteUnmarshalTextFunc(l *LineWriter) {
	l.Write("// UnmarshalText implements encoding.TextUnmarshaler.")
	l.Write("func (e *%s) UnmarshalText(text []byte) error {", e.Name)
	l.Write("v, err := Parse%s(string(text))", e.Name)
	l.Write("*e = v")
	l.Write("return err")
	l.Write("}")
}

func (e Enum) WriteMarshalTextFunc(l *LineWriter) {
	l.Write("// MarshalText implements encoding.TextMarshaler.")
	l.Write("func (e %s) MarshalText() (text []byte, err error) {", e.Name)
	l.Write("return []byte(e.String()), nil")
	l.Write("}")
}

func strnorm(s string) string {
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	return s
}

func writeStrnorm(l *LineWriter) {
	l.Write(`func strnorm(s string) string {`)
	l.Write(`s = strings.ReplaceAll(s, ".", "")`)
	l.Write(`s = strings.ReplaceAll(s, "_", "")`)
	l.Write(`s = strings.ReplaceAll(s, "-", "")`)
	l.Write(`s = strings.TrimSpace(s)`)
	l.Write(`s = strings.ToLower(s)`)
	l.Write(`return s`)
	l.Write(`}`)
}

func (e Enum) WriteConsts(l *LineWriter) {
	l.Write("const (")
	if !e.HasZero {
		l.Write("%[1]sUnknown %[1]s = 0", e.Name)
	}
	defer l.Write(")")
	for _, v := range e.Values {
		var sb strings.Builder
		if e.CamelCase {
			sb.WriteString(v.Word)
		} else {
			upper := true
			for _, c := range v.Word {
				switch c {
				case '_':
					upper = true
				default:
					s := string([]rune{c})
					if upper {
						sb.WriteString(strings.ToUpper(s))
					} else {
						sb.WriteString(strings.ToLower(s))
					}
					upper = false
				}
			}
		}

		l.Write("%s%s %s = %d", e.Name, sb.String(), e.Name, v.Value)
	}
}
