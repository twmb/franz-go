package main

import (
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
func (Uuid) TypeName() string                  { return "[16]byte" }
func (String) TypeName() string                { return "string" }
func (NullableString) TypeName() string        { return "*string" }
func (Bytes) TypeName() string                 { return "[]byte" }
func (NullableBytes) TypeName() string         { return "[]byte" }
func (VarintString) TypeName() string          { return "string" }
func (VarintBytes) TypeName() string           { return "[]byte" }
func (a Array) TypeName() string               { return "[]" + a.Inner.TypeName() }
func (Throttle) TypeName() string              { return "int32" }
func (s Struct) TypeName() string              { return s.Name }
func (FieldLengthMinusBytes) TypeName() string { return "[]byte" }

func (e Enum) TypeName() string { return e.Name }
func (e Enum) WriteAppend(l *LineWriter) {
	l.Write("{")
	l.Write("v := %s(v)", e.Type.TypeName())
	e.Type.WriteAppend(l)
	l.Write("}")
}

func (e Enum) WriteDecode(l *LineWriter) {
	l.Write("var t %s", e.Name)
	l.Write("{")
	e.Type.WriteDecode(l)
	l.Write("t = %s(v)", e.Name)
	l.Write("}")
	l.Write("v := t")
}

// primAppend corresponds to the primitive append functions in
// kmsg/primitives.go.
func primAppend(name string, l *LineWriter) {
	l.Write("dst = kbin.Append%s(dst, v)", name)
}

func compactAppend(fromFlexible bool, name string, l *LineWriter) {
	if fromFlexible {
		l.Write("if isFlexible {")
		primAppend("Compact"+name, l)
		l.Write("} else {")
		defer l.Write("}")
	}
	primAppend(name, l)
}

func (Bool) WriteAppend(l *LineWriter)         { primAppend("Bool", l) }
func (Int8) WriteAppend(l *LineWriter)         { primAppend("Int8", l) }
func (Int16) WriteAppend(l *LineWriter)        { primAppend("Int16", l) }
func (Uint16) WriteAppend(l *LineWriter)       { primAppend("Uint16", l) }
func (Int32) WriteAppend(l *LineWriter)        { primAppend("Int32", l) }
func (Int64) WriteAppend(l *LineWriter)        { primAppend("Int64", l) }
func (Float64) WriteAppend(l *LineWriter)      { primAppend("Float64", l) }
func (Uint32) WriteAppend(l *LineWriter)       { primAppend("Uint32", l) }
func (Varint) WriteAppend(l *LineWriter)       { primAppend("Varint", l) }
func (Uuid) WriteAppend(l *LineWriter)         { primAppend("Uuid", l) }
func (VarintString) WriteAppend(l *LineWriter) { primAppend("VarintString", l) }
func (VarintBytes) WriteAppend(l *LineWriter)  { primAppend("VarintBytes", l) }
func (Throttle) WriteAppend(l *LineWriter)     { primAppend("Int32", l) }

func (v String) WriteAppend(l *LineWriter) { compactAppend(v.FromFlexible, "String", l) }
func (v NullableString) WriteAppend(l *LineWriter) {
	if v.NullableVersion > 0 {
		l.Write("if version < %d {", v.NullableVersion)
		l.Write("var vv string")
		l.Write("if v != nil {")
		l.Write("vv = *v")
		l.Write("}")
		l.Write("{")
		l.Write("v := vv")
		compactAppend(v.FromFlexible, "String", l)
		l.Write("}")
		l.Write("} else {")
		defer l.Write("}")
	}
	compactAppend(v.FromFlexible, "NullableString", l)
}
func (v Bytes) WriteAppend(l *LineWriter)         { compactAppend(v.FromFlexible, "Bytes", l) }
func (v NullableBytes) WriteAppend(l *LineWriter) { compactAppend(v.FromFlexible, "NullableBytes", l) }

func (FieldLengthMinusBytes) WriteAppend(l *LineWriter) {
	l.Write("dst = append(dst, v...)")
}

func (a Array) WriteAppend(l *LineWriter) {
	writeNullable := func() {
		if a.FromFlexible {
			l.Write("if isFlexible {")
			l.Write("dst = kbin.AppendCompactNullableArrayLen(dst, len(v), v == nil)")
			l.Write("} else {")
			l.Write("dst = kbin.AppendNullableArrayLen(dst, len(v), v == nil)")
			l.Write("}")
		} else {
			l.Write("dst = kbin.AppendNullableArrayLen(dst, len(v), v == nil)")
		}
	}

	writeNormal := func() {
		if a.FromFlexible {
			l.Write("if isFlexible {")
			l.Write("dst = kbin.AppendCompactArrayLen(dst, len(v))")
			l.Write("} else {")
			l.Write("dst = kbin.AppendArrayLen(dst, len(v))")
			l.Write("}")
		} else {
			l.Write("dst = kbin.AppendArrayLen(dst, len(v))")
		}
	}

	switch {
	case a.IsVarintArray:
		l.Write("dst = kbin.AppendVarint(dst, int32(len(v)))")
	case a.IsNullableArray:
		if a.NullableVersion > 0 {
			l.Write("if version > %d {", a.NullableVersion)
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
	if _, isStruct := a.Inner.(Struct); isStruct {
		// If the array elements are structs, we avoid copying the
		// struct out and instead grab a pointer to the element.
		l.Write("v := &v[i]")
	} else {
		l.Write("v := v[i]")
	}
	a.Inner.WriteAppend(l)
	l.Write("}")
}

func (s Struct) WriteAppend(l *LineWriter) {
	tags := make(map[int]StructField)
	for _, f := range s.Fields {
		if onlyTag := f.writeBeginAndTag(l, tags); onlyTag {
			continue
		}
		// If the struct field is a struct itself, we avoid copying it
		// and instead grab a pointer.
		if _, isStruct := f.Type.(Struct); isStruct {
			l.Write("v := &v.%s", f.FieldName)
		} else {
			l.Write("v := v.%s", f.FieldName)
		}
		f.Type.WriteAppend(l)
		l.Write("}")
	}

	if !s.FromFlexible {
		return
	}

	l.Write("if isFlexible {")
	defer l.Write("}")

	var tagsCanDefault bool
	for i := 0; i < len(tags); i++ {
		f, exists := tags[i]
		if !exists {
			die("saw %d tags, but did not see tag %d; expected monotonically increasing", len(tags), i)
		}
		if _, tagsCanDefault = f.Type.(Defaulter); tagsCanDefault {
			break
		}
	}

	defer l.Write("dst = v.UnknownTags.AppendEach(dst)")

	if tagsCanDefault {
		l.Write("var toEncode []uint32")
		for i := 0; i < len(tags); i++ {
			f := tags[i]
			canDefault := false
			if d, ok := f.Type.(Defaulter); ok {
				canDefault = true
				def, has := d.GetDefault()
				if !has {
					def = d.GetTypeDefault()
				}
				switch f.Type.(type) {
				case Struct:
					l.Write("if !reflect.DeepEqual(v.%s, %v) {", f.FieldName, def)
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
		l.Write("dst = kbin.AppendUvarint(dst, %d + uint32(v.UnknownTags.Len()))", len(tags))
	}

	for i := 0; i < len(tags); i++ {
		if tagsCanDefault {
			l.Write("case %d:", i)
		}
		f := tags[i]

		l.Write("{")
		l.Write("v := v.%s", f.FieldName)
		l.Write("dst = kbin.AppendUvarint(dst, %d)", i) // tag num
		switch f.Type.(type) {
		case Bool, Int8:
			l.Write("dst = kbin.AppendUvarint(dst, 1)") // size
			f.Type.WriteAppend(l)
		case Int16, Uint16:
			l.Write("dst = kbin.AppendUvarint(dst, 2)")
			f.Type.WriteAppend(l)
		case Int32, Uint32:
			l.Write("dst = kbin.AppendUvarint(dst, 4)")
			f.Type.WriteAppend(l)
		case Int64, Float64:
			l.Write("dst = kbin.AppendUvarint(dst, 8)")
			f.Type.WriteAppend(l)
		case Varint:
			l.Write("dst = kbin.AppendUvarint(dst, kbin.VarintLen(v))")
			f.Type.WriteAppend(l)
		case Uuid:
			l.Write("dst = kbin.AppendUvarint(dst, 16)")
			f.Type.WriteAppend(l)
		case Array, Struct, String, NullableString, Bytes, NullableBytes:
			l.Write("sized := false")
			l.Write("lenAt := len(dst)")
			l.Write("f%s:", f.FieldName)
			f.Type.WriteAppend(l)
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

// writeBeginAndTag begins a struct field encode/decode and adds the field to
// the tags map if necessary. If this field is only tagged, this returns true.
func (f StructField) writeBeginAndTag(l *LineWriter, tags map[int]StructField) (onlyTag bool) {
	if f.MinVersion == -1 && f.MaxVersion > 0 {
		die("unexpected negative min version %d while max version %d on field %s", f.MinVersion, f.MaxVersion, f.FieldName)
	}
	if f.Tag >= 0 {
		if _, exists := tags[f.Tag]; exists {
			die("unexpected duplicate tag %d on field %s", f.Tag, f.FieldName)
		}
		tags[f.Tag] = f
	}
	switch {
	case f.MaxVersion > -1:
		l.Write("if version >= %d && version <= %d {", f.MinVersion, f.MaxVersion)
	case f.MinVersion > 0:
		l.Write("if version >= %d {", f.MinVersion)
	case f.MinVersion == -1:
		if f.Tag < 0 {
			die("unexpected min version -1 with tag %d on field %s", f.Tag, f.FieldName)
		}
		return true
	default:
		l.Write("{")
	}
	return false
}

// primDecode corresponds to the binreader primitive decoding functions in
// kmsg/primitives.go.
func primDecode(name string, l *LineWriter) {
	l.Write("v := b.%s()", name)
}

func unsafeDecode(l *LineWriter, fn func(string)) {
	l.Write("if unsafe {")
	fn("Unsafe")
	l.Write("} else {")
	fn("")
	l.Write("}")
}

func flexDecode(supports bool, l *LineWriter, fn func(string)) {
	if supports {
		l.Write("if isFlexible {")
		fn("Compact")
		l.Write("} else {")
		defer l.Write("}")
	}
	fn("")
}

func primUnsafeDecode(name string, l *LineWriter) {
	l.Write("var v string")
	unsafeDecode(l, func(u string) {
		l.Write("v = b.%s%s()", u, name)
	})
}

func compactDecode(fromFlexible, hasUnsafe bool, name, typ string, l *LineWriter) {
	if fromFlexible {
		l.Write("var v %s", typ)
		fn := func(u string) {
			l.Write("if isFlexible {")
			l.Write("v = b.%sCompact%s()", u, name)
			l.Write("} else {")
			l.Write("v = b.%s%s()", u, name)
			l.Write("}")
		}
		if hasUnsafe {
			unsafeDecode(l, fn)
		} else {
			fn("")
		}
	} else {
		if hasUnsafe {
			primUnsafeDecode(name, l)
		} else {
			primDecode(name, l)
		}
	}
}

func (Bool) WriteDecode(l *LineWriter)         { primDecode("Bool", l) }
func (Int8) WriteDecode(l *LineWriter)         { primDecode("Int8", l) }
func (Int16) WriteDecode(l *LineWriter)        { primDecode("Int16", l) }
func (Uint16) WriteDecode(l *LineWriter)       { primDecode("Uint16", l) }
func (Int32) WriteDecode(l *LineWriter)        { primDecode("Int32", l) }
func (Int64) WriteDecode(l *LineWriter)        { primDecode("Int64", l) }
func (Float64) WriteDecode(l *LineWriter)      { primDecode("Float64", l) }
func (Uint32) WriteDecode(l *LineWriter)       { primDecode("Uint32", l) }
func (Varint) WriteDecode(l *LineWriter)       { primDecode("Varint", l) }
func (Uuid) WriteDecode(l *LineWriter)         { primDecode("Uuid", l) }
func (VarintString) WriteDecode(l *LineWriter) { primUnsafeDecode("VarintString", l) }
func (VarintBytes) WriteDecode(l *LineWriter)  { primDecode("VarintBytes", l) }
func (Throttle) WriteDecode(l *LineWriter)     { primDecode("Int32", l) }

func (v String) WriteDecode(l *LineWriter) {
	compactDecode(v.FromFlexible, true, "String", "string", l)
}

func (v Bytes) WriteDecode(l *LineWriter) {
	compactDecode(v.FromFlexible, false, "Bytes", "[]byte", l)
}

func (v NullableBytes) WriteDecode(l *LineWriter) {
	compactDecode(v.FromFlexible, false, "NullableBytes", "[]byte", l)
}

func (v NullableString) WriteDecode(l *LineWriter) {
	// If there is a nullable version, we write a "read string, then set
	// pointer" block.
	l.Write("var v *string")
	if v.NullableVersion > 0 {
		l.Write("if version < %d {", v.NullableVersion)
		l.Write("var vv string")
		flexDecode(v.FromFlexible, l, func(compact string) {
			unsafeDecode(l, func(u string) {
				l.Write("vv = b.%s%sString()", u, compact)
			})
		})
		l.Write("v = &vv")
		l.Write("} else {")
		defer l.Write("}")
	}

	flexDecode(v.FromFlexible, l, func(compact string) {
		unsafeDecode(l, func(u string) {
			l.Write("v = b.%s%sNullableString()", u, compact)
		})
	})
}

func (f FieldLengthMinusBytes) WriteDecode(l *LineWriter) {
	l.Write("v := b.Span(int(s.%s) - %d)", f.Field, f.LengthMinus)
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
		flexDecode(a.FromFlexible, l, func(compact string) {
			l.Write("l = b.%sArrayLen()", compact)
		})
		if a.IsNullableArray {
			l.Write("if version < %d || l == 0 {", a.NullableVersion)
			l.Write("a = %s{}", a.TypeName())
			l.Write("}")
		}
	}

	l.Write("if !b.Ok() {")
	l.Write("return b.Complete()")
	l.Write("}")

	l.Write("a = a[:0]")

	l.Write("if l > 0 {")
	l.Write("a = append(a, make(%s, l)...)", a.TypeName())
	l.Write("}")

	l.Write("for i := int32(0); i < l; i++ {")
	switch a.Inner.(type) {
	case Struct:
		l.Write("v := &a[i]")
		l.Write("v.Default()") // set defaults first
	case Array:
		// With nested arrays, we declare a new v and introduce scope
		// so that the next level will not collide with our current "a".
		l.Write("v := a[i]")
		l.Write("{")
	}

	a.Inner.WriteDecode(l)

	if _, isArray := a.Inner.(Array); isArray {
		// With nested arrays, now we release our scope.
		l.Write("}")
	}

	if _, isStruct := a.Inner.(Struct); !isStruct {
		l.Write("a[i] = v")
	}

	l.Write("}") // close the for loop

	l.Write("v = a")
}

func (f StructField) WriteDecode(l *LineWriter) {
	switch f.Type.(type) {
	case Struct:
		// For decoding a nested struct, we copy a pointer out.
		// The nested version will then set the fields directly.
		l.Write("v := &s.%s", f.FieldName)
		l.Write("v.Default()")
	case Array:
		// For arrays, we need to copy the array into a v
		// field so that the array function can use it.
		l.Write("v := s.%s", f.FieldName)
	default:
		// All other types use primDecode, which does a `v :=`.
	}
	f.Type.WriteDecode(l)

	_, isStruct := f.Type.(Struct)
	if !isStruct {
		// If the field was not a struct, we need to copy the
		// changes back.
		l.Write("s.%s = v", f.FieldName)
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
		l.Write("s.UnknownTags = internalReadTags(&b)")
		l.Write("}")
		return
	}
	defer l.Write("}")

	l.Write("for i := b.Uvarint(); i > 0; i-- {")
	defer l.Write("}")

	l.Write("switch key := b.Uvarint(); key {")
	defer l.Write("}")

	l.Write("default:")
	l.Write("s.UnknownTags.Set(key, b.Span(int(b.Uvarint())))")

	for i := 0; i < len(tags); i++ {
		f, exists := tags[i]
		if !exists {
			die("saw %d tags, but did not see tag %d; expected monotonically increasing", len(tags), i)
		}

		l.Write("case %d:", i)
		l.Write("b := kbin.Reader{Src: b.Span(int(b.Uvarint()))}")
		f.WriteDecode(l)
		l.Write("if err := b.Complete(); err != nil {")
		l.Write("return err")
		l.Write("}")
	}
}

func (s Struct) WriteDefault(l *LineWriter) {
	if len(s.Fields) == 0 {
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
		l.Write(s.Comment)
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
		if f.MinVersion > 0 {
			versionTag = " // v" + strconv.Itoa(f.MinVersion) + "+"
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

func (s Struct) WriteResponseKindFunc(l *LineWriter) {
	l.Write("func (v *%s) ResponseKind() Response { return &%s{Version: v.Version }}", s.Name, s.ResponseKind)
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

func (s Struct) WriteTimeoutMillisFuncs(l *LineWriter) {
	l.Write("func (v *%s) Timeout() int32 { return v.TimeoutMillis }", s.Name)
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
		l.Write(e.Comment)
		l.Write("// ")
	}
	l.Write("// Possible values and their meanings:")
	l.Write("// ")
	for _, v := range e.Values {
		l.Write("// * %d (%s)", v.Value, v.Word)
		if len(v.Comment) > 0 {
			l.Write(v.Comment)
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
