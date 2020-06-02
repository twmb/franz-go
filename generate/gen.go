package main

import "strconv"

func (Bool) TypeName() string                  { return "bool" }
func (Int8) TypeName() string                  { return "int8" }
func (Int16) TypeName() string                 { return "int16" }
func (Int32) TypeName() string                 { return "int32" }
func (Int64) TypeName() string                 { return "int64" }
func (Float64) TypeName() string               { return "float64" }
func (Uint32) TypeName() string                { return "uint32" }
func (Varint) TypeName() string                { return "int32" }
func (Varlong) TypeName() string               { return "int64" }
func (String) TypeName() string                { return "string" }
func (NullableString) TypeName() string        { return "*string" }
func (Bytes) TypeName() string                 { return "[]byte" }
func (NullableBytes) TypeName() string         { return "[]byte" }
func (VarintString) TypeName() string          { return "string" }
func (VarintBytes) TypeName() string           { return "[]byte" }
func (a Array) TypeName() string               { return "[]" + a.Inner.TypeName() }
func (s Struct) TypeName() string              { return s.Name }
func (FieldLengthMinusBytes) TypeName() string { return "[]byte" }

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
func (Int32) WriteAppend(l *LineWriter)        { primAppend("Int32", l) }
func (Int64) WriteAppend(l *LineWriter)        { primAppend("Int64", l) }
func (Float64) WriteAppend(l *LineWriter)      { primAppend("Float64", l) }
func (Uint32) WriteAppend(l *LineWriter)       { primAppend("Uint32", l) }
func (Varint) WriteAppend(l *LineWriter)       { primAppend("Varint", l) }
func (Varlong) WriteAppend(l *LineWriter)      { primAppend("Varlong", l) }
func (VarintString) WriteAppend(l *LineWriter) { primAppend("VarintString", l) }
func (VarintBytes) WriteAppend(l *LineWriter)  { primAppend("VarintBytes", l) }

func (v String) WriteAppend(l *LineWriter) { compactAppend(v.FromFlexible, "String", l) }
func (v NullableString) WriteAppend(l *LineWriter) {
	if v.NullableVersion > 0 {
		panic("unhandled non-nullable to nullable string")
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

	if a.IsVarintArray {
		l.Write("dst = kbin.AppendVarint(dst, int32(len(v)))")
	} else if a.IsNullableArray {
		if a.NullableVersion > 0 {
			l.Write("if version > %d {", a.NullableVersion)
			writeNullable()
			l.Write("} else {")
			writeNormal()
			l.Write("}")
		} else {
			writeNullable()
		}
	} else {
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

	if len(tags) > 0 {
		die("tagged fields in appending is unsupported! fix this!")
	}

	l.Write("if isFlexible {")
	l.Write("dst = append(dst, 0)")
	l.Write("}")
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
	if f.MaxVersion > -1 {
		l.Write("if version >= %d && version <= %d {", f.MinVersion, f.MaxVersion)
	} else if f.MinVersion > 0 {
		l.Write("if version >= %d {", f.MinVersion)
	} else if f.MinVersion == -1 {
		if f.Tag < 0 {
			die("unexpected min version -1 with tag %d on field %s", f.Tag, f.FieldName)
		}
		return true
	} else {
		l.Write("{")
	}
	return false
}

// primDecode corresponds to the binreader primitive decoding functions in
// kmsg/primitives.go.
func primDecode(name string, l *LineWriter) {
	l.Write("v := b.%s()", name)
}

func compactDecode(fromFlexible bool, name, typ string, l *LineWriter) {
	if fromFlexible {
		l.Write("var v %s", typ)
		l.Write("if isFlexible {")
		l.Write("v = b.Compact%s()", name)
		l.Write("} else {")
		l.Write("v = b.%s()", name)
		l.Write("}")
	} else {
		l.Write("v := b.%s()", name)
	}
}

func (Bool) WriteDecode(l *LineWriter)         { primDecode("Bool", l) }
func (Int8) WriteDecode(l *LineWriter)         { primDecode("Int8", l) }
func (Int16) WriteDecode(l *LineWriter)        { primDecode("Int16", l) }
func (Int32) WriteDecode(l *LineWriter)        { primDecode("Int32", l) }
func (Int64) WriteDecode(l *LineWriter)        { primDecode("Int64", l) }
func (Float64) WriteDecode(l *LineWriter)      { primDecode("Float64", l) }
func (Uint32) WriteDecode(l *LineWriter)       { primDecode("Uint32", l) }
func (Varint) WriteDecode(l *LineWriter)       { primDecode("Varint", l) }
func (Varlong) WriteDecode(l *LineWriter)      { primDecode("Varlong", l) }
func (VarintString) WriteDecode(l *LineWriter) { primDecode("VarintString", l) }
func (VarintBytes) WriteDecode(l *LineWriter)  { primDecode("VarintBytes", l) }

func (v String) WriteDecode(l *LineWriter) { compactDecode(v.FromFlexible, "String", "string", l) }
func (v Bytes) WriteDecode(l *LineWriter)  { compactDecode(v.FromFlexible, "Bytes", "[]byte", l) }
func (v NullableBytes) WriteDecode(l *LineWriter) {
	compactDecode(v.FromFlexible, "NullableBytes", "[]byte", l)
}

func (v NullableString) WriteDecode(l *LineWriter) {
	// If there is a nullable version, we write a "read string, then set
	// pointer" block.
	if v.NullableVersion > 0 {
		l.Write("var v *string")
		l.Write("if version < %d {", v.NullableVersion)
		l.Write("var vv string")
		if v.FromFlexible {
			l.Write("if isFlexible {")
			l.Write("vv = b.CompactString()")
			l.Write("} else {")
			l.Write("vv = b.String()")
			l.Write("}")
		} else {
			l.Write("vv = b.String()")
		}
		l.Write("v = &vv")
		l.Write("} else {")
		defer l.Write("}")
	}

	if v.FromFlexible {
		// If we had a nullable version, then we already declared v and
		// do not need to again.
		if v.NullableVersion == 0 {
			l.Write("var v *string")
		}
		l.Write("if isFlexible {")
		l.Write("v = b.CompactNullableString()")
		l.Write("} else {")
		l.Write("v = b.NullableString()")
		l.Write("}")
	} else {
		// If we had a nullable version, v has been declared and we
		// reuse it, if not, we declare v.
		if v.NullableVersion == 0 {
			l.Write("v := b.NullableString()")
		} else {
			l.Write("v = b.NullableString()")
		}
	}
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
		if a.FromFlexible {
			l.Write("if isFlexible {")
			l.Write("l = b.CompactArrayLen()")
			l.Write("} else {")
			l.Write("l = b.ArrayLen()")
			l.Write("}")
		} else {
			l.Write("l = b.ArrayLen()")
		}
		if a.IsNullableArray {
			l.Write("if version < %d || l == 0 {", a.NullableVersion)
			l.Write("a = %s{}", a.TypeName())
			l.Write("}")
		}
	}

	l.Write("if !b.Ok() {")
	l.Write("return b.Complete()")
	l.Write("}")

	l.Write("if l > 0 {")
	l.Write("a = make(%s, l)", a.TypeName())
	l.Write("}")

	l.Write("for i := int32(0); i < l; i++ {")
	if _, isStruct := a.Inner.(Struct); isStruct {
		l.Write("v := &a[i]")
	} else if _, isArray := a.Inner.(Array); isArray {
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
		l.Write("version := b.Int16()")
		l.Write("v.Version = version")
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
		l.Write("SkipTags(&b)")
		l.Write("}")
		return
	}

	l.Write("for i := b.Uvarint(); i > 0; i-- {")
	defer l.Write("}")

	l.Write("tag, size := b.Uvarint(), int(b.Uvarint())")
	defer l.Write("}")

	l.Write("switch tag {")
	defer l.Write("}")

	l.Write("default:")
	l.Write("b.Span(size)") // unknown tag

	for i := 0; i < len(tags); i++ {
		f, exists := tags[i]
		if !exists {
			die("saw %d tags, but did not see tag %d; expected monotonically increasing", len(tags), i)
		}

		switch f.Type.(type) {
		case Bool, Int8, Int16, Int32, Int64, Float64, Uint32, Varint:
		default:
			die("type %v unsupported in decode! fix this!", f.Type)
		}

		l.Write("case %d:", i)
		l.Write("b := kbin.Reader{Src: b.Span(size)}")
		f.WriteDecode(l)
		l.Write("if err := b.Complete(); err != nil {")
		l.Write("return err")
		l.Write("}")
	}
}

func (s Struct) WriteDefn(l *LineWriter) {
	if s.Comment != "" {
		l.Write(s.Comment)
	}
	l.Write("type %s struct {", s.Name)
	if s.TopLevel {
		// Top level messages always have a Version field.
		l.Write("\t// Version is the version of this message used with a Kafka broker.")
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
func (s Struct) WriteIsFlexibleFunc(l *LineWriter) {
	if s.FlexibleAt >= 0 {
		l.Write("func (v *%s) IsFlexible() bool { return v.Version >= %d }", s.Name, s.FlexibleAt)
	} else {
		l.Write("func (v *%s) IsFlexible() bool { return false }", s.Name)
	}
}

func (s Struct) WriteAppendFunc(l *LineWriter) {
	l.Write("func (v *%s) AppendTo(dst []byte) []byte {", s.Name)
	if s.TopLevel || s.WithVersionField {
		l.Write("version := v.Version")
		l.Write("_ = version")
	}
	if s.TopLevel && s.FlexibleAt >= 0 {
		l.Write("isFlexible := version >= %d", s.FlexibleAt)
		l.Write("_ = isFlexible")
	}
	s.WriteAppend(l)
	l.Write("return dst")
	l.Write("}")
}

func (s Struct) WriteDecodeFunc(l *LineWriter) {
	l.Write("func (v *%s) ReadFrom(src []byte) error {", s.Name)
	if s.TopLevel {
		l.Write("version := v.Version")
		l.Write("_ = version")
	}
	if s.TopLevel && s.FlexibleAt >= 0 {
		l.Write("isFlexible := version >= %d", s.FlexibleAt)
		l.Write("_ = isFlexible")
	}
	l.Write("b := kbin.Reader{Src: src}")
	s.WriteDecode(l)
	l.Write("return b.Complete()")
	l.Write("}")
}
