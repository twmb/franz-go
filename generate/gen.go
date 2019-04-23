package main

import "strconv"

func (Bool) TypeName() string                  { return "bool" }
func (Int8) TypeName() string                  { return "int8" }
func (Int16) TypeName() string                 { return "int16" }
func (Int32) TypeName() string                 { return "int32" }
func (Int64) TypeName() string                 { return "int64" }
func (Uint32) TypeName() string                { return "uint32" }
func (Varint) TypeName() string                { return "int32" }
func (Varlong) TypeName() string               { return "int64" }
func (String) TypeName() string                { return "string" }
func (NullableString) TypeName() string        { return "*string" }
func (Bytes) TypeName() string                 { return "[]byte" }
func (NullableBytes) TypeName() string         { return "*[]byte" }
func (VarintString) TypeName() string          { return "string" }
func (VarintBytes) TypeName() string           { return "[]byte" }
func (a Array) TypeName() string               { return "[]" + a.Inner.TypeName() }
func (s Struct) TypeName() string              { return s.Name }
func (FieldLengthMinusBytes) TypeName() string { return "[]byte" }
func (n SizedStruct) TypeName() string         { return n.Type.TypeName() }

// primAppend corresponds to the primitive append functions in
// kmsg/primitives.go.
func primAppend(name string, l *LineWriter) {
	l.Write("dst = kbin.Append%s(dst, v)", name)
}

func (Bool) WriteAppend(l *LineWriter)           { primAppend("Bool", l) }
func (Int8) WriteAppend(l *LineWriter)           { primAppend("Int8", l) }
func (Int16) WriteAppend(l *LineWriter)          { primAppend("Int16", l) }
func (Int32) WriteAppend(l *LineWriter)          { primAppend("Int32", l) }
func (Int64) WriteAppend(l *LineWriter)          { primAppend("Int64", l) }
func (Uint32) WriteAppend(l *LineWriter)         { primAppend("Uint32", l) }
func (Varint) WriteAppend(l *LineWriter)         { primAppend("Varint", l) }
func (Varlong) WriteAppend(l *LineWriter)        { primAppend("Varlong", l) }
func (String) WriteAppend(l *LineWriter)         { primAppend("String", l) }
func (NullableString) WriteAppend(l *LineWriter) { primAppend("NullableString", l) }
func (Bytes) WriteAppend(l *LineWriter)          { primAppend("Bytes", l) }
func (NullableBytes) WriteAppend(l *LineWriter)  { primAppend("Bool", l) }
func (VarintString) WriteAppend(l *LineWriter)   { primAppend("VarintString", l) }
func (VarintBytes) WriteAppend(l *LineWriter)    { primAppend("VarintBytes", l) }

func (n SizedStruct) WriteAppend(l *LineWriter) {
	l.Write("lenStart := len(dst)")
	l.Write("dst = append(dst, 0, 0, 0, 0)")
	l.Write("{")
	n.Type.WriteAppend(l)
	l.Write("}")
	l.Write("kbin.AppendArrayLen(dst[:lenStart], len(dst))")
}

func (FieldLengthMinusBytes) WriteAppend(l *LineWriter) {
	l.Write("dst = append(dst, v...)")
}

func (a Array) WriteAppend(l *LineWriter) {
	if a.IsVarintArray {
		l.Write("dst = kbin.AppendVarint(dst, int32(len(v)))")
	} else if a.IsNullableArray {
		l.Write("dst = kbin.AppendNullableArrayLen(dst, len(v), v == nil)")
	} else if a.IsUnboundedArray {
		l.Write("{") // fill in size after writing everything
		l.Write("lenStart := len(dst)")
		l.Write("dst = append(dst, 0, 0, 0, 0)")
	} else {
		l.Write("dst = kbin.AppendArrayLen(dst, len(v))")
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

	if a.IsUnboundedArray {
		l.Write("kbin.AppendArrayLen(dst[:lenStart], len(dst))")
		l.Write("}")
	}
}

func (s Struct) WriteAppend(l *LineWriter) {
	for _, f := range s.Fields {
		if f.MaxVersion > -1 {
			l.Write("if version >= %d && version <= %d {", f.MinVersion, f.MaxVersion)
		} else if f.MinVersion != 0 {
			l.Write("if version >= %d {", f.MinVersion)
		} else {
			l.Write("{")
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
}

// primDecode corresponds to the binreader primitive decoding functions in
// kmsg/primitives.go.
func primDecode(name string, l *LineWriter) {
	l.Write("v := b.%s()", name)
}

func (Bool) WriteDecode(l *LineWriter)           { primDecode("Bool", l) }
func (Int8) WriteDecode(l *LineWriter)           { primDecode("Int8", l) }
func (Int16) WriteDecode(l *LineWriter)          { primDecode("Int16", l) }
func (Int32) WriteDecode(l *LineWriter)          { primDecode("Int32", l) }
func (Int64) WriteDecode(l *LineWriter)          { primDecode("Int64", l) }
func (Uint32) WriteDecode(l *LineWriter)         { primDecode("Uint32", l) }
func (Varint) WriteDecode(l *LineWriter)         { primDecode("Varint", l) }
func (Varlong) WriteDecode(l *LineWriter)        { primDecode("Varlong", l) }
func (String) WriteDecode(l *LineWriter)         { primDecode("String", l) }
func (NullableString) WriteDecode(l *LineWriter) { primDecode("NullableString", l) }
func (Bytes) WriteDecode(l *LineWriter)          { primDecode("Bytes", l) }
func (NullableBytes) WriteDecode(l *LineWriter)  { primDecode("Bool", l) }
func (VarintString) WriteDecode(l *LineWriter)   { primDecode("VarintString", l) }
func (VarintBytes) WriteDecode(l *LineWriter)    { primDecode("VarintBytes", l) }

func (n SizedStruct) WriteDecode(l *LineWriter) {
	// Sized structs must read exactly what the size is; if they do
	// not, we Span(-1) to force an error.
	l.Write("if ss := b.ArrayLen(); ss > 0 {")
	l.Write("at := len(b.Src)")

	n.Type.WriteDecode(l)

	// used: at - remaining
	// needed: ss
	// skip: needed - used
	l.Write("if int(ss) != (at - len(b.Src)) {")
	l.Write("b.Span(-1)")
	l.Write("}")
	l.Write("}")
}

func (f FieldLengthMinusBytes) WriteDecode(l *LineWriter) {
	l.Write("v := b.Span(int(s.%s) - %d)", f.Field, f.LengthMinus)
}

func (a Array) WriteDecode(l *LineWriter) {
	// For decoding arrays, we copy our "v" variable to our own "a"
	// variable so that the scope opened just below can use its own
	// v variable. At the end, we reset v with any updates to a.
	l.Write("a := v")
	if a.IsVarintArray {
		l.Write("for i := b.Varint(); i > 0; i-- {")
	} else if a.IsUnboundedArray {
		// For unbounded arrays, we use a new binreader and consume
		// it until we get ErrNotEnoughData.
		l.Write("{")
		l.Write("b := kbin.Reader{Src: b.Span(int(b.ArrayLen()))}")
		l.Write("for len(b.Src) > 0 {")
	} else {
		l.Write("for i := b.ArrayLen(); i > 0; i-- {")
	}

	if s, isStruct := a.Inner.(Struct); isStruct {
		// With structs, we append early and use a pointer to the
		// new element, avoiding double copying.
		l.Write("a = append(a, %s{})", s.Name)
		l.Write("v := &a[len(a)-1]")
	} else if a, isArray := a.Inner.(Array); isArray {
		// With nested arrays, we declare a new v and introduce scope
		// so that the next level will not collide with our current "a".
		l.Write("v := %s{}", a.TypeName())
		l.Write("{")
	}

	a.Inner.WriteDecode(l)

	if _, isArray := a.Inner.(Array); isArray {
		// With nested arrays, now we release our scope.
		l.Write("}")
	} else if _, isStruct := a.Inner.(Struct); !isStruct {
		// With non structs, we append after since the type is small.
		l.Write("a = append(a, v)")
	}

	l.Write("}") // close the for loop

	// With unbounded arrays, if we completed with NotEnoughData, the
	// final element was partial and we discard it.
	if a.IsUnboundedArray {
		l.Write("if b.Complete() == kbin.ErrNotEnoughData {")
		l.Write("a = a[:len(a)-1]")
		l.Write("}")

		l.Write("}") // close the new binreader scope
	}

	l.Write("v = a")
}

func (s Struct) WriteDecode(l *LineWriter) {
	if len(s.Fields) == 0 {
		return
	}
	l.Write("{")
	l.Write("s := v")
	for _, f := range s.Fields {
		if f.MaxVersion > -1 {
			l.Write("if version >= %d && version <= %d {", f.MinVersion, f.MaxVersion)
		} else if f.MinVersion > 0 {
			l.Write("if version >= %d {", f.MinVersion)
		} else {
			l.Write("{")
		}
		switch f.Type.(type) {
		case Struct,
			SizedStruct:
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
		_, isSizedStruct := f.Type.(SizedStruct)
		if !isStruct && !isSizedStruct {
			// If the field was not a struct, we need to copy the
			// changes back.
			l.Write("s.%s = v", f.FieldName)
		}
		l.Write("}")
	}
	l.Write("}")
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
		version := ""
		if f.MinVersion > 0 {
			version = " // v" + strconv.Itoa(f.MinVersion) + "+"
		}
		l.Write("%s %s%s", f.FieldName, f.Type.TypeName(), version)
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
func (s Struct) WriteMinVersionFunc(l *LineWriter) {
	l.Write("func (*%s) MinVersion() int16 { return %d }", s.Name, s.MinVersion)
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
func (s Struct) WriteResponseKindFunc(l *LineWriter) {
	l.Write("func (v *%s) ResponseKind() Response { return &%s{Version: v.Version }}", s.Name, s.ResponseKind)
}

func (s Struct) WriteAppendFunc(l *LineWriter) {
	l.Write("func (v *%s) AppendTo(dst []byte) []byte {", s.Name)
	l.Write("version := v.Version")
	l.Write("_ = version")
	s.WriteAppend(l)
	l.Write("return dst")
	l.Write("}")
}

func (s Struct) WriteDecodeFunc(l *LineWriter) {
	l.Write("func (v *%s) ReadFrom(src []byte) error {", s.Name)
	l.Write("version := v.Version")
	l.Write("_ = version")
	l.Write("b := kbin.Reader{Src: src}")
	s.WriteDecode(l)
	l.Write("return b.Complete()")
	l.Write("}")
}
