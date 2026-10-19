package main

import (
	"fmt"
	"sort"
	"strconv"
)

// An array's length is read before its elements, and decoding allocates
// every element up front. ArrayLen bounds the length by the bytes left, a
// byte an element, but an element takes many times its encoded size in
// memory: a 16 MiB Metadata v12 request of empty topic entries allocated
// 512 MiB. The decoders bound the length by the element's minimum encoded
// size at the message's version instead (Reader.FitsArray), so a length
// never allocates more elements than the bytes left can encode.

// minSizeStructs are the structs whose minimum size a decoder or another
// minimum size function calls for: each gets a minSize<Name> function.
var minSizeStructs = map[string]Struct{}

// minSizeExpr is a Go expression of the fewest bytes a value of t encodes in,
// in terms of the decoder's version and flex, the expression of whether the
// version is flexible ("false" where no flexible version is). It mirrors
// what WriteDecode reads: a length or a presence byte, and no more, for an
// empty or null value.
func minSizeExpr(t Type, flex string) string {
	switch t := t.(type) {
	case Bool, Int8:
		return "1"
	case Int16, Uint16:
		return "2"
	case Int32, Uint32, Throttle, Timeout:
		return "4"
	case Int64, Float64:
		return "8"
	case Varint, Varlong, VarintString, VarintBytes:
		return "1"
	case Uuid:
		return "16"
	case FieldLengthMinusBytes:
		return "0"
	case String:
		return compactOr(t.FromFlexible, flex, 1, 2)
	case NullableString:
		return compactOr(t.FromFlexible, flex, 1, 2)
	case Bytes:
		return compactOr(t.FromFlexible, flex, 1, 4)
	case NullableBytes:
		return compactOr(t.FromFlexible, flex, 1, 4)
	case Array:
		if t.IsVarintArray {
			return "1"
		}
		return compactOr(t.FromFlexible, flex, 1, 4)
	case Struct:
		if t.Nullable {
			return "1" // its presence byte
		}
		if t.WithVersionField {
			// Its decoder reads its fields by the version it holds,
			// which a nested one does not hold (Struct.WriteDecode skips
			// the field): none is nested in an array today.
			die("no minimum size for %s, a struct with its own version field, in an array", t.Name)
		}
		minSizeStructs[t.Name] = t
		return fmt.Sprintf("minSize%s(version, %s)", t.Name, flex)
	case Enum:
		return minSizeExpr(t.Type, flex)
	}
	die("no minimum size for %T", t)
	return ""
}

// compactOr is the size of a length, compact in the flexible versions.
func compactOr(fromFlexible bool, flex string, compact, classic int) string {
	if !fromFlexible || flex == "false" {
		return strconv.Itoa(classic)
	}
	return fmt.Sprintf("kbin.Pick(%s, %d, %d)", flex, compact, classic)
}

// writeArrayBound writes the check that an array's length l fits in the
// bytes left at its elements' minimum size.
func writeArrayBound(a Array, l *LineWriter) {
	flex := "false"
	if a.FromFlexible {
		flex = "isFlexible"
	}
	min := minSizeExpr(a.Inner, flex)
	if min == "0" || min == "1" { // ArrayLen bounds a byte an element
		return
	}
	l.Write("if !b.FitsArray(l, %s) {", min)
	l.Write("return b.Complete()")
	l.Write("}")
}

// writeMinSizeFuncs writes a minSize<Name> function for every struct
// minSizeExpr named, and for every one those name.
func writeMinSizeFuncs(l *LineWriter) {
	done := map[string]bool{}
	for {
		var names []string
		for name := range minSizeStructs {
			if !done[name] {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			return
		}
		sort.Strings(names)
		for _, name := range names {
			done[name] = true
			writeMinSizeFunc(minSizeStructs[name], l)
		}
	}
}

func writeMinSizeFunc(s Struct, l *LineWriter) {
	flex := "false"
	if s.FromFlexible {
		flex = "isFlexible"
	}
	l.Write("// minSize%s is the fewest bytes a %s encodes in at version.", s.Name, s.Name)
	l.Write("func minSize%s(version int16, isFlexible bool) int {", s.Name)
	l.Write("_, _ = version, isFlexible")
	l.Write("n := 0")
	for _, f := range s.Fields {
		var cond string
		switch {
		case f.MinVersion == -1: // a tag only
			continue
		case f.MaxVersion > -1:
			cond = fmt.Sprintf("version >= %d && version <= %d", f.MinVersion, f.MaxVersion)
		case f.MinVersion > 0:
			cond = fmt.Sprintf("version >= %d", f.MinVersion)
		}
		min := minSizeExpr(f.Type, flex)
		if min == "0" {
			continue
		}
		if cond == "" {
			l.Write("n += %s", min)
		} else {
			l.Write("if %s {", cond)
			l.Write("n += %s", min)
			l.Write("}")
		}
	}
	if s.FromFlexible {
		l.Write("if isFlexible {")
		l.Write("n++ // its tagged fields' count")
		l.Write("}")
	}
	l.Write("return n")
	l.Write("}")
}
