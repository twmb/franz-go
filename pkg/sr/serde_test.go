package sr

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestSerde(t *testing.T) {
	type (
		overridden struct {
			Ignored string `json:"ignored,omitempty"`
		}
		overrides struct {
			One string `json:"one,omitempty"`
		}
		idx1 struct {
			Two   int64 `json:"two,omitempty"`
			Three int64 `json:"three,omitempty"`
		}
		idx2 struct {
			Biz string `json:"biz,omitempty"`
			Baz string `json:"baz,omitempty"`
		}
		idx3 struct {
			Boz int8 `json:"boz,omitempty"`
		}
		idx4 struct {
			Bingo string `json:"bingo,omitempty"`
		}
		oneidx struct {
			Foo string `json:"foo,omitempty"`
			Bar string `json:"bar,omitempty"`
		}
		moved struct {
			Moved string `json:"moved,omitempty"`
		}
	)

	serde := NewSerde(
		EncodeFn(json.Marshal),
		DecodeFn(json.Unmarshal),
	)
	serde.Register(126, moved{}, Index(1))
	serde.Register(128, moved{})
	serde.Register(128, moved{}, Index(1))      // moved now encodes with ID 128, index 1
	serde.Register(126, overridden{}, Index(1)) // overrides moved's old 126, index 1
	serde.Register(128, overridden{})           // overrides moved's old 128
	serde.Register(127, overridden{}, GenerateFn(func() any { return new(overridden) }))
	serde.Register(127, overrides{})
	serde.Register(3, idx1{}, Index(0))
	serde.Register(3, idx2{}, Index(1), AppendEncodeFn(func(b []byte, v any) ([]byte, error) {
		bb, err := json.Marshal(v)
		if err != nil {
			return b, err
		}
		return append(b, bb...), nil
	}))
	serde.Register(3, idx4{}, Index(0, 0, 1))
	serde.Register(3, idx3{}, Index(0, 0))
	serde.Register(5, oneidx{}, Index(0), GenerateFn(func() any { return &oneidx{Foo: "defoo", Bar: "debar"} }))
	serde.Register(101, struct{}{}, DecodeFn(nil))

	for i, test := range []struct {
		enc    any
		expEnc []byte
		expDec any
		expErr bool
	}{
		{
			enc:    overridden{},
			expErr: true,
		},
		{
			enc:    overrides{"foo"},
			expEnc: append([]byte{0, 0, 0, 0, 127}, `{"one":"foo"}`...),
		},
		{
			enc:    idx1{Two: 2, Three: 3},
			expEnc: append([]byte{0, 0, 0, 0, 3, 0}, `{"two":2,"three":3}`...),
		},
		{
			enc:    idx2{Biz: "bizzy", Baz: "bazzy"},
			expEnc: append([]byte{0, 0, 0, 0, 3, 2, 2}, `{"biz":"bizzy","baz":"bazzy"}`...),
		},
		{
			enc:    idx3{Boz: 8},
			expEnc: append([]byte{0, 0, 0, 0, 3, 4, 0, 0}, `{"boz":8}`...),
		},
		{
			enc:    idx4{Bingo: "bango"},
			expEnc: append([]byte{0, 0, 0, 0, 3, 6, 0, 0, 2}, `{"bingo":"bango"}`...),
		},
		{
			enc:    moved{"m"},
			expEnc: append([]byte{0, 0, 0, 0, 128, 2, 2}, `{"moved":"m"}`...),
		},
		{
			enc:    oneidx{Bar: "bar"},
			expEnc: append([]byte{0, 0, 0, 0, 5, 0}, `{"bar":"bar"}`...),
			expDec: oneidx{Foo: "defoo", Bar: "bar"},
		},
	} {
		b, err := serde.Encode(test.enc)
		gotErr := err != nil
		if gotErr != test.expErr {
			t.Errorf("#%d Encode: got err? %v, exp err? %v", i, gotErr, test.expErr)
			continue
		}
		if test.expErr {
			continue
		}

		if !bytes.Equal(b, test.expEnc) {
			t.Errorf("#%d: Encode(%v) != exp(%v)", i, b, test.expEnc)
			continue
		}

		if b2 := serde.MustEncode(test.enc); !bytes.Equal(b2, b) {
			t.Errorf("#%d got MustEncode(%v) != Encode(%v)", i, b2, b)
		}
		if b2 := serde.MustAppendEncode([]byte("foo"), test.enc); !bytes.Equal(b2, append([]byte("foo"), b...)) {
			t.Errorf("#%d got MustAppendEncode(%v) != Encode(foo%v)", i, b2, b)
		}

		bIndented, err := Encode(test.enc, serde.header(), 100, []int{0}, func(v any) ([]byte, error) {
			return json.MarshalIndent(v, "", "  ")
		})
		if err != nil {
			t.Errorf("#%d Encode[ID=100]: got err? %v, exp err? %v", i, gotErr, test.expErr)
			continue
		}
		if i := bytes.IndexByte(bIndented, '{'); !bytes.Equal(bIndented[:i], []byte{0, 0, 0, 0, 100, 0}) {
			t.Errorf("#%d got Encode[ID=100](%v) != exp(%v)", i, bIndented[:i], []byte{0, 0, 0, 0, 100, 0})
		} else if expIndented := extractIndentedJSON(b); !bytes.Equal(bIndented[i:], expIndented) {
			t.Errorf("#%d got Encode[ID=100](%v) != exp(%v)", i, bIndented[i:], expIndented)
		}

		v, err := serde.DecodeNew(b)
		if err != nil {
			t.Errorf("#%d DecodeNew: got unexpected err %v", i, err)
			continue
		}
		v = reflect.Indirect(reflect.ValueOf(v)).Interface() // DecodeNew returns a pointer, we compare values below

		exp := test.expDec
		if exp == nil {
			exp = test.enc
		}
		if !reflect.DeepEqual(v, exp) {
			t.Errorf("#%d round trip: got %v != exp %v", i, v, exp)
			continue
		}
	}

	if _, err := serde.DecodeNew([]byte{1, 0, 0, 0, 0, 0}); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if _, err := serde.DecodeNew([]byte{0, 0, 0, 0, 3}); err != io.EOF {
		t.Errorf("got %v != exp io.EOF", err)
	}
	if _, err := serde.DecodeNew([]byte{0, 0, 0, 0, 3, 8}); err != ErrNotRegistered {
		t.Errorf("got %v != exp ErrNotRegistered", err)
	}
	if _, err := serde.DecodeNew([]byte{0, 0, 0, 0, 99}); err != ErrNotRegistered {
		t.Errorf("got %v != exp ErrNotRegistered", err)
	}
	if _, err := serde.DecodeNew([]byte{0, 0, 0, 0, 101, 0}); err != ErrNotRegistered {
		// schema is registered but no decode function
		t.Errorf("got %v != exp ErrNotRegistered", err)
	}
}

func extractIndentedJSON(in []byte) []byte {
	i := bytes.IndexByte(in, '{') // skip header
	var out bytes.Buffer
	err := json.Indent(&out, in[i:], "", "  ")
	if err != nil {
		panic(err)
	}
	return out.Bytes()
}

func TestConfluentHeader(t *testing.T) {
	var h ConfluentHeader

	for i, test := range []struct {
		id        int
		newID     uint32
		index     []int
		expEnc    []byte
		expEncUpd []byte
	}{
		{id: 1, newID: 2, index: nil, expEnc: []byte{0, 0, 0, 0, 1}, expEncUpd: []byte{0, 0, 0, 0, 2}},
		{id: 256, newID: 65536, index: nil, expEnc: []byte{0, 0, 0, 1, 0}, expEncUpd: []byte{0, 0, 1, 0, 0}},
		{id: 2, newID: 3, index: []int{0}, expEnc: []byte{0, 0, 0, 0, 2, 0}, expEncUpd: []byte{0, 0, 0, 0, 3, 0}},
		{id: 3, newID: 4, index: []int{1}, expEnc: []byte{0, 0, 0, 0, 3, 2, 2}, expEncUpd: []byte{0, 0, 0, 0, 4, 2, 2}},
		{id: 4, newID: 5, index: []int{1, 2, 3}, expEnc: []byte{0, 0, 0, 0, 4, 6, 2, 4, 6}, expEncUpd: []byte{0, 0, 0, 0, 5, 6, 2, 4, 6}},
	} {
		b, err := h.AppendEncode(nil, test.id, test.index)
		if err != nil {
			t.Errorf("#%d AppendEncode: got unexpected err %v", i, err)
			continue
		}
		if !bytes.Equal(b, test.expEnc) {
			t.Errorf("#%d: AppendEncode(%v) != exp(%v)", i, b, test.expEnc)
			continue
		}

		if b2, _ := h.AppendEncode([]byte("foo"), test.id, test.index); !bytes.Equal(b2, append([]byte("foo"), b...)) {
			t.Errorf("#%d got AppendEncode(%v) != AppendEncode(foo%v)", i, b2, b)
		}

		id, b2, err := h.DecodeID(b)
		if err != nil {
			t.Errorf("#%d DecodeID: got unexpected err %v", i, err)
			continue
		}
		if id != test.id {
			t.Errorf("#%d: DecodeID: id(%v) != exp(%v)", i, id, test.id)
			continue
		}
		if test.index == nil && len(b2) != 0 {
			t.Errorf("#%d: DecodeID: bytes(%v) != exp([])", i, b2)
			continue
		}

		if test.index != nil {
			index, b3, err := h.DecodeIndex(b2, len(test.index))
			if err != nil {
				t.Errorf("#%d DecodeIndex: got unexpected err %v", i, err)
				continue
			}
			if !reflect.DeepEqual(index, test.index) {
				t.Errorf("#%d: DecodeIndex: index(%v) != exp(%v)", i, index, test.index)
				continue
			}
			if len(b3) != 0 {
				t.Errorf("#%d: DecodeIndex: bytes(%v) != exp([])", i, b3)
				continue
			}
		}

		if err := h.UpdateID(b, test.newID); err != nil {
			t.Errorf("#%d UpdateID: got unexpected err %v", i, err)
			continue
		}
		if !bytes.Equal(b, test.expEncUpd) {
			t.Errorf("#%d: UpdateID(%v) != exp(%v)", i, b, test.expEncUpd)
			continue
		}
	}

	if _, _, err := h.DecodeID([]byte{1, 0, 0, 0, 0, 1}); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if _, _, err := h.DecodeID([]byte{0, 0, 0, 0}); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if err := h.UpdateID([]byte{1, 0, 0, 0, 0, 1}, 42); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if err := h.UpdateID([]byte{0, 0, 0, 0}, 42); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if _, _, err := h.DecodeIndex([]byte{2}, 1); err != io.EOF {
		t.Errorf("got %v != exp io.EOF", err)
	}
	if _, _, err := h.DecodeIndex([]byte{6, 2, 4, 6}, 2); err != ErrNotRegistered {
		t.Errorf("got %v != exp ErrNotRegistered", err)
	}
	if _, _, err := h.DecodeIndex([]byte{1}, 2); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if _, _, err := h.DecodeIndex(binary.AppendVarint(nil, 1<<62), 0); err != io.EOF {
		t.Errorf("got %v != exp io.EOF", err)
	}
}

const testGUID = "01234567-89ab-cdef-0123-456789abcdef"

var testGUIDHeader = []byte{1, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}

func TestConfluentHeaderAppendEncodeGUID(t *testing.T) {
	for _, test := range []struct {
		name   string
		b      []byte
		guid   string
		index  []int
		exp    []byte
		expErr bool
	}{
		{name: "nil index", guid: testGUID, exp: testGUIDHeader},
		{name: "empty index", guid: testGUID, index: []int{}, exp: testGUIDHeader},
		{name: "first index", guid: testGUID, index: []int{0}, exp: slices.Concat(testGUIDHeader, []byte{0})},
		{name: "one index", guid: testGUID, index: []int{1}, exp: slices.Concat(testGUIDHeader, []byte{2, 2})},
		{name: "nested index", guid: testGUID, index: []int{1, 2, 3}, exp: slices.Concat(testGUIDHeader, []byte{6, 2, 4, 6})},
		{name: "uppercase", guid: strings.ToUpper(testGUID), exp: testGUIDHeader},
		{name: "appends to b", b: []byte("foo"), guid: testGUID, exp: slices.Concat([]byte("foo"), testGUIDHeader)},

		{name: "empty", guid: "", expErr: true},
		{name: "too short", guid: testGUID[:35], expErr: true},
		{name: "too long", guid: testGUID + "0", expErr: true},
		{name: "no hyphens", guid: "0123456789abcdef0123456789abcdef", expErr: true},
		{name: "hyphens at end", guid: "0123456789abcdef0123456789abcdef----", expErr: true},
		{name: "misplaced hyphen", guid: "0123456-789ab-cdef-0123-456789abcdef", expErr: true},
		{name: "not hex", guid: "0123456g-89ab-cdef-0123-456789abcdef", expErr: true},
		{name: "braces", guid: "{" + testGUID + "}", expErr: true},
		{name: "urn", guid: "urn:uuid:" + testGUID, expErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var h ConfluentHeader
			got, err := h.AppendEncodeGUID(test.b, test.guid, test.index)
			if test.expErr {
				if err == nil {
					t.Errorf("got nil err, exp err")
				}
				return
			}
			if err != nil {
				t.Fatalf("got unexpected err %v", err)
			}
			if !bytes.Equal(got, test.exp) {
				t.Errorf("got bytes(%v) != exp(%v)", got, test.exp)
			}
		})
	}
}

func TestConfluentHeaderDecodeGUID(t *testing.T) {
	for _, test := range []struct {
		name    string
		in      []byte
		expGUID string
		expOut  []byte
		expErr  error
	}{
		{name: "guid", in: testGUIDHeader, expGUID: testGUID},
		{name: "guid and rest", in: slices.Concat(testGUIDHeader, []byte("msg")), expGUID: testGUID, expOut: []byte("msg")},

		{name: "empty", in: nil, expErr: ErrBadHeader},
		{name: "magic only", in: testGUIDHeader[:1], expErr: ErrBadHeader},
		{name: "truncated", in: testGUIDHeader[:16], expErr: ErrBadHeader},
		{name: "id header", in: []byte{0, 0, 0, 0, 7, 2, 2}, expErr: ErrBadHeader},
		{name: "magic 2", in: slices.Concat([]byte{2}, testGUIDHeader[1:]), expErr: ErrBadHeader},
		{name: "magic 0xff", in: slices.Concat([]byte{0xff}, testGUIDHeader[1:]), expErr: ErrBadHeader},
	} {
		t.Run(test.name, func(t *testing.T) {
			var h ConfluentHeader
			guid, out, err := h.DecodeGUID(test.in)
			if err != test.expErr {
				t.Errorf("got err(%v) != exp(%v)", err, test.expErr)
			}
			if guid != test.expGUID {
				t.Errorf("got guid(%q) != exp(%q)", guid, test.expGUID)
			}
			if !bytes.Equal(out, test.expOut) {
				t.Errorf("got bytes(%v) != exp(%v)", out, test.expOut)
			}
		})
	}
}

func TestConfluentHeaderDecodeIDOrGUID(t *testing.T) {
	for _, test := range []struct {
		name    string
		in      []byte
		expID   int
		expGUID string
		expOut  []byte
		expErr  error
	}{
		{name: "id", in: []byte{0, 0, 0, 0, 7}, expID: 7},
		{name: "id and rest", in: []byte{0, 0, 0, 0, 7, 2, 2}, expID: 7, expOut: []byte{2, 2}},
		{name: "guid", in: testGUIDHeader, expGUID: testGUID},
		{name: "guid and rest", in: slices.Concat(testGUIDHeader, []byte("msg")), expGUID: testGUID, expOut: []byte("msg")},

		{name: "empty", in: nil, expErr: ErrBadHeader},
		{name: "truncated id", in: []byte{0, 0, 0, 0}, expErr: ErrBadHeader},
		{name: "truncated guid", in: testGUIDHeader[:16], expErr: ErrBadHeader},
		{name: "magic 2", in: slices.Concat([]byte{2}, testGUIDHeader[1:]), expErr: ErrBadHeader},
		{name: "magic 0xff", in: slices.Concat([]byte{0xff}, testGUIDHeader[1:]), expErr: ErrBadHeader},
	} {
		t.Run(test.name, func(t *testing.T) {
			var h ConfluentHeader
			id, guid, out, err := h.DecodeIDOrGUID(test.in)
			if err != test.expErr {
				t.Errorf("got err(%v) != exp(%v)", err, test.expErr)
			}
			if id != test.expID || guid != test.expGUID {
				t.Errorf("got id(%d), guid(%q) != exp id(%d), guid(%q)", id, guid, test.expID, test.expGUID)
			}
			if !bytes.Equal(out, test.expOut) {
				t.Errorf("got bytes(%v) != exp(%v)", out, test.expOut)
			}
		})
	}
}

func TestConfluentHeaderGUIDIndex(t *testing.T) {
	for _, test := range []struct {
		name  string
		index []int
	}{
		{name: "first index", index: []int{0}},
		{name: "one index", index: []int{1}},
		{name: "nested index", index: []int{1, 2, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var h ConfluentHeader
			b, err := h.AppendEncodeGUID(nil, testGUID, test.index)
			if err != nil {
				t.Fatalf("AppendEncodeGUID: got unexpected err %v", err)
			}
			guid, rest, err := h.DecodeGUID(append(b, "msg"...))
			if err != nil || guid != testGUID {
				t.Fatalf("DecodeGUID: got guid(%q), err(%v) != exp guid(%q), nil err", guid, err, testGUID)
			}
			index, out, err := h.DecodeIndex(rest, len(test.index))
			if err != nil {
				t.Fatalf("DecodeIndex: got unexpected err %v", err)
			}
			if !slices.Equal(index, test.index) {
				t.Errorf("got index(%v) != exp(%v)", index, test.index)
			}
			if string(out) != "msg" {
				t.Errorf("got bytes(%q) != exp(msg)", out)
			}
		})
	}
}

func FuzzConfluentHeaderDecodeIDOrGUID(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 1, 2, 2, 'm'})
	f.Add(append([]byte{1}, make([]byte, 16)...))
	f.Add(append([]byte{1}, make([]byte, 20)...))
	f.Add(append([]byte{2}, make([]byte, 16)...))
	f.Fuzz(func(t *testing.T, in []byte) {
		var h ConfluentHeader
		id, guid, out, err := h.DecodeIDOrGUID(in)
		_, _, idErr := h.DecodeID(in)
		_, _, guidErr := h.DecodeGUID(in)
		if err != nil {
			if err != ErrBadHeader || idErr == nil || guidErr == nil {
				t.Fatalf("got err(%v), DecodeID err(%v), DecodeGUID err(%v)", err, idErr, guidErr)
			}
			return
		}
		var enc []byte
		if guid != "" {
			if idErr == nil || guidErr != nil {
				t.Fatalf("GUID form: got DecodeID err(%v), DecodeGUID err(%v)", idErr, guidErr)
			}
			enc, err = h.AppendEncodeGUID(nil, guid, nil)
		} else {
			if idErr != nil || guidErr == nil {
				t.Fatalf("ID form: got DecodeID err(%v), DecodeGUID err(%v)", idErr, guidErr)
			}
			enc, err = h.AppendEncode(nil, id, nil)
		}
		if err != nil {
			t.Fatalf("re-encode: got unexpected err %v", err)
		}
		if got := append(enc, out...); !bytes.Equal(got, in) {
			t.Fatalf("re-encode: bytes(%v) != exp(%v)", got, in)
		}
	})
}

func FuzzConfluentHeaderAppendEncodeGUID(f *testing.F) {
	f.Add("")
	f.Add("01234567-89ab-cdef-0123-456789abcdef")
	f.Add("01234567-89AB-CDEF-0123-456789ABCDEF")
	f.Add("0123456789abcdef0123456789abcdef")
	f.Add("0123456g-89ab-cdef-0123-456789abcdef")
	f.Fuzz(func(t *testing.T, guid string) {
		var h ConfluentHeader
		b, err := h.AppendEncodeGUID(nil, guid, nil)
		if err != nil {
			if b != nil {
				t.Fatalf("got bytes(%v) with err %v", b, err)
			}
			return
		}
		exp := strings.ToLower(guid)
		got, out, err := h.DecodeGUID(b)
		if err != nil || len(out) != 0 || got != exp {
			t.Fatalf("got guid(%q), bytes(%v), err(%v) != exp guid(%q), bytes([]), nil err", got, out, err, exp)
		}
	})
}

func TestIsRegistered(t *testing.T) {
	serde := NewSerde()
	serde.Register(2, struct{}{}, EncodeFn(json.Marshal), DecodeFn(json.Unmarshal))
	serde.Register(3, struct{}{}, AppendEncodeFn(func(b []byte, _ any) ([]byte, error) { return b, nil }), GenerateFn(func() any { return new(struct{}) }))
	serde.Register(5, struct{}{}, DecodeFn(json.Unmarshal), Index(0))
	serde.Register(6, struct{}{}, DecodeFn(json.Unmarshal), Index(1))

	for i, test := range []struct {
		id  int
		fns []any
		exp bool
	}{
		{99, []any{EncodeFn}, false}, // missing ID
		{2, nil, false},              // no functions to check
		{2, []any{EncodeFn, DecodeFn}, true},
		{2, []any{"EncodeFn"}, true},
		{2, []any{"encodeFn"}, false}, // unknown name
		{2, []any{nil}, false},
		{2, []any{EncodeFn, GenerateFn}, false},
		{2, []any{AppendEncodeFn}, false},
		{3, []any{AppendEncodeFn, GenerateFn}, true},
		{3, []any{EncodeFn}, false},
		{3, []any{DecodeFn}, false},
		{5, []any{DecodeFn}, true}, // default index
		{5, []any{EncodeFn}, false},
		{6, []any{DecodeFn}, false}, // only a non-default index
	} {
		if got := serde.IsRegistered(test.id, test.fns...); got != test.exp {
			t.Errorf("#%d: got IsRegistered(%d) = %v != exp %v", i, test.id, got, test.exp)
		}
	}
}
