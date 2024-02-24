package sr

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
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
	)

	serde := NewSerde(
		EncodeFn(json.Marshal),
		DecodeFn(json.Unmarshal),
	)
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

	for i, test := range []struct {
		enc    any
		expEnc []byte
		expDec any
		expErr bool
		expMap map[string]any
	}{
		{
			enc:    overridden{},
			expErr: true,
		},
		{
			enc:    overrides{"foo"},
			expEnc: append([]byte{0, 0, 0, 0, 127}, `{"one":"foo"}`...),
			expMap: map[string]any{"one": "foo"},
		},
		{
			enc:    idx1{Two: 2, Three: 3},
			expEnc: append([]byte{0, 0, 0, 0, 3, 0}, `{"two":2,"three":3}`...),
			expMap: map[string]any{"two": float64(2), "three": float64(3)},
		},
		{
			enc:    idx2{Biz: "bizzy", Baz: "bazzy"},
			expEnc: append([]byte{0, 0, 0, 0, 3, 2, 2}, `{"biz":"bizzy","baz":"bazzy"}`...),
			expMap: map[string]any{"biz": "bizzy", "baz": "bazzy"},
		},
		{
			enc:    idx3{Boz: 8},
			expEnc: append([]byte{0, 0, 0, 0, 3, 4, 0, 0}, `{"boz":8}`...),
			expMap: map[string]any{"boz": float64(8)},
		},
		{
			enc:    idx4{Bingo: "bango"},
			expEnc: append([]byte{0, 0, 0, 0, 3, 6, 0, 0, 2}, `{"bingo":"bango"}`...),
			expMap: map[string]any{"bingo": "bango"},
		},
		{
			enc:    oneidx{Bar: "bar"},
			expEnc: append([]byte{0, 0, 0, 0, 5, 0}, `{"bar":"bar"}`...),
			expDec: oneidx{Foo: "defoo", Bar: "bar"},
			expMap: map[string]any{"bar": "bar"},
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

		bIndented, err := Encode(test.enc, 100, []int{0}, serde.header(), func(v any) ([]byte, error) {
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
	if _, err := serde.DecodeNew([]byte{0, 0, 0, 0, 100, 0}); err != ErrNotRegistered {
		// schema is registered but type is unknown
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
		id     int
		index  []int
		expEnc []byte
	}{
		{id: 1, index: nil, expEnc: []byte{0, 0, 0, 0, 1}},
		{id: 256, index: nil, expEnc: []byte{0, 0, 0, 1, 0}},
		{id: 2, index: []int{0}, expEnc: []byte{0, 0, 0, 0, 2, 0}},
		{id: 3, index: []int{1}, expEnc: []byte{0, 0, 0, 0, 3, 2, 2}},
		{id: 4, index: []int{1, 2, 3}, expEnc: []byte{0, 0, 0, 0, 4, 6, 2, 4, 6}},
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
	}

	if _, _, err := h.DecodeID([]byte{1, 0, 0, 0, 0, 1}); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if _, _, err := h.DecodeID([]byte{0, 0, 0, 0}); err != ErrBadHeader {
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
}
