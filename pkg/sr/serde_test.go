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

	var serde Serde
	serde.SetDefaults(
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
	serde.Register(100, nil, Index(0), EncodeFn(func(v any) ([]byte, error) {
		return json.MarshalIndent(v, "", "  ")
	}))

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
			enc:    oneidx{Bar: "bar"},
			expEnc: append([]byte{0, 0, 0, 0, 5, 0}, `{"bar":"bar"}`...),
			expDec: oneidx{Foo: "defoo", Bar: "bar"},
		},
	} {
		if _, err := serde.Encode(test.enc, ID(99)); err != ErrNotRegistered {
			t.Errorf("got %v != exp ErrNotRegistered", err)
		}

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

		bIndented := serde.MustEncode(test.enc, ID(100), Index(0))
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
		}
	}

	if _, err := serde.DecodeNew([]byte{1, 0, 0, 0, 0, 0}); err != ErrBadHeader {
		t.Errorf("got %v != exp ErrBadHeader", err)
	}
	if _, err := serde.DecodeNew([]byte{0, 0, 0, 0, 3}); err != io.EOF {
		t.Errorf("got %v != exp io.EOF", err)
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
