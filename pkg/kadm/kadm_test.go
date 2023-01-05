package kadm

import (
	"errors"
	"reflect"
	"testing"
)

func input[V any](v V) V { return v }

func inputErr[V any](v V, err error) (V, error) { return v, err }

func TestFirst(t *testing.T) {
	for _, test := range []struct {
		in     []int
		inErr  error
		exp    int
		expErr bool
	}{
		{[]int{3, 4, 5}, nil, 3, false},
		{[]int{4}, nil, 4, false},
		{[]int{4}, errors.New("foo"), 4, true},
		{nil, errors.New("foo"), 0, true},
	} {
		got, err := FirstOrErr(inputErr(test.in, test.inErr))
		gotErr := err != nil
		if gotErr != test.expErr {
			t.Errorf("got err? %v, exp err? %v", gotErr, test.expErr)
		}
		if !gotErr && !test.expErr {
			if !reflect.DeepEqual(got, test.exp) {
				t.Errorf("got %v != exp %v", got, test.exp)
			}
		}

		got, exists := First(input(test.in))
		if len(test.in) == 0 {
			if exists {
				t.Error("got exists, expected no")
			}
			continue
		}
		if !exists {
			t.Error("got not exists, expected yes")
		}
		if !reflect.DeepEqual(got, test.exp) {
			t.Errorf("got %v != exp %v", got, test.exp)
		}
	}
}

func TestAny(t *testing.T) {
	for _, test := range []struct {
		in     map[int]string
		inErr  error
		exp    string
		expErr bool
	}{
		{map[int]string{3: "foo"}, nil, "foo", false},
		{map[int]string{3: "foo"}, errors.New("foo"), "foo", true},
		{nil, errors.New("foo"), "", true},
	} {
		got, err := AnyOrErr(inputErr(test.in, test.inErr))
		gotErr := err != nil
		if gotErr != test.expErr {
			t.Errorf("got err? %v, exp err? %v", gotErr, test.expErr)
		}
		if !gotErr && !test.expErr {
			if !reflect.DeepEqual(got, test.exp) {
				t.Errorf("got %v != exp %v", got, test.exp)
			}
		}

		got, exists := Any(input(test.in))
		if len(test.in) == 0 {
			if exists {
				t.Error("got exists, expected no")
			}
			continue
		}
		if !exists {
			t.Error("got not exists, expected yes")
		}
		if !reflect.DeepEqual(got, test.exp) {
			t.Errorf("got %v != exp %v", got, test.exp)
		}
	}
}
