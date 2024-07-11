package kadm

import (
	"context"
	"errors"
	"testing"
)

func TestShardErrors_Unwrap(t *testing.T) {
	err1 := errors.New("test error 1")
	err2 := errors.New("test error 2")

	errs := &ShardErrors{Errs: []ShardError{{Err: err1}, {Err: context.Canceled}}}
	if !errors.Is(errs, err1) {
		t.Errorf("ShardErrors does not match error %v", err1)
	}
	if !errors.Is(errs, context.Canceled) {
		t.Errorf("ShardErrors does not match error %v", context.Canceled)
	}
	if errors.Is(errs, err2) {
		t.Errorf("ShardErrors matches error %v but it not expected to match it", err2)
	}
}
