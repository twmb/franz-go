package kgo

import (
	"testing"
	"time"
)

func TestIDStableLongEnough(t *testing.T) {
	t.Parallel()
	if idStableLongEnough(time.Time{}) {
		t.Error("a zero agreed-at time must never count as stable")
	}
	if idStableLongEnough(time.Now()) {
		t.Error("a just-agreed ID must not count as stable")
	}
	if !idStableLongEnough(time.Now().Add(-recreationStableIDAge - time.Second)) {
		t.Error("an ID held longer than the stable age must count as stable")
	}
}
