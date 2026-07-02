package kgo

import (
	"fmt"
	"io"
	"testing"
)

// io.ErrUnexpectedEOF is the mid-frame sibling of io.EOF: a graceful FIN
// landing inside a length-prefixed frame surfaces it from io.ReadFull. It
// must classify retryable like every other disconnect shape; before the fix
// it alone surfaced after zero retries, tearing down group sessions and
// insta-failing buffered records via bumpRepeatedLoadErr.
func TestUnexpectedEOFRetriable(t *testing.T) {
	t.Parallel()
	if !isRetryableBrokerErr(io.ErrUnexpectedEOF) {
		t.Error("io.ErrUnexpectedEOF is not retryable")
	}
	if !isRetryableBrokerErr(fmt.Errorf("read fail: %w", io.ErrUnexpectedEOF)) {
		t.Error("wrapped io.ErrUnexpectedEOF is not retryable")
	}
	// The first-read SASL pessimism must keep applying now that the error
	// is retryable in general.
	if isRetryableBrokerErr(&ErrFirstReadEOF{kind: firstReadSASL, err: io.ErrUnexpectedEOF}) {
		t.Error("first-read io.ErrUnexpectedEOF should stay non-retryable")
	}
	if isRetryableBrokerErr(&ErrFirstReadEOF{kind: firstReadSASL, err: io.EOF}) {
		t.Error("first-read io.EOF should stay non-retryable")
	}
}

// The broker validates ApiVersions software name/version against
// [a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])? and answers INVALID_REQUEST
// otherwise; since ApiVersions is the first request on every connection, an
// invalid value bricked the client with an opaque error. NewClient now
// validates.
func TestSoftwareNameVersionValidation(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"", "my app", "app_1", "-app", "app-", ".app", "app/1"} {
		if _, err := NewClient(SeedBrokers("localhost:1"), SoftwareNameAndVersion(bad, "1.0.0")); err == nil {
			t.Errorf("software name %q: expected NewClient error", bad)
		}
	}
	for _, good := range []string{"kgo", "my-app", "app.1", "a", "A9"} {
		cl, err := NewClient(SeedBrokers("localhost:1"), SoftwareNameAndVersion(good, "1.0.0"))
		if err != nil {
			t.Errorf("software name %q: unexpected error %v", good, err)
			continue
		}
		cl.Close()
	}
	if _, err := NewClient(SeedBrokers("localhost:1"), TransactionalID("")); err == nil {
		t.Error("empty transactional id: expected NewClient error")
	}
}
