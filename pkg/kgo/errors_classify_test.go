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

func TestIsDecompressErr(t *testing.T) {
	t.Parallel()

	// A direct errDecompress should be recognized.
	direct := &errDecompress{err: io.ErrUnexpectedEOF}
	if !isDecompressErr(direct) {
		t.Error("direct *errDecompress not detected")
	}

	// A wrapped errDecompress should be recognized through the chain.
	wrapped := fmt.Errorf("process failed: %w", direct)
	if !isDecompressErr(wrapped) {
		t.Error("wrapped *errDecompress not detected")
	}

	// A non-decompress error must not match.
	if isDecompressErr(io.ErrUnexpectedEOF) {
		t.Error("plain io.ErrUnexpectedEOF falsely detected as decompress error")
	}

	// nil must not match.
	if isDecompressErr(nil) {
		t.Error("nil falsely detected as decompress error")
	}
}
