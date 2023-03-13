package klogr

import (
	"fmt"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLogger_Log(t *testing.T) {
	for _, test := range []struct {
		level        kgo.LogLevel
		expectOutput bool
	}{
		{level: kgo.LogLevelInfo, expectOutput: true},
		{level: kgo.LogLevelError, expectOutput: true},
		{level: kgo.LogLevelNone, expectOutput: false},
	} {
		t.Run(fmt.Sprintf("logging level %s", test.level), func(t *testing.T) {
			l, hasOutput := loggerOutput()
			l.Log(test.level, msg, keyvals...)

			if *hasOutput != test.expectOutput {
				t.Errorf("expect output: %v, has output: %t", test.expectOutput, *hasOutput)
			}
		})
	}
}

func BenchmarkLogError(b *testing.B) {
	benchLogger(b, kgo.LogLevelError)
}

func BenchmarkLogInfo(b *testing.B) {
	benchLogger(b, kgo.LogLevelInfo)
}

func BenchmarkLogNone(b *testing.B) {
	benchLogger(b, kgo.LogLevelNone)
}

var (
	msg     = "message"
	keyvals = []interface{}{
		"bool", true,
		"string", "str",
		"int", 42,
		"float", 3.14,
		"struct", struct{ A, B int }{13, 37},
		"err", fmt.Errorf("error"),
	}
)

func loggerOutput() (*Logger, *bool) {
	var hasOutput bool
	write := func(prefix, args string) { hasOutput = true }
	lr := funcr.New(write, funcr.Options{Verbosity: 42})
	return New(lr), &hasOutput
}

func benchLogger(b *testing.B, level kgo.LogLevel) {
	l, _ := loggerOutput()
	for i := 0; i < b.N; i++ {
		l.Log(level, msg, keyvals...)
	}
}
