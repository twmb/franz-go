package klogrus_test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/klogrus"
)

func ExampleNew() {
	l := klogrus.New(logrus.New())

	l.Log(kgo.LogLevelInfo, "test message", "test-key", "test-val")
	// Output:
}

func TestFieldLogger(t *testing.T) {
	logger, hook := test.NewNullLogger()

	l := klogrus.NewFieldLogger(logger)

	level := l.Level()
	assert.Equal(t, kgo.LogLevelInfo, level)

	l.Log(kgo.LogLevelInfo, "test message", "test-key", "test-val")

	require.Equal(t, 1, len(hook.Entries))
	lastEntry := hook.LastEntry()

	assert.Equal(t, logrus.InfoLevel, lastEntry.Level)
	assert.Equal(t, "test message", lastEntry.Message)

	value, ok := lastEntry.Data["test-key"]
	assert.True(t, ok)
	assert.Equal(t, "test-val", value)

	hook.Reset()
	assert.Nil(t, hook.LastEntry())
}
