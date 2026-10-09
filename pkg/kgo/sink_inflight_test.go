package kgo

import (
	"testing"
	"time"
)

// TestSinkInflight checks that sink.inflight stays in step with how newSink
// and firstRespCheck fill and free the inflight sem.
func TestSinkInflight(t *testing.T) {
	t.Parallel()

	// take simulates drain taking a slot for n requests.
	take := func(t *testing.T, s *sink, n int) {
		t.Helper()
		for range n {
			select {
			case s.inflightSem <- struct{}{}:
			default:
				t.Fatalf("inflight sem full with %d slots in use", len(s.inflightSem))
			}
		}
	}
	requireInflight := func(t *testing.T, s *sink, want int) {
		t.Helper()
		if got := s.inflight(); got != want {
			t.Errorf("inflight: got %d, want %d", got, want)
		}
	}
	idempotentSink := func() *sink {
		return (&Client{cfg: defaultCfg()}).newSink(1)
	}

	t.Run("idempotent_before_first_response", func(t *testing.T) {
		t.Parallel()
		s := idempotentSink()
		requireInflight(t, s, 0)
		take(t, s, 1)
		requireInflight(t, s, 1)
	})

	t.Run("idempotent_first_response_v3", func(t *testing.T) {
		t.Parallel()
		s := idempotentSink()
		take(t, s, 1)
		s.firstRespCheck(true, 3)
		requireInflight(t, s, 1)
		if len(s.inflightSem) != cap(s.inflightSem) {
			t.Fatalf("v3 broker: sem should still admit only one request")
		}
		<-s.inflightSem
		requireInflight(t, s, 0)
	})

	t.Run("idempotent_first_response_v4", func(t *testing.T) {
		t.Parallel()
		s := idempotentSink()
		take(t, s, 1)
		s.firstRespCheck(true, 4)
		requireInflight(t, s, 1)
		take(t, s, 4)
		requireInflight(t, s, 5)
		<-s.inflightSem
		requireInflight(t, s, 4)
	})

	t.Run("not_idempotent", func(t *testing.T) {
		t.Parallel()
		cfg := defaultCfg()
		cfg.disableIdempotency = true
		cfg.maxProduceInflight = 3
		s := (&Client{cfg: cfg}).newSink(1)
		requireInflight(t, s, 0)
		take(t, s, 1)
		s.firstRespCheck(false, 9)
		requireInflight(t, s, 1)
		take(t, s, 2)
		requireInflight(t, s, 3)
	})
}

type produceRequestStartHook struct {
	meta    []BrokerMetadata
	metrics []ProduceRequestStartMetrics
}

func (h *produceRequestStartHook) OnProduceRequestStart(meta BrokerMetadata, m ProduceRequestStartMetrics) {
	h.meta = append(h.meta, meta)
	h.metrics = append(h.metrics, m)
}

// TestProduceRequestStartHook checks that startProduceRequest calls
// HookProduceRequestStart once with the broker, the sink's in-flight count
// and the wait it is given.
func TestProduceRequestStartHook(t *testing.T) {
	t.Parallel()

	hook := new(produceRequestStartHook)
	cfg := defaultCfg()
	cfg.disableIdempotency = true
	cfg.maxProduceInflight = 3
	cfg.hooks = hooks{hook}
	s := (&Client{cfg: cfg}).newSink(1)
	s.inflightSem <- struct{}{}
	s.inflightSem <- struct{}{}

	br := &broker{meta: BrokerMetadata{NodeID: 1}}
	s.startProduceRequest(br, 5*time.Millisecond)

	if len(hook.meta) != 1 || hook.meta[0].NodeID != 1 {
		t.Errorf("hook brokers %+v, want one call for node 1", hook.meta)
	}
	want := ProduceRequestStartMetrics{Inflight: 2, InflightWait: 5 * time.Millisecond}
	if len(hook.metrics) != 1 || hook.metrics[0] != want {
		t.Errorf("hook metrics %+v, want one call with %+v", hook.metrics, want)
	}
}
