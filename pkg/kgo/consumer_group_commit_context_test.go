package kgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCommitRecordsContextWhileWaiting(t *testing.T) {
	for _, prior := range []string{"sync", "async", "automatic"} {
		for _, expiration := range []string{"cancel", "deadline"} {
			for _, api := range []string{"records", "offsets"} {
				t.Run(prior+"/"+expiration+"/"+api, func(t *testing.T) {
					cl := &Client{cfg: cfg{group: "commit-context", logger: new(nopLogger)}}
					g := &groupConsumer{cl: cl, cfg: &cl.cfg, tps: newTopicsPartitions()}
					cl.consumer.g = g
					record := &Record{Topic: "topic", Partition: 0, Offset: 1}
					offsets := map[string]map[int32]EpochOffset{"topic": {0: {Offset: 1}}}

					// Keep the prior commit active without needing a Kafka server.
					// Its pre-commit hook returns an error after the gate is released,
					// so none of the requests in this test reach the network.
					entered := make(chan struct{})
					release := make(chan struct{})
					var releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					defer unblock()
					stopped := errors.New("stopped before sending")
					priorCtx, cancelPrior := context.WithCancel(context.Background())
					defer cancelPrior()
					priorCtx = PreCommitFnContext(priorCtx, func(*kmsg.OffsetCommitRequest) error {
						close(entered)
						<-release
						return stopped
					})
					firstDone := make(chan error, 1)
					callback := func(_ *Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
						firstDone <- err
					}
					switch prior {
					case "sync":
						go func() { firstDone <- cl.CommitRecords(priorCtx, record) }()
					case "async":
						cl.CommitOffsets(priorCtx, offsets, callback)
					case "automatic":
						// Autocommit calls commit without holding syncCommitMu.
						g.noCommitDuringJoinAndSync.RLock()
						g.mu.Lock()
						g.commit(priorCtx, toCommit{offsets: offsets}, func(cl *Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
							g.noCommitDuringJoinAndSync.RUnlock()
							callback(cl, req, resp, err)
						})
						g.mu.Unlock()
					}
					select {
					case <-entered:
					case <-time.After(time.Second):
						t.Fatal("prior commit did not start")
					}

					var ctx context.Context
					var cancel context.CancelFunc
					want := context.Canceled
					if expiration == "deadline" {
						ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
						want = context.DeadlineExceeded
					} else {
						ctx, cancel = context.WithCancel(context.Background())
					}
					defer cancel()
					var sent atomic.Bool
					ctx = PreCommitFnContext(ctx, func(*kmsg.OffsetCommitRequest) error {
						sent.Store(true)
						return stopped
					})
					done := make(chan error, 1)
					go func() {
						if api == "records" {
							done <- cl.CommitRecords(ctx, record)
						} else {
							cl.CommitOffsets(ctx, offsets, func(_ *Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
								done <- err
							})
						}
					}()
					if expiration == "cancel" {
						cancel()
					}
					select {
					case err := <-done:
						if !errors.Is(err, want) {
							t.Errorf("commit returned %v, want %v", err, want)
						}
					case <-time.After(time.Second):
						unblock()
						<-done
						t.Fatal("commit did not return after its context expired")
					}
					if sent.Load() {
						t.Error("canceled commit reached the pre-commit hook")
					}
					if err := priorCtx.Err(); err != nil {
						t.Errorf("later caller canceled the prior commit: %v", err)
					}

					// A canceled queued commit must retain the prior commit's
					// ordering barrier: a third commit cannot overtake it.
					thirdStarted := make(chan struct{})
					thirdDone := make(chan error, 1)
					thirdCtx := PreCommitFnContext(context.Background(), func(*kmsg.OffsetCommitRequest) error {
						close(thirdStarted)
						return stopped
					})
					go func() { thirdDone <- cl.CommitRecords(thirdCtx, record) }()
					select {
					case <-thirdStarted:
						t.Error("third commit overtook the prior commit")
					case <-time.After(30 * time.Millisecond):
					}
					unblock()
					for _, result := range []<-chan error{firstDone, thirdDone} {
						select {
						case err := <-result:
							if !errors.Is(err, stopped) {
								t.Errorf("uncanceled commit returned %v, want %v", err, stopped)
							}
						case <-time.After(time.Second):
							t.Fatal("commit did not finish after releasing the prior commit")
						}
					}
				})
			}
		}
	}
}
