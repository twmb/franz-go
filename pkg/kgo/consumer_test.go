package kgo

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func TestConsumeRecreated(t *testing.T) {
	for _, group := range []string{"", "fill"} {
		group := group
		t.Run("group_"+group, func(t *testing.T) {
			t.Parallel()

			if group != "" {
				randGroup, groupCleanup := tmpGroup(t)
				defer groupCleanup()
				group = randGroup
			}

			topic, cleanup := tmpTopic(t)
			defer cleanup()

			cl, _ := NewClient(
				getSeedBrokers(),
				DefaultProduceTopic(topic),
				UnknownTopicRetries(-1),
				ConsumeTopics(topic),
				FetchMaxWait(time.Second),
				ConsumeRecreatedTopics(),
				ConsumerGroup(group),
			)
			defer cl.Close()

			{
				req := kmsg.NewPtrApiVersionsRequest()
				resp, err := req.RequestWith(context.Background(), cl)
				if err != nil {
					t.Fatalf("unable to request api version: %v", err)
				}
				v := kversion.FromApiVersionsResponse(resp)
				if max, ok := v.LookupMaxKeyVersion(1); !ok || max < 13 {
					t.Skip("skipping test, the broker does not support topic IDs, so we consume the recreated topic by default and reset to the end with OffsetForLeaderEpoch")
					return
				}
			}

			produce := func() {
				for {
					err := cl.ProduceSync(context.Background(), StringRecord("foo")).FirstErr()
					switch err {
					case nil:
						return
					case errMissingMetadataPartition:
					default:
						t.Fatalf("produce failure, group %q: %v", group, err)
					}
				}
			}

			checkPoll := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				fs := cl.PollFetches(ctx)
				if errs := fs.Errors(); len(errs) != 0 {
					t.Fatalf("consume failure, group %q: %v", group, errs)
				}
				recs := fs.Records()
				if len(recs) != 1 || string(recs[0].Value) != "foo" {
					t.Fatalf("consume records incorrect, group %q: %v", group, recs)
				}
			}

			produce()
			checkPoll()
			cleanup()
			tmpNamedTopic(t, topic)
			produce()
			checkPoll()
		})
	}
}
