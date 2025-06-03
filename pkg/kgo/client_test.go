package kgo

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestMaxVersions(t *testing.T) {
	if ours, main := new(fetchRequest).MaxVersion(), new(kmsg.FetchRequest).MaxVersion(); ours != main {
		t.Errorf("our fetch request max version %d != kmsg's %d", ours, main)
	}
	if ours, main := (&produceRequest{can12: true}).MaxVersion(), new(kmsg.ProduceRequest).MaxVersion(); ours != main {
		t.Errorf("our produce request max version %d != kmsg's %d", ours, main)
	}
}

func TestParseBrokerAddr(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		expected hostport
	}{
		{
			"IPv4",
			"127.0.0.1:1234",
			hostport{"127.0.0.1", 1234},
		},
		{
			"IPv4 + default port",
			"127.0.0.1",
			hostport{"127.0.0.1", 9092},
		},
		{
			"host",
			"localhost:1234",
			hostport{"localhost", 1234},
		},
		{
			"host + default port",
			"localhost",
			hostport{"localhost", 9092},
		},
		{
			"IPv6",
			"[2001:1000:2000::1]:1234",
			hostport{"2001:1000:2000::1", 1234},
		},
		{
			"IPv6 + default port",
			"[2001:1000:2000::1]",
			hostport{"2001:1000:2000::1", 9092},
		},
		{
			"IPv6 literal",
			"::1",
			hostport{"::1", 9092},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseBrokerAddr(test.addr)
			if err != nil {
				t.Fatal(err)
			}
			if result != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, result)
			}
		})
	}
}

func TestParseBrokerAddrErrors(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{
			"IPv4 invalid port",
			"127.0.0.1:foo",
		},
		{
			"host invalid port",
			"localhost:foo",
		},

		{
			"IPv6 invalid port",
			"[2001:1000:2000::1]:foo",
		},
		{
			"IPv6 missing closing bracket",
			"[2001:1000:2000::1:1234",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseBrokerAddr(test.addr)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestUnknownGroupOffsetFetchPinned(t *testing.T) {
	req := kmsg.NewOffsetFetchRequest()
	req.Group = "unknown-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	cl, _ := newTestClient()
	defer cl.Close()
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("fetch panicked: %v", err)
		}
	}()
	req.RequestWith(context.Background(), cl)
}

func TestProcessHooks(t *testing.T) {
	var (
		aHook     = Hook(&someHook{index: 10})
		xs        = []Hook{&someHook{index: 1}, &someHook{index: 2}}
		ys        = []Hook{&someHook{index: 3}, &someHook{index: 4}, &someHook{index: 5}}
		all       = append(append(xs, ys...), aHook)
		sliceHook = new(intSliceHook)
	)

	tests := []struct {
		name     string
		hooks    []Hook
		expected []Hook
	}{
		{
			name:     "all",
			hooks:    all,
			expected: all,
		},
		{
			name:     "nested slice",
			hooks:    []Hook{all},
			expected: all,
		},
		{
			name:     "hooks and slices",
			hooks:    []Hook{xs, ys, aHook},
			expected: all,
		},
		{
			name:     "slice that implements a hook",
			hooks:    []Hook{sliceHook},
			expected: []Hook{sliceHook},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hooks, err := processHooks(test.hooks)
			if err != nil {
				t.Fatal("expected no error", err)
			}
			if !reflect.DeepEqual(hooks, test.expected) {
				t.Fatalf("didn't get expected hooks back after processing, %+v, %+v", hooks, test.expected)
			}
		})
	}
}

func TestProcessHooksErrors(t *testing.T) {
	tests := []struct {
		name  string
		hooks []Hook
	}{
		{
			name:  "useless slice",
			hooks: []Hook{&[]int{}},
		},
		{
			name:  "deep useless slice",
			hooks: []Hook{[]Hook{&[]int{}}},
		},
		{
			name:  "mixed useful and useless",
			hooks: []Hook{&someHook{}, &notAHook{}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := processHooks(test.hooks)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

type notAHook struct{}

type someHook struct {
	index int
}

func (*someHook) OnNewClient(*Client) {
	// ignore
}

type intSliceHook []int

func (*intSliceHook) OnNewClient(*Client) {
	// ignore
}

func TestPing(t *testing.T) {
	t.Parallel()

	cl, _ := newTestClient()
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := cl.Ping(ctx)
	if err != nil {
		t.Errorf("unable to ping: %v", err)
	}
}

func TestIssue1034(t *testing.T) {
	t.Parallel()

	cl, _ := newTestClient()
	defer cl.Close()

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorType = 0
	req.CoordinatorKeys = []string{"foo", "bar", "biz"}
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("unable to request coordinators: %v", err)
	}
	need := make(map[string]struct{})
	for _, k := range req.CoordinatorKeys {
		need[k] = struct{}{}
	}
	for _, c := range resp.Coordinators {
		if c.Key == "" {
			t.Errorf("response coordinator erroneously has an empty key")
			continue
		}
		delete(need, c.Key)
	}
	if len(need) > 0 {
		t.Errorf("coordinator key responses missing: %v", need)
	}
}
