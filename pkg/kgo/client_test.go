package kgo

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestMaxVersions(t *testing.T) {
	if ours, main := new(fetchRequest).MaxVersion(), new(kmsg.FetchRequest).MaxVersion(); ours != main {
		t.Errorf("our fetch request max version %d != kmsg's %d", ours, main)
	}
	if ours, main := new(produceRequest).MaxVersion(), new(kmsg.ProduceRequest).MaxVersion(); ours != main {
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
