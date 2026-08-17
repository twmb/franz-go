package kgo

import (
	"math/rand"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests from the client.go audit sweep (round 10).

// A recreated topic comes back under a new topic ID; storing the new entry
// must drop the old ID's byID mapping, else the cache accumulates stale IDs
// forever and keeps resolving IDs that no longer exist.
func TestStoreCachedMetaTopicIDChange(t *testing.T) {
	t.Parallel()
	cl := &Client{cfg: defaultCfg()}
	mkmeta := func(id byte) *kmsg.MetadataResponse {
		resp := kmsg.NewPtrMetadataResponse()
		rt := kmsg.NewMetadataResponseTopic()
		rt.Topic = kmsg.StringPtr("foo")
		rt.TopicID = [16]byte{id}
		resp.Topics = append(resp.Topics, rt)
		return resp
	}
	cl.storeCachedMeta(mkmeta(1), false, nil)
	cl.storeCachedMeta(mkmeta(2), false, nil)

	cl.metaCache.mu.Lock()
	defer cl.metaCache.mu.Unlock()
	if name, ok := cl.metaCache.byID[[16]byte{1}]; ok {
		t.Errorf("stale byID mapping for the old topic ID survived the ID change (resolves to %q)", name)
	}
	if name := cl.metaCache.byID[[16]byte{2}]; name != "foo" {
		t.Errorf("byID mapping for the new topic ID = %q, want %q", name, "foo")
	}
	if ct := cl.metaCache.topics["foo"]; ct.id != ([16]byte{2}) {
		t.Errorf("cached topic id = %v, want the new ID", ct.id)
	}
}

// New clients with many seeds used to always try seed_0 first because
// anySeedIdx's zero value directly selected loadSeeds()[0]. A fleet restarting
// at once could therefore stampede the first bootstrap broker. The starting
// cursor is now randomized per client, distributing first bootstrap attempts
// across the configured brokers while preserving their configured order.
func TestSeedBrokersStartDistributedAcrossClients(t *testing.T) {
	t.Parallel()

	seeds := []hostport{
		{host: "seed-0", port: 9092},
		{host: "seed-1", port: 9092},
		{host: "seed-2", port: 9092},
		{host: "seed-3", port: 9092},
	}

	rng := rand.New(rand.NewSource(1))
	const clients = 1000
	counts := make([]int, len(seeds))
	for range clients {
		cl := &Client{
			rng: func(fn func(*rand.Rand)) { fn(rng) },
		}

		seedBrokers := make([]*broker, 0, len(seeds))
		for i, hp := range seeds {
			seedBrokers = append(seedBrokers, cl.newBroker(unknownSeedID(i), hp.host, hp.port, nil))
		}
		cl.anySeedIdx = cl.randomSeedIdx(len(seedBrokers))
		cl.seeds.Store(seedBrokers)

		got := cl.broker().meta.NodeID
		found := false
		for i := range seeds {
			if got == unknownSeedID(i) {
				counts[i]++
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("first selected seed broker = %s, want one of the configured seeds", NodeName(got))
		}
	}

	want := clients / len(seeds)
	// With 1000 clients and 4 seeds, each seed should be picked about 250
	// times. 75 allows range of accepted values while still catching the old
	// behavior, where seed_0 would be picked 1000 times.
	const maxSkew = 75
	for i, got := range counts {
		if got < want-maxSkew || got > want+maxSkew {
			t.Fatalf("seed %s was selected first %d times out of %d clients, want roughly %d; counts=%v",
				NodeName(unknownSeedID(i)), got, clients, want, counts)
		}
	}
}

// OptValue(TransactionalID) must return the string input (like ClientID and
// InstanceID, and as documented), not the internal *string; the Share group
// options must be present in the switch at all.
func TestOptValuesTxnIDAndShare(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:1"), // never successfully dialed
		TransactionalID("txid"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if v := cl.OptValue(TransactionalID); v != "txid" {
		t.Errorf("OptValue(TransactionalID) = %v (%T), want the string %q", v, v, "txid")
	}
	if vs := cl.OptValues(TransactionalID); len(vs) != 2 || vs[0] != "txid" || vs[1] != any(true) {
		t.Errorf("OptValues(TransactionalID) = %v, want [txid true]", vs)
	}

	shcl, err := NewClient(
		SeedBrokers("127.0.0.1:1"),
		ShareGroup("sg"),
		ConsumeTopics("t"),
		ShareMaxRecords(10),
		ShareMaxRecordsStrict(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer shcl.Close()
	if v := shcl.OptValue(ShareGroup); v != "sg" {
		t.Errorf("OptValue(ShareGroup) = %v, want %q", v, "sg")
	}
	if v := shcl.OptValue(ShareMaxRecords); v != int32(10) {
		t.Errorf("OptValue(ShareMaxRecords) = %v (%T), want int32(10)", v, v)
	}
	if v := shcl.OptValue(ShareMaxRecordsStrict); v != any(true) {
		t.Errorf("OptValue(ShareMaxRecordsStrict) = %v, want true", v)
	}
	if vs := shcl.OptValues(ShareAckCallback); vs == nil {
		t.Errorf("OptValues(ShareAckCallback) = nil; the option exists and must be returned")
	}
}
