package kgo

import (
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
