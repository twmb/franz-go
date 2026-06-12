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
