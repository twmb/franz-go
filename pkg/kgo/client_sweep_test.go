package kgo

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests for client.go internals.

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
	cl.storeCachedMeta(mkreq("foo"), mkmeta(1), true, nil)
	cl.storeCachedMeta(mkreq("foo"), mkmeta(2), true, nil)

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

// Every exported option constructor in config.go must have a case in
// OptValues; a new option is easy to add without touching the switch.
func TestOptValuesCoversEveryOption(t *testing.T) {
	t.Parallel()

	f, err := parser.ParseFile(token.NewFileSet(), "config.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := NewClient(SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	var n int
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !fn.Name.IsExported() || fn.Type.Results == nil || len(fn.Type.Results.List) != 1 {
			continue
		}
		ret, ok := fn.Type.Results.List[0].Type.(*ast.Ident)
		if !ok {
			continue
		}
		switch ret.Name {
		case "Opt", "ProducerOpt", "ConsumerOpt", "GroupOpt":
		default:
			continue
		}
		n++
		if cl.OptValues(fn.Name.Name) == nil {
			t.Errorf("OptValues(%q) = nil; the option exists and must be returned", fn.Name.Name)
		}
	}
	if n == 0 {
		t.Fatal("found no option constructors in config.go")
	}
}

// mkreq returns a metadata request for the given topics.
func mkreq(topics ...string) *kmsg.MetadataRequest {
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	for _, topic := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, rt)
	}
	return req
}

// A brokers-only metadata response replaces cl.brokers and leaves the topic
// cache alone. RequestCachedMetadata must not pair those newer brokers with
// the cached leaders: that response never existed, and the leader is missing
// from Brokers.
func TestRequestCachedMetadataBrokersFromSameResponse(t *testing.T) {
	t.Parallel()
	cl := &Client{cfg: defaultCfg()}

	rack := "r"
	meta := kmsg.NewPtrMetadataResponse()
	for _, id := range []int32{1, 2, 3} {
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID = id
		b.Rack = &rack
		meta.Brokers = append(meta.Brokers, b)
	}
	meta.ControllerID = 3
	meta.ClusterID = kmsg.StringPtr("c")
	rt := kmsg.NewMetadataResponseTopic()
	rt.Topic = kmsg.StringPtr("foo")
	rp := kmsg.NewMetadataResponseTopicPartition()
	rp.Leader = 3
	rt.Partitions = append(rt.Partitions, rp)
	meta.Topics = append(meta.Topics, rt)

	all := kmsg.NewPtrMetadataRequest() // nil Topics: all topics
	cl.storeCachedMeta(all, meta, true, nil)

	// Broker 3 has since left. The connection table, the controller the
	// client dials, and the cluster id it holds are all from a later
	// response. The topic cache still says 3 leads foo.
	cl.brokers = []*broker{
		{meta: BrokerMetadata{NodeID: 1}},
		{meta: BrokerMetadata{NodeID: 2}},
	}
	cl.controllerID = 1
	live := "live"
	cl.clusterID.Store(&live)

	ctx := context.Background()
	resp, err := cl.RequestCachedMetadata(ctx, all, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int32
	for _, b := range resp.Brokers {
		ids = append(ids, b.NodeID)
	}
	if !slices.Equal(ids, []int32{1, 2, 3}) {
		t.Errorf("cached brokers = %v, want [1 2 3]", ids)
	}
	if resp.ControllerID != 3 {
		t.Errorf("cached controller = %d, want 3", resp.ControllerID)
	}
	if resp.ClusterID == nil || *resp.ClusterID != "c" {
		t.Errorf("cached cluster id = %v, want %q", resp.ClusterID, "c")
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].Leader != 3 {
		t.Fatalf("cached topics = %+v, want foo led by 3", resp.Topics)
	}
	// dups must not hand back the snapshot's Rack, nor the one on the
	// response the snapshot was built from. A caller writing through it
	// would change the cache.
	if resp.Brokers[0].Rack == &rack || resp.Brokers[0].Rack == meta.Brokers[0].Rack {
		t.Error("returned Rack aliases internal state")
	}

	// A topic-bearing response that reports no controller returns -1,
	// not the id the client still dials. A request with no topics keeps
	// that dial id, since it has no snapshot to take one from.
	unknown := kmsg.NewPtrMetadataResponse()
	unknown.ControllerID = -1
	ub := kmsg.NewMetadataResponseBroker()
	ub.NodeID = 1
	unknown.Brokers = append(unknown.Brokers, ub)
	ut := kmsg.NewMetadataResponseTopic()
	ut.Topic = kmsg.StringPtr("foo")
	up := kmsg.NewMetadataResponseTopicPartition()
	up.Leader = 1
	ut.Partitions = append(ut.Partitions, up)
	unknown.Topics = append(unknown.Topics, ut)
	cl.controllerID = 5
	cl.storeCachedMeta(all, unknown, true, nil)

	resp, err = cl.RequestCachedMetadata(ctx, all, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ControllerID != -1 {
		t.Errorf("cached controller = %d, want -1 from the response", resp.ControllerID)
	}
	resp, err = cl.RequestCachedMetadata(ctx, mkreq(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ControllerID != 5 {
		t.Errorf("no-topics controller = %d, want the live 5", resp.ControllerID)
	}
}

// We evict cached topics only from a request that says something about what
// we want cached. A broker only request asks for no topics, and internal
// targeted fetches (topic IDs, unknown produce topics, the group's topics)
// ask only about their own, so neither can drop what else we have.
func TestStoreCachedMetaEviction(t *testing.T) {
	t.Parallel()

	foo := func() *kmsg.MetadataResponse {
		resp := kmsg.NewPtrMetadataResponse()
		rt := kmsg.NewMetadataResponseTopic()
		rt.Topic = kmsg.StringPtr("foo")
		resp.Topics = append(resp.Topics, rt)
		return resp
	}

	for _, test := range []struct {
		name  string
		req   *kmsg.MetadataRequest
		prune bool
		exp   bool // whether foo survives
	}{
		{"broker only request", mkreq(), true, true},
		{"targeted fetch of another topic", mkreq("bar"), false, true},
		{"interested request for another topic", mkreq("bar"), true, false},
	} {
		cl := &Client{cfg: defaultCfg()}
		cl.storeCachedMeta(mkreq("foo"), foo(), true, nil)

		// Age the entry past metadataMinAge; a fresh entry is never
		// pruned.
		cl.metaCache.mu.Lock()
		ct := cl.metaCache.topics["foo"]
		ct.when = time.Now().Add(-time.Hour)
		cl.metaCache.topics["foo"] = ct
		cl.metaCache.mu.Unlock()

		bar := kmsg.NewPtrMetadataResponse()
		if len(test.req.Topics) > 0 {
			rt := kmsg.NewMetadataResponseTopic()
			rt.Topic = kmsg.StringPtr("bar")
			bar.Topics = append(bar.Topics, rt)
		}
		cl.storeCachedMeta(test.req, bar, test.prune, nil)

		cl.metaCache.mu.Lock()
		_, got := cl.metaCache.topics["foo"]
		cl.metaCache.mu.Unlock()
		if got != test.exp {
			t.Errorf("%s: foo cached = %v, expected %v", test.name, got, test.exp)
		}
	}
}
