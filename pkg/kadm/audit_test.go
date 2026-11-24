package kadm

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// An ACL filter builder that never sets the pattern or operations must
// default both to "any" (the builder's unset-means-any philosophy); it
// previously sent pattern UNKNOWN(0) -- rejected by real brokers at request
// parse -- and generated ZERO filters for unset operations, silently doing
// nothing.
func TestACLBuilderFilterDefaults(t *testing.T) {
	if _, _, err := createDelDescACL(NewACLs().Topics("a").Allow().AllowHosts().Deny().DenyHosts()); err == nil {
		t.Error("expected an error filtering with no operations")
	}

	b := NewACLs().Topics("a").Operations().Allow().AllowHosts().Deny().DenyHosts()
	dels, descs, err := createDelDescACL(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 1 || len(descs) != 1 {
		t.Fatalf("got %d deletions, %d describes, want 1 and 1", len(dels), len(descs))
	}
	if got := dels[0].ResourcePatternType; got != kmsg.ACLResourcePatternTypeLiteral {
		t.Errorf("deletion pattern %v, want LITERAL", got)
	}
	if got := descs[0].ResourcePatternType; got != kmsg.ACLResourcePatternTypeLiteral {
		t.Errorf("describe pattern %v, want LITERAL", got)
	}
	if got := dels[0].Operation; got != kmsg.ACLOperationAny {
		t.Errorf("deletion operation %v, want ANY", got)
	}
	if dels[0].ResourceName == nil || *dels[0].ResourceName != "a" {
		t.Errorf("deletion resource name %v, want a", dels[0].ResourceName)
	}
}

// Builder list setters must be last-call-wins: an empty call previously set
// a sticky any-flag that a later call with names did not clear, silently
// broadening delete filters to ALL names of that resource type.
func TestACLBuilderLastCallWins(t *testing.T) {
	b := NewACLs().Topics().Topics("a").Operations().Allow().Allow("User:x").AllowHosts().Deny().DenyHosts()
	dels, _, err := createDelDescACL(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dels {
		if d.ResourceName == nil || *d.ResourceName != "a" {
			t.Errorf("deletion resource name %v, want a (any-topic flag was sticky)", d.ResourceName)
		}
	}
	var explicitPrincipal bool
	for _, d := range dels {
		if d.Principal != nil && *d.Principal == "User:x" {
			explicitPrincipal = true
		}
		if d.PermissionType == kmsg.ACLPermissionTypeAllow && d.Principal == nil {
			t.Error("allow filter has any principal; the any-allow flag was sticky")
		}
	}
	if !explicitPrincipal {
		t.Error("no filter carries the explicitly allowed principal")
	}

	// MaybeOperations with nothing clears the operations, rather than
	// keeping the earlier OpRead.
	b = NewACLs().Topics("a").Operations(OpRead).MaybeOperations().Allow().AllowHosts()
	if _, _, err := createDelDescACL(b); err == nil {
		t.Error("expected an error filtering with no operations after MaybeOperations()")
	}
}

// VersionGuess previously dereferenced the nil response of a broker whose
// ApiVersions request failed.
func TestVersionGuessFailedBroker(t *testing.T) {
	if got := (&BrokerApiVersions{Err: errors.New("dial failed")}).VersionGuess(); got != "unknown custom version" {
		t.Errorf("got %q, want unknown custom version", got)
	}
}

func TestValidateCreateDefaults(t *testing.T) {
	b := NewACLs().Topics("a").Allow("User:x")
	if err := b.ValidateCreate(); err == nil {
		t.Error("expected an error for creating with no operations")
	}
	b.Operations(OpRead)
	if err := b.ValidateCreate(); err != nil {
		t.Errorf("unset pattern should validate (defaults to literal): %v", err)
	}
}

// shardErrEachBroker must record response-level errors from the callback:
// kgo deliberately strips retry-exhausted response errors from shard.Err so
// callers can parse the ErrorCode, and dropping what the callback found made
// broadcast list APIs return silently short results with a nil error.
func TestShardErrEachBrokerRecordsResponseErrors(t *testing.T) {
	req := kmsg.NewPtrListGroupsRequest()
	shards := []kgo.ResponseShard{{
		Meta: kgo.BrokerMetadata{NodeID: 1},
		Req:  req,
		Resp: kmsg.NewPtrListGroupsResponse(),
	}}

	err := shardErrEachBroker(req, shards, func(BrokerDetail, kmsg.Response) error {
		return kerr.CoordinatorLoadInProgress
	})
	se, ok := errors.AsType[*ShardErrors](err)
	if !ok {
		t.Fatalf("got %v, want ShardErrors", err)
	}
	if len(se.Errs) != 1 || !errors.Is(se.Errs[0].Err, kerr.CoordinatorLoadInProgress) {
		t.Fatalf("got %v, want one CoordinatorLoadInProgress shard error", se.Errs)
	}
	if !se.AllFailed {
		t.Error("AllFailed false with every shard errored")
	}

	// Auth errors still return immediately as *AuthError.
	autherr := &AuthError{Err: kerr.GroupAuthorizationFailed}
	err = shardErrEachBroker(req, shards, func(BrokerDetail, kmsg.Response) error {
		return autherr
	})
	if _, ok := errors.AsType[*AuthError](err); !ok {
		t.Fatalf("got %v, want AuthError", err)
	}
}

// mergeShardErrs must not lose a non-ShardErrors error: a first-round
// AuthError followed by an error-free rerequest round previously merged to
// nil, reporting partial results as clean.
func TestMergeShardErrs(t *testing.T) {
	ae := &AuthError{Err: kerr.GroupAuthorizationFailed}
	if got := mergeShardErrs(ae, nil); got != error(ae) {
		t.Errorf("merge(auth, nil) = %v, want the auth error", got)
	}
	se := &ShardErrors{Errs: []ShardError{{}}}
	if got := mergeShardErrs(nil, se); got != error(se) {
		t.Errorf("merge(nil, se) = %v, want the shard errors", got)
	}
	merged := mergeShardErrs(
		&ShardErrors{Errs: []ShardError{{}}},
		&ShardErrors{Errs: []ShardError{{}}},
	)
	if mse, ok := errors.AsType[*ShardErrors](merged); !ok || len(mse.Errs) != 2 {
		t.Errorf("merge(se, se) = %v, want two merged shard errors", merged)
	}
}

// Duplicate requested types must not duplicate results (the inner loop
// previously continued instead of breaking).
func TestFilterTypesNoDuplicates(t *testing.T) {
	l := ListedConfigResources{Resources: []ConfigResource{{Type: kmsg.ConfigResourceTypeTopic, Name: "a"}}}
	got := l.FilterTypes(kmsg.ConfigResourceTypeTopic, kmsg.ConfigResourceTypeTopic)
	if len(got) != 1 {
		t.Errorf("got %d results, want 1", len(got))
	}
}

// LessBySize previously fell back to Less when the receiver was larger, so a
// larger partition 0 and a smaller partition 1 were each less than the other.
func TestLessBySize(t *testing.T) {
	large := DescribedLogDirPartition{Partition: 0, Size: 100}
	small := DescribedLogDirPartition{Partition: 1, Size: 10}
	tie := DescribedLogDirPartition{Partition: 2, Size: 10}
	if !small.LessBySize(large) || large.LessBySize(small) {
		t.Error("LessBySize did not order the smaller partition first")
	}
	if !small.LessBySize(tie) || tie.LessBySize(small) {
		t.Error("LessBySize did not fall back to Less for equal sizes")
	}
}

// CreateTopicResponses.Sorted and DeleteTopicResponses.Sorted previously
// compared names whenever the left ID was not less, so a topic with a greater
// ID and a lesser name was less than the other topic in both directions, and
// the order followed map iteration. Topics share IDs in pairs, so names must
// still break ties.
func TestTopicResponsesSortedByID(t *testing.T) {
	creates, deletes := make(CreateTopicResponses), make(DeleteTopicResponses)
	for i, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		id := TopicID{15: byte(4 - i/2)}
		creates[name] = CreateTopicResponse{Topic: name, ID: id}
		deletes[name] = DeleteTopicResponse{Topic: name, ID: id}
	}
	for range 30 {
		var c, d string
		for _, r := range creates.Sorted() {
			c += r.Topic
		}
		for _, r := range deletes.Sorted() {
			d += r.Topic
		}
		if c != "ghefcdab" || d != "ghefcdab" {
			t.Fatalf("got create order %s, delete order %s, want ghefcdab", c, d)
		}
	}
}

// A partition that is neither assigned nor committed to previously had its
// lag calculated from the start offset even if listing its end offset failed:
// the errored end offset of -1 minus the start offset rounded up to a lag of
// 0, rather than the -1 that every other errored partition has.
func TestCalculateGroupLagEndOffsetErr(t *testing.T) {
	commit := OffsetResponses{"t": {
		0: {Offset: Offset{Topic: "t", Partition: 0, At: 15}},
	}}
	start := ListedOffsets{"t": {
		1: {Topic: "t", Partition: 1, Offset: 10},
		2: {Topic: "t", Partition: 2, Offset: 10},
	}}
	end := ListedOffsets{"t": {
		1: {Topic: "t", Partition: 1, Offset: 20},
		2: {Topic: "t", Partition: 2, Offset: -1, Err: kerr.LeaderNotAvailable},
	}}

	lag := CalculateGroupLagWithStartOffsets(DescribedGroup{}, commit, start, end)

	for _, test := range []struct {
		p   int32
		lag int64
		err error
	}{
		{1, 10, nil}, // not committed: end - start
		{2, -1, kerr.LeaderNotAvailable},
	} {
		got, ok := lag.Lookup("t", test.p)
		if !ok {
			t.Errorf("partition %d: missing from lag", test.p)
			continue
		}
		if got.Lag != test.lag || got.Err != test.err {
			t.Errorf("partition %d: got lag %d err %v, want lag %d err %v", test.p, got.Lag, got.Err, test.lag, test.err)
		}
	}
}
