package kadm

import (
	"context"
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
	b := NewACLs().Topics("a").Allow().AllowHosts().Deny().DenyHosts()
	dels, descs, err := createDelDescACL(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 1 || len(descs) != 1 {
		t.Fatalf("got %d deletions, %d describes, want 1 and 1", len(dels), len(descs))
	}
	if got := dels[0].ResourcePatternType; got != kmsg.ACLResourcePatternTypeAny {
		t.Errorf("deletion pattern %v, want ANY", got)
	}
	if got := descs[0].ResourcePatternType; got != kmsg.ACLResourcePatternTypeAny {
		t.Errorf("describe pattern %v, want ANY", got)
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
	b := NewACLs().Topics().Topics("a").Allow().Allow("User:x").AllowHosts().Deny().DenyHosts()
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
	var se *ShardErrors
	if !errors.As(err, &se) {
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
	var ae *AuthError
	if !errors.As(err, &ae) {
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
	var mse *ShardErrors
	if !errors.As(merged, &mse) || len(mse.Errs) != 2 {
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

// FetchOffsetsForTopics previously filtered the caller's variadic slice in
// place, rewriting its backing array.
func TestFetchOffsetsForTopicsDoesNotMutateInput(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := NewClient(cl)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	topics := []string{FetchAllGroupTopics, "a", "b"}
	adm.FetchOffsetsForTopics(ctx, "g", topics...) //nolint:errcheck // the canceled ctx fails the call; we assert only non-mutation
	if topics[0] != FetchAllGroupTopics || topics[1] != "a" || topics[2] != "b" {
		t.Errorf("caller slice was mutated: %v", topics)
	}
}
