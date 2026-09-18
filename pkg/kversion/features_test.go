package kversion

import (
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// featureString renders a version's features one per line, "name min-max
// finalized", so a test can pin a whole table in one string.
func featureString(vs *Versions) string {
	var sb strings.Builder
	finalized := make(map[string]int16)
	vs.EachFinalizedFeature(func(name string, level int16) { finalized[name] = level })
	vs.EachSupportedFeature(func(name string, min, max int16) {
		fmt.Fprintf(&sb, "%s %d-%d %d\n", name, min, max, finalized[name])
		delete(finalized, name)
	})
	for name, level := range finalized {
		fmt.Fprintf(&sb, "%s finalized only %d\n", name, level)
	}
	return sb.String()
}

func TestFeatures(t *testing.T) {
	for _, test := range []struct {
		name string
		vs   *Versions
		exp  string
	}{
		{"v3.2", V3_2_0(), ""},
		{"v3.3", V3_3_0(), "metadata.version 1-7 7\n"},
		{"v3.6", V3_6_0(), "metadata.version 1-14 14\n"},
		{"v3.8", V3_8_0(), "metadata.version 1-20 20\n"},
		{"v3.9", V3_9_0(), "kraft.version 0-1 1\nmetadata.version 1-21 21\n"},
		{"v4.0", V4_0_0(), "" +
			"eligible.leader.replicas.version 0-1 0\n" +
			"group.version 0-1 1\n" +
			"kraft.version 0-1 1\n" +
			"metadata.version 7-25 25\n" +
			"transaction.version 0-2 2\n"},
		{"v4.1", V4_1_0(), "" +
			"eligible.leader.replicas.version 0-1 1\n" +
			"group.version 0-1 1\n" +
			"kraft.version 0-1 1\n" +
			"metadata.version 7-27 27\n" +
			"share.version 0-1 0\n" +
			"transaction.version 0-2 2\n"},
		{"v4.2", V4_2_0(), "" +
			"eligible.leader.replicas.version 0-1 1\n" +
			"group.version 0-1 1\n" +
			"kraft.version 0-1 1\n" +
			"metadata.version 7-29 29\n" +
			"share.version 0-1 1\n" +
			"streams.version 0-1 1\n" +
			"transaction.version 0-2 2\n"},
		{"v4.4", V4_4_0(), "" +
			"eligible.leader.replicas.version 0-1 1\n" +
			"group.version 0-1 1\n" +
			"kraft.version 0-1 1\n" +
			"metadata.version 7-33 33\n" +
			"share.version 0-2 2\n" +
			"streams.version 0-1 1\n" +
			"transaction.version 0-2 2\n"},
		{"stable", Stable(), featureString(V4_4_0())},
		{"tip", Tip(), featureString(V4_4_0())},
		{"from string 4.1", FromString("4.1"), featureString(V4_1_0())},
		{"from string v3.5", FromString("v3.5.2"), featureString(V3_5_0())},
		{"from string 2.8", FromString("2.8"), ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := featureString(test.vs); got != test.exp {
				t.Errorf("got:\n%sexp:\n%s", got, test.exp)
			}
		})
	}
}

func TestFeaturesFromApiVersionsResponse(t *testing.T) {
	mkresp := func(epoch int64) *kmsg.ApiVersionsResponse {
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.Version = 3
		key := kmsg.NewApiVersionsResponseApiKey()
		key.ApiKey, key.MinVersion, key.MaxVersion = 0, 0, 12
		resp.ApiKeys = append(resp.ApiKeys, key)
		for _, f := range []struct {
			name     string
			min, max int16
		}{
			{"transaction.version", 0, 2},
			{"metadata.version", 7, 25},
		} {
			sf := kmsg.NewApiVersionsResponseSupportedFeature()
			sf.Name, sf.MinVersion, sf.MaxVersion = f.name, f.min, f.max
			resp.SupportedFeatures = append(resp.SupportedFeatures, sf)
		}
		resp.FinalizedFeaturesEpoch = epoch
		for _, f := range []struct {
			name  string
			level int16
		}{
			{"metadata.version", 22},
			{"transaction.version", 1},
		} {
			ff := kmsg.NewApiVersionsResponseFinalizedFeature()
			ff.Name, ff.MinVersionLevel, ff.MaxVersionLevel = f.name, f.level, f.level
			resp.FinalizedFeatures = append(resp.FinalizedFeatures, ff)
		}
		return resp
	}

	for _, test := range []struct {
		name  string
		epoch int64
		exp   string
	}{
		{"epoch 5", 5, "metadata.version 7-25 22\ntransaction.version 0-2 1\n"},
		{"epoch 0", 0, "metadata.version 7-25 22\ntransaction.version 0-2 1\n"},
		{"epoch -1", -1, "metadata.version 7-25 0\ntransaction.version 0-2 0\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			vs := FromApiVersionsResponse(mkresp(test.epoch))
			if got := featureString(vs); got != test.exp {
				t.Errorf("got:\n%sexp:\n%s", got, test.exp)
			}
			if v, ok := vs.LookupMaxKeyVersion(0); !ok || v != 12 {
				t.Errorf("produce max: got %d, %v; exp 12, true", v, ok)
			}
		})
	}

	// A v2 response, and one from a raw struct, carry no features.
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.Version = 2
	if got := featureString(FromApiVersionsResponse(resp)); got != "" {
		t.Errorf("v2 response: got features %q", got)
	}
	if got := featureString(FromApiVersionsResponse(new(kmsg.ApiVersionsResponse))); got != "" {
		t.Errorf("zero response: got features %q", got)
	}
}

func TestFeatureLevelDescription(t *testing.T) {
	for _, test := range []struct {
		name  string
		level int16
		exp   string
	}{
		{"metadata.version", 27, "4.1-IV1: replica fetcher sends Fetch v18 (KIP-1166)"},
		{"metadata.version", 0, ""},
		{"metadata.version", 35, ""},
		{"metadata.version", -1, ""},
		{"share.version", 2, "dead letter queue for share groups (KIP-1191)"},
		{"share.version", 3, ""},
		{"kraft.version", 0, "the original KRaft quorum"},
		{"unknown.version", 1, ""},
	} {
		if got := FeatureLevelDescription(test.name, test.level); got != test.exp {
			t.Errorf("%s %d: got %q != exp %q", test.name, test.level, got, test.exp)
		}
	}

	// Every level a release's table reaches has a description.
	V4_4_0().EachSupportedFeature(func(name string, min, max int16) {
		for level := min; level <= max; level++ {
			if FeatureLevelDescription(name, level) == "" {
				t.Errorf("%s %d has no description", name, level)
			}
		}
	})
}
