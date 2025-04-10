package kversion

import (
	"math"
	"testing"
)

func TestSetMaxKeyVersion(t *testing.T) {
	var vs Versions
	for i := int16(0); i < math.MaxInt16; i++ {
		vs.SetMaxKeyVersion(i, i)
	}
	for i, req := range vs.reqs {
		if i != req.vmax {
			t.Errorf("set incorrect: at %d got %d != exp %d", i, req.vmax, i)
		}
	}
}

func TestVersionGuess(t *testing.T) {
	// Cases where last can be empty.
	{
		v := V0_8_0()
		if got, exp := v.VersionGuess(), "v0.8.0"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
		v.SetMaxKeyVersion(0, -1)
		if got, exp := v.VersionGuess(), "not even v0.8.0"; got != exp {
			t.Errorf("got %s != exp %s unsetting produce", got, exp)
		}
		v.SetMaxKeyVersion(0, 100)
		if got, exp := v.VersionGuess(), "between v0.8.0 and v0.8.1"; got != exp {
			t.Errorf("got %s != exp %s maxing produce", got, exp)
		}
		v.SetMaxKeyVersion(1, -1)
		if got, exp := v.VersionGuess(), "unknown custom version"; got != exp {
			t.Errorf("got %s != exp %s maxing produce and unsetting fetch", got, exp)
		}
	}

	// In between and into the next version.
	{
		v := V0_9_0()
		if got, exp := v.VersionGuess(), "v0.9.0"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
		v.SetMaxKeyVersion(17, 0)
		if got, exp := v.VersionGuess(), "between v0.9.0 and v0.10.0"; got != exp {
			t.Errorf("got %s != exp %s setting sasl handshake to 0", got, exp)
		}
		v.SetMaxKeyVersion(0, 2)
		v.SetMaxKeyVersion(1, 2)
		v.SetMaxKeyVersion(3, 1)
		v.SetMaxKeyVersion(6, 2)
		v.SetMaxKeyVersion(18, 0)
		if got, exp := v.VersionGuess(), "v0.10.0"; got != exp {
			t.Errorf("got %s != exp %s setting api versions to 0", got, exp)
		}
	}

	// This hits the case where versions are -1.
	{
		v := V2_7_0()
		v.SetMaxKeyVersion(100, -1)
		if got, exp := v.VersionGuess(), "v2.7"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
	}

	{ // Here, we ensure we skip 4, 5, 6, and 7 by default.
		v := V2_7_0()
		v.SetMaxKeyVersion(4, -1)
		v.SetMaxKeyVersion(5, -1)
		v.SetMaxKeyVersion(6, -1)
		v.SetMaxKeyVersion(7, -1)

		if got, exp := v.VersionGuess(), "v2.7"; got != exp {
			t.Errorf("got %s != exp %s for v2.7 with 4,5,6,7 unset", got, exp)
		}

		if got, exp := v.VersionGuess(SkipKeys()), "unknown custom version"; got != exp {
			t.Errorf("got %s != exp %s for v2.7 with 4,5,6,7 unset without skipping them", got, exp)
		}
	}
}

func TestEqual(t *testing.T) {
	l := V2_7_0()
	l.SetMaxKeyVersion(100, -1)

	r := V2_7_0()

	if !l.Equal(r) {
		t.Errorf("unexpectedly not equal")
	}

	l.SetMaxKeyVersion(0, -1)
	if l.Equal(r) {
		t.Errorf("unexpectedly equal after unsetting produce in left")
	}

	r.SetMaxKeyVersion(0, -1)
	if !l.Equal(r) {
		t.Errorf("unexpectedly not equal after unsetting produce in both")
	}

	l = V0_8_0()
	r = V0_8_1()
	if l.Equal(r) {
		t.Errorf("unexpectedly equal v0.8.0 to v0.8.1")
	}

	r.SetMaxKeyVersion(8, -1)
	r.SetMaxKeyVersion(9, -1)
	if !l.Equal(r) {
		t.Errorf("unexpectedly not equal after backing v0.8.1 down to v0.8.0")
	}
	if !r.Equal(l) {
		t.Errorf("unexpectedly not equal after backing v0.8.1 down to v0.8.0, opposite direction")
	}
}

func TestVersionProbeKafka3_1(t *testing.T) {
	versions := map[int16]int16{
		0:  9,  // Produce
		1:  13, // Fetch
		2:  7,  // ListOffsets
		3:  12, // Metadata
		4:  5,  // LeaderAndISR
		5:  3,  // StopReplica
		6:  7,  // UpdateMetadata
		7:  3,  // ControlledShutdown
		8:  8,  // OffsetCommit
		9:  8,  // OffsetFetch
		10: 4,  // FindCoordinator
		11: 7,  // JoinGroup
		12: 4,  // Heartbeat
		13: 4,  // LeaveGroup
		14: 5,  // SyncGroup
		15: 5,  // DescribeGroups
		16: 4,  // ListGroups
		17: 1,  // SASLHandshake
		18: 3,  // ApiVersions
		19: 7,  // CreateTopics
		20: 6,  // DeleteTopics
		21: 2,  // DeleteRecords
		22: 4,  // InitProducerID
		23: 4,  // OffsetForLeaderEpoch
		24: 3,  // AddPartitionsToTxn
		25: 3,  // AddOffsetsToTxn
		26: 3,  // EndTxn
		27: 1,  // WriteTxnMarkers
		28: 3,  // TxnOffsetCommit
		29: 2,  // DescribeACLs
		30: 2,  // CreateACLs
		31: 2,  // DeleteACLs
		32: 4,  // DescribeConfigs
		33: 2,  // AlterConfigs
		34: 2,  // AlterReplicaLogDirs
		35: 2,  // DescribeLogDirs
		36: 2,  // SASLAuthenticate
		37: 3,  // CreatePartitions
		38: 2,  // CreateDelegationToken
		39: 2,  // RenewDelegationToken
		40: 2,  // ExpireDelegationToken
		41: 2,  // DescribeDelegationToken
		42: 2,  // DeleteGroups
		43: 2,  // ElectLeaders
		44: 1,  // IncrementalAlterConfigs
		45: 0,  // AlterPartitionAssignments
		46: 0,  // ListPartitionReassignments
		47: 0,  // OffsetDelete
		48: 1,  // DescribeClientQuotas
		49: 1,  // AlterClientQuotas
		50: 0,  // DescribeUserSCRAMCredentials
		51: 0,  // AlterUserSCRAMCredentials
		56: 0,  // AlterPartition
		57: 0,  // UpdateFeatures
		60: 0,  // DescribeCluster
		61: 0,  // DescribeProducers
		65: 0,  // DescribeTransactions
		66: 0,  // ListTransactions
		67: 0,  // AllocateProducerIDs
	}

	var vs Versions
	for k, v := range versions {
		vs.SetMaxKeyVersion(k, v)
	}
	if guess := vs.VersionGuess(); guess != "v3.1" {
		t.Errorf("unexpected version guess, got %s != exp %s", guess, "v3.1")
	}
}

func TestFromString(t *testing.T) {
	for _, test := range []struct {
		in, out string
	}{
		{"v0.8.0", "v0.8.0"},
		{"v0.8.1.0", "v0.8.1"},
		{"v2.1", "v2.1"},
		{"v2.1.3", "v2.1"},
		{"v3.1", "v3.1"},
		{"3.1", "v3.1"},

		{"v0.7.0", ""},     // too low
		{"v999.9", ""},     // too high
		{"v3", ""},         // not enough digits
		{"v0.8.1.0.3", ""}, // too many digits for v0
		{"v3.1.0.3", ""},   // too many digits for v1+
	} {
		v := FromString(test.in)
		if v == nil {
			if test.out != "" {
				t.Errorf("unexpectedly got nil for %s", test.in)
			}
			continue
		}
		out := v.VersionGuess()
		if out != test.out {
			t.Errorf("got %s != exp %s for %s", out, test.out, test.in)
		}
	}
}

func TestGuessVersions(t *testing.T) {
	for _, test := range []struct {
		vs  *Versions
		exp string
	}{
		{V0_8_0(), "v0.8.0"},
		{V0_8_1(), "v0.8.1"},
		{V0_8_2(), "v0.8.2"},
		{V0_9_0(), "v0.9.0"},
		{V0_10_0(), "v0.10.0"},
		{V0_10_1(), "v0.10.1"},
		{V0_10_2(), "v0.10.2"},
		{V0_11_0(), "v0.11.0"},
		{V1_0_0(), "v1.0"},
		{V1_1_0(), "v1.1"},
		{V2_0_0(), "v2.0"},
		{V2_1_0(), "v2.1"},
		{V2_2_0(), "v2.2"},
		{V2_3_0(), "v2.3"},
		{V2_4_0(), "v2.4"},
		{V2_5_0(), "v2.5"},
		{V2_6_0(), "v2.6"},
		{V2_7_0(), "v2.7"},
		{V2_8_0(), "v2.8"},
		{V3_0_0(), "v3.0"},
		{V3_1_0(), "v3.1"},
		{V3_2_0(), "v3.2"},
		{V3_3_0(), "v3.4"}, // v3.3 has no zookeeper differences from 3.4, and we prefer detecting higher
		{V3_4_0(), "v3.4"},
		{V3_5_0(), "v3.5"},
		{V3_6_0(), "v3.6"},
		{V3_7_0(), "v3.7"},
		{V3_8_0(), "v3.8"},
		{V3_9_0(), "v3.9"},
		{V4_0_0(), "v4.0"},

		// Stable is zk, controller, broker merged; we do not guess the
		// stable version, but we do want to ensure it is at least our
		// latest release
		{Stable(), "at least v4.0"},
	} {
		got := test.vs.VersionGuess()
		if got != test.exp {
			t.Errorf("got %s != exp %s", got, test.exp)
		}
	}
}
