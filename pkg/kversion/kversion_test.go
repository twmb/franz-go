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
	for i, v := range vs.k2v {
		if int16(i) != v {
			t.Errorf("set incorrect: at %d got %d != exp %d", i, v, i)
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
		if got, exp := v.VersionGuess(), "unknown custom version at least v0.8.0"; got != exp {
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
		v.SetMaxKeyVersion(int16(len(v.k2v)+1), -1)
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
	l.SetMaxKeyVersion(int16(len(l.k2v)+1), -1)

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
