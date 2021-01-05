// Package kversion specifies max versions for Kafka request keys.
//
// Kafka technically has internal broker versions that bump multiple times per
// release. This package only defines releases and tip.
package kversion

import (
	"bytes"
	"fmt"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Versions is a list of versions, with each item corresponding to a Kafka key
// and each item's value corresponding to the max version supported.
type Versions struct {
	// If any version is -1, then it is left out in that version.
	// This was first done in version 2.7.0, where Kafka added support
	// for 52, 53, 54, 55, but it was not a part of the 2.7.0 release,
	// so ApiVersionsResponse goes from 51 to 56.
	k2v []int16
}

// HasKey returns true if the versions contains the given key.
func (vs Versions) HasKey(k int16) bool {
	_, has := vs.LookupVersion(k)
	return has
}

// LookupVersion returns the version for the given key and whether the key
// exists. If the key does not exist, this returns (-1, false).
func (vs Versions) LookupVersion(k int16) (int16, bool) {
	if k < 0 {
		return -1, false
	}
	if int(k) >= len(vs.k2v) {
		return -1, false
	}
	version := vs.k2v[k]
	if version < 0 {
		return -1, false
	}
	return version, true
}

// Returns whether two versions are equal.
func (vs Versions) Equal(other Versions) bool {
	// We allow the version slices to be of different lengths, so long as
	// the versions for keys in one and not the other are -1.
	//
	// Basically, all non-negative-one keys must be equal.
	long, short := vs.k2v, other.k2v
	if len(short) > len(long) {
		long, short = short, long
	}
	for i, v := range short {
		if v != long[i] {
			return false
		}
	}
	for _, v := range long[len(short):] {
		if v >= 0 {
			return false
		}
	}
	return true
}

// SetKeyVersion sets the version for the given key.
//
// Setting a version to -1 unsets the key.
//
// Versions are backed by a slice; if the slice is not long enough, it is
// extended to fit the key.
func (vs *Versions) SetKeyVersion(k, v int16) {
	if k < 0 {
		return
	}
	needLen := int(k + 1)
	for len(vs.k2v) < needLen {
		vs.k2v = append(vs.k2v, -1)
	}
	vs.k2v[k] = v
}

func (vs Versions) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	for k, v := range vs.k2v {
		if v < 0 {
			continue
		}
		fmt.Fprintf(w, "%s\t%d\n", kmsg.NameForKey(int16(k)), v)
	}
	w.Flush()
	return buf.String()
}

func V0_8_0() Versions {
	v := []int16{
		0, // 0 produce
		0, // 1 fetch
		0, // 2 list offset
		0, // 3 metadata
		0, // 4 leader and isr
		0, // 5 stop replica
		0, // 6 update metadata, actually not supported for a bit
		0, // 7 controlled shutdown, actually not supported for a bit
	}
	return Versions{v}
}

func V0_8_1() Versions {
	v := V0_8_0()
	v.k2v = append(v.k2v,
		0, // 8 offset commit KAFKA-965 db37ed0054
		0, // 9 offset fetch (same)
	)
	return v
}

func V0_8_2() Versions {
	v := V0_8_1()
	v.k2v[8]++ // 1 offset commit KAFKA-1462
	v.k2v[9]++ // 1 offset fetch KAFKA-1841 161b1aa16e I think?
	v.k2v = append(v.k2v,
		0, // 10 find coordinator KAFKA-1012 a670537aa3
		0, // 11 join group (same)
		0, // 12 heartbeat (same)
	)
	return v
}

func V0_9_0() Versions {
	v := V0_8_2()
	v.k2v[0]++ // 1 produce KAFKA-2136 436b7ddc38; KAFKA-2083 ?? KIP-13
	v.k2v[1]++ // 1 fetch (same)
	v.k2v[6]++ // 1 update metadata KAFKA-2411 d02ca36ca1
	v.k2v[7]++ // 1 controlled shutdown (same)
	v.k2v[8]++ // 2 offset commit KAFKA-1634
	v.k2v = append(v.k2v,
		0, // 13 leave group KAFKA-2397 636e14a991
		0, // 14 sync group KAFKA-2464 86eb74d923
		0, // 15 describe groups KAFKA-2687 596c203af1
		0, // 16 list groups KAFKA-2687 596c203af1
	)
	return v
}

func V0_10_0() Versions {
	v := V0_9_0()
	v.k2v[0]++ // 2 produce KAFKA-3025 45c8195fa1 KIP-31 KIP-32
	v.k2v[1]++ // 2 fetch (same)
	v.k2v[3]++ // 1 metadata KAFKA-3306 33d745e2dc
	v.k2v[6]++ // 2 update metadata KAFKA-1215 951e30adc6
	v.k2v = append(v.k2v,
		0, // 17 sasl handshake KAFKA-3149 5b375d7bf9
		0, // 18 api versions KAFKA-3307 8407dac6ee
	)
	return v
}

func V0_10_1() Versions {
	v := V0_10_0()
	v.k2v[1]++  // 3 fetch KAFKA-2063 d04b0998c0 KIP-74
	v.k2v[2]++  // 1 list offset KAFKA-4148 eaaa433fc9 KIP-79
	v.k2v[3]++  // 2 metadata KAFKA-4093 ecc1fb10fa KIP-78
	v.k2v[11]++ // 1 join group KAFKA-3888 40b1dd3f49 KIP-62
	v.k2v = append(v.k2v,
		0, // 19 create topics KAFKA-2945 fc47b9fa6b
		0, // 20 delete topics KAFKA-2946 539633ba0e
	)
	return v
}

func V0_10_2() Versions {
	v := V0_10_1()
	v.k2v[6]++  // 3 update metadata KAFKA-4565 d25671884b KIP-103
	v.k2v[19]++ // 1 create topics KAFKA-4591 da57bc27e7 KIP-108
	return v
}

func V0_11_0() Versions {
	v := V0_10_2()
	v.k2v[0]++  // 3 produce KAFKA-4816 5bd06f1d54 KIP-98
	v.k2v[1]++  // 4 fetch (same)
	v.k2v[1]++  // 5 fetch KAFKA-4586 8b05ad406d KIP-107
	v.k2v[3]++  // 4 metadata KAFKA-5291 7311dcbc53 (3 below)
	v.k2v[9]++  // 3 offset fetch KAFKA-3853 c2d9b95f36 KIP-98
	v.k2v[10]++ // 1 find coordinator KAFKA-5043 d0e7c6b930 KIP-98
	v.k2v = append(v.k2v,
		0, // 21 delete records KAFKA-4586 see above
		0, // 22 init producer id KAFKA-4817 bdf4cba047 KIP-98
		0, // 23 offsets for leader epoch KAFKA-1211 0baea2ac13 KIP-101
		0, // 24 add partitions to txn KAFKA-4990 865d82af2c KIP-98
		0, // 25 add offsets to txn (same)
		0, // 26 end txn (same)
		0, // 27 write txn markers (same)
		0, // 28 txn offset commit (same)
		0, // 29 describe acls KAFKA-3266 9815e18fef KIP-140
		0, // 30 create acls (same)
		0, // 31 delete acls (same)
		0, // 32 describe configs KAFKA-3267 972b754536 KIP-133
		0, // 33 alter configs (same)
	)

	// KAFKA-4954 0104b657a1 KIP-124
	v.k2v[2]++  // 2 list offset (reused in e71dce89c0 KIP-98)
	v.k2v[3]++  // 3 metadata
	v.k2v[8]++  // 3 offset commit
	v.k2v[9]++  // 3 offset fetch
	v.k2v[11]++ // 2 join group
	v.k2v[12]++ // 1 heartbeat
	v.k2v[13]++ // 1 leave group
	v.k2v[14]++ // 1 sync group
	v.k2v[15]++ // 1 describe groups
	v.k2v[16]++ // 1 list group
	v.k2v[18]++ // 1 api versions
	v.k2v[19]++ // 2 create topics
	v.k2v[20]++ // 1 delete topics

	return v
}

func V1_0_0() Versions {
	v := V0_11_0()
	v.k2v[0]++ // 4 produce KAFKA-4763 fc93fb4b61 KIP-112
	v.k2v[1]++ // 6 fetch (same)
	v.k2v[3]++ // 5 metadata (same)
	v.k2v[4]++ // 1 leader and isr (same)
	v.k2v[6]++ // 4 update metadata (same)

	v.k2v[0]++  // 5 produce KAFKA-5793 94692288be
	v.k2v[17]++ // 1 sasl handshake KAFKA-4764 8fca432223 KIP-152

	v.k2v = append(v.k2v,
		0, // 34 alter replica log dirs KAFKA-5694 adefc8ea07 KIP-113
		0, // 35 describe log dirs (same)
		0, // 36 sasl authenticate KAFKA-4764 (see above)
		0, // 37 create partitions KAFKA-5856 5f6393f9b1 KIP-195
	)

	return v
}

func V1_1_0() Versions {
	v := V1_0_0()
	v.k2v = append(v.k2v,
		0, // 38 create delegation token KAFKA-4541 27a8d0f9e7 under KAFKA-1696 KIP-48
		0, // 39 renew delegation token (same)
		0, // 40 expire delegation token (same)
		0, // 41 describe delegation token (same)
		0, // 42 delete groups KAFKA-6275 1ed6da7cc8 KIP-229
	)

	v.k2v[1]++  // 7 fetch KAFKA-6254 7fe1c2b3d3 KIP-227
	v.k2v[32]++ // 1 describe configs KAFKA-6241 b814a16b96 KIP-226

	return v
}

func V2_0_0() Versions {
	v := V1_1_0()
	v.k2v[0]++  // 6 produce KAFKA-6028 1facab387f KIP-219
	v.k2v[1]++  // 8 fetch (same)
	v.k2v[2]++  // 3 list offset (same)
	v.k2v[3]++  // 6 metadata (same)
	v.k2v[8]++  // 4 offset commit (same)
	v.k2v[9]++  // 4 offset fetch (same)
	v.k2v[10]++ // 2 find coordinator (same)
	v.k2v[11]++ // 3 join group (same)
	v.k2v[12]++ // 2 heartbeat (same)
	v.k2v[13]++ // 2 leave group (same)
	v.k2v[14]++ // 2 sync group (same)
	v.k2v[15]++ // 2 describe groups (same)
	v.k2v[16]++ // 2 list group (same)
	v.k2v[18]++ // 2 api versions (same)
	v.k2v[19]++ // 3 create topics (same)
	v.k2v[20]++ // 2 delete topics (same)
	v.k2v[21]++ // 1 delete records (same)
	v.k2v[22]++ // 1 init producer id (same)
	v.k2v[24]++ // 1 add partitions to txn (same)
	v.k2v[25]++ // 1 add offsets to txn (same)
	v.k2v[26]++ // 1 end txn (same)
	v.k2v[28]++ // 1 txn offset commit (same)
	// 29, 30, 31 bumped below, but also had throttle changes
	v.k2v[32]++ // 2 describe configs (same)
	v.k2v[33]++ // 1 alter configs (same)
	v.k2v[34]++ // 1 alter replica log dirs (same)
	v.k2v[35]++ // 1 describe log dirs (same)
	v.k2v[37]++ // 1 create partitions (same)
	v.k2v[38]++ // 1 create delegation token (same)
	v.k2v[39]++ // 1 renew delegation token (same)
	v.k2v[40]++ // 1 expire delegation token (same)
	v.k2v[41]++ // 1 describe delegation token (same)
	v.k2v[42]++ // 1 delete groups (same)

	v.k2v[29]++ // 1 describe acls KAFKA-6841 b3aa655a70 KIP-290
	v.k2v[30]++ // 1 create acls (same)
	v.k2v[31]++ // 1 delete acls (same)

	v.k2v[23]++ // 1 offsets for leader epoch KAFKA-6361 9679c44d2b KIP-279
	return v
}

func V2_1_0() Versions {
	v := V2_0_0()
	v.k2v[8]++ // 5 offset commit KAFKA-4682 418a91b5d4 KIP-211

	v.k2v[20]++ // 3 delete topics KAFKA-5975 04770916a7 KIP-322

	v.k2v[1]++  // 9 fetch KAFKA-7333 05ba5aa008 KIP-320
	v.k2v[2]++  // 4 list offset (same)
	v.k2v[3]++  // 7 metadata (same)
	v.k2v[8]++  // 6 offset commit (same)
	v.k2v[9]++  // 5 offset fetch (same)
	v.k2v[23]++ // 2 offsets for leader epoch (same, also in Kafka PR #5635 79ad9026a6)
	v.k2v[28]++ // 2 txn offset commit (same)

	v.k2v[0]++ // 7 produce KAFKA-4514 741cb761c5 KIP-110
	v.k2v[1]++ // 10 fetch (same)
	return v
}

func V2_2_0() Versions {
	v := V2_1_0()
	v.k2v[2]++  // 5 list offset KAFKA-2334 152292994e KIP-207
	v.k2v[11]++ // 4 join group KAFKA-7824 9a9310d074 KIP-394
	v.k2v[36]++ // 1 sasl authenticate KAFKA-7352 e8a3bc7425 KIP-368

	v.k2v[1]++  // 11 fetch KAFKA-8365 e2847e8603 KIP-392
	v.k2v[23]++ // 3 offsets for leader epoch (same)

	v.k2v[4]++ // 2 leader and isr KAFKA-7235 2155c6d54b KIP-380
	v.k2v[5]++ // 1 stop replica (same)
	v.k2v[6]++ // 5 update metadata (same)
	v.k2v[7]++ // 2 controlled shutdown (same)

	v.k2v = append(v.k2v,
		0, // 43 elect preferred leaders KAFKA-5692 269b65279c KIP-183
	)
	return v
}

func V2_3_0() Versions {
	v := V2_2_0()
	v.k2v[3]++  // 8 metadata KAFKA-7922 a42f16f980 KIP-430
	v.k2v[15]++ // 3 describe groups KAFKA-7922 f11fa5ef40 KIP-430

	v.k2v[11]++ // 5 join group KAFKA-7862 0f995ba6be KIP-345
	v.k2v[8]++  // 7 offset commit KAFKA-8225 9fa331b811 KIP-345
	v.k2v[12]++ // 3 heartbeat (same)
	v.k2v[14]++ // 3 sync group (same)

	v.k2v = append(v.k2v,
		0, // 44 incremental alter configs KAFKA-7466 3b1524c5df KIP-339
	)
	return v
}

func V2_4_0() Versions {
	v := V2_3_0()
	v.k2v[4]++  // 3 leader and isr KAFKA-8345 81900d0ba0 KIP-455
	v.k2v[15]++ // 4 describe groups KAFKA-8538 f8db022b08 KIP-345
	v.k2v[19]++ // 4 create topics KAFKA-8305 8e161580b8 KIP-464
	v.k2v[43]++ // 1 elect preferred leaders KAFKA-8286 121308cc7a KIP-460
	v.k2v = append(v.k2v,
		0, // 45 alter partition reassignments KAFKA-8345 81900d0ba0 KIP-455
		0, // 46 list partition reassignments (same)
		0, // 47 offset delete KAFKA-8730 e24d0e22ab KIP-496
	)

	v.k2v[13]++ // 3 leave group KAFKA-8221 74c90f46c3 KIP-345

	// introducing flexible versions; 24 were bumped
	v.k2v[3]++  // 9 metadata KAFKA-8885 apache/kafka#7325 KIP-482
	v.k2v[4]++  // 4 leader and isr (same)
	v.k2v[5]++  // 2 stop replica (same)
	v.k2v[6]++  // 6 update metadata (same)
	v.k2v[7]++  // 3 controlled shutdown (same)
	v.k2v[8]++  // 8 offset commit (same)
	v.k2v[9]++  // 6 offset fetch (same)
	v.k2v[10]++ // 3 find coordinator (same)
	v.k2v[11]++ // 6 join group (same)
	v.k2v[12]++ // 4 heartbeat (same)
	v.k2v[13]++ // 4 leave group (same)
	v.k2v[14]++ // 4 sync group (same)
	v.k2v[15]++ // 5 describe groups (same)
	v.k2v[16]++ // 3 list group (same)
	v.k2v[18]++ // 3 api versions (same, also KIP-511 [non-flexible fields added])
	v.k2v[19]++ // 5 create topics (same)
	v.k2v[20]++ // 4 delete topics (same)
	v.k2v[22]++ // 2 init producer id (same)
	v.k2v[38]++ // 2 create delegation token (same)
	v.k2v[42]++ // 2 delete groups (same)
	v.k2v[43]++ // 2 elect preferred leaders (same)
	v.k2v[44]++ // 1 incremental alter configs (same)
	// also 45, 46; not bumped since in same release

	// Create topics (19) was bumped up to 5 in KAFKA-8907 5d0052fe00
	// KIP-525, then 6 in the above bump, then back down to 5 once the
	// tagged PR was merged (KAFKA-8932 1f1179ea64 for the bump down).

	v.k2v[0]++ // 8 produce KAFKA-8729 f6f24c4700 KIP-467

	return v
}

func V2_5_0() Versions {
	v := V2_4_0()
	v.k2v[22]++ // 3 init producer id KAFKA-8710 fecb977b25 KIP-360
	v.k2v[9]++  // 7 offset fetch KAFKA-9346 6da70f9b95 KIP-447

	// more flexible versions, KAFKA-9420 0a2569e2b99 KIP-482
	// 6 bumped, then sasl handshake reverted later in 1a8dcffe4
	v.k2v[36]++ // 2 sasl authenticate
	v.k2v[37]++ // 2 create partitions
	v.k2v[39]++ // 2 renew delegation token
	v.k2v[40]++ // 2 expire delegation token
	v.k2v[41]++ // 2 describe delegation token

	v.k2v[28]++ // 3 txn offset commit KAFKA-9365 ed7c071e07f KIP-447

	v.k2v[29]++ // 2 describe acls KAFKA-9026 40b35178e5 KIP-482 (for flexible versions)
	v.k2v[30]++ // 2 create acls KAFKA-9027 738e14edb KIP-482 (flexible)
	v.k2v[31]++ // 2 delete acls KAFKA-9028 738e14edb KIP-482 (flexible)

	v.k2v[11]++ // 7 join group KAFKA-9437 96c4ce480 KIP-559
	v.k2v[14]++ // 5 sync group (same)

	return v
}

func V2_6_0() Versions {
	v := V2_5_0()

	v.k2v[21]++ // 2 delete records KAFKA-8768 f869e33ab KIP-482 (opportunistic bump for exlible versions)
	v.k2v[35]++ // 2 describe log dirs KAFKA-9435 4f1e8331ff9 KIP-482 (same)

	v.k2v = append(v.k2v,
		0, // 48 describe client quotas KAFKA-7740 227a7322b KIP-546
		0, // 49 alter client quotas (same)
	)

	v.k2v[5]++ // 3 stop replica KAFKA-9539 7c7d55dbd KIP-570

	v.k2v[16]++ // 4 list group KAFKA-9130 fe948d39e KIP-518
	v.k2v[32]++ // 3 describe configs KAFKA-9494 af3b8b50f2 KIP-569

	return v
}

func V2_7_0() Versions {
	v := V2_6_0()

	// KAFKA-10163 a5ffd1ca44c KIP-599
	v.k2v[37]++ // 3 create partitions
	v.k2v[19]++ // 6 create topics (same)
	v.k2v[20]++ // 5 delete topics (same)

	// KAFKA-9911 b937ec7567 KIP-588
	v.k2v[22]++ // 4 init producer id
	v.k2v[24]++ // 2 add partitions to txn
	v.k2v[25]++ // 2 add offsets to txn
	v.k2v[26]++ // 2 end txn

	v.k2v = append(v.k2v,
		0, // 50 describe user scram creds, KAFKA-10259 e8524ccd8fca0caac79b844d87e98e9c055f76fb KIP-554
		0, // 51 alter user scram creds, same
	)

	// KAFKA-10435 634c9175054cc69d10b6da22ea1e95edff6a4747 KIP-595
	// This opted in fetch request to flexible versions.
	//
	// KAFKA-10487: further change in aa5263fba903c85812c0c31443f7d49ee371e9db
	v.k2v[1]++ // 12 fetch

	// KAFKA-10492 b7c8490cf47b0c18253d6a776b2b35c76c71c65d KIP-595
	//
	// These are actually not supported in 2.7.0, but their slots are
	// reserved. Kafka does not return them in an ApiVersions request.
	v.k2v = append(v.k2v,
		-1, // 52 vote
		-1, // 53 begin quorum epoch
		-1, // 54 end quorum epoch
		-1, // 55 describe quorum
	)

	// KAFKA-8836 57de67db22eb373f92ec5dd449d317ed2bc8b8d1 KIP-497
	v.k2v = append(v.k2v,
		0, // 56 alter isr
	)

	// KAFKA-10028 fb4f297207ef62f71e4a6d2d0dac75752933043d KIP-584
	v.k2v = append(v.k2v,
		0, // 57 update features
	)

	return v
}

// Stable is a shortcut for the latest _released_ Kafka versions.
//
// This is the default version used in kgo to avoid breaking tip changes.
func Stable() Versions {
	return V2_7_0()
}

// Tip is the latest defined Kafka key versions; this may be slightly out of date.
func Tip() Versions {
	v := Stable()

	// KAFKA-10181 KAFKA-10181 KIP-590
	v.k2v = append(v.k2v,
		0, // 58 envelope
	)

	// KAFKA-10729 85f94d50271c952c3e9ee49c4fc814c0da411618 KIP-482
	// (flexible bumps)
	v.k2v[0]++  // 9 produce
	v.k2v[2]++  // 6 list offsets
	v.k2v[23]++ // 4 offsets for leader epoch
	v.k2v[24]++ // 3 add partitions to txn
	v.k2v[25]++ // 3 add offsets to txn
	v.k2v[26]++ // 3 end txn
	v.k2v[27]++ // 1 write txn markers
	v.k2v[32]++ // 4 describe configs
	v.k2v[33]++ // 2 alter configs
	v.k2v[34]++ // 2 alter replica log dirs
	v.k2v[48]++ // 1 describe client quotas
	v.k2v[49]++ // 1 alter client quotas

	// KAFKA-10547 5c921afa4a593478f7d1c49e5db9d787558d0d5e KIP-516
	v.k2v[3]++ // 10 metadata
	v.k2v[6]++ // 7 update metadata

	// KAFKA-10545 1dd1e7f945d7a8c1dc177223cd88800680f1ff46 KIP-516
	v.k2v[4]++ // 5 leader and isr

	// KAFKA-10427 2023aed59d863278a6302e03066d387f994f085c KIP-630
	v.k2v = append(v.k2v,
		0, // 59 fetch snapshot
	)

	return v
}
