// Package kversion specifies max versions for Kafka request keys.
//
// Kafka technically has internal broker versions that bump multiple times per
// release. This package only defines releases and tip.
package kversion

import (
	"bytes"
	"fmt"
	"text/tabwriter"

	"github.com/twmb/kafka-go/pkg/kmsg"
)

// Versions is a list of versions, with each item corresponding to a Kafka key
// and each item's value corresponding to the max version supported.
//
// The length of this slice differs per major version.
type Versions []int16

func (vs Versions) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	for k, v := range vs {
		fmt.Fprintf(w, "%s\t%d\n", kmsg.NameForKey(int16(k)), v)
	}
	w.Flush()
	return string(buf.Bytes())
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
	return v
}

func V0_8_1() Versions {
	v := V0_8_0()
	v = append(v,
		0, // 8 offset commit KAFKA-965 db37ed0054
		0, // 9 offset fetch (same)
	)
	return v
}

func V0_8_2() Versions {
	v := V0_8_1()
	v[8]++ // 1 offset commit KAFKA-1462
	v[9]++ // 1 offset fetch KAFKA-1841 161b1aa16e I think?
	v = append(v,
		0, // 10 find coordinator KAFKA-1012 a670537aa3
		0, // 11 join group (same)
		0, // 12 heartbeat (same)
	)
	return v
}

func V0_9_0() Versions {
	v := V0_8_2()
	v[0]++ // 1 produce KAFKA-2136 436b7ddc38; KAFKA-2083 ?? KIP-13
	v[1]++ // 1 fetch (same)
	v[6]++ // 1 update metadata KAFKA-2411 d02ca36ca1
	v[7]++ // 1 controlled shutdown (same)
	v[8]++ // 2 offset commit KAFKA-1634
	v = append(v,
		0, // 13 leave group KAFKA-2397 636e14a991
		0, // 14 sync group KAFKA-2464 86eb74d923
		0, // 15 describe groups KAFKA-2687 596c203af1
		0, // 16 list groups KAFKA-2687 596c203af1
	)
	return v
}

func V0_10_0() Versions {
	v := V0_9_0()
	v[0]++ // 2 produce KAFKA-3025 45c8195fa1 KIP-31 KIP-32
	v[1]++ // 2 fetch (same)
	v[3]++ // 1 metadata KAFKA-3306 33d745e2dc
	v[6]++ // 2 update metadata KAFKA-1215 951e30adc6
	v = append(v,
		0, // 17 sasl handshake KAFKA-3149 5b375d7bf9
		0, // 18 api versions KAFKA-3307 8407dac6ee
	)
	return v
}

func V0_10_1() Versions {
	v := V0_10_0()
	v[1]++  // 3 fetch KAFKA-2063 d04b0998c0 KIP-74
	v[2]++  // 1 list offset KAFKA-4148 eaaa433fc9 KIP-79
	v[3]++  // 2 metadata KAFKA-4093 ecc1fb10fa KIP-78
	v[11]++ // 1 join group KAFKA-3888 40b1dd3f49 KIP-62
	v = append(v,
		0, // 19 create topics KAFKA-2945 fc47b9fa6b
		0, // 20 delete topics KAFKA-2946 539633ba0e
	)
	return v
}

func V0_10_2() Versions {
	v := V0_10_1()
	v[6]++  // 3 update metadata KAFKA-4565 d25671884b KIP-103
	v[19]++ // 1 create topics KAFKA-4591 da57bc27e7 KIP-108
	return v
}

func V0_11_0() Versions {
	v := V0_10_2()
	v[0]++  // 3 produce KAFKA-4816 5bd06f1d54 KIP-98
	v[1]++  // 4 fetch (same)
	v[1]++  // 5 fetch KAFKA-4586 8b05ad406d KIP-107
	v[3]++  // 4 metadata KAFKA-5291 7311dcbc53 (3 below)
	v[9]++  // 3 offset fetch KAFKA-3853 c2d9b95f36 KIP-98
	v[10]++ // 1 find coordinator KAFKA-5043 d0e7c6b930 KIP-98
	v = append(v,
		0, // 21 delete records KAFKA-4586 see above
		0, // 22 init producer id KAFKA-4817 bdf4cba047 KIP-98
		0, // 23 offsets for leader epoch KAFKA-1211 0baea2ac13 KIP-101
		0, // 24 add partitions to txn KAFKA-4990 865d82af2c KIP-98
		0, // 25 add offsets to txn (same)
		0, // 26 end txn (same)
		0, // 27 write txn (same)
		0, // 28 txn offset commit (same)
		0, // 29 describe acls KAFKA-3266 9815e18fef KIP-140
		0, // 30 create acls (same)
		0, // 31 delete acls (same)
		0, // 32 describe configs KAFKA-3267 972b754536 KIP-133
		0, // 33 alter configs (same)
	)

	// KAFKA-4954 0104b657a1 KIP-124
	v[2]++  // 2 list offset (reused in e71dce89c0 KIP-98)
	v[3]++  // 3 metadata
	v[8]++  // 3 offset commit
	v[9]++  // 3 offset fetch
	v[11]++ // 2 join group
	v[12]++ // 1 heartbeat
	v[13]++ // 1 leave group
	v[14]++ // 1 sync group
	v[15]++ // 1 describe groups
	v[16]++ // 1 list group
	v[18]++ // 1 api versions
	v[19]++ // 2 create topics
	v[20]++ // 1 delete topics

	return v
}

func V1_0_0() Versions {
	v := V0_11_0()
	v[0]++ // 4 produce KAFKA-4763 fc93fb4b61 KIP-112
	v[1]++ // 6 fetch (same)
	v[3]++ // 5 metadata (same)
	v[4]++ // 1 leader and isr (same)
	v[6]++ // 4 update metadata (same)

	v[0]++  // 5 produce KAFKA-5793 94692288be
	v[17]++ // 1 sasl handshake KAFKA-4764 8fca432223 KIP-152

	v = append(v,
		0, // 34 alter replica log dirs KAFKA-5694 adefc8ea07 KIP-113
		0, // 35 describe log dirs (same)
		0, // 36 sasl authenticate KAFKA-4764 (see above)
		0, // 37 create partitions KAFKA-5856 5f6393f9b1 KIP-195
	)

	return v
}

func V1_1_0() Versions {
	v := V1_0_0()
	v = append(v,
		0, // 38 create delegation token KAFKA-4541 27a8d0f9e7 under KAFKA-1696 KIP-48
		0, // 39 renew delegation token (same)
		0, // 40 expire delegation token (same)
		0, // 41 describe delegation token (same)
		0, // 42 delete groups KAFKA-6275 1ed6da7cc8 KIP-229
	)

	v[1]++  // 7 fetch KAFKA-6254 7fe1c2b3d3 KIP-227
	v[32]++ // 1 describe configs KAFKA-6241 b814a16b96 KIP-226

	return v
}

func V2_0_0() Versions {
	v := V1_1_0()
	v[0]++  // 6 produce KAFKA-6028 1facab387f KIP-219
	v[1]++  // 8 fetch (same)
	v[2]++  // 3 list offset (same)
	v[3]++  // 6 metadata (same)
	v[8]++  // 4 offset commit (same)
	v[9]++  // 4 offset fetch (same)
	v[10]++ // 2 find coordinator (same)
	v[11]++ // 3 join group (same)
	v[12]++ // 2 heartbeat (same)
	v[13]++ // 2 leave group (same)
	v[14]++ // 2 sync group (same)
	v[15]++ // 2 describe groups (same)
	v[16]++ // 2 list group (same)
	v[18]++ // 2 api versions (same)
	v[19]++ // 3 create topics (same)
	v[20]++ // 2 delete topics (same)
	v[21]++ // 1 delete records (same)
	v[22]++ // 1 init producer id (same)
	v[24]++ // 1 add partitions to txn (same)
	v[25]++ // 1 add offsets to txn (same)
	v[26]++ // 1 end txn (same)
	v[28]++ // 1 txn offset commit (same)
	v[32]++ // 2 describe configs (same)
	v[33]++ // 1 alter configs (same)
	v[34]++ // 1 alter replica log dirs (same)
	v[35]++ // 1 describe log dirs (same)
	v[37]++ // 1 create partitions (same)
	v[38]++ // 1 create delegation token (same)
	v[39]++ // 1 renew delegation token (same)
	v[40]++ // 1 expire delegation token (same)
	v[41]++ // 1 describe delegation token (same)
	v[42]++ // 1 delete groups (same)

	v[29]++ // 1 describe acls KAFKA-6841 b3aa655a70 KIP-290
	v[30]++ // 1 create acls (same)
	v[31]++ // 1 delete acls (same)

	v[23]++ // 1 offsets for leader epoch KAFKA-6361 9679c44d2b KIP-279
	return v
}

func V2_1_0() Versions {
	v := V2_0_0()
	v[8]++ // 5 offset commit KAFKA-4682 418a91b5d4 KIP-211

	v[20]++ // 3 delete topics KAFKA-5975 04770916a7 KIP-322

	v[1]++  // 9 fetch KAFKA-7333 05ba5aa008 KIP-320
	v[2]++  // 4 list offset (same)
	v[3]++  // 7 metadata (same)
	v[8]++  // 6 offset commit (same)
	v[9]++  // 5 offset fetch (same)
	v[23]++ // 2 offsets for leader epoch (same, also in Kafka PR #5635 79ad9026a6)
	v[28]++ // 2 txn offset commit (same)

	v[0]++ // 7 produce KAFKA-4514 741cb761c5 KIP-110
	v[1]++ // 10 fetch (same)
	return v
}

func V2_2_0() Versions {
	v := V2_1_0()
	v[2]++  // 5 list offset KAFKA-2334 152292994e KIP-207
	v[11]++ // 4 join group KAFKA-7824 9a9310d074 KIP-394
	v[36]++ // 1 sasl authenticate KAFKA-7352 e8a3bc7425 KIP-368

	v[1]++  // 11 fetch KAFKA-8365 e2847e8603 KIP-392
	v[23]++ // 3 offsets for leader epoch (same)

	v[4]++ // 2 leader and isr KAFKA-7235 2155c6d54b KIP-380
	v[5]++ // 1 stop replica (same)
	v[6]++ // 5 update metadata (same)
	v[7]++ // 2 controlled shutdown (same)

	v = append(v,
		0, // 43 elect preferred leaders KAFKA-5692 269b65279c KIP-183
	)
	return v
}

func V2_3_0() Versions {
	v := V2_2_0()
	v[3]++  // 8 metadata KAFKA-7922 a42f16f980 KIP-430
	v[15]++ // 3 describe group KAFKA-7922 f11fa5ef40 KIP-430

	v[11]++ // 5 join group KAFKA-7862 0f995ba6be KIP-345
	v[13]++ // 3 leave group KAFKA-8221 74c90f46c3 KIP-345
	v[8]++  // 7 offset commit KAFKA-8225 9fa331b811 KIP-345
	v[12]++ // 3 heartbeat (same)
	v[14]++ // 3 sync group (same)

	v = append(v,
		0, // 44 incremental alter configs KAFKA-7466 3b1524c5df KIP-339
	)
	return v
}

func V2_4_0() Versions {
	v := V2_3_0()
	v[4]++  // 3 leader and isr KAFKA-8345 81900d0ba0 KIP-455
	v[15]++ // 4 describe group KAFKA-8538 f8db022b08 KIP-345
	v[19]++ // 4 create topics KAFKA-8305 8e161580b8 KIP-464
	v[43]++ // 1 elect preferred leaders KAFKA-8286 121308cc7a KIP-460
	v = append(v,
		0, // 45 alter partition reassignments KAFKA-8345 81900d0ba0 KIP-455
		0, // 46 list partition reassignments (same)
		0, // 47 offset delete KAFKA-8730 e24d0e22ab KIP-496
	)

	// introducing flexible versions; 24 were bumped
	v[3]++  // 9 metadata KAFKA-8885 apache/kafka#7325 KIP-482
	v[4]++  // 4 leader and isr (same)
	v[5]++  // 2 stop replica (same)
	v[6]++  // 6 update metadata (same)
	v[7]++  // 3 controlled shutdown (same)
	v[8]++  // 8 offset commit (same)
	v[9]++  // 6 offset fetch (same)
	v[10]++ // 3 find coordinator (same)
	v[11]++ // 6 join group (same)
	v[12]++ // 4 heartbeat (same)
	v[13]++ // 4 leave group (same)
	v[14]++ // 4 sync group (same)
	v[15]++ // 5 describe groups (same)
	v[16]++ // 3 list group (same)
	v[18]++ // 3 api versions (same, also KIP-511 [non-flexible fields added])
	v[19]++ // 5 create topics (same)
	v[20]++ // 4 delete topics (same)
	v[22]++ // 2 init producer id (same)
	v[38]++ // 2 create delegation token (same)
	v[42]++ // 2 delete groups (same)
	v[43]++ // 2 elect preferred leaders (same)
	v[44]++ // 1 incremental alter configs (same)
	// also 45, 46; not bumped since in same release

	// Create topics (19) was bumped up to 5 in KAFKA-8907 5d0052fe00
	// KIP-525, then 6 in the above bump, then back down to 5 once the
	// tagged PR was merged (KAFKA-8932 1f1179ea64 for the bump down).

	v[0]++ // 8 produce KAFKA-8729 f6f24c4700 KIP-467

	return v
}

// Tip is the latest defined Kafka key versions; this may be slightly out of date.
func Tip() Versions {
	v := V2_4_0()
	v[22]++ // 3 init producer id KAFKA-8710 fecb977b25 KIP-360
	v[9]++  // 7 offset fetch KAFKA-9346 6da70f9b95 KIP-447
	return v
}
