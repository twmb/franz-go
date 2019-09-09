kgo
===

TODO
====
- KIP-320 (fetcher log truncation detection)

- KIP-467 (produce response error change for per-record errors)
- KIP-359 (verify leader epoch in produce requests)

- KIP-455 (two new commands)
- KIP-392 (new field in fetch; for brokers only)

- KIP-345 (static group membership, see KAFKA-8224)
- KIP-429 (incremental rebalance, see KAFKA-8179)

- KIP-380 (sticky partition producing)

- KIP-423 (no rebalance on JoinGroup from leader in certain cases; under discussion)

- add support for MessageSet v0/v1
- cleanup generating (names are not great)


OffsetCommit (internal, `__consumer_offsets`):
KIP-211: v2
KIP-320: v3

KIP-384: ?

GroupMemberMetadata (internal, `__consumer_offsets` metadata):
KIP-211: v2
KIP-345: v3
