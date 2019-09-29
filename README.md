kgo
===

TODO
====

- cleanup generating (names are not great)

ACL descriptions in KIP-133

KIPS
====

TODO
----
- KIP-11 (authorizor interface)
- KIP-12 (sasl & ssl)
- KIP-43 (sasl enhancements)
- KIP-48 (create / describe delegation token; 1.1.0)
- KIP-140 (describe/create/delete ACLs; 0.11.0)
- KIP-152 (more sasl, introduce sasl authenticate; 1.0.0)
- KIP-255 (oauth via sasl/oauthbearer; 2.0.0)
- KIP-368 (periodically reauth sasl; 2.2.0)

- KIP-98 (EOS; 0.11.0)
- KIP-345 (static group membership, see KAFKA-8224)
- KIP-392 (fetch request from closest replica w/ rack; 2.2.0)
- KIP-429 (incremental rebalance, see KAFKA-8179; 2.4.0)

NOT YET (KIP under discussion / unmerged PR)
-------
- KIP-359 (verify leader epoch in produce requests)
- KIP-360 (safe epoch bumping for `UNKNOWN_PRODUCER_ID`)
- KIP-392 (new field in fetch; for brokers only)
- KIP-423 (no rebalance on JoinGroup from leader in certain cases; under discussion)
- KIP-447 (transaction changes to better support group changes)
- KIP-467 (produce response error change for per-record errors; 2.4.0)
- KIP-482 (tagged fields; KAFKA-8885)
- KIP-497 (new inter broker admin command "alter isr")
- KIP-516 (topic.id field in some commands, including fetch)
- KIP-518 (list groups by state command change)

DONE
----
- KIP-13 (throttling; supported but not obeyed)
- KIP-31 (relative offsets in message set; 0.10.0)
- KIP-32 (timestamps in message set v1; 0.10.0)
- KIP-35 (adds ApiVersion; 0.10.0)
- KIP-36 (rack aware replica assignment; 0.10.0)
- KIP-40 (ListGroups and DescribeGroup v0; 0.9.0)
- KIP-54 (sticky group assignment)
- KIP-62 (join group rebalnce timeout, background thread heartbeats; v0.10.1)
- KIP-74 (fetch response size limit; 0.10.1)
- KIP-78 (cluster id in metadata; 0.10.1)
- KIP-79 (list offset req/resp timestamp field; 0.10.1)
- KIP-101 (offset for leader epoch introduced; broker usage yet; 0.11.0)
- KIP-107 (delete records; 0.11.0)
- KIP-108 (validate create topic; 0.10.2)
- KIP-110 (zstd; 2.1.0)
- KIP-112 (JBOD disk failure, protocol changes; 1.0.0)
- KIP-113 (JBOD log dir movement, protocol additions; 1.0.0)
- KIP-124 (request rate quotas; 0.11.0)
- KIP-133 (describe & alter configs; 0.11.0)
- KIP-183 (elect preferred leaders; 2.2.0)
- KIP-185 (idempotent is default; 1.0.0)
- KIP-195 (create partitions request; 1.0.0)
- KIP-207 (new error in list offset request; 2.2.0)
- KIP-219 (throttling happens after response; 2.0.0)
- KIP-226 (describe configs v1; 1.1.0)
- KIP-227 (incremental fetch requests; supported but not used; 1.1.0)
- KIP-229 (delete groups request; 1.1.0)
- KIP-279 (leader / follower failover; changed offsets for leader epoch; 2.0.0)
- KIP-320 (fetcher log truncation detection; 2.1.0)
- KIP-322 (new error when delete topics is disabled; 2.1.0)
- KIP-339 (incremental alter configs; 2.3.0)
- KIP-341 (sticky group bug fix)
- KIP-369 (always round robin produce partitioner; 2.4.0)
- KIP-380 (inter-broker command changes; 2.2.0)
- KIP-394 (require member.id for initial join; 2.2.0)
- KIP-412 (dynamic log levels with incremental alter configs; 2.4.0)
- KIP-430 (include authorized ops in describe groups; 2.3.0)
- KIP-455 (admin replica reassignment; 2.4.0)
- KIP-460 (admin leader election; 2.4.0)
- KIP-464 (defaults for create topic, KIP-464; 2.4.0)
- KIP-480 (sticky partition producing; 2.4.0)
- KIP-496 (offset delete admin command; 2.4.0)
- KIP-525 (create topics v5 returns configs; 2.4.0)
- KIP-526 (reduce metadata lookups; done minus part 2, which we wont do)
