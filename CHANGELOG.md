TBD
===

- [`85243c5c5`](https://github.com/twmb/kafka-go/commit/85243c5c5aee1e23f6d360efdc82480fd83fcfc3): Breaking API change for enums in ACL & config managing request / response types
- [`4bc453bc2`](https://github.com/twmb/kafka-go/commit/4bc453bc237bd534947cebdc5bc72f8bd7e855d9): Add NewPtr functions for generated structs in kmsg
- [`dcaf32daa`](https://github.com/twmb/kafka-go/commit/dcaf32daa16fdc20e1c9cf91847b74342d13e36f): Generate RequestWith function for all requests in kmsg
- [`c27b41c3e`](https://github.com/twmb/kafka-go/commit/c27b41c3e85df89651d9634cd024a2114f273cbd): Add Requestor interface to kmsg package
- [`2262f3a8f`](https://github.com/twmb/kafka-go/commit/2262f3a8f90db2f459203b8ae665b089c19901d2): Add New functions for generated structs in kmsg
- [`7fc3a701e`](https://github.com/twmb/kafka-go/commit/7fc3a701ec2155417cc02d2535510fcb5c9cdb6d): Add UpdateFeatures for 2.7.0 (KIP-584)
- [`f09690186`](https://github.com/twmb/kafka-go/commit/f09690186ce2697f78626ba06e7ccfb586f1c7ee): Add shortcuts to get sasl.Mechanisms from sasl structs
- [PR #12](https://github.com/twmb/kafka-go/pull/12): Add clarifying wording on NewClient (thanks @weeco!)
- [`9687df7ad`](https://github.com/twmb/kafka-go/commit/9687df7ad71c552c61eea86376ebf3a9f6bff09e): Breaking API change for kgo.Dialer
- [`aca99bcf1`](https://github.com/twmb/kafka-go/commit/aca99bcf19e741850378adbfe64c62b009340d7d): New APIs for KIP-497 (alter isr)
- [`a5f66899f`](https://github.com/twmb/kafka-go/commit/a5f66899f6b492de37d689566d34869f50744717): New APIs for KIP-595 (raft)
- [`cd499f4ea`](https://github.com/twmb/kafka-go/commit/cd499f4eaceaa0bd73452b5737c7713fe2b60ca9): Fetch request changes for KIP-595 (raft)
- [`7fd87da1c`](https://github.com/twmb/kafka-go/commit/7fd87da1c8562943b095a54b5bc6258e2d2bdc6c): Support defaults in generated code (and a few commits after adding defaults)
- [`74c7f57e8`](https://github.com/twmb/kafka-go/commit/74c7f57e81dd45033e9eec3bacbc4dca75fef83d): Support for KIP-584 (ApiVersions changes for min/max features)

Multiple generator changes were made to better handle tags, add support for
enums, and parse many files in a `definitions` directory. Minor changes were
made in kgo to use the new `RequestWith` function.

v0.4.9
======

Typo fix for the prior release (in 2.7.0 APIs).

v0.4.8
======

- [`2e235164d`](https://github.com/twmb/kafka-go/commit/2e235164daca64cafc715642582b13424e494e5c): Further prepare for KIP-588 (recover on txn timed out)
- [`60de0dafb`](https://github.com/twmb/kafka-go/commit/60de0dafbeb7bf72996ad5f24690820962d2f584): Further audit for KIP-360 correctness (recoverable producer id)
- [`2eb27c653`](https://github.com/twmb/kafka-go/commit/2eb27c653a4cdb815bf366894a2b87a3555ee50b): client.Request: shard DeleteRecords, {AlterReplica,Describe}LogDirs
- [`ebadfdae1`](https://github.com/twmb/kafka-go/commit/ebadfdae1d975a5a937a1f1d67fd909b728c7386): Add support for KIP-554 (SCRAM APIs)
- [`4ea4c4297`](https://github.com/twmb/kafka-go/commit/4ea4c4297e0402fcf37ef913f4a161203ff83dd4): Un-admin a lot of requests and fix OffsetDelete to be a group request

v0.4.7
======

- [`598261505`](https://github.com/twmb/kafka-go/commit/598261505033d0255c37dc06b9b6c1112818a1be): Breaking API change in kmsg to add validating & erroring when reading records/messages

v0.4.6
======

- Introduces the CHANGELOG file
- [`ff1dc467e`](https://github.com/twmb/kafka-go/commit/ff1dc467e32b6be41656c1f3bc57cb4d45e32a0c): Breaking API change in kmsg to support future non-breaking changes
- [`4eb594044`](https://github.com/twmb/kafka-go/commit/4eb594044cfc611b75352530f7122c596b15764c): Correctly copies over Version/ThrottleMillis when merging ListGroups
