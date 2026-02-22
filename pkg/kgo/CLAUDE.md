# CLAUDE.md

## Package Overview

franz-go is a pure Go Kafka client library. The `kgo` package is the main client package.

## Style

- Never use non-ASCII characters in code or comments, use simple characters instead: dashes, => for arrows, etc.
- Always run `gofmt` before committing
- Internal comments should explain WHY we are doing something, not just WHAT we are doing. WHAT comments are almost never useful, unless the block that follows is complex.
- Comments around subtle race conditions (logic or data) should contain a walkthrough of how the race is encountered. Don't JUST say what the race is, but also how a sequence of events can encounter the race.

## Commit Style

- Format: `kgo: <description>` (lowercase, no period)
- Body should explain the "why" not just the "what"
- Reference KIPs (Kafka Improvement Proposals) for protocol changes
- Include `Closes #<issue>` when fixing GitHub issues

## Protocol Behavior

When debugging protocol issues:
1. The **Java Kafka client** is the reference implementation - check `clients/src/main/java/org/apache/kafka/` (user has it at `~/src/apache/kafka`)
2. **KIPs** define protocol behavior: https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals

## Approach

Before implementing a fix for any bug, first verify whether existing code
already handles the scenario. Analyze the current codebase's safety mechanisms
(e.g., prerevoke logic, error handlers, retry paths) before writing new code.

## Key Files

- `broker.go` - Connection handling, SASL authentication, request/response
- `config.go` - Client configuration options
- `client.go` - Main client logic
- `source.go` - Fetch logic, what actually gets data from the broker - this uses an abstraction called a "cursor" to move consume and move forward in a single partition. A "source" owns many cursors fetching from one broker (the source is the broker)
- `sink.go` - Produce logic, what actually sends data to the broker - this uses an abstraction called a "recBuf" (record buffer) to produce to a single partition. A "sink" owns many recBufs. A recBuf owns many recBatch's (record batch)
- `consumer.go` - The consumer abstraction around fetching. A consumer owns many sources, and controls what source a cursor belongs to. A consumer also validates the offset a cursor is at - did data loss occur (via OffsetForLeaderEpoch)? What is the "epoch" of the partition, and which broker (source) should the cursor be placed on (ListOffsets)?
- `producer.go` - The producer abstraction around producing. A producer owns many sinks. The producer file is responsible for picking which sink a record is produced to, for finishing record promises, and contains functions execute on all sinks (flushing, failing records)
- `consumer_group.go` - Group consumers: this contains logic for managing a consumer group. An internal "consumer" is a separate but related concept to a "group consumer" - the group consumer manages figuring out WHAT partitions to consume, and hookes into the consumer to update the subscription list. The bulk of the file is about managing the group and committing offsets.
- `consumer_direct.go` - Direct consumer: you assign partitions to directly consume from. Not much logic here besides capturing which new partitions to consume based on metadata updates and your list of topics / regex topics.
- `txn.go` - GroupTransactSession; this file manages transactions. The transact session bundles some of the group management logic to transaction logic to make it safe - the implementation is paranoid, favoring aborting transactions if anything occurs so that we prevent duplicates.
- `metadata.go` - Periodically updates information about topics / partitions the client is consuming, and feeding those updates to the producer or consumer.

## Testing

- `go test ./...` requires a local Kafka broker to be running on port 9092.
- `../kfake/` provides a fake Kafka broker for testing
- kfake is a separate Go module; use a `go.work` in `pkg/kfake/` to test against local kgo changes
- kfake `ControlKey` intercepts requests by type; without `KeepControl`, consumed after one use
- Multiple ControlKeys for the same key run in FIFO order; returning `handled=false` continues to the next
- Observer pattern: `KeepControl` + `return nil, nil, false` to count/signal without intercepting
- kfake tests live in `pkg/kfake/issues_test.go` (for tests when we fix user reported issues) and `pkg/kfake/behavior_test.go` (for new tests when we add new behavior)
- kfake is in-process so round-trips are near-instant - always run `go test -race` to catch races not seen with real brokers
- Unit tests should be fast: tests longer than a few seconds, EVEN IF THEY PASS, are suspicious and should be analyzed. The integration tests in kgo (TestGroupETL / TestTxnETL) are the only ones that should take a long time - up to two minutes without -race, and ~3 to ~4min in race mode.

## Design / concurrency.

- Refer to ../../DESIGN.md for a high level design of all the operations that can happen.
- Update the design file as necessary / when making significant changes.
