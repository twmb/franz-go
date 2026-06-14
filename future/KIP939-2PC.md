# KIP-939 (2PC) producer design + feasibility -- TABLED

> **Status: designed, NOT being built. Tabled 2026-06-14.**
>
> **Why tabled:** the broker side is not GA. On Kafka trunk (`4.4.0-SNAPSHOT`),
> InitProducerId v6 is `latestVersionUnstable: true` (a normal broker won't even
> advertise it) and the crash-resume path (`keepPreparedTxn=true`) is *stubbed*
> -- the coordinator returns `UNSUPPORTED_VERSION` and never populates the
> ongoing-txn response fields. The Java public 2PC APIs were also reverted out of
> the 4.1 and 4.2 releases (`KAFKA-19414`). So the most valuable half (recover a
> prepared txn after a crash) cannot be exercised against any real broker yet.
>
> **Un-table trigger (resume this when ALL hold):** (1) a released Kafka ships
> InitProducerId v6 as a *stable* version (no `latestVersionUnstable`, advertised
> via ApiVersions), AND (2) the broker implements `keepPreparedTxn=true` to
> return `OngoingTxnProducerId/Epoch` instead of `UNSUPPORTED_VERSION`. Re-verify
> both against `~/src/apache/kafka` before starting. Until then there is nothing
> to build against.
>
> This doc is written to be self-contained so a future session can resume cold.
> Everything below was verified 2026-06-14 against `~/src/apache/kafka` trunk
> (HEAD `27b8a5eaa8`), the KIP-939 wiki page, and the franz-go tree at that time.
> File:line references will drift -- re-grep, don't trust them blindly.

---

## 0. What this is, in one paragraph

KIP-939 lets an **external** transaction coordinator (a database, a Flink/Spark
job, your own orchestrator) decide the outcome of a Kafka transaction. The
producer "prepares" a transaction (flushes everything, freezes it) and hands
back a small durable `{producerID, epoch}` token. The external system persists
that token atomically with its own commit decision. Later -- possibly after a
crash and restart -- it tells the producer to commit or abort that exact
transaction. The enabling broker change is that a 2PC transaction is **never
auto-aborted on timeout**, so it can stay prepared across a crash until the
external coordinator resolves it.

---

## 1. Recommendation

- **Build 939 when the un-table trigger fires; ship it disabled-by-default and
  experimental even then.** The client work is small (~3 public methods + 1
  option + 1 value type) and maps cleanly onto existing txn machinery. No new RPC.
- **Defer KIP-1289 (share-group hierarchical txn acks) regardless** -- see section 4.

---

## 2. Broker-side facts (verified, will need re-verification on resume)

### Wire schema
No new RPC (confirmed against `clients/.../protocol/ApiKeys.java`). Prepare and
complete reuse **InitProducerId v6** + unchanged **EndTxn**.

InitProducerId bumped to **v6** (`clients/.../message/InitProducerIdRequest.json`,
`latestVersionUnstable: true`, `validVersions "0-6"`):

| field | type | versions | meaning |
|---|---|---|---|
| `Enable2Pc` | bool | 6+ (default false) | client participates in 2PC |
| `KeepPreparedTxn` | bool | 6+ (default false) | keep the ongoing txn instead of aborting it on init |

InitProducerIdResponse v6 (plain `versions: "6+"`, NOT tagged):

| field | type | versions | meaning |
|---|---|---|---|
| `OngoingTxnProducerId` | int64 (entityType producerId) | 6+ (default -1) | pid of the kept prepared txn, -1 if none |
| `OngoingTxnProducerEpoch` | int16 | 6+ (default -1) | epoch of that pid, -1 if none |

> The KIP page text calls the response field `OngoingTxnEpoch`; the committed
> JSON schema name is `OngoingTxnProducerEpoch`. **Schema name wins** for kmsg.

EndTxn is **unchanged** (still v0-5, no 2PC fields). A 2PC commit/abort is an
ordinary EndTxn carrying the prepared pid/epoch.

### ACL
New `AclOperation TWO_PHASE_COMMIT((byte) 15)` on `TRANSACTIONAL_ID`.
`InitProducerId(enable2Pc=true)` requires **both** WRITE **and**
TWO_PHASE_COMMIT, else `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. Broker config
gate `transaction.two.phase.commit.enable` (default false); if off, a 2PC init
also returns `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.

### No auto-abort on timeout
`enable2Pc=true` => coordinator forces the txn timeout to `Int.MaxValue`. The
timeout scanner skips any txn where `isDistributedTwoPhaseCommitTxn`
(== `txnTimeoutMs == Integer.MAX_VALUE`). **No new broker state** -- the
exemption is keyed entirely off the infinite-timeout sentinel. A prepared 2PC
txn sits in `Ongoing` forever until an explicit EndTxn or admin
`forceTerminateTransaction` (which is `InitProducerId(keepPreparedTxn=false)`).

### GA status (the blocker)
- v6 is `latestVersionUnstable` => not advertised to normal clients.
- On trunk, `keepPreparedTxn=true` => `UNSUPPORTED_VERSION` ("the feature hasn't
  been implemented yet"); `OngoingTxnProducerId/Epoch` are never set anywhere in
  broker code.
- Java public 2PC APIs reverted out of 4.1/4.2 (`KAFKA-19414`); live only on trunk.

---

## 3. franz-go design

### 3a. Existing machinery (anchors -- re-grep on resume)
- PID in `cl.producer.id` (`atomic.Value` of `*producerID{id,epoch,err}`),
  `producer.go`. `doInitProducerID` (`producer.go` ~1079) builds the request;
  `idVersion` records the negotiated version.
- Txn state: `producer.inTxn bool` (under `txnMu`), `producingTxn atomic.Bool`
  (gates `Produce`), `producedInTxn atomic.Bool`, `tx890p2 atomic.Bool`.
- Public txn API (`txn.go`): `TransactionalID` opt, `BeginTransaction`,
  `EndTransaction(ctx, TryCommit/TryAbort)`, `AbortBufferedRecords`,
  `GroupTransactSession.{Begin,End}`. EndTxn built in `EndTransaction` (~txn.go:807).
- Recovery: `failProducerID` (`producer.go` ~1028) attaches an error to the pid;
  `maybeRecoverProducerID` (`txn.go` ~881) decides recoverability and, if
  recoverable, re-inits -- which **aborts + bumps the epoch**.
- Version clamping: `broker.go` ~388-390 clamps our requested version **down** to
  the broker's advertised max. There is **no** path to force a version *above*
  advertised; `maxVersions`/`minVersions` opts only clamp down.

### 3b. New public API (sketch)

```go
// ProducerOpt. Sets enable2Pc on InitProducerId and requires InitProducerId v6.
// Requires TransactionalID. Broker must allow 2PC and principal must hold the
// TWO_PHASE_COMMIT ACL, else InitProducerId => TRANSACTIONAL_ID_AUTHORIZATION_FAILED.
// EXPERIMENTAL: requires a broker advertising the (currently unstable) v6.
func Enable2PC() ProducerOpt

// PreparedTxnState is the durable handle to a prepared 2PC txn: a (pid, epoch)
// pair. The external coordinator persists this atomically with its own commit
// decision and replays it after a crash.
type PreparedTxnState struct {
    ProducerID    int64
    ProducerEpoch int16
}
func (p PreparedTxnState) String() string                       // "<id>:<epoch>"
func ParsePreparedTxnState(s string) (PreparedTxnState, error)
func (p PreparedTxnState) Empty() bool                          // ProducerID < 0

// PrepareTransaction flushes all buffered records, finalizes partition/offset
// registration, and returns the prepared state WITHOUT sending EndTxn. After
// this, Produce errors until CompleteTransaction. The broker holds the txn open
// indefinitely (2PC => infinite timeout). Must be in a transaction.
func (cl *Client) PrepareTransaction(ctx context.Context) (PreparedTxnState, error)

// CompleteTransaction finishes a prepared txn by sending EndTxn. The state arg
// is compared against the producer's current prepared pid/epoch: match => commit,
// mismatch (or empty) => abort. Only legal terminal action once prepared.
func (cl *Client) CompleteTransaction(ctx context.Context, state PreparedTxnState) (committed bool, err error)

// RecoverPreparedTransaction is the crash-resume entry point. Issues
// InitProducerId(enable2Pc=true, keepPreparedTxn=true). If the broker reports an
// ongoing prepared txn, the returned state is non-empty and the client is placed
// directly into the prepared state (CompleteTransaction is the only legal next
// call). If none, returns Empty and the client is a normal ready 2PC producer.
// Call right after NewClient, before BeginTransaction.
func (cl *Client) RecoverPreparedTransaction(ctx context.Context) (PreparedTxnState, error)
```

### 3c. Why `Enable2PC` is a client option and NOT inferred from `PrepareTransaction`

This question came up and the answer is load-bearing, so it is recorded here.

`enable2Pc` rides on **InitProducerId**, which fires at the *start* of the
producer's transactional life -- lazily on the first `BeginTransaction`/`Produce`
-- long before `PrepareTransaction` is ever called. You cannot retroactively set
a flag that already went out on the wire. More importantly, `enable2Pc=true` does
two things *at init time* that must hold for the **entire** transaction:

1. **It makes the broker-side txn timeout infinite.** Without it, the txn gets
   the normal timeout (~60s default). Sequence: init without 2PC -> produce ->
   60s of work -> call `PrepareTransaction` -> too late, the broker **already
   auto-aborted** the txn at 60s because it thought it was an ordinary txn. The
   no-timeout exemption is locked in at InitProducerId, not at prepare.
2. **It requires the `TWO_PHASE_COMMIT` ACL** -- a deployment-level grant known
   up front, not discovered mid-transaction.

Also, `RecoverPreparedTransaction` re-inits with `enable2Pc=true` on a brand-new
client after a crash, *before any transaction exists* -- there is no
`PrepareTransaction` call to infer intent from at all.

So the producer must know "I am a 2PC producer" before it sends its first
InitProducerId. That is why it is a client-level option (also matches Java, where
it rides on `initTransactions(keepPreparedTxn)` -- init time, not prepare time).

**Alternative considered:** put the flag on `BeginTransaction` instead (the PID
*can* be re-inited per-transaction). Legitimate, but 2PC-ness is genuinely a
deployment property (needs an ACL, "this app participates in external
coordination"), so a client opt is the cleaner fit. It cannot, in any case, be
inferred from `PrepareTransaction`.

### 3d. State-machine deltas
Add a `prepared` dimension to `producer` (under `txnMu`) plus storage for the
prepared pid/epoch.
- `BeginTransaction`: as today; if `Enable2PC()` set, `doInitProducerID` carries
  `Enable2Pc=true`.
- `PrepareTransaction`: Flush (error if any record failed, same contract as
  `EndTransaction`'s flush); capture current `(id, epoch)`; set `prepared`, clear
  `producingTxn` (Produce now errors). Send **no** RPC.
- `CompleteTransaction`: compute commit/abort by comparing arg vs stored prepared
  state; send EndTxn (reuse `EndTransaction` body /
  `doWithConcurrentTransactions("EndTxn", ...)`); on success clear `prepared` and
  `inTxn`. EndTxn v5 (KIP-890p2) still bumps epoch on the response -- fine, orthogonal.
- `RecoverPreparedTransaction`: eager `doInitProducerID` with
  `Enable2Pc=true, KeepPreparedTxn=true`. If `OngoingTxnProducerId >= 0`: store
  that pid/epoch as both the live producer id and the prepared state; set
  `inTxn=true, prepared=true, producingTxn=false`. Else normal ready state.
- `Produce` guard: false while `prepared`. `BeginTransaction` rejects if `prepared`.

### 3e. Interaction with fencing / recovery (the subtle part)
1. **Disable auto-abort recovery while prepared.** Today `maybeRecoverProducerID`
   may re-init the PID on a fatal txn error, which **aborts + bumps**. For a
   *prepared* 2PC txn that silent abort would destroy a transaction the external
   coordinator still owns. Once `prepared`, the only legal transitions are
   `CompleteTransaction` and `RecoverPreparedTransaction`; hard-gate the re-init
   on `!prepared` and never `failProducerID` into an auto-abort while prepared.
2. **No timeout safety net.** With 2PC's infinite timeout, a crashed-and-never-
   resumed 2PC producer wedges the partition LSO **forever** until explicit
   completion or admin `forceTerminate`. By design (external coordinator's job),
   but `Enable2PC`/`PrepareTransaction` docs must say so loudly: a prepared txn
   that is never completed blocks read_committed consumers indefinitely.
3. **Resume vs normal init divergence.** Normal InitProducerId aborts any ongoing
   txn; resume must use `keepPreparedTxn=true` so the broker does NOT abort. A
   stray normal init between prepare and complete (e.g. lazy init firing) would
   abort the prepared txn. `RecoverPreparedTransaction` forces eager init so no
   lazy init is left to fire -- assert this.
4. **Epoch fencing still applies.** If `CompleteTransaction` is called with a
   state whose epoch no longer matches (someone else resumed/fenced it), the
   comparison yields abort and/or EndTxn returns `PRODUCER_FENCED`/
   `INVALID_PRODUCER_EPOCH`. Surface as terminal -- do NOT auto-recover (point 1).

### 3f. kmsg changes
- `generate/definitions/22_init_producer_id`: bump max 5 -> 6; add
  `Enable2Pc: bool // v6+`, `KeepPreparedTxn: bool // v6+` (request);
  `OngoingTxnProducerId: int64(-1) // v6+`, `OngoingTxnProducerEpoch: int16(-1) // v6+`
  (response). Regenerate.
- `generate/definitions/enums` `ACLOperation`: currently tops out at
  `14: DESCRIBE_TOKENS`. Add `15: TWO_PHASE_COMMIT` so kadm users can grant the
  ACL. (Client enforces nothing; this is purely for kadm management.)
- EndTxn: no change.
- **Version escape hatch (required to ever hit the wire).** Because v6 is
  `latestVersionUnstable`, brokers won't advertise it and `broker.go` ~388 clamps
  us down to the advertised max -- the 2PC fields would silently drop and we'd
  send a normal v5 init. Options:
  - (a) Document that 2PC needs a broker started with `unstable.api.versions.enable=true`
    (makes it advertise v6). No kgo change; simplest; matches "experimental".
  - (b) Add a narrow opt-in that forces InitProducerId to v6 even when
    unadvertised (a force-*up* analog of the existing `pinReq` clamp, for this one
    key). More code; only worth it if a target broker implemented but didn't
    advertise.
  Recommend (a) first; revisit (b) on demand. NOTE: once v6 is GA/stable
  (the un-table trigger), this whole escape hatch becomes unnecessary -- the
  broker advertises v6 and normal negotiation works. Re-evaluate which option
  (or neither) is needed at resume time.

---

## 4. kfake plan (the only place the full flow is testable now)

kfake's txn model (`txns.go`) collapses the broker state machine to `inTx bool` +
`lastWasCommit bool` and completes synchronously (comment ~txns.go:18-36). For
2PC we need just enough of the prepared concept -- NOT the full
PrepareCommit/PrepareAbort log machinery:

1. **InitProducerId v6** (`22_init_producer_id.go`: `regKey(22,0,5)` -> `...,6`):
   parse `Enable2Pc`/`KeepPreparedTxn`.
   - `Enable2Pc=true`: mark the pid `is2PC` and treat its txn timeout as infinite
     (exclude from the timeout scanner in `pids.updateTimer`, ~txns.go:124).
   - `KeepPreparedTxn=true`: if the pid has an ongoing txn, return
     `OngoingTxnProducerId/OngoingTxnProducerEpoch` = current pid/epoch and do NOT
     abort/bump. If none, return -1/-1 and behave like a normal init. (This is the
     half real brokers stub out -- kfake implementing it is what makes resume
     testable at all.)
   - Add an optional control/config knob to emulate a real broker's current
     `UNSUPPORTED_VERSION` on `keepPreparedTxn` and the ACL failure path, so we can
     test franz-go's handling of not-yet-GA broker behavior.
2. **Prepared state**: add `is2PC bool` (+ infinite-timeout exemption) to
   `pidinfo`. No new states: a "prepared" txn is just an `Ongoing` txn on a 2PC
   pid that the scanner ignores. EndTxn already finalizes it (commit/abort) exactly
   as complete requires -- **no EndTxn handler change**.
3. **Timeout** (~txns.go:124-191): skip `is2PC` pids so prepared txns are never
   auto-aborted.
4. **No new RPC handlers.** KIP-939 adds NO RPC (verified against `ApiKeys.java`).
   An earlier exploration mistakenly guessed PrepareAddPartitionsToTxn/PrepareEndTxn
   (keys 67/68) -- that is wrong; do not add handlers.
5. **Persistence (optional, strongest test)**: kfake persists pid entries
   (`persistPIDEntry`). To test true crash-resume across a kfake restart, persist
   `is2PC` + ongoing-txn fields so a restarted cluster can answer
   `keepPreparedTxn=true` with the surviving pid/epoch. Lower priority than 1-3.

Tests (proportionate, `-race -skip ETL` + targeted repros): prepare->complete
commit; prepare->complete abort; prepare->RecoverPreparedTransaction returns the
ongoing pid/epoch and completes; prepared txn survives a timeout window while a
parallel non-2PC txn is aborted; prepared txn blocks a read_committed consumer's
LSO until completion.

---

## 5. KIP-1289 (share-group hierarchical txn acks) -- DEFER regardless

**What it is** (KAFKA-19883 / KIP-1289, early `dev@` discussion as of 2026-02/03):
exactly-once for *share groups* (KIP-932 queues), targeting the Flink/Spark
driver+executor shape. Hierarchical two-level model:
- **BatchTransaction** (driver/coordinator): spans the whole batch, coordinates
  many task transactions, commits atomically across workers.
- **TaskTransaction** (executor/worker): per-worker; acknowledges share records
  in-txn, does phase-1 prepare independently, rolls back to re-deliver on failure.
- Floated *consumer* methods: `beginBatchTransaction(id)`,
  `beginTaskTransaction(parent, taskId)`, `acknowledge(TaskTransaction, record, type)`,
  `prepareTaskTransaction()`, `commitTaskTransaction()`; configs
  `share.acknowledgement.mode=transactional`, `share.batch.transaction.timeout.ms`,
  `share.task.transaction.timeout.ms`. Built on 939 (2PC) + 932 (share groups).

**Status**: early, unsettled. Andrew Schofield's `dev@` review already contests
core API shape -- transactional-id-as-arg vs config (AS2), new RPCs vs adapting
existing txn RPCs + WriteTxnMarkers (AS1), how to fuse a share-ack and a produce
in one txn (AS3). No accepted protocol, no broker implementation.

**Verdict for franz-go: defer.**
1. Stacked deps, none GA: needs 939 (not GA, resume stubbed) + 932 share groups +
   a share-group transactional-ack protocol that does not exist yet.
2. Driver/executor topology is Flink/Spark-shaped, not library-shaped. The
   Batch/Task hierarchy encodes a *distributed scheduler's* coordination model. A
   Go client should expose the *primitive* (2PC prepare/complete from 939) and let
   a Go framework that actually has a driver/executor build the hierarchy on top.
3. No franz-go user demand on record.
4. 939 already serves the realistic Go EOS-sink case (write to Kafka + commit to a
   DB atomically via persisted `PreparedTxnState`). The hierarchical layer adds
   value only for multi-worker queue consumers doing checkpointed EOS -- a shape no
   current franz-go user has asked for.

**Revisit trigger for 1289**: a concrete franz-go user needs share-group EOS AND
KIP-1289 + its broker support have stabilized (accepted protocol, broker impl,
share-group transactional acks GA). Then reassess whether to expose the hierarchy
or just the per-task 2PC primitive and let the caller coordinate.

---

## 6. Build order (when un-tabled)
1. kmsg: InitProducerId v6 fields + `ACLOperation` 15; regenerate. (mechanical)
2. kfake: `is2PC` + infinite-timeout exemption + `keepPreparedTxn` resume; optional
   persistence + emulated-real-broker error knobs. Tests.
3. kgo: `Enable2PC()` opt threads `Enable2Pc` into `doInitProducerID`;
   `PreparedTxnState`; `PrepareTransaction`/`CompleteTransaction`/
   `RecoverPreparedTransaction`; prepared-state gating of `Produce`,
   `BeginTransaction`, `maybeRecoverProducerID`. Document the infinite-timeout/LSO
   caveat and the broker-version requirement.
4. Decide version escape hatch (section 3f) based on whether v6 is stable by then.
5. README KIP table: add 939 as experimental once kfake-green.
