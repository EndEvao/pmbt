# Live paper trading implementation plan

Status: first runnable stage implemented; production hardening, AWS deployment, and crash recovery remain planned. See `paper-trading.md` for the implemented command and limitations.
Prepared: 9 November 2026. Updated 14 November 2026 with accepted decisions and the first implementation stage.

Accepted decisions: trade both YES and NO with shared capital; validate buy-and-hold first, then the quoter; retain working orders through connection gaps, reset queue estimates on fresh books, and flag possible missed fills as unknown; deliver in stages; target one EC2 instance initially. These decisions supersede the original single-token scope and order-invalidation recommendation. The original estimates below predate the two-outcome expansion and need reassessment after prototype validation.

Implemented now: shared YES/NO accounting, the live command, basic reconnect/resync with retained orders, bounded input queues, rotated JSONL captures, status snapshots, and offline replay. These include preliminary pieces of M2; they do not establish the M2 soak/deployment or M3 recovery acceptance criteria.

## 1. Outcome and scope

Run an existing strategy against a live Polymarket market-data stream, using the existing simulation engine to generate local orders, fills, positions, and P&L. Preserve enough information to explain and reproduce results, operate unattended on AWS, and ultimately restore a session after a crash.

The accepted scope is **both outcome tokens of one binary market sharing one cash and reservation ledger**. Each outcome has independent book/order/strategy context, processed serially through the same execution kernel. The original estimates assumed one token; shared-capital correctness is now included in the first-stage tests.

| Milestone | Cumulative estimate | Deliverable |
|---|---|---|
| M1: working prototype | 2–4 engineering days | Both outcomes, existing strategies, live simulated fills, local records, clean stop |
| M2: reliable service | 5–10 engineering days, approximately 1–2 working weeks | Reconnection, explicit gap policy, continuous recording, monitoring, repeatable AWS deployment |
| M3: recoverable session | 10–20 engineering days, approximately 2–4 working weeks | Versioned checkpoints, durable replay, strategy restoration, tested crash recovery |

Estimates assume one engineer familiar with Rust and this repository, a usable AWS account, and existing deterministic strategies. They are cumulative, not three estimates to add together. Treat the lower bounds as optimistic. Soak tests need elapsed time and may extend the calendar schedule. Novel strategies, a dashboard, multi-market shared capital, regional disaster recovery, or a change to the execution model require re-estimation.

“Seamless recovery” means restoring our own committed state without duplicate fills or resetting strategy memory. It cannot reconstruct unobserved exchange activity. If market data was missed, restoration is followed by the same explicit gap policy used for a network outage.

## 2. Existing foundations and required changes

| Current component | Reuse | Required work |
|---|---|---|
| `crates/pmbt/src/feed.rs` | `EventSource`, book/trade/time events, conformance tests | Live source, clock contract, separate health/control inputs; do not treat transient disconnection as end-of-feed |
| `crates/pmbt/src/clob/decode.rs` | Raw CLOB frame decoding | Handle heartbeat text before JSON; validate current payloads; support lifecycle events deliberately |
| `crates/pmbt/src/clob/book.rs` | Delta folding, snapshots, best-price consistency checks | Reset on connection generation changes; expose invalidation; keep exchange and receive timestamps |
| `crates/pmbt/src/engine.rs` | Matching, reservations, fees, orders, timers | Extract mutable session state from `run_recorded`; add step/control boundary, execution-health gate, nonterminal pause |
| `crates/pmbt/src/strategy.rs` | Existing market/time callbacks | Define session-clock semantics; later add versioned persistence and gap recovery behavior |
| `crates/pmbt/src/report/stream.rs` | Incremental Parquet buffers and accumulators | Rotate files, publish live status, preserve session-wide counters across rotations |
| `crates/pmbt-cli/src/main.rs` | Command entry point and existing strategy selection | `paper`, configuration validation, stop/resume/status behavior |
| `.github/workflows/ci.yml` | Existing Linux checks | Deterministic live-feed tests, release artifacts, recovery tests |

The engine already supports another event source, but the complete service needs engine lifecycle changes. In particular, current run-loop locals include pending orders, latest books, depleted depth, perception buffers, and counters. They must become explicit state before they can be checkpointed. Current end-of-feed handling expires orders and may settle a position; a network failure or checkpoint must not invoke that behavior.

## 3. Main design choices

User decisions above are accepted. Other entries remain proposed defaults for subsequent stages.

| Decision | Recommendation | Tradeoff / alternative |
|---|---|---|
| Instrument scope | Both YES and NO with one shared portfolio | Independent outcome contexts; multiple prediction markets remain out of scope |
| Live clock | Receive-time session clock, monotonically advancing; preserve original exchange time separately | Exchange-time ordering with a reorder buffer is useful for delayed research replay, but adds latency and cannot guarantee arbitrarily late events arrive first |
| Execution latency | Real feed arrival supplies perception delay; simulate only configured order transit and applicable exchange hold after a decision | Reusing historical feed-delay presets unchanged double-counts delay |
| Outage policy | Suspend execution, retain working orders/cash/positions, refresh both books, reset queue estimates, flag unknown missed fills, resume | Keeping hypothetical orders alive across missing trades leaves their fills and queue position unknowable; strict research mode may end the session instead |
| Recovery promise | Recover exactly through durable local history, then explicitly handle any missing market-data interval | Continuous exchange-equivalent recovery needs a separately reliable capture/backfill source; outside this scope |
| Strategy persistence | Versioned snapshots for supported strategies plus deterministic replay | Arbitrary third-party strategy memory cannot be safely restored without an explicit contract |
| Packaging | A new `pmbt-live` workspace crate for transport/runtime/storage; small generic lifecycle changes in `pmbt` | Keeps networking dependencies out of backtest-only consumers; introduces one crate boundary |
| Concurrency | One async network worker and one serial engine owner connected by bounded queues | Simplifies ordering and accounting; avoid locking the portfolio from multiple workers |
| Durable storage | Versioned append-only journal plus atomic checkpoints on EBS; Parquet is derived analysis output | SQLite transactions are a reasonable substitute if implementation review favors them; choose one authoritative store, not both |
| AWS deployment | Linux binary, EC2, `systemd`, persistent EBS, S3 artifact backups, CloudWatch | ECS/Fargate offers managed task replacement but still needs explicit durable state and single-writer ownership |
| Live visibility | CLI status, structured logs, periodically updated JSON status, metrics | Browser dashboard can be added independently after service behavior is stable |
| Market rules | Resolve supported per-asset rules at startup, retain provenance, process changes explicitly | Category defaults remain available only as clearly labeled approximations |
| Market close | Stop placements on a verified closed/nontrading state; settle only on verified resolution | Socket closure or process shutdown is not market resolution |

### 3.1 Clock and ordering contract

Each input envelope should carry a session ID, connection generation, local sequence number, receive UTC time, receive monotonic time, original exchange time when present, and event kind. Journal the normalized event actually given to the engine, not just the raw socket frame.

Use a monotonic clock anchored to UTC for session scheduling. Detect material wall-clock drift. Across restart, reconstruct a new clock anchor from persisted state and trusted current time; never serialize a process-local monotonic-clock handle and pretend it survives reboot. A backwards or implausible clock jump blocks execution until reconciled.

For M1, preserve receive order between frames. Within a single received batch, deterministically emit applicable trades before its resulting book snapshot where required by the engine's accounting contract. Use sequence numbers for stable ordering. If the legacy microsecond API requires distinct normalized timestamps between frames to preserve its tie contract, allocate them monotonically and record that normalization explicitly. Never claim this restores true exchange ordering.

The scheduler must arbitrate pending engine deadlines and already-received messages. Drain eligible queued messages before a later timer; do not let a timer jump ahead of data already received before its deadline. A genuinely late exchange event is processed as newly observed information, with its original timestamp retained. Serious book inconsistency triggers resynchronization rather than silent timestamp clamping hiding the issue.

Live `MarketState.timestamp` would represent session time. Document this distinction from historical exchange-time replay and expose exchange time separately for diagnostics or strategies that need it. Add a live-mode conformance suite without weakening existing replay guarantees.

Set artificial feed/perception delay to zero for live observation mode. Schedule simulated order arrival from decision time plus transit and configured hold. This still matches against the latest locally observed book, not an omniscient view of the exchange. Preserve that limitation in results. Comparing a live journal replay should be exact; comparing an exchange-timestamp historical backtest need not be.

### 3.2 Connection and gap state machine

Normal lifecycle:

```text
STARTING -> CONNECTING -> SYNCING -> WARMING -> RUNNING
                            ^                    |
                            |                    v
                         BACKOFF <- DEGRADED / GAP

Any state -> STOPPING -> STOPPED
Invalid config / corrupt durable state -> FAILED
Verified market closure -> CLOSED -> optionally SETTLED
```

Heartbeat success proves the connection responds; it does not prove every subscribed book is correct. Track connection liveness, book validity, processing backlog, and last market update separately. A quiet market can be healthy. Best-price agreement also cannot prove that all deeper levels or trades are complete.

On disconnect, queue overflow, resync failure, or a confirmed invalid book:

1. Mark the input stream degraded before further execution; a reserved control path or shared health generation must make this possible even when the data queue is full.
2. Suppress new placements and all maker/taker fills against untrusted data. Expiries and administrative cleanup may still occur without executing trades.
3. Record the last trusted input and gap detection time. Actual loss may begin earlier than detection; distinguish known and uncertain boundaries.
4. Under the accepted policy, retain pending/resting orders and reservations. Reset queue estimates only after fresh snapshots. Apply overdue cancels/expiries before resumed execution; never claim to know gap-period fills. The account is held unchanged by modeling choice, not evidence that nothing happened.
5. Preserve cash, inventory, realized P&L, and fees. Mark unrealized equity stale while the book is invalid. Never invent gap-period fills or liquidation.
6. Discard the old reconstructed market book and any old connection-generation messages. Reconnect, resubscribe, and wait for a validated full snapshot.
7. Reset or rewarm gap-sensitive strategy history; notify it about the interruption and any orders expired/cancelled on resume. Resume only after the configured readiness conditions hold.

For existing stateless/simple strategies, warming may be immediate. A strategy using a time window must specify how much fresh history it requires. Missed wakeups must not all fire as a burst after recovery; collapse or discard them under a recorded policy and let the strategy reschedule.

An optional `stop` gap policy ends a research segment instead of automatically resuming. Both policies must expose gap duration/count and policy interventions alongside P&L so a run with missing data is never presented as uninterrupted performance.

## 4. M1 — two-outcome prototype, cumulative days 2–4

### Work packages

**A. Contracts and a small engine lifecycle extraction — approximately 0.5–1 day**

- Record clock, tie ordering, gap, and shutdown choices as short architecture decisions.
- Extract run-local mutable state into an internal session object and introduce a single-step boundary. Keep existing `Engine::run` / `run_recorded` wrappers and output behavior.
- Distinguish event processing, status inspection, pause, and terminal finalization. Avoid adding persistence serializers yet.
- Add a feed-health gate that also covers pending orders and timer-triggered execution. Initially a disconnect can stop the prototype rather than reconnect.
- Verify existing historical behavior stays unchanged using the existing regression suites.

**B. Live adapter — approximately 0.5–1 day**

- Add `pmbt-live`; select maintained Rust WebSocket/TLS and async runtime dependencies during implementation and lock versions.
- Connect to the public market channel, subscribe to the configured asset, and wait for a full book before enabling strategy actions.
- Handle text `PING`/`PONG` outside JSON decoding, and WebSocket control frames through the transport library.
- Reuse `decode_frame` and `BookBuilder`; adapt trades and book snapshots into normalized session inputs. Reject unrelated assets before they reach the single-market engine.
- Drive real timers even when the market sends no updates. Keep networking heartbeats independent of strategy callbacks.
- Capture raw frames and normalized input sequence for diagnosis and later replay.

**C. CLI, simulation configuration, and output — approximately 0.5–1 day**

- Add a command with the intended shape `pmbt paper --config <paper.toml> --record-dir <dir>`; the configuration names both outcome IDs and the strategy.
- Validate capital, asset identity, rule assumptions, simulation settings, and output ownership before connection.
- Start with existing buy-and-hold and quoter/accumulator strategies to exercise both taker and maker paths.
- Show connection state, cash, inventory, realized/unrealized P&L, fees, open orders, and last input age. Clearly label all execution as simulated.
- Support a maximum run duration and SIGINT/SIGTERM. Flush records on normal stop; do not settle merely because the process stopped.

**D. Verification and small live run — approximately 0.5–1 day**

- Mock-server scenarios: initial snapshot, deltas, trade, quiet interval, heartbeat, malformed input, disconnect.
- Check that a trade cannot fill an order submitted after that trade was observed.
- Check no strategy action before initial synchronization; no fill after a detected outage.
- Verify fee/funding accounting and reservation release through the existing engine assertions/tests.
- Run a short live smoke test and replay the normalized captured session offline. Use deterministic fixtures to prove maker fills; do not depend on the public market producing a particular trade.

### M1 acceptance

- Both configured assets produce observable simulated orders and fills using existing strategies.
- Both taker and maker paths pass deterministic examples, including a timer during market silence.
- Replaying normalized inputs reproduces order/fill outcomes for the same strategy/configuration.
- Disconnect stops safely and leaves a clearly incomplete/terminated session record.
- Existing backtest checks pass. No exchange order-submission path or trading keys are needed.

M1 explicitly has no automatic session recovery. A fresh invocation starts a new named session.

## 5. M2 — reliable service, cumulative days 5–10

### Work packages

**E. Reconnect, synchronization, and overload behavior — approximately 1–2 days**

- Implement the state machine and default gap retention policy above.
- Use exponential reconnect backoff with jitter and a cap; reset the backoff after sustained health rather than immediately on socket open.
- Introduce configurable connection, heartbeat-response, and initial-snapshot timeouts. A provisional response deadline is 20 seconds after an unanswered ping; validate in live testing rather than treating it as an exchange requirement.
- Retry transport failures. Classify invalid configuration, unsupported protocol, and corrupted state separately to avoid endless restart loops.
- Use connection generations to reject messages from obsolete sockets. Invalidate before resubscription, even if the old top of book still looks plausible.
- Prefer a fresh snapshot on a new subscription. If an HTTP snapshot fallback is added, explicitly reconcile buffered deltas; never race an unversioned HTTP snapshot against a mutating stream and assume consistency.
- Bound buffers by both count and bytes, monitor oldest queued input age, and declare a gap on overflow or excessive lag. Do not silently drop price changes or trades.
- Avoid aggressive deduplication by price/size/timestamp: two identical trades can be real. Only suppress duplicates when an identity is demonstrably sufficient; do not assume transaction hash alone uniquely identifies every trade event.

**F. Rules and lifecycle handling — approximately 0.5–1 day**

- Load or explicitly configure fee parameters, tick size, minimum order size, and delay assumptions with source/time provenance. Verify current market/API support during implementation.
- Enforce supported per-asset constraints at placement and execution. Do not silently round a strategy's price; reject or apply an explicitly configured rounding rule.
- Apply tick/rule changes through ordered control events; define treatment of existing orders. Unknown relevant rule changes pause execution.
- Extend decoding for enabled lifecycle events; preserve and diagnose unsupported events rather than silently interpreting them as harmless.
- Handle verified closure/resolution idempotently. If resolution cannot be confirmed, finish with an unresolved position, not a guessed redemption.

**G. Continuous recording and live status — approximately 1–1.5 days**

- Rotate Parquet segments by duration and maximum size, closing footers before publishing completed segments. Start with hourly rotation and a size cap, then tune from observed volume.
- Keep session-wide cash/P&L/order counters separate from per-segment buffers; file rotation must not reset strategy or engine state.
- Add a versioned append-only input/control journal with sequence numbers, length framing, and checksums. This becomes M3's recovery source; raw socket captures remain diagnostic.
- Give segments deterministic IDs and publish manifests atomically. Readers ignore unfinished segments; a missing footer must not become a falsely complete artifact.
- Publish periodic status JSON atomically, including data validity, gap counters, current session ID, and latest committed/exported sequence.
- Treat durable-write failure or full disk as a stop-execution condition. Pause export only if authoritative journaling still works and bounded backlog remains available.
- Compute live status incrementally. Generate historical HTML reports from closed segments/offline replay rather than retaining the entire live history in RAM.
- Add retention and upload retry policies. Keep recovery-required journal ranges until a durable checkpoint/backup makes deletion safe.

**H. AWS packaging, monitoring, and runbook — approximately 0.5–1.5 days**

- Build a reproducible Linux release artifact in CI with a source revision and configuration/schema version. Select CPU architecture deliberately; do not copy a macOS binary to EC2.
- Supply versioned infrastructure configuration, installation/bootstrap script, and a `systemd` unit with graceful stop, restart backoff, and restart-on-failure.
- Use a dedicated service user and a retained EBS data volume. Configure deletion behavior explicitly and test mounting after reboot.
- Use an instance role for S3/CloudWatch access and Systems Manager for administration where available. Scope access to the session bucket/prefix and required metrics/logs.
- Require outbound connectivity to data endpoints; the worker needs no public application port. Select a network layout without accidentally paying for unnecessary NAT infrastructure.
- Ship logs, resource metrics, and service-health metrics to CloudWatch; archive closed segments to S3. Set log retention and a cost budget before a long run.
- Document start, stop, inspect, upgrade, rollback, disk recovery, configuration changes, and locating artifacts. Avoid overlapping writers during deployment.

### M2 monitoring and acceptance

Monitor connection state, reconnect attempts, unanswered heartbeat age, book validity, last-update age, input queue depth/bytes/oldest age, engine processing lag, journal/export lag, recorder errors, memory, CPU, free disk, and gap interventions. A healthy connection with a quiet book is distinct from an invalid feed.

Provisional alerts: immediate recorder failure or corrupt data; prolonged degraded state beyond 60 seconds; repeated reconnects; queue lag over the configured trading tolerance; sustained low disk space. Final thresholds should follow measured event rate and strategy cadence, not arbitrary submillisecond targets.

- Complete a 48–72-hour soak with bounded queues and no unexplained memory growth. Account for intentionally retained session state and rolling buffers.
- Force repeated disconnects, missed pongs, malformed frames, book inconsistencies, and producer overload; recover or stop according to policy without stale-book fills.
- Preserve cash/positions across connection gaps; show retained working orders, queue resets, and gaps in output.
- Verify every completed recording segment can be read while the next segment is being written.
- Deploy to AWS from the documented configuration and verify service reboot/start behavior and S3 uploads.
- Measure CPU, memory, disk growth, and bytes uploaded before choosing a long-term instance size or quoting monthly cost.
- Before M3, a process crash requires a new session and explicitly marks the previous one interrupted. Do not silently reset balances while presenting the same session identity.

## 6. M3 — durable recovery, cumulative days 10–20

### Work packages

**I. Complete and version the checkpoint state — approximately 1.5–3 days**

Checkpoint at an engine-event boundary, including:

| State group | Required content |
|---|---|
| Identity | Session ID, asset ID, source revision, config hash, rule version, checkpoint schema, committed sequence |
| Portfolio | Initial capital, cash, positions, cost basis, realized P&L, fees |
| Orders | Next order ID, pending orders/deadlines, resting orders/remaining sizes, queue estimates, reservations, pending cancels |
| Engine clock | Last normalized time, requested wakeups, latency configuration, clock-anchor metadata |
| Books and accounting | Latest observed book, depleted execution depth, trade depletion/queue reconciliation state, perception buffers if applicable |
| Strategy | Type/version, parameters, internal history, outstanding-order references, RNG state if used, warmup state |
| Lifecycle | Feed validity, gap state, market closure/resolution state, exactly-once settlement marker |
| Recording | Session counters/accumulators, journal sequence, output IDs/cursors needed to regenerate derived artifacts |

Implement explicit serializable DTOs instead of serializing trait objects or arbitrary Rust memory layouts. Preserve numeric values losslessly. Verify conservation and reservation invariants after loading. Book state may be restored for replay but must be considered untrusted for new live execution until resynchronized.

Write checkpoint to a temporary file, flush and sync it, atomically rename it on the same filesystem, and sync the containing directory. Retain a previous valid checkpoint and enough journal history to replay from it. Checkpoint cadence should be configurable; start with 30 seconds or an event-count threshold and measure overhead.

**J. Strategy recovery contract — approximately 1–2 days**

- Add an object-safe, opt-in persistence capability or companion trait with stable strategy ID/schema version and snapshot/restore operations.
- Implement persistence for supported built-in strategies, including quote IDs awaiting cancellation and any “already bought” flags.
- Define reset/rewarm behavior separately from restore: restoring pre-crash memory does not make a rolling market-data window valid after a data gap.
- Require replay-deterministic behavior: use supplied clock and seeded/persisted RNG; external data must enter through journaled events. Direct wall-clock reads, unrecorded network requests, or nondeterministic side effects disqualify exact replay.
- Refuse `resume` for unsupported or incompatible strategy/config versions. Offer an explicit new session instead of a silent reset.

**K. Durable journal and replay protocol — approximately 2–3 days**

Define the durability boundary before optimizing throughput:

1. Assign a stable sequence to each normalized market, timer, metadata, health, gap, or administrative input. Journal all information that can influence a strategy decision.
2. Append and durably persist the input before applying it. Use one ordered engine owner.
3. Apply it deterministically in memory, staging resulting order/fill/status records.
4. Append the resulting records and an event commit marker, then durably sync before publishing outputs as committed or advancing to the next input. On failure, stop and restore from the journal rather than continuing with uncommitted memory.
5. Identify derived outputs by session ID, input sequence, and output index. Replays/upserts must not count the same fill twice.
6. Checkpoint only at a committed boundary. On recovery, load the newest valid checkpoint and replay the retained journal suffix, verifying committed outcomes. An input with no commit may be deterministically completed once; discard partial trailing records only under the journal format's explicit recovery rules.
7. Regenerate missing Parquet/status artifacts from committed journal data. Publishing artifacts before updating an export cursor must remain idempotent after a crash.

Start with synchronous per-event durability for correctness, then measure it against live event throughput. If batching is needed, define atomic batch publication and its durability boundary explicitly. Do not advertise zero data loss for received frames still only in memory. Local sequence numbers guarantee local replay order; they do not establish that the upstream stream had no gaps.

Handle a torn final record differently from corruption in the middle of committed history. The latter is a hard failure or an explicitly reported restore from an older verified backup. Never silently skip it and continue P&L.

**L. Resume lifecycle, ownership, and operational recovery — approximately 1–2 days**

- Add explicit `--resume <session>` behavior and auto-resume configuration for the AWS service after M3 is accepted.
- Acquire an exclusive session-directory/process lock before opening journals. Store ownership diagnostics. A local lock is sufficient only for the chosen single-host/single-volume setup; distributed failover needs a lease/fencing design and is outside this estimate.
- Verify config, strategy, and schema compatibility, restore state, replay committed history, and reconcile output cursors before opening execution.
- Record an interruption/gap when capture continuity cannot be demonstrated. Apply gap retention and queue-reset policy to restored working orders, retain positions, synchronize a fresh book, and rewarm before resuming.
- Test graceful service stops using checkpoint/pause semantics rather than terminal expiry/settlement. Explicitly ending a paper session remains a separate operation.
- Make a restore procedure for a replacement instance attaching the retained EBS volume, and a separate older-backup restore path from S3. Report the latter's possible loss window.

### M3 acceptance and recovery targets

- With an identical fully journaled input stream, killing and restoring the process at multiple boundaries produces the same cash, inventory, order IDs, fills, fees, counters, and strategy state as an uninterrupted run.
- With missing market data, recovery produces an explicit gap and documented queue resets, not a falsely continuous run.
- Crash tests cover before/after input sync, during state application, before/after commit sync, during checkpoint writing/rename, and during segment/manifest publication.
- A corrupt newest checkpoint falls back only when the previous checkpoint and required journal suffix validate. Incompatible schema/config fails clearly.
- A second writer cannot mutate the same session. A repeated resolution event cannot settle twice.
- Recovery-point target: no loss of locally committed inputs/outcomes when the durable volume survives. This excludes socket frames never durably received and loss of the entire volume.
- Recovery-time target: start reconnecting within 60 seconds of process restart at the tested journal size. Time until trading resumes additionally depends on network health, initial snapshot, and strategy warmup; measure and report it separately.
- Backup recovery-point target: initially five minutes if upload cadence and checkpoint/journal coverage support it; validate through a restore drill. This is a separate, weaker promise from local crash recovery.

## 7. Proposed change sequence and dependencies

| PR | Scope | Depends on | Main review evidence |
|---|---|---|---|
| 1 | Clock/lifecycle contracts and engine state extraction | Design choices | Existing replay parity, order/timer regressions |
| 2 | Live transport, bounded bridge, two-outcome CLI, basic capture | 1 | Mock-server tests, recorded-session replay, live smoke test |
| 3 | Health transitions, gap retention, reconnect/resync | 2 | Fault injection, no stale fills, reservation invariants |
| 4 | Per-asset rules, metadata/lifecycle events | 2–3 | Tick/rule changes, closure without false settlement |
| 5 | Rotated recording, versioned journal, status/metrics | 3 | Rotation readback, storage failure, bounded-memory run |
| 6 | AWS release/deployment and operations guide | 5 | Deployment rehearsal, reboot, 48–72-hour soak |
| 7 | Checkpoint DTOs and supported strategy persistence | 1, 4–5 | Round-trip state tests, version mismatch handling |
| 8 | Durable commit/replay and resume ownership | 7 | Crash matrix, uninterrupted-vs-restored equivalence |
| 9 | Automatic resume, backup restore drill, final documentation | 6, 8 | AWS crash/reboot/restore evidence |

Keep PRs 1–2 limited enough to preserve the prototype estimate. If the state extraction exposes broad engine coupling, re-estimate at that point instead of hiding foundational work in M3. Journal schema and checkpoint boundary design start early even though full recovery ships last.

The critical path is: clock and lifecycle semantics → working live source → honest gap handling → durable recording → explicit state serialization → recovery proof. AWS setup is not the dominant engineering risk.

## 8. Validation strategy and estimate risks

Use the existing Rust formatting, Clippy, debug/release, library, CLI, and documentation checks. Add deterministic fake-clock/mock-WebSocket tests for the new behavior; public-network tests should be opt-in so CI does not depend on a quiet or unavailable market.

Essential scenarios: initial partial data, unrelated asset, empty book side, same-time trade/book/timer, late exchange timestamp, scheduled cancel racing a trade, partially filled maker at gap onset, queue overflow, a quiet healthy market, repeated resync failures, malformed relevant messages, full disk, interrupted rotation, rule change, market close, repeated resolution, checkpoint corruption, process restart, and second-writer rejection.

Replay parity means the normalized live journal reproduces itself. It does not mean paper fills equal actual fills, or that an exchange-time backtest will match a receive-time live run. Queue inference, missing data, latest-observed-book execution, fees/delay configuration, and the existing crossing-limit taker-hold approximation remain named modeling assumptions.

Main schedule risks:

- Live time ordering conflicts with existing assumptions in strategies or the event-source conformance suite.
- The current API payload/rule surface differs from captured historical data; integration must verify it.
- Durable sync throughput falls below peak event rate, requiring careful batching.
- Strategy state contains implicit wall-clock/random/external dependencies.
- Full-depth capture/storage volume is much higher than expected.
- AWS permissions or account setup delays the deployment/restore drills.
- Guaranteeing continuity through missed exchange events is requested; that requires a separate capture/backfill design and a new estimate.

Do not broaden the initial project to real-money execution, multiple active engines sharing capital, cross-host failover, a UI dashboard, or a new market-impact model. Those can build on this work but change its acceptance criteria and schedule.

## 9. External references checked for this plan

- The public market stream documents text `PING` every 10 seconds with `PONG` responses, asset subscriptions, book/delta/trade events, and opt-in lifecycle messages. Keep these separate from authenticated order heartbeats. [Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data).
- ECS services replace failed tasks, but that facility alone does not restore our application state. [Amazon ECS services](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html).
- EBS provides persistent block storage independent of the running instance; volume attachment is constrained by Availability Zone. Configure retention and backups explicitly. [Amazon EBS volumes](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volumes.html).

Protocol timing is externally documented; timeout thresholds, service layout, recovery targets, storage format, and implementation estimates above are proposed engineering choices.
