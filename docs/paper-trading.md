# Running the paper-trading prototype

The `paper` command runs buy-and-hold or the quoter on both YES and NO with one shared cash balance. It subscribes to public market data and uses the existing matching engine locally. It has no exchange order-submission API and needs no trading credentials.

This is the first runnable stage of the [implementation plan](paper-trading-plan.md). Network reconnection retains the session in memory. **Process-crash recovery and AWS deployment are not implemented yet.**

## Start with buy-and-hold

Copy `paper.example.toml` to a local configuration and replace both asset IDs with the two outcome tokens of one binary market. The adapter verifies the two initial books belong to the same condition; the labels themselves come from your configuration. It waits for fresh books for both outcomes before either strategy runs.

```sh
cargo run --release --locked -- paper --config paper.toml --record-dir runs/buyandhold-001 --duration-secs 300
```

Buy-and-hold attempts one FOK buy of the configured size on each outcome, then holds. A rejected order is reported and is not automatically retried. Capital and reservations are shared; the two outcomes cannot each spend the full balance. Separate strategy instances keep per-outcome quote/cancel memory isolated, while order IDs are globally unique within the session.

Review the recorded fills, shared cash, positions, fees, and rejections. Change `strategy` to `quoter` for sustained testing, using a new output directory:

```sh
cargo run --release --locked -- paper --config paper.toml --record-dir runs/quoter-001 --duration-secs 0
```

The default duration is 300 seconds; zero runs until Ctrl-C/SIGTERM. A clean stop preserves the final positions and last valid marks in the report; it does not redeem the market or claim working orders filled. The quoter joins each outcome's bid and offers inventory it actually owns. It does not mint, merge, redeem, or create synthetic YES/NO trades.

All execution parameters in the example are assumptions. Set fees, transit, taker delay, and size deliberately. Current limitations inherited from the backtester include minimum-order-size/tick enforcement, L2 queue uncertainty, and crossing-limit taker-hold approximation. The prototype stops on a tick-size change or unsupported relevant protocol message so it cannot silently continue under changed rules. Automatic metadata discovery and verified market resolution belong to the next stage.

## Connection losses

The socket worker sends the documented text `PING` every ten seconds, handles `PONG`, and operates independently of strategy execution and file writes. Connection establishment, writes, unanswered heartbeats, and initial snapshots have timeouts. Reconnects use capped exponential backoff with jitter.

On disconnect, an invalid reconstructed book, excessive input backlog, or queue overflow:

1. Suspend simulated executions and mark valuation data invalid.
2. Keep cash, positions, strategy memory, orders, and reservations.
3. Record a gap and reconnect. Discard old connection messages and reconstructed books.
4. Wait for fresh books for both outcomes. Reset resting queue estimates to the displayed depth, preserving the configured queue-model behavior.
5. Apply overdue expiries/cancels; pending orders otherwise become eligible against the fresh book. Continue the strategy without replaying missed trades or generating a burst of old wakeups.

Possible fills during the gap are **unknown**, not zero by observation. The simulator leaves the account unchanged over that interval. Records and the final gap count preserve this modeling intervention. Quiet markets are not considered disconnected merely because their book stops changing.

The queue holds at most 64 frames, each capped at 1 MiB; a backlog over two seconds requests resynchronization. Protocol/invariant failures not classified as recoverable, and storage failures, stop the command with an error.

## Recording and replay

Every output directory must be new or empty. A creation lock prevents two processes claiming it. Files include:

- `manifest.json`: format version, strategy/asset configuration, and simulation assumptions.
- `events-000000.jsonl`, etc.: normalized inputs, raw frames, asset-attributed order/fill events, and the final summary. Segments rotate at 64 MiB or one hour when writing.
- `events-*.jsonl.part`: the active segment, renamed only after it is closed/synced.
- `status.json`: periodically replaced status including readiness, data validity, shared cash, reservations, positions, last mark timestamps, P&L, fills, rejections, and gaps.

Only one segment and small status structures are held in memory. Completed captures can be replayed through the same strategies and matching engine:

```sh
cargo run --release --locked -- paper --replay runs/buyandhold-001 --record-dir runs/buyandhold-replay-001
```

Replay uses the manifest configuration and recorded receive-time clock, timer, and gap inputs. It is intended for the same engine/strategy revision. The capture format version checks compatibility of records, not behavioral compatibility of future code versions. Compare fill/order records and final status. A partial capture, missing input sequence, or missing clean stop is rejected; the prototype does not claim to restore interrupted sessions.

These JSONL files are streaming diagnostic/replay artifacts. Per-event crash durability, checkpoints, resumable strategies, Parquet export/rotation, S3 upload, and remote monitoring remain subsequent work. A new process starts a new account/session and must use a new directory.

## Timing and interpretation

Live events use a monotonic local receive-time clock anchored to UTC. Raw exchange timestamps remain inside captured frames. Feed latency is already present in receipt, so the live adapter disables artificial perception/feed delay and applies configured outgoing transit/hold only. Messages sharing a batch are normalized deterministically; this is not a reconstruction of true exchange ordering.

Fills use the latest locally observed depth and public trade prints. Late exchange messages, missing trades, unobservable queue position, and our hypothetical orders' absence from the actual market limit realism. Marked P&L is an estimate; a clean stop reports marks with their actual last-update times. Replay equality proves reproducibility of this simulation, not actual trading profitability.

## Validation and next stages

The tests exercise shared funding/reservations, outcome isolation, maker queue reseeding, quiet-market deadlines, gap preservation, replay equality, capture ownership, wrong-market rejection, and a local WebSocket server that checks subscriptions/heartbeats and forces reconnection. The WebSocket test needs permission to bind a localhost socket; it does not contact Polymarket.

Next: per-asset rule/lifecycle handling, richer continuous analytics, a long soak, EC2 packaging/monitoring, then durable checkpoints and tested process recovery. Do not configure a restart supervisor to imply account continuity before that recovery stage exists.

Validation on 14 November 2026: the release workspace suite passed 278 tests;
seven pre-existing capture-dependent tests remained ignored. Debug regressions,
Clippy with warnings denied, formatting, and API documentation checks passed.
A 30-second public-feed buy-and-hold smoke test produced one simulated fill on
each outcome. A 60-second quoter smoke test maintained both quotes without maker
fills in that interval; deterministic tests cover its maker buy/sell cycle.
Both live captures replayed byte-for-byte to the same event records and summary.
The smoke tests explicitly used zero simulated fees and do not assess profitability.
