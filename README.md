# pmbt — a Polymarket simulation engine

Backtests trading strategies over **full order-book history** from Polymarket's CLOB — and is built, seam by seam, to grow into a live paper-trading engine on the same core.

> **Status: active work-in-progress.** The historical backtester is complete and heavily tested; live paper trading and alternative-data feeds are the next milestones (see [Roadmap](#roadmap)).

```console
$ cargo run -- run buyandhold data/telonex/<token>_<date>_book_snapshot_full.parquet \
    --fill sweep --resolution 1

buyandhold
  capital     $    10000.00  ->  $10064.00
  net P&L     $       64.00   (+0.64%)
    trading   $        0.00
    settled   $       64.00
    fees      $       -0.00
  settlement  100 shares redeemed at $1.00
  fills       1       (0 maker, 1 taker)
```

That run is real: a full day of book snapshots for a resolved market, a 100-share position lifted at the touch, held to resolution, and redeemed at $1 — the historically true outcome, verified against the exchange catalogue. (Trading and settlement P&L are reported apart because redemption is not a trade.)

## What it does today

- **Replays full-depth order books.** Streams Telonex-format Parquet (`book_snapshot_full`: every book change, all levels, exchange-time microseconds) in constant memory. One file = one token's complete market history, paired with its trades tape.
- **Runs on an event clock, not a snapshot loop.** Time advances to the engine's next *appointment* — an order's latency window closing, a cancel landing, a GTD expiring, a strategy-requested wake-up — never by "wait for the next snapshot," which silently imports information from the future. With nothing scheduled the clock never ticks, so a marketable-only run walks the feed exactly as before. Closing this removed four separate lookaheads.
- **Plays the exchange, not just the tape.** Every order faces the gates Polymarket would apply: no shorting, no spending cash you don't have — funding checked against the *actual walked cost* of the fill, not the touch price — plus the two-phase submit-then-execute lifecycle of a real taker delay.
- **Marketable and resting orders.** Four time-in-force types (GTC, GTD, FOK, FAK — Polymarket's set, no more). Depth-aware fill models: `naive` (full size at the touch, the optimistic baseline) and `sweep` (walks the ladder level by level, stopping where a limit stops crossing). A resting order joins the book and is filled from the **trades tape**.
- **Realistic maker fills via a queue model.** Polymarket publishes only L2 (aggregate size per price), so the line ahead of a resting order is *inferred*: seeded from displayed size, drained by trades, with shrinkage the trades don't explain attributed to cancellations. Three positions — `proportional` (default, neutral), `front` (upper bound), `trades-only` (lower bound) — turn "how optimistic is my maker P&L?" into a measurable range. Cancels are messages in flight, so a trade can pick off an order before its cancel lands.
- **Latency and perception.** Orders wait out a configurable decision-to-execution delay (feed + transit + Polymarket's real per-category taker delays of 250 ms–3 s) and execute against the book as it stands *when they land*. Optional **perception delay** shows the strategy each snapshot as late as a real observer would receive it. Every effect toggles per run or per universe.
- **Taker fees.** Polymarket's real taker-only fee — `qty × rate × p(1−p)`, per category tier — deducted per fill and reported on its own line. Makers pay nothing.
- **Terminal settlement.** `--resolution 0|1` states what the token redeemed at; the position settles through the ledger after the feed drains — never through the order pipeline, because redemption is not a trade. Trading and settlement P&L are reported separately.
- **Batch universes & realism comparison.** `pmbt batch` runs a fresh engine per market across a TOML universe and aggregates. `pmbt compare` runs one strategy over one feed under a matrix of configs — a cumulative friction ladder whose step-to-step P&L deltas are each realism component's cost.
- **Reports and data export.** `--report` writes a self-contained HTML page (price/PnL, depth ladder, trade blotter, stats); `--record-dir` writes the full time series as Parquet tables for pandas (`scripts/load_run.py`).
- **Converts raw websocket captures into backtestable data.** A transport-free `clob` module (typed wire decode + delta-folding book builder) turns recorded CLOB streams into Telonex-parity Parquet — book **and** trades. Dropped timeframes are handled honestly: every `price_change` is checked against the exchange's own inline best-bid/best-ask, and a disagreeing book is quarantined until the next full-book resync. Gap statistics are reported loudly.

## Why the numbers can be trusted

- **No lookahead.** The event clock advances to the exact instant of each appointment, so an order never fills on a book update that hadn't happened yet, a delayed snapshot never arrives early, and a resting order is never filled by a print from after it was pulled.
- **Prices are fixed-point integers** (micro-ticks, `i64`), parsed through a strict decimal grammar — an off-grid price is corruption and fails the run, not rounded into the book.
- **Books are validated at birth.** Ladders are strictly sorted best-first and immutable-by-type after construction (`Box<[Level]>`). Locked/crossed books, which real captures genuinely contain, pass through deliberately — execution realism is the fill layer's concern.
- **Cash and P&L are separate ledgers.** Cash moves only when money moves; P&L travels as return values. `final_cash = initial + total_pnl` holds identically whenever nothing is left open.
- **Fail-fast everywhere.** The first malformed row, out-of-order timestamp, or unknown websocket event aborts the run.
- **Validated against the exchange and by contract.** The book builder replays gigabytes of recorded websocket reality and checks its reconstructed top-of-book against the exchange's stated values at every delta. The data-feed abstraction is held to an executable conformance suite every source must pass. **192 unit tests** cover the accounting identities, gating, sweep walks, queue model, settlement algebra, and boundary validation; a slow suite replays the real data files end to end.

## Architecture

A backtest is a single streaming pass driven by an event clock. The seams below are the design; the code's doc comments carry the *why* in depth.

```
EventSource (feed)                Engine (the exchange + the clock)
  next_event(deadline) ─▶ Book / Trade / Time   (earliest of data, appointment, deadline)
                              │
                              ▼
      advance resting orders · expire GTD · land cancels · execute due latency windows
                              │
       Book ─▶ strategy.on_market_state()   Time ─▶ strategy.on_time()
                              │   Vec<Action>: Place | Cancel | WakeAt   (intent only)
                              ▼
      gates ─▶ fill model (on the residual book) ─▶ time-in-force ─▶ affordability
                              ▼
      Portfolio (pure ledger: cash + positions, never rejects) ─▶ settle at resolution
```

The seams are the design: strategies are pure deciders that never see resolutions (lookahead) or assume fills (intent ≠ execution); fill models are portfolio-blind; the ledger never rejects (gating is the engine's job); and the `clob` decode/book-building core is synchronous and transport-free, so the live adapter is transport + reconnect around unchanged, replay-tested code.

## Usage

```bash
# backtest one token's book
cargo run -- run <strategy> <book_snapshot_full.parquet> [--resolution 0|1] [--fill naive|sweep] \
                 [--latency zero|dublin|retail-eu|us-east] [--category <cat>] [--queue proportional|front|trades-only]

# maker strategies + an HTML report
cargo run -- run quoter <book.parquet> --resolution 1 --fill sweep --category sports --latency dublin --report out.html

# keep the realism settings in a TOML profile (CLI flags still override)
cargo run -- run <strategy> <book.parquet> --config universes/single.toml

# a whole universe of markets, aggregated
cargo run -- batch <strategy> universes/example.toml --fill sweep --report batch.html

# compare one strategy across a matrix of configs (realism cost waterfall)
cargo run -- compare <strategy> <book.parquet> --category <cat> --report cmp.html

# export the run as Parquet tables for pandas (see scripts/load_run.py)
cargo run -- run quoter <book.parquet> --record-dir runs/quoter/

# convert a raw websocket capture into per-asset Telonex-parity parquet (book + trades)
cargo run -- convert <capture.jsonl> <out_dir>

cargo test                   # unit suite
cargo test -- --ignored      # slow replays over the real data files
```

Example strategies live in [`strategies/`](strategies/) (a layer built on the engine, kept separate from `src/`): `buyandhold`, `buyandsell`, `quoter`, `quoter-improve`, `accumulator` — see the [folder readme](strategies/README.md) for what each demonstrates. Adding one is a file there implementing the single `Strategy` trait, a `mod` line in `src/lib.rs`, and one match arm in `build()` (`src/main.rs`); fill models register the same way. Data acquisition lives in `scripts/`. Backtest data is not committed — point the tool at your own Telonex pulls or converted captures.

## Roadmap

Done: fees, latency + perception, limit orders and the four time-in-force types, the queue model, the event clock, batch universes, the realism-comparison view, HTML/Parquet reporting, and raw-capture conversion with a trades tape.

Next, in rough order:

1. **Live paper trading** — a websocket source over the existing `clob` core (the engine is already generic over the source and its error type, so this needs no engine change), a streaming recorder that doesn't accumulate in memory, and a report shape for a run that never drains.
2. **Alternative-data feeds** — signals (scores, prices, sentiment) merged into the same timestamp-ordered event stream, so a strategy can react to non-market data without lookahead.
3. **Calibration** — checking the inferred queue model against on-chain fill data.

## License

MIT — see [`LICENSE`](LICENSE). The license covers this code only; using Polymarket's API and data is subject to their own terms.
