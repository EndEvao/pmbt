# pmbt — a Polymarket backtesting engine

`pmbt` replays a Polymarket market's **full order-book history** and plays the exchange back to your strategy, tick for tick. It is not a snapshot-loop that marks a curve to mid — it is a small matching engine that gates every order the way Polymarket would, charges the fees Polymarket would, makes orders wait out the latency they really face, and settles the position at resolution through a separate ledger. The point is a P&L you can trust: one where the friction is modelled rather than assumed away, and every optimistic assumption is named and turned into a knob you can move.

It is written in Rust and streams its input data without retaining the full history. Tests cover accounting identities, order gates, execution timing, liquidity consumption, queue modeling, settlement, and reporting, with additional suites that replay captured data end to end. It is a research backtester with explicit modeling limitations, designed to support future live paper trading on the same core (see [Roadmap](#roadmap)).

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

That run is real: a full day of book snapshots for a resolved market, a 100-share position lifted at the touch, held to resolution, and redeemed at $1 — the historically true outcome. Trading, settlement, and fees are reported apart because redemption is not a trade.

---

## Features

### Fees

Polymarket charges the **taker only** (makers pay nothing) a fee in USDC on each fill:

```
fee = qty × rate × p × (1 − p)
```

It peaks at `p = 0.5` and is symmetric around it — a trade at 30¢ costs the same as one at 70¢, and trades near the extremes are nearly free. The `rate` is a function of the market's category, mapped onto Polymarket's published tiers:

| tier | rate | categories |
|---|---|---|
| crypto | 0.07 | crypto |
| standard | 0.05 | sports, culture, science, other |
| low | 0.04 | finance, politics, tech |
| free | 0.00 | geopolitics (world events) |

The fee is deducted per fill, on both buys and sells, and reported on its own line so it never hides inside the trading number. It is modelled as config keyed off category rather than looked up from the data: the exact per-market rate lives in the CLOB `fd` field, but the historical value for a past day is not recoverable, so the tier table is the honest approximation. `--fees off` drops the cost entirely (the pre-fee behaviour, byte for byte).

### Latency

Time passes between "the exchange's book changed" and "my order interacts with it." That gap is modelled in three components, kept separate because they answer to different owners:

- **feed** — exchange event → you know about it (the websocket path to your collector).
- **transit** — your order's trip to the matching engine (REST + signing + network).
- **taker_delay** — Polymarket's *deliberate* per-market-type hold on marketable orders: **sports 3 s, crypto & finance 250 ms, everything else immediate.**

Placement presets bundle feed + transit for an observer at a given location:

| preset | feed + transit | who |
|---|---|---|
| `zero` | 0 | no latency — reproduces latency-free backtests exactly (default) |
| `dublin` | ~13 ms | a well-placed bot, one AWS region hop from the CLOB (eu-west-2) |
| `retail-eu` | ~50 ms | a European home connection, no colocation |
| `us-east` | ~135 ms | a US East Coast box, across the Atlantic each way |

An order decided on the snapshot at `t` executes at `t + delay` against the latest exchange book available at that instant. The event clock does not wait for another snapshot. Pending orders are sorted by execution deadline, with submission order breaking ties. A marketable order serves the full delay (feed + transit + taker hold); a resting limit order serves only feed + transit, because it *adds* liquidity rather than aggressing, so the exchange's taker brake does not apply to it. A cancel travels feed + transit too — which is why it can lose a race (see [Gate](#gate)).

**Perception delay** (optional) closes the other half of the loop: it delivers each snapshot to the strategy as late as a real observer would receive it (the feed component), so a signal over "the last 2 s of updates" can't secretly include updates the observer hadn't gotten yet. Execution latency applies either way — the exchange always trades on the true book. Every effect toggles per run or per universe.

### Queue modelling

Polymarket publishes only **L2** data — aggregate size per price, never individual orders — so the line ahead of a resting order can't be observed; it has to be inferred. The model seeds the queue from the displayed size at your price when you join, drains it with trades, and attributes any shrinkage the trades *don't* explain to cancellations. The one genuinely unobservable quantity — whether those cancels came from ahead of you or behind — is bracketed by three modes:

- **`front`** — no queue at all; any flow reaching your price fills you. An **upper bound** on maker fills, not an estimate.
- **`proportional`** (default) — join the back; trades drain the line; cancellations advance you in proportion to how much of the level they removed. Claims nothing about *who* cancelled, which is exactly the state of the knowledge — the neutral choice.
- **`trades-only`** — join the back; only trades drain the line. A **lower bound**: on a venue where most depletion is cancellation, a queued order essentially never fills.

Running the same strategy under all three turns "how optimistic is my maker P&L?" into a measurable range. The matcher keeps price-then-time priority among your own orders, caps every fill by the trade's size (you never receive more shares than actually changed hands), and guards against double-counting a trade's volume as a phantom cancellation. Because a cancel is a message in flight, a trade can pick your order off before the cancel lands — the maker's central risk, modelled rather than assumed away.

### Fill

The fill model is the swappable seam that decides *how* an order matches the book. Two ship:

- **`naive`** — fills the full requested size at the touch, no depth limit, no slippage. Deliberately unrealistic: the optimistic baseline every realistic model is measured against.
- **`sweep`** — walks the ladder best-first, consuming liquidity level by level (buys up the asks, sells down the bids), emitting **one fill per level at that level's exact tick price** — the record matches what the exchange would report for a multi-level match. A limit order stops walking once the ladder crosses its price.

Fill models are **portfolio-blind**: they model the exchange's matching, not the trader's wallet — affordability is the engine's job, checked against the actual walked cost. Orders share a **residual book** until the next real book snapshot. Clock and trade events preserve consumed depth; only a new snapshot restores the displayed liquidity. The strategy still observes the historical market book, independently of this execution ledger. Resting maker orders are filled by a different path entirely — against the trades tape, see [Queue modelling](#queue-modelling).

### Order types (time-in-force)

Four time-in-force types — Polymarket's exact set, no more:

| TIF | behaviour |
|---|---|
| **GTC** (Good-Till-Cancelled) | rests on the book until filled or cancelled |
| **GTD** (Good-Till-Date) | rests like GTC, but the exchange retires it on a timer |
| **FOK** (Fill-Or-Kill) | fill the whole size immediately as taker, or nothing |
| **FAK** (Fill-And-Kill) | fill what's immediately available, drop the remainder |

GTD carries the exchange's real rules: an order is retired **one minute before** its stated expiry (a security buffer), and a stated expiry must be at least **three minutes** out or the exchange rejects it outright. The shortest GTD life actually obtainable is therefore `180 − 60 = 120 s` — which makes GTD a stale-order guard for slower strategies, not a substitute for cancel/replace at quoting frequency. Order constructors enforce finite positive quantities and restrict marketable (price-less) orders to FOK or FAK in both debug and release builds. `try_market` and `try_limit` return validation errors; the convenience constructors panic on invalid strategy constants. These are runtime checks, not compile-time type restrictions.

### Limit order mechanics

A limit order commits its resources **at placement**, before it even reaches the book: a buy holds cash at `limit × qty` (the worst it could ever pay), a sell holds the shares it promises. So a burst of orders can't collectively overdraw during the latency window — each is gated against what the previous ones already hold. Its price is on-grid *by construction* (it takes a `Price`, which has no off-grid constructor).

When the order's latency window elapses, the fill model computes what crosses right now (bounded by the limit price), and the time-in-force decides the unmatched remainder: FOK kills the whole order, FAK drops the rest, GTC/GTD rest it as a maker order. Before applying any fills, the engine checks the immediate cost plus fees plus the cash needed for the resting remainder. An underfunded order is rejected as a whole. Whatever rests joins the book, re-holds resources for the size still outstanding, and seeds its queue position from the displayed size at its price. A limit order that isn't marketable on arrival simply rests without trading.

> Not yet enforced: Polymarket's per-market `minimum_order_size` and per-market tick (0.01–0.0001). Both are exchange config not yet plumbed in, so the only size floor is `quantity > 0` and the price grid is the finer 6-decimal micro-tick grid.

### Data / IO

The engine reads **Telonex-format Parquet**: `book_snapshot_full` (every book change, all levels, exchange time in microseconds) paired with its sibling `_trades` tape. Each file covers one token and day; consecutive day-files can be chained without loading their full history. A `MergedFeed` interleaves the book and trades streams into one time-ordered event stream of `Book` / `Trade` / `Time` events.

The feed is an `EventSource` trait held to an **executable conformance suite** — five rules every source (including the future live one) must pass, because prose alone once let two implementations disagree and one ticked a replay into 2037. A missing trades tape degrades gracefully (the book replays fine; only resting orders need trades to fill), but a *malformed* one still fails loudly. Fail-fast throughout: the first bad row, out-of-order timestamp, or off-grid price aborts the run, because a P&L over silently-skipped data isn't a result.

Getting data in and out:

- **`pmbt convert`** turns a raw CLOB websocket capture into Telonex-parity Parquet — book **and** trades — via a transport-free `clob` module (typed wire decode + delta-folding book builder). Dropped timeframes are handled honestly: every `price_change` is checked against the exchange's own inline best-bid/best-ask, and a disagreeing book is quarantined until the next full-book resync. Gap statistics are reported loudly.
- **`--report`** writes a self-contained HTML page; **`--record`** dumps the full time series as JSON; **`--record-dir`** writes it as Parquet tables for pandas (`scripts/load_run.py`), ~50× smaller than the JSON.

### Recording memory and failures

A standalone `--record-dir` uses incremental Parquet writes with bounded row buffers (4,096 buffered records, plus a depth frame), retaining scalar statistics and currently resting order IDs. It rereads timestamp/equity columns at completion to compute the same adaptive-grid statistics as buffered reports. Parquet footer metadata still grows with the number of row groups; this is reduced-memory recording, not a strict constant-memory guarantee for arbitrarily long files.

The output directory must be absent or empty. All five tables are created even for an empty run. A successful `summary.json` is published only after the tables close; an interrupted or failed export can leave partial tables without a summary. Recorder errors abort execution through `RunError::Recorder`; feed errors are returned as `RunError::Feed`.

`--record` JSON, HTML reports, and exports requested alongside either retain the full history in memory. Use standalone `--record-dir` for large runs. Portfolio samples include trade and timer events at their actual timestamps; depth frames represent only real book updates.

### Gate

Every order faces the gates Polymarket would apply, in the exchange's own two-phase order:

1. **At submission** — a cheap sanity check (the required side exists; a GTD's horizon clears the three-minute floor).
2. **At execution** — the authoritative pass, against the snapshot the order actually lands on: no shorting (sells are covered against *free* inventory — shares already promised to a resting sell can't be sold twice); then the fill model runs; then funding is verified against the **actual walked cost plus fee**, not the touch price — a sweep costs more than `touch × qty`, so affordability can only be judged after the walk.

Cancels are messages in flight: they land on their scheduled event-clock deadline, with a trade at exactly the same timestamp processed first, so a maker can be picked off before a cancel arrives. A cancel that finds nothing has lost the race — counted separately (`picked off`) rather than silently dropped, because that is exactly the adverse selection worth measuring. The portfolio ledger itself **never rejects** — gating is the engine's responsibility, keeping the ledger a pure accounting layer.

### PnL calculations + results

**Cash and P&L are separate ledgers.** Cash moves only when money actually moves; realized P&L travels as return values off each fill. The identity `final_cash = initial_capital + total_pnl` holds exactly whenever nothing is left open — a continuous invariant, not a reconciliation step.

Net P&L is always broken into three legs, so the total is never a black box:

```
total_pnl = trading (realized)  +  settlement  −  fees
```

- **Trading** — gross P&L realized by buying and selling during the run.
- **Settlement** — terminal redemption. `--resolution 0|1` states what the token redeemed at; the position settles through the ledger **after the feed drains**, never through the order pipeline, because redemption is not a trade. The engine summary carries an unresolved holding at cost. The HTML report additionally shows its marked equity and separates unrealized P&L from realized trading.
- **Fees** — total taker fees, always ≥ 0.

Reporting turns a run into an artifact you can read or analyse. The single-run HTML report opens with five headline metrics, synchronized P&L/drawdown/inventory charts, and a visible attribution waterfall. The full-capital benchmark has an optional separate scale; the synthetic complement is also opt-in. Configuration, input coverage, execution diagnostics, a paginated blotter, and depth inspection follow. Charts retain bucket extrema; calculations use the full event history. Capital deployment is time-weighted inventory cost divided by initial capital, excluding order reservations. Additional statistics include: **Sharpe, Sortino, and volatility** computed on the equity curve resampled to a uniform grid (with the grid period and annualization factor printed alongside, so the numbers are never a silent assumption), plus path metrics — max drawdown, exposure, turnover, total return — on the raw event-timed data. The terminal summary carries the order flow that produced the P&L: fills (with the maker/taker split), rejections, cancels, cancels missed, orders expired in flight, resting orders left open, and GTD orders the exchange retired.

### TOML features + config

Realism settings live in a `[simulation]` section so they don't have to be retyped on every command:

```toml
[simulation]
fill        = "sweep"          # naive | sweep
latency     = "dublin"         # zero | dublin | retail-eu | us-east
perception  = true             # deliver snapshots late (feed component)
taker_delay = true             # apply the exchange's category hold
fees        = true             # apply the category taker fee
category    = "sports"         # drives taker delay + fee tier (run/compare)
queue       = "proportional"   # front | proportional | trades-only
```

Any TOML carrying that section is a **profile** you can pass with `--config` — including a whole universe file, whose market entries are then ignored (loudly, never silently). Precedence is **CLI flag > file > built-in default**, applied identically everywhere. Parsing is strict: `deny_unknown_fields` means a typo like `fil = "sweep"` or `category = "sprots"` fails at startup rather than silently getting the wrong behaviour later.

A **universe file** describes a set of markets to run over:

```toml
name     = "example"
data_dir = "data/telonex_example"

[[markets]]
market_id  = "1006980253…779656"
category   = "geopolitics"     # world events — the fee-free tier
resolution = 0                 # token redeemed at $0 (omit if unresolved)
```

Each entry carries its own category (hence its own taker delay and fee tier) and resolution; the data file is discovered by name under `data_dir`, or named explicitly with `file`. Duplicates, empty universes, and ambiguous data-file matches are all caught up front, before any engine runs.

### Batch processing & comparison presets

Two commands run *many* engines and reduce them for you:

- **`pmbt batch <strategy> <universe.toml>`** runs a fresh engine per market across a universe (the single-market assumption is never touched) and aggregates the results — combined P&L, per-market attribution, an HTML report with a cumulative curve. Fails fast: the first market that can't load or run aborts the batch, because a partial aggregate is a lie.
- **`pmbt compare <strategy> <book>`** runs the *same* strategy over the *same* feed under a matrix of configs. The default `realism` axis is a **cumulative friction ladder** —

  ```
  frictionless → +latency → +taker-delay → +fees → +perception
  ```

  — where each rung adds exactly one friction, so the step-to-step P&L delta *is* that component's cost. Other axes (`latency`, `fees`, `perception`, `taker-delay`) sweep a single knob for an A/B. The feed is re-opened per config rather than buffered, keeping memory bounded.

### Engineering decisions

- **An event clock, not a snapshot loop.** Time advances to the engine's next *appointment* — a latency window closing, a cancel landing, a GTD expiring, a strategy wake-up — never "wait for the next snapshot," which silently imports information from the future. Closing this removed four separate lookaheads, and it is also how live data arrives (book and trade frames independently), so the live source will be the same loop.
- **Fixed-point prices.** Prices are `i64` micro-ticks (`1_000_000` per unit), parsed through a strict decimal grammar. An off-grid price is corruption and fails the run, not something rounded quietly into the book.
- **Books validated at birth.** Ladders are strictly sorted best-first and immutable-by-type after construction (`Box<[Level]>`) — the grow/shrink operations simply don't exist. Locked/crossed books, which real captures genuinely contain, pass through deliberately: whether an order should fill against one is the fill layer's concern.
- **Clean seams.** Strategies are pure deciders that never see resolutions (lookahead) or assume fills (intent ≠ execution). Fill models are portfolio-blind. The ledger never rejects. The `clob` decode/book-building core is synchronous and transport-free, so the live adapter is transport + reconnect wrapped around unchanged, replay-tested code. The engine is generic over the source *and its error type*, so a live websocket source needs no engine change.
- **Extensible by one line.** Strategies, fill models, latency presets, queue models, and categories each live behind a small registry — adding one is a new file (for a strategy) and a single match arm.

### Assumptions / notes

The honest caveats, stated rather than buried:

- **Front-of-queue optimism** is the biggest one, which is exactly why the [queue model](#queue-modelling) exists — treat `front` fills as an upper bound and read maker P&L as a range across the three modes.
- **Timers run between snapshots.** GTD expiry, cancel arrival, and strategy callbacks use the event clock. For equal timestamps, the current tie policy processes a trade before expiry/cancellation. A historical run stops when both data streams are exhausted; it does not manufacture later time.
- **A limit order that crosses on arrival** strictly should serve the taker hold on the crossing portion; modelling that would split one order across two arrival times, so a small understatement is accepted and noted.
- **Latency is constant per run** — no jitter distribution yet, though the per-component structure is there to attach one.
- **Fees and taker delays are modelled as config, not looked up**, because the exact historical per-day value is unrecoverable from the exchange.
- **The feed is single-market**, resolution is binary (`0`/`1`), and per-market minimum size and tick are not yet enforced.

---

## Usage

```bash
# backtest one token's book
cargo run -- run <strategy> <book_snapshot_full.parquet> [--resolution 0|1] [--fill naive|sweep] \
                 [--latency zero|dublin|retail-eu|us-east] [--category <cat>] [--queue proportional|front|trades-only]

# a maker strategy with the exchange's friction and an HTML report
cargo run -- run quoter <book.parquet> --resolution 1 --fill sweep --category sports --latency dublin --market-name "Market question" --outcome "Yes" --report out.html

# keep the realism settings in a TOML profile (CLI flags still override)
cargo run -- run <strategy> <book.parquet> --config universes/single.toml

# a whole universe of markets, aggregated
cargo run -- batch <strategy> universes/example.toml --fill sweep --report batch.html

# compare one strategy across a matrix of configs (the realism cost ladder)
cargo run -- compare <strategy> <book.parquet> --category <cat> --report cmp.html

# export the run as Parquet tables for pandas (see scripts/load_run.py)
cargo run -- run quoter <book.parquet> --record-dir runs/quoter/

# convert a raw websocket capture into per-asset Telonex-parity parquet (book + trades)
cargo run -- convert <capture.jsonl> <out_dir>

cargo test                   # unit and regression suites
cargo test --release         # also verifies release-build validation
cargo clippy --all-targets -- -D warnings
cargo test -- --ignored      # slow replays over the real data files
```

Example strategies live in [`strategies/`](strategies/) — `buyandhold`, `buyandsell`, `quoter`, `quoter-improve`, `accumulator` — each a single file implementing the one `Strategy` trait; see the [folder readme](strategies/README.md) for what each demonstrates. Data acquisition lives in [`scripts/`](scripts/). Backtest data is not committed — point the tool at your own Telonex pulls or converted captures.

## Roadmap

**Done:** fees, latency + perception, the four time-in-force types and limit-order mechanics, the queue model, the event clock, batch universes, the realism-comparison ladder, HTML/JSON/Parquet reporting, and raw-capture conversion with a trades tape.

**Next, in rough order:**

1. **Live paper trading** — a websocket source over the existing `clob` core (the engine is already generic over the source and its error type, so this needs no engine change), a recorder lifecycle for continuous output rotation, and a report shape for a run that never drains.
2. **Alternative-data feeds** — signals (scores, prices, sentiment) merged into the same timestamp-ordered event stream, so a strategy can react to non-market data without lookahead.


## License

MIT — see [`LICENSE`](LICENSE). The license covers this code only; using Polymarket's API and data is subject to their own terms.
