# strategies

These demonstrations belong to the CLI binary and use only pmbt's public API.
They are not exported by the engine library. Each implements the `Strategy`
trait from [`Strategy`](../../../pmbt/src/strategy.rs).

A strategy is a **pure decider**. It reacts to the market (`on_market_state`) or
to time passing (`on_time`), folds what it sees into its own `&mut self` state,
and returns an ordered `Vec<Action>` — `Place`, `Cancel`, or `WakeAt`. It never
sees resolutions (that would be lookahead) and never assumes an order filled (a
request is intent, not execution). The world always arrives borrowed and
read-only.

## The examples, in increasing sophistication

| file | name(s) | what it shows |
|---|---|---|
| [`buyandhold.rs`](buyandhold.rs) | `buyandhold` | The minimal strategy: lift 100 shares at the touch on the first snapshot, hold to settlement. The template to copy. |
| [`buyandsell.rs`](buyandsell.rs) | `buyandsell` | A round trip — buy, then sell — so realized trading P&L (not just settlement) is exercised. |
| [`quoter.rs`](quoter.rs) | `quoter`, `quoter-improve` | A two-sided **maker**: rests limit orders around the touch (join, or step inside the spread), cancels and re-quotes on a reprice, and caps inventory while counting orders still in flight. Exercises limit orders, cancels, and the queue model. |
| [`accumulator.rs`](accumulator.rs) | `accumulator` | A patient GTD maker: rests one bid and lets the exchange retire it on its timer, replacing it via the `on_time` hook. Exercises GTD, time-driven decisions, and strategy-requested wake-ups. |

## Private strategies in another project

Implement `pmbt::strategy::Strategy` in your own crate and pass an instance to
`Backtest::run`, or a factory to the batch/comparison APIs. No registration in
this folder, `src/lib.rs`, or the CLI is needed. Read snapshots through public
accessors such as `market_id()`, `timestamp()`, `ask().map(|level| level.price())`,
and `bids()`. Keep strategy parameters in your application's configuration and
pass display values through `StrategyMetadata`.

See the [library guide](../../../../docs/library.md) and
[external-style example](../../../pmbt/examples/library_usage.rs).

## Adding a bundled CLI example

1. Drop a new `<name>.rs` in this folder implementing `Strategy` (see
   `buyandhold.rs` for the minimal shape).
2. Declare the module in [`mod.rs`](mod.rs).
3. Add one match arm in `build()` in [`src/main.rs`](../main.rs).

Fill models and queue models are separate seams (`src/fill/`, `src/resting.rs`);
a strategy only decides *what* to ask for, never *how* it fills.
