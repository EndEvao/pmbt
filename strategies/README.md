# strategies

Example strategies that plug into the `pmbt` engine. They live here — one level
up from `src/` — because they are a distinct layer built *on* the engine, not
part of its internals. Each is a single file implementing the `Strategy` trait
from [`src/strategy.rs`](../src/strategy.rs); the engine compiles them in via
`#[path]` declarations in [`src/lib.rs`](../src/lib.rs), so their module names
are unchanged (`crate::quoter::Quoter`, …).

A strategy is a **pure decider**. It reacts to the market (`on_market_state`) or
to time passing (`on_time`), folds what it sees into its own `&mut self` state,
and returns an ordered `Vec<Action>` — `Place`, `Cancel`, or `WakeAt`. It never
sees resolutions (that would be lookahead) and never assumes an order filled (a
request is intent, not execution). The world always arrives borrowed and
read-only.

## The examples

| file | name(s) | what it shows |
|---|---|---|
| [`buyandhold.rs`](buyandhold.rs) | `buyandhold` | The minimal strategy: lift 100 shares at the touch on the first snapshot, hold to settlement. The template to copy. |
| [`buyandsell.rs`](buyandsell.rs) | `buyandsell` | A round trip — buy, then sell — so realized trading P&L (not just settlement) is exercised. |
| [`quoter.rs`](quoter.rs) | `quoter`, `quoter-improve` | A two-sided **maker**: rests limit orders around the touch (join, or step inside the spread), cancels and re-quotes on a reprice, and caps inventory while counting orders still in flight. Exercises limit orders, cancels, and the queue model. |
| [`accumulator.rs`](accumulator.rs) | `accumulator` | A patient GTD maker: rests one bid and lets the exchange retire it on its timer, replacing it via the `on_time` hook. Exercises GTD, time-driven decisions, and strategy-requested wake-ups. |

## Adding your own

1. Drop a new `<name>.rs` in this folder implementing `Strategy` (see
   `buyandhold.rs` for the minimal shape).
2. Add `#[path = "../strategies/<name>.rs"] pub mod <name>;` in
   [`src/lib.rs`](../src/lib.rs).
3. Add one match arm in `build()` in [`src/main.rs`](../src/main.rs).

Fill models and queue models are separate seams (`src/fill/`, `src/resting.rs`);
a strategy only decides *what* to ask for, never *how* it fills.
