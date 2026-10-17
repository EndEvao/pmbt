# Review remediation plan

Each fix is a separate commit. Regression tests demonstrate the original failure and verify the corrected behavior. Changes to existing user work are limited to removing registrations for the absent momentum strategy; the explanatory strategy-module comment is retained.

1. **Restore compilation.** Remove the missing momentum module/import/registry entries. Do not invent its trading logic. Acceptance: offline build and existing unit suite pass.
2. **Preserve consumed liquidity.** Retain the depleted execution book through time and trade events; reset only on a new book snapshot. Keep the observed market book independent. Acceptance: ten displayed shares cannot fill twenty shares between snapshots; a new snapshot restores depth.
3. **Keep partial limit orders funded.** Check taker cost plus fees plus the cash needed for any resting remainder before applying any fills. Reject the entire order if underfunded. Acceptance: a partially crossing GTC/GTD cannot leave negative free cash or later overdraw; sufficiently funded orders still fill/rest.
4. **Schedule orders by execution deadline.** Insert pending orders in stable deadline order, retaining cancellation by order ID. Acceptance: a fast limit order is not blocked by a delayed market order; equal deadlines preserve submission order.
5. **Deliver scheduled callbacks at data boundaries.** Service due wakeups even when their timestamp coincides with a book event; remove due requests before calling the strategy. Acceptance: coincident and later callbacks fire once, including under perception delay.
6. **Validate production order construction.** Add checked constructors with explicit errors, keep existing constructors as always-checked convenience wrappers, and test invalid quantities/TIF in release mode. Acceptance: NaN/infinite/nonpositive quantities and resting market orders cannot reach execution in any build profile.
7. **Record portfolio event timing.** Add a recorder hook for non-book events and record current portfolio/quotes using the carried market mark without fabricating depth updates. Acceptance: intrabook fills affect equity/exposure at execution time, including trailing trades after the last snapshot.
8. **Bound recording memory.** Provide incremental disk recording with bounded buffers and explicit error propagation; wire the CLI to the streaming path for standalone Parquet exports. Preserve full-history JSON/HTML behavior and document its memory cost. Acceptance: multi-batch output reads back correctly, records empty tables, and fails on write errors without publishing a success summary.
9. **Align documentation and checks.** Remove obsolete descriptions of snapshot-only timers and front-only matching; state actual memory guarantees, constructor validation, and known modeling limitations. Add automated debug/release checks so compilation and behavioral regressions are caught.

Final verification: formatting, offline debug/release tests, Clippy, and available real-data replay tests. Record any fixture-dependent failures explicitly; do not turn missing data into a false pass.

## Completed implementation

| Fix | Commit |
|---|---|
| Restore compilation and save this plan | `529b5e5` |
| Preserve execution liquidity between real books | `3f30206` |
| Fund fees and resting remainder together | `0132694` |
| Execute in stable deadline order | `d22d8b6` |
| Deliver wakeups coinciding with book events | `4b29339` |
| Validate orders in release builds | `c3a1fb0` |
| Sample portfolio changes at event times | `6c55ccd` |
| Incremental Parquet recording and error propagation | `483a735` |
| Additional reservation checks found by the release suite | `9b8b99e` |
| Documentation, formatting, and CI | This documentation commit |

The original incomplete momentum registration was uncommitted and has been removed; its original diff was preserved at `/private/tmp/pmbt-original-momentum.patch`. The strategy implementation itself was absent and has not been invented.

### Verification

- Debug suite: 219 passed; seven real-data tests excluded by default.
- Release suite: 219 passed; seven real-data tests excluded by default.
- Real-data replay suite: all seven passed separately in release mode.
- `cargo fmt --check`, `cargo clippy --offline --all-targets -- -D warnings`, and `git diff --check` passed.
- Streaming and buffered exports produce identical tables and summaries across empty, single-sample, unresolved, and settled multi-batch runs.
- A simulated full disk aborts the engine without publishing a success summary.
- A CLI replay of the local example market streamed 65,812 samples successfully.

### Compatibility and remaining scope

`Engine::run` and `run_recorded` now return `RunError<S::Error>` to distinguish feed failures from recorder failures. Existing callers using `?` continue to work; callers matching a concrete feed error must match `RunError::Feed`.

Standalone `--record-dir` now requires an absent or empty output directory and streams rows. JSON/HTML and combined exports remain buffered. Parquet row buffers are bounded, but per-file footer metadata grows with row-group count, so arbitrary-duration strict constant-memory output would require file rotation. This limitation is explicitly documented rather than claiming every recording mode is constant-memory.

The existing exchange-model approximations (category-configured fees/delays, missing per-market tick/minimum size, L2 queue uncertainty, and the crossing-limit taker-hold approximation) remain documented limitations. The fixes address the review's implementation defects; they do not establish exchange-equivalent execution or profitable strategies.
