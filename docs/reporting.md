# Research reports

Single-market, universe, and comparison reports share a restrained grayscale
layout. Reports are standalone HTML with embedded data and styles; no network
connection or external fonts are required. Light mode is the default, with a
dark-mode control and print/PDF action.

## Reading the result

The single-market overview presents net marked P&L, return on initial capital,
maximum drawdown, ending equity, and execution count. Account balances and a
P&L reconciliation precede the performance charts. Run settings, source details,
data coverage, and advanced risk statistics remain available further down.

Accounting definitions:

- Ending equity = ending cash + marked holdings.
- Net marked P&L = ending equity − initial capital.
- Net marked P&L = realized trading + settlement + unrealized holdings − fees.
- Return = net marked P&L / the initial capital assigned to that run.

The recorder values holdings using midpoint, available touch, or previous mark,
in that order. If none exists, the current recorder uses zero. A mark is not a guaranteed liquidation price. The
single-market report discloses this policy and the observed one-sided books.

Universe reports show allocated capital and return per entry, together with
realized, settlement, unrealized, and fee contributions. Portfolio return uses
the total initial portfolio budget. Allocations are fixed; they do not imply
cash transfers or rebalancing. Each entry's ending mark comes from its own last
observation, so differing observation windows must be considered when reviewing
portfolio values. If a held position lacks recorded valuation history, its
marked P&L, the aggregate marked P&L, and ending equity are unavailable rather
than assumed to be zero. A combined curve requires history for every entry.

Comparison tables and realism waterfalls use net marked P&L, matching their
marked equity curves. A waterfall is omitted when a required ending valuation
is unavailable. Line patterns distinguish configurations without relying on
different hues. Chart times are UTC.

## Statistics and execution terminology

An execution is a fill event; multiple partial fills are not multiple submitted
orders. The single-market ledger labels its sell-based statistics as closing
sell fills, not full position round trips. These realized fill statistics are
gross of fees; headline P&L is net of fees.

Sharpe requires at least two sampled returns and nonzero return variance.
Sortino additionally requires nonzero downside deviation. Unavailable ratios
display as “Not defined”; they are not evidence of zero risk or performance.
The comparison table shows grid-period Sharpe. Annualized ratios remain in
the single-market advanced section with their sampling basis and an explicit
short-window interpretation note.

## Data compatibility

Existing scalar P&L fields retain their realized/settlement meaning. The batch
JSON model adds per-market `marked_pnl`, `unrealized`, and `return_on_capital`,
plus aggregate `marked_pnl`, `unrealized`, and `ending_equity`. Unknown values
are JSON `null`. The comparison model adds `marked_pnl`, `marked_return`, and
`sharpe_defined`; its waterfall now uses marked P&L. Statistics add
`sharpe_defined` and `sortino_defined`. Legacy numeric ratio fields retain their
zero fallback for compatibility; new consumers must consult the validity flags.

## Scope of this iteration

This iteration changes generated historical reports and their presentation
models. It does not change order execution, fees, capital allocation, or strategy
behavior. It does not yet generate these HTML reports directly from live paper
captures. Paper-report adaptation, richer order/fill attribution, sorting and
filtering, and post-fill price-movement analysis remain subsequent work.

Validation includes accounting regressions for open and unmarked holdings,
ratio-validity checks, and JavaScript tests for all three report families:

```sh
node scripts/check_report.mjs
node scripts/check_report_variants.mjs
```

The first script optionally accepts a generated single-market report. The second
accepts generated universe/comparison reports. These exercise rendering logic
and controls using a simulated DOM; they do not verify browser layout.
