//! Turning the raw per-snapshot samples into series the stats and charts want.
//!
//! Three transforms live here, all pure:
//!
//! - [`equity_curve`] stitches the in-run mark-to-market equity together with
//!   the *terminal* settlement, so a resolved run's curve ends at the real
//!   redeemed cash rather than the last pre-settlement mark.
//! - [`resample_uniform`] projects an irregularly-timed curve onto a fixed time
//!   grid (last-observation-carried-forward). Book snapshots arrive on events,
//!   not a clock; return-based stats (Sharpe, vol) are meaningless on that
//!   uneven spacing, so they run on the grid instead — see [`super::stats`].
//! - [`downsample_price`] time-buckets the price path into a bounded number of
//!   OHLC bars for plotting, independent of how many snapshots a run produced.

use crate::engine::{BacktestResult, Settlement};

use super::record::Sample;

/// The finest resampling grid we will ever pick: one second, in microseconds.
/// A floor, not a default — [`choose_grid`] never goes below it, so a short run
/// can't be sliced into sub-second bars that just measure book micro-jitter.
pub const DEFAULT_GRID_US: i64 = 1_000_000;

/// Target number of resampling bars across a run when choosing the grid
/// adaptively. Enough to resolve the equity shape, few enough that each bar
/// spans real movement rather than a stretch of carried-forward (flat) values.
pub const TARGET_BARS: i64 = 480;

/// Pick a resampling grid for a run: aim for about [`TARGET_BARS`] bars across
/// the run's span, but never finer than [`DEFAULT_GRID_US`]. Adaptive on
/// purpose — a *fixed* fine grid buries the return series under LOCF-carried
/// zeros on a long run and then annualizes off a bar count that is mostly
/// padding, distorting Sharpe. Scaling the grid to the span keeps most bars
/// carrying a genuine move. Empty/degenerate input falls back to the floor.
pub fn choose_grid(samples: &[Sample]) -> i64 {
    let span = match (samples.first(), samples.last()) {
        (Some(first), Some(last)) => (last.ts - first.ts).max(1),
        _ => return DEFAULT_GRID_US,
    };
    (span / TARGET_BARS).max(DEFAULT_GRID_US)
}

/// The mark-to-market equity path as `(timestamp_us, equity)` points, with the
/// terminal settlement appended for resolved runs.
///
/// During the run each sample's equity marks the open position at the live
/// book. Settlement happens *after* the feed drains — it never appears in the
/// samples — so for a resolved run (settled or ended flat) we append a final
/// point at `final_cash` at the last timestamp. That is the redeemed equity
/// (the position is now cash), and it makes the curve, the drawdown, and the
/// total-return figure all agree. Unresolved runs are left as-is: their last
/// mark *is* their ending equity.
pub fn equity_curve(samples: &[Sample], result: &BacktestResult) -> Vec<(i64, f64)> {
    let mut points: Vec<(i64, f64)> = samples.iter().map(|s| (s.ts, s.equity)).collect();

    let resolved = !matches!(result.settlement, Settlement::Unresolved);
    if resolved {
        if let Some(&(last_ts, _)) = points.last() {
            // A step to the redeemed cash at the same instant the feed ended.
            points.push((last_ts, result.final_cash));
        }
    }
    points
}

/// The passive buy-and-hold baseline the strategy is measured against: take the
/// position at the first available ask with *all* of `initial_capital`, mark it
/// along the same price path the strategy saw, and — if the market resolved —
/// redeem the shares at `resolution`. Answers "what if I'd just bought the token
/// and held it?", which is the honest yardstick for any active strategy.
///
/// `pnl` is cumulative PnL aligned 1:1 with `samples` (0 before entry); marking
/// mirrors the recorder's own rule (mid, else a touch, else the carried mark) so
/// the two curves are comparable point-for-point. `terminal_pnl` is the
/// post-settlement figure for the benchmark's final point and its return.
pub struct Benchmark {
    pub pnl: Vec<f64>,
    pub terminal_pnl: f64,
}

pub fn benchmark(samples: &[Sample], initial_capital: f64, resolution: Option<f64>) -> Benchmark {
    let mut shares = 0.0;
    let mut entered = false;
    let mut last_mark: Option<f64> = None;
    let mut pnl = Vec::with_capacity(samples.len());

    for s in samples {
        // Enter once — buy at the first ask we can actually lift, spending the
        // whole account (a taker fill, the real cost of getting in).
        if !entered {
            if let Some(ask) = s.ask {
                if ask > 0.0 {
                    shares = initial_capital / ask;
                    entered = true;
                }
            }
        }
        let mark = s.mid.or(s.bid).or(s.ask).or(last_mark);
        if let Some(m) = mark {
            last_mark = Some(m);
        }
        let equity = if entered {
            shares * mark.unwrap_or(0.0)
        } else {
            initial_capital
        };
        pnl.push(equity - initial_capital);
    }

    // The benchmark holds to the end, so it redeems at the resolution price when
    // the market resolved; otherwise its last mark is its terminal value.
    let terminal_pnl = match resolution {
        Some(r) if entered => shares * r - initial_capital,
        _ => *pnl.last().unwrap_or(&0.0),
    };
    Benchmark { pnl, terminal_pnl }
}

/// Sample a `(ts, value)` curve onto a uniform grid of `grid_us` spacing, from
/// the first timestamp to the last, carrying the last observation forward.
/// Returns just the values (the grid times are implicit: `t0 + i*grid_us`).
///
/// Empty in, empty out; a zero-span curve returns its single value.
pub fn resample_uniform(curve: &[(i64, f64)], grid_us: i64) -> Vec<f64> {
    debug_assert!(grid_us > 0);
    let (Some(&(t0, _)), Some(&(t_end, _))) = (curve.first(), curve.last()) else {
        return Vec::new();
    };
    if t_end == t0 {
        return vec![curve[0].1];
    }

    let mut out = Vec::with_capacity(((t_end - t0) / grid_us) as usize + 1);
    let mut j = 0; // index of the last point at or before the current grid time
    let mut t = t0;
    while t <= t_end {
        while j + 1 < curve.len() && curve[j + 1].0 <= t {
            j += 1;
        }
        out.push(curve[j].1);
        t += grid_us;
    }
    out
}

/// One time-bucket of the price path: OHLC of the mid plus the closing touch,
/// for drawing the price line at a bounded resolution. Prices are `Option`
/// because a bucket can span one-sided books (no mid); a bucket with no mid at
/// all is dropped by [`downsample_price`].
#[derive(Debug, Clone, PartialEq)]
pub struct PriceBar {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Closing touch of the bucket, for the Yes/No lines and the spread.
    pub bid: Option<f64>,
    pub ask: Option<f64>,
}

/// Time-bucket the samples' mid path into at most `max_bars` OHLC bars. Buckets
/// with no two-sided quote in them are skipped, so the output length is `<=
/// max_bars`. A run with fewer samples than `max_bars` effectively passes
/// through one bar per sample.
pub fn downsample_price(samples: &[Sample], max_bars: usize) -> Vec<PriceBar> {
    if samples.is_empty() || max_bars == 0 {
        return Vec::new();
    }
    let t0 = samples[0].ts;
    let t_end = samples[samples.len() - 1].ts;
    let span = (t_end - t0).max(1);
    let width = (span as usize / max_bars).max(1) as i64;

    let mut bars: Vec<PriceBar> = Vec::new();
    for sample in samples {
        let Some(mid) = sample.mid else { continue };
        let bucket = ((sample.ts - t0) / width).min(max_bars as i64 - 1);
        let bucket_ts = t0 + bucket * width;

        match bars.last_mut() {
            Some(bar) if bar.ts == bucket_ts => {
                bar.high = bar.high.max(mid);
                bar.low = bar.low.min(mid);
                bar.close = mid;
                bar.bid = sample.bid;
                bar.ask = sample.ask;
            }
            _ => bars.push(PriceBar {
                ts: bucket_ts,
                open: mid,
                high: mid,
                low: mid,
                close: mid,
                bid: sample.bid,
                ask: sample.ask,
            }),
        }
    }
    bars
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::position::Position;

    fn sample(ts: i64, mid: Option<f64>, qty: f64, equity: f64) -> Sample {
        Sample {
            ts,
            bid: mid.map(|m| m - 0.005),
            ask: mid.map(|m| m + 0.005),
            mid,
            cash: 0.0,
            qty,
            avg_cost: 0.0,
            equity,
            pnl: equity,
            quote_bid: None,
            quote_ask: None,
        }
    }

    fn settled_result(final_cash: f64) -> BacktestResult {
        BacktestResult {
            initial_capital: 10_000.0,
            final_cash,
            realized_pnl: 0.0,
            total_pnl: final_cash - 10_000.0,
            fees: 0.0,
            settlement: Settlement::NothingHeld,
            open_position: None,
            fills: 0,
            rejections: 0,
            cancels: 0,
            cancels_missed: 0,
            expired: 0,
            maker_fills: 0,
            taker_fills: 0,
            expired_resting: 0,
            expired_gtd: 0,
            resting_quantity: 0.0,
            resting_reserved: 0.0,
        }
    }

    fn unresolved_result() -> BacktestResult {
        BacktestResult {
            settlement: Settlement::Unresolved,
            open_position: Some(Position::new()),
            ..settled_result(10_000.0)
        }
    }

    #[test]
    fn equity_curve_appends_terminal_for_resolved_runs() {
        let samples = vec![sample(1, Some(0.5), 100.0, 9_999.5)];
        let curve = equity_curve(&samples, &settled_result(10_064.0));
        assert_eq!(curve, vec![(1, 9_999.5), (1, 10_064.0)]);
    }

    #[test]
    fn equity_curve_leaves_unresolved_runs_alone() {
        let samples = vec![sample(1, Some(0.5), 100.0, 9_999.5)];
        let curve = equity_curve(&samples, &unresolved_result());
        assert_eq!(curve, vec![(1, 9_999.5)]);
    }

    #[test]
    fn choose_grid_scales_to_span_with_a_floor() {
        // ~8 min span over TARGET_BARS(480) → 1s exactly, at the floor.
        let short = vec![
            sample(0, Some(0.5), 0.0, 0.0),
            sample(480_000_000, Some(0.5), 0.0, 0.0),
        ];
        assert_eq!(choose_grid(&short), DEFAULT_GRID_US);
        // A 24h span picks a coarser grid: 86_400s / 480 = 180s.
        let day = vec![
            sample(0, Some(0.5), 0.0, 0.0),
            sample(86_400_000_000, Some(0.5), 0.0, 0.0),
        ];
        assert_eq!(choose_grid(&day), 180_000_000);
        // A very short / empty run falls back to the floor.
        let tiny = vec![
            sample(0, Some(0.5), 0.0, 0.0),
            sample(1_000, Some(0.5), 0.0, 0.0),
        ];
        assert_eq!(choose_grid(&tiny), DEFAULT_GRID_US);
        assert_eq!(choose_grid(&[]), DEFAULT_GRID_US);
    }

    #[test]
    fn benchmark_holds_from_first_ask_and_redeems() {
        // Enter at the first ask (0.505 → mid 0.5 + 0.005). 10_000 / 0.505 ≈
        // 19_802 shares. Price rises to mid 0.60, then resolves at 1.0.
        let samples = vec![
            sample(0, Some(0.50), 0.0, 0.0),
            sample(1, Some(0.60), 0.0, 0.0),
        ];
        let b = benchmark(&samples, 10_000.0, Some(1.0));
        let shares = 10_000.0 / 0.505;
        assert!((b.pnl[0] - (shares * 0.50 - 10_000.0)).abs() < 1e-6);
        assert!((b.pnl[1] - (shares * 0.60 - 10_000.0)).abs() < 1e-6);
        // Redeems all shares at $1.
        assert!((b.terminal_pnl - (shares * 1.0 - 10_000.0)).abs() < 1e-6);
    }

    #[test]
    fn benchmark_unresolved_ends_at_last_mark() {
        let samples = vec![
            sample(0, Some(0.40), 0.0, 0.0),
            sample(1, Some(0.50), 0.0, 0.0),
        ];
        let b = benchmark(&samples, 10_000.0, None);
        assert_eq!(b.terminal_pnl, *b.pnl.last().unwrap());
    }

    #[test]
    fn resample_carries_last_observation_forward() {
        // Points at 0, 1s, 3s; a 1s grid fills the 2s gap by carrying 20.
        let curve = vec![(0, 10.0), (1_000_000, 20.0), (3_000_000, 40.0)];
        let grid = resample_uniform(&curve, 1_000_000);
        assert_eq!(grid, vec![10.0, 20.0, 20.0, 40.0]);
    }

    #[test]
    fn resample_edge_cases() {
        assert!(resample_uniform(&[], 1_000_000).is_empty());
        assert_eq!(resample_uniform(&[(5, 7.0)], 1_000_000), vec![7.0]);
    }

    #[test]
    fn downsample_buckets_mids_into_ohlc() {
        // Four samples across [0, 4] into 2 buckets (width = 2): bucket 0 gets
        // ts 0,1; bucket 1 gets ts 2,4 (bucket = (ts/width).min(max-1)).
        let samples = vec![
            sample(0, Some(0.50), 0.0, 0.0),
            sample(1, Some(0.60), 0.0, 0.0),
            sample(2, Some(0.40), 0.0, 0.0),
            sample(4, Some(0.55), 0.0, 0.0),
        ];
        let bars = downsample_price(&samples, 2);
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].open, 0.50);
        assert_eq!(bars[0].high, 0.60);
        assert_eq!(bars[1].low, 0.40);
        assert_eq!(bars[1].close, 0.55);
    }

    #[test]
    fn downsample_skips_one_sided_buckets() {
        let samples = vec![sample(0, None, 0.0, 0.0), sample(1, Some(0.5), 0.0, 0.0)];
        let bars = downsample_price(&samples, 8);
        // Only the two-sided sample yields a bar.
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].close, 0.5);
    }
}
