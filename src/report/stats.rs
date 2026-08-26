//! Summary statistics over a finished run.
//!
//! Everything here is pure — it reads the recorded series and the scalar
//! result and produces a [`Stats`] block for the report tiles. Two families of
//! metric with deliberately different treatments:
//!
//! - **Path metrics** (max drawdown, exposure, turnover, total return) are
//!   computed on the raw, event-timed data. Drawdown is path-dependent and
//!   doesn't care about even spacing; exposure and turnover are time- and
//!   trade-weighted directly.
//! - **Return metrics** (Sharpe, Sortino, volatility) need a return series, and
//!   a return series needs even spacing — otherwise it just measures how often
//!   the book ticked. So they run on the equity curve *resampled to a uniform
//!   grid* ([`super::series::resample_uniform`]), and the grid and its implied
//!   annualization factor travel in the output so the numbers are never a
//!   silent assumption.

use serde::Serialize;

use crate::engine::BacktestResult;

use super::record::Sample;
use super::series;

/// Seconds in a (Julian) year, for annualizing grid-period statistics.
const SECONDS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;

/// A peak-to-trough drop in equity: the worst one over the run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct MaxDrawdown {
    /// Fraction of the peak lost (`0.20` = down 20%).
    pub frac: f64,
    /// Dollars lost from peak to trough.
    pub dollars: f64,
    /// Timestamp of the peak the drop started from.
    pub peak_ts: i64,
    /// Timestamp of the trough.
    pub trough_ts: i64,
    /// Peak-to-trough duration in microseconds.
    pub duration_us: i64,
}

/// The report's statistics block. Serialized straight into the JSON/HTML.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Stats {
    // --- transparency: the annualization basis, stated outright ---
    /// Resampling grid, in seconds (each return covers this much time).
    pub bar_seconds: f64,
    /// Number of grid returns the return-metrics are computed from.
    pub bars: usize,
    /// Grid periods per year — the `sqrt` of this scales Sharpe/vol.
    pub bars_per_year: f64,

    // --- headline ---
    /// Net PnL as a fraction of initial capital (includes settlement & fees).
    pub total_return: f64,

    // --- return metrics (on the uniform grid, annualized, rf = 0) ---
    pub sharpe: f64,
    pub sortino: f64,
    /// Per-bar (un-annualized) reward/risk ratios. The annualized figures above
    /// are exactly these times `sqrt(bars_per_year)`; reporting both makes the
    /// annualization a visible multiplier rather than a hidden assumption —
    /// important because annualizing a single short/resolving run extrapolates
    /// one window many times over.
    pub sharpe_raw: f64,
    pub sortino_raw: f64,
    /// Annualized standard deviation of grid log-returns.
    pub annual_vol: f64,
    /// Fraction of grid periods with a positive return.
    pub pct_positive_bars: f64,

    // --- path metrics (on raw event-timed data) ---
    pub max_drawdown: MaxDrawdown,
    /// Fraction of run *time* spent holding a non-zero position.
    pub exposure: f64,
    /// Traded notional over the run as a multiple of initial capital.
    pub turnover: f64,
}

/// Compute the statistics for a run. `grid_us` is the resampling spacing for the
/// return metrics (see [`series::DEFAULT_GRID_US`]).
pub fn compute(
    samples: &[Sample],
    fills: &[super::record::FillEvent],
    result: &BacktestResult,
    grid_us: i64,
) -> Stats {
    let curve = series::equity_curve(samples, result);
    let grid = series::resample_uniform(&curve, grid_us);
    let returns = log_returns(&grid);

    let bar_seconds = grid_us as f64 / 1_000_000.0;
    let bars_per_year = SECONDS_PER_YEAR / bar_seconds;
    let annualize = bars_per_year.sqrt();

    let mean = mean(&returns);
    let sd = std_dev(&returns, mean);
    let downside = downside_dev(&returns);

    // Guarded per-bar ratios: a flat (zero-variance) curve has an undefined
    // Sharpe/Sortino; report 0 rather than a NaN. Annualized = raw × factor.
    let sharpe_raw = if sd > 0.0 { mean / sd } else { 0.0 };
    let sortino_raw = if downside > 0.0 { mean / downside } else { 0.0 };

    Stats {
        bar_seconds,
        bars: returns.len(),
        bars_per_year,
        total_return: result.total_pnl / result.initial_capital,
        sharpe: sharpe_raw * annualize,
        sortino: sortino_raw * annualize,
        sharpe_raw,
        sortino_raw,
        annual_vol: sd * annualize,
        pct_positive_bars: fraction(&returns, |r| *r > 0.0),
        max_drawdown: max_drawdown(&curve),
        exposure: exposure(samples),
        turnover: turnover(fills, result.initial_capital),
    }
}

/// Log-returns of consecutive equity values. Equity is strictly positive for a
/// long-only book (max loss is the cost basis, cash never goes negative), so
/// the logs are always well-defined.
fn log_returns(equity: &[f64]) -> Vec<f64> {
    equity
        .windows(2)
        .map(|w| (w[1] / w[0]).ln())
        .filter(|r| r.is_finite())
        .collect()
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// Population standard deviation (divide by N) — we have the whole run, not a
/// sample of it.
fn std_dev(xs: &[f64], mean: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / xs.len() as f64;
    var.sqrt()
}

/// Downside deviation: RMS of the negative returns only (target = 0).
fn downside_dev(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let sum_sq = xs.iter().map(|x| x.min(0.0).powi(2)).sum::<f64>();
    (sum_sq / xs.len() as f64).sqrt()
}

fn fraction(xs: &[f64], predicate: impl Fn(&f64) -> bool) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().filter(|x| predicate(x)).count() as f64 / xs.len() as f64
}

/// Worst peak-to-trough drop over the equity curve. The peak recorded is the
/// running maximum that *preceded* the deepest trough.
fn max_drawdown(curve: &[(i64, f64)]) -> MaxDrawdown {
    let mut peak = f64::NEG_INFINITY;
    let mut peak_ts = curve.first().map_or(0, |&(ts, _)| ts);
    let mut cur_peak_ts = peak_ts;

    let mut worst = MaxDrawdown {
        frac: 0.0,
        dollars: 0.0,
        peak_ts,
        trough_ts: peak_ts,
        duration_us: 0,
    };

    for &(ts, equity) in curve {
        if equity > peak {
            peak = equity;
            cur_peak_ts = ts;
        }
        let dollars = peak - equity;
        let frac = if peak > 0.0 { dollars / peak } else { 0.0 };
        if frac > worst.frac {
            peak_ts = cur_peak_ts;
            worst = MaxDrawdown {
                frac,
                dollars,
                peak_ts,
                trough_ts: ts,
                duration_us: ts - peak_ts,
            };
        }
    }
    worst
}

/// Time-weighted fraction of the run spent holding inventory. Each sample owns
/// the interval until the next; a sample with a live position counts its
/// interval as exposed.
fn exposure(samples: &[Sample]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let mut exposed = 0i64;
    let mut total = 0i64;
    for pair in samples.windows(2) {
        let dt = pair[1].ts - pair[0].ts;
        total += dt;
        if pair[0].qty != 0.0 {
            exposed += dt;
        }
    }
    if total > 0 {
        exposed as f64 / total as f64
    } else {
        0.0
    }
}

/// Total traded notional (Σ price·qty over fills) as a multiple of initial
/// capital — how many times over the book turned the account.
fn turnover(fills: &[super::record::FillEvent], initial_capital: f64) -> f64 {
    if initial_capital <= 0.0 {
        return 0.0;
    }
    let notional: f64 = fills.iter().map(|f| f.price * f.qty).sum();
    notional / initial_capital
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Settlement;

    fn sample(ts: i64, qty: f64, equity: f64) -> Sample {
        Sample {
            ts,
            bid: None,
            ask: None,
            mid: None,
            cash: 0.0,
            qty,
            avg_cost: 0.0,
            equity,
            pnl: equity,
            quote_bid: None,
            quote_ask: None,
        }
    }

    fn fill(price: f64, qty: f64) -> super::super::record::FillEvent {
        super::super::record::FillEvent {
            ts: 0,
            side: "buy",
            price,
            qty,
        }
    }

    #[test]
    fn max_drawdown_finds_worst_peak_to_trough() {
        let curve = vec![(0, 100.0), (1, 120.0), (2, 90.0), (3, 130.0)];
        let dd = max_drawdown(&curve);
        // Peak 120 → trough 90: 30 dollars, 25%.
        assert!((dd.frac - 0.25).abs() < 1e-12);
        assert!((dd.dollars - 30.0).abs() < 1e-12);
        assert_eq!(dd.peak_ts, 1);
        assert_eq!(dd.trough_ts, 2);
        assert_eq!(dd.duration_us, 1);
    }

    #[test]
    fn max_drawdown_is_zero_for_a_monotone_curve() {
        let curve = vec![(0, 100.0), (1, 110.0), (2, 130.0)];
        assert_eq!(max_drawdown(&curve).frac, 0.0);
    }

    #[test]
    fn exposure_is_time_weighted() {
        // qty>0 over [1,2) and [2,3): 2 of 3 total units of time.
        let samples = vec![
            sample(0, 0.0, 0.0),
            sample(1, 5.0, 0.0),
            sample(2, 5.0, 0.0),
            sample(3, 0.0, 0.0),
        ];
        assert!((exposure(&samples) - 2.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn turnover_is_notional_over_capital() {
        let fills = vec![fill(0.5, 100.0), fill(0.25, 200.0)];
        // 50 + 50 = 100 notional over 10_000 capital.
        assert!((turnover(&fills, 10_000.0) - 0.01).abs() < 1e-12);
    }

    #[test]
    fn return_metrics_run_on_the_grid_and_stay_finite() {
        // Equity wiggles so variance is non-zero: Sharpe should be finite and
        // its sign should follow the drift (here net up over the window).
        let samples = vec![
            sample(0, 1.0, 10_000.0),
            sample(1_000_000, 1.0, 10_050.0),
            sample(2_000_000, 1.0, 10_020.0),
            sample(3_000_000, 1.0, 10_090.0),
        ];
        let result = BacktestResult {
            initial_capital: 10_000.0,
            final_cash: 10_090.0,
            realized_pnl: 90.0,
            total_pnl: 90.0,
            fees: 0.0,
            settlement: Settlement::Unresolved,
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
        };
        let stats = compute(&samples, &[], &result, series::DEFAULT_GRID_US);

        assert_eq!(stats.bars, 3);
        assert!(stats.sharpe.is_finite() && stats.sharpe > 0.0);
        assert!(stats.annual_vol.is_finite() && stats.annual_vol > 0.0);
        assert!((stats.total_return - 0.009).abs() < 1e-12);
        assert!((stats.pct_positive_bars - 2.0 / 3.0).abs() < 1e-12);
        // Annualized is exactly the raw per-bar ratio times sqrt(bars/year).
        let annualize = stats.bars_per_year.sqrt();
        assert!(stats.sharpe_raw > 0.0);
        assert!((stats.sharpe - stats.sharpe_raw * annualize).abs() < 1e-9);
        assert!((stats.sortino - stats.sortino_raw * annualize).abs() < 1e-9);
    }

    #[test]
    fn flat_equity_gives_zero_sharpe_not_nan() {
        let samples = vec![
            sample(0, 0.0, 10_000.0),
            sample(1_000_000, 0.0, 10_000.0),
            sample(2_000_000, 0.0, 10_000.0),
        ];
        let result = BacktestResult {
            initial_capital: 10_000.0,
            final_cash: 10_000.0,
            realized_pnl: 0.0,
            total_pnl: 0.0,
            fees: 0.0,
            settlement: Settlement::Unresolved,
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
        };
        let stats = compute(&samples, &[], &result, series::DEFAULT_GRID_US);
        assert_eq!(stats.sharpe, 0.0);
        assert_eq!(stats.sortino, 0.0);
        assert_eq!(stats.sharpe_raw, 0.0);
        assert_eq!(stats.sortino_raw, 0.0);
        assert_eq!(stats.annual_vol, 0.0);
    }
}
