//! The realism-comparison report: the *same* strategy over the *same* feed under
//! a matrix of simulation configs, so the cost of each realism component is a
//! measured difference rather than a guess.
//!
//! Because every config replays one feed, their sample timestamps align exactly
//! — so, unlike the batch report's calendar merge, the PnL curves overlay
//! point-for-point on a shared time axis. For the cumulative **realism ladder**
//! (each config adds one friction: latency → taker delay → fees → perception)
//! the consecutive differences in total PnL form a **cost waterfall**: what each
//! effect took off the frictionless run.
//!
//! Entirely downstream of the engine — `main` runs the matrix and hands the
//! finished runs here; this module only reduces and renders them.

use std::path::Path;

use serde::Serialize;

use crate::engine::BacktestResult;

use super::record::TimeSeriesRecorder;
use super::{series, stats};

/// Points kept per config curve. All configs share one timeline, so this is the
/// plot cap directly (no union step like the batch report needs).
const MAX_POINTS: usize = 1_500;

/// One config's finished run, reduced to its scalar result, return stats and a
/// downsampled PnL curve `(ts_us, pnl)`. The full recorder is dropped after.
pub struct CompareRun {
    /// Short axis label, e.g. `"frictionless"`, `"+fees"`, `"us-east"`.
    pub label: String,
    /// Human-readable config, e.g. `"latency dublin · taker-delay on · fees on"`.
    pub detail: String,
    pub result: BacktestResult,
    pub stats: stats::Stats,
    pub curve: Vec<(i64, f64)>,
}

impl CompareRun {
    pub fn from_recorded(
        label: String,
        detail: String,
        result: BacktestResult,
        recorder: &TimeSeriesRecorder,
    ) -> Self {
        let equity = series::equity_curve(recorder.samples(), &result);
        let grid = series::choose_grid(recorder.samples());
        let stats = stats::compute(recorder.samples(), recorder.fills(), &result, grid);
        let curve = downsample_curve(&equity, result.initial_capital, MAX_POINTS);
        Self {
            label,
            detail,
            result,
            stats,
            curve,
        }
    }
}

/// Time-bucket a `(ts, equity)` curve down to `max` points of `(ts, pnl)`,
/// keeping the closing value in each bucket. (Same reduction the batch report
/// uses; duplicated to keep the two report models independent.)
fn downsample_curve(equity: &[(i64, f64)], initial: f64, max: usize) -> Vec<(i64, f64)> {
    if equity.is_empty() {
        return Vec::new();
    }
    let t0 = equity[0].0;
    let span = (equity[equity.len() - 1].0 - t0).max(1);
    let width = (span as usize / max).max(1) as i64;
    let mut out: Vec<(i64, f64)> = Vec::new();
    let mut cur_bucket = i64::MIN;
    for &(ts, eq) in equity {
        let bucket = (ts - t0) / width;
        let point = (ts, eq - initial);
        if bucket == cur_bucket {
            *out.last_mut().expect("bucket started") = point;
        } else {
            cur_bucket = bucket;
            out.push(point);
        }
    }
    out
}

/// The cost waterfall for the realism ladder: the frictionless baseline, then
/// the total-PnL delta each added friction cost (usually negative), then the
/// realistic total. Empty for the non-ladder axes (their configs aren't a
/// cumulative chain, so consecutive deltas have no "cost" meaning).
fn waterfall(runs: &[CompareRun], ladder: bool) -> Vec<WaterStep> {
    if !ladder || runs.len() < 2 {
        return Vec::new();
    }
    let mut steps = Vec::with_capacity(runs.len() + 1);
    steps.push(WaterStep {
        label: runs[0].label.clone(),
        delta: runs[0].result.total_pnl,
        total: false,
    });
    for pair in runs.windows(2) {
        steps.push(WaterStep {
            label: pair[1].label.clone(),
            delta: pair[1].result.total_pnl - pair[0].result.total_pnl,
            total: false,
        });
    }
    steps.push(WaterStep {
        label: "realistic".to_string(),
        delta: runs[runs.len() - 1].result.total_pnl,
        total: true,
    });
    steps
}

// --- serialized model -------------------------------------------------------

#[derive(Serialize)]
struct CurvePoint {
    /// Timestamp, milliseconds (JS `Date`-friendly).
    t: i64,
    #[serde(rename = "p")]
    pnl: f64,
}

#[derive(Serialize)]
struct RunOut {
    label: String,
    detail: String,
    total_pnl: f64,
    total_return: f64,
    realized: f64,
    settlement: f64,
    fees: f64,
    sharpe: f64,
    sharpe_raw: f64,
    max_dd: f64,
    fills: u64,
    rejections: u64,
    expired: u64,
    curve: Vec<CurvePoint>,
}

#[derive(Serialize)]
struct WaterStep {
    label: String,
    delta: f64,
    total: bool,
}

#[derive(Serialize)]
pub struct CompareModel<'a> {
    title: &'a str,
    subtitle: &'a str,
    /// True for the cumulative realism ladder (drives the cost waterfall).
    ladder: bool,
    /// Whether a category was set — taker-delay/fees only bite when it is, so
    /// the report flags those rungs as no-ops when it's false.
    category_known: bool,
    runs: Vec<RunOut>,
    waterfall: Vec<WaterStep>,
}

/// µs → ms.
fn ms(us: i64) -> i64 {
    us / 1_000
}

pub fn build_model<'a>(
    title: &'a str,
    subtitle: &'a str,
    ladder: bool,
    category_known: bool,
    runs: &[CompareRun],
) -> CompareModel<'a> {
    let out_runs = runs
        .iter()
        .map(|r| {
            let s = &r.stats;
            let res = &r.result;
            let settlement = match &res.settlement {
                crate::engine::Settlement::Settled { pnl, .. } => *pnl,
                _ => 0.0,
            };
            RunOut {
                label: r.label.clone(),
                detail: r.detail.clone(),
                total_pnl: res.total_pnl,
                total_return: s.total_return,
                realized: res.realized_pnl,
                settlement,
                fees: res.fees,
                sharpe: s.sharpe,
                sharpe_raw: s.sharpe_raw,
                max_dd: s.max_drawdown.frac,
                fills: res.fills,
                rejections: res.rejections,
                expired: res.expired,
                curve: r
                    .curve
                    .iter()
                    .map(|&(t, pnl)| CurvePoint { t: ms(t), pnl })
                    .collect(),
            }
        })
        .collect();

    CompareModel {
        title,
        subtitle,
        ladder,
        category_known,
        runs: out_runs,
        waterfall: waterfall(runs, ladder),
    }
}

/// The self-contained comparison template, inlined at compile time.
const TEMPLATE: &str = include_str!("../../assets/report_compare.html");

/// Render the comparison as one self-contained HTML page.
pub fn render(
    title: &str,
    subtitle: &str,
    ladder: bool,
    category_known: bool,
    runs: &[CompareRun],
) -> String {
    let model = build_model(title, subtitle, ladder, category_known, runs);
    // Neutralize any "</" so the JSON can't close the host <script> early.
    let json = serde_json::to_string(&model)
        .expect("comparison model serializes")
        .replace("</", "<\\/");
    TEMPLATE
        .replace("__DATA__", &json)
        .replace("__TITLE__", &super::html::escape_html(title))
}

/// Write the comparison report to a file.
pub fn write_html(
    path: &Path,
    title: &str,
    subtitle: &str,
    ladder: bool,
    category_known: bool,
    runs: &[CompareRun],
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, render(title, subtitle, ladder, category_known, runs))?;
    Ok(())
}

/// Write the comparison model as pretty JSON — the data contract for the view.
pub fn write_json(
    path: &Path,
    title: &str,
    subtitle: &str,
    ladder: bool,
    category_known: bool,
    runs: &[CompareRun],
) -> Result<(), Box<dyn std::error::Error>> {
    let model = build_model(title, subtitle, ladder, category_known, runs);
    let file = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(std::io::BufWriter::new(file), &model)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Settlement;

    fn run(label: &str, total_pnl: f64) -> CompareRun {
        // A minimal CompareRun with a hand-set total; stats/curve unused here.
        let result = BacktestResult {
            initial_capital: 10_000.0,
            final_cash: 10_000.0 + total_pnl,
            realized_pnl: total_pnl,
            total_pnl,
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
        let stats = stats::compute(&[], &[], &result, series::DEFAULT_GRID_US);
        CompareRun {
            label: label.to_string(),
            detail: String::new(),
            result,
            stats,
            curve: Vec::new(),
        }
    }

    #[test]
    fn waterfall_deltas_bridge_frictionless_to_realistic() {
        // 100 → 80 → 50: latency cost −20, fees cost −30.
        let runs = vec![
            run("frictionless", 100.0),
            run("+latency", 80.0),
            run("+fees", 50.0),
        ];
        let steps = waterfall(&runs, true);
        assert_eq!(steps.len(), 4); // baseline + 2 deltas + total
        assert_eq!(steps[0].delta, 100.0);
        assert!((steps[1].delta + 20.0).abs() < 1e-9);
        assert!((steps[2].delta + 30.0).abs() < 1e-9);
        assert!(steps[3].total);
        assert_eq!(steps[3].delta, 50.0);
        // Baseline + interior deltas reconstruct the realistic total.
        let recon: f64 = steps[..steps.len() - 1].iter().map(|s| s.delta).sum();
        assert!((recon - 50.0).abs() < 1e-9);
    }

    #[test]
    fn non_ladder_axes_have_no_waterfall() {
        let runs = vec![run("zero", 10.0), run("us-east", 4.0)];
        assert!(waterfall(&runs, false).is_empty());
    }
}
