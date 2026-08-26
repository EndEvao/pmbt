//! The batch (universe) report: cumulative PnL across many markets, with the
//! per-market attribution that says where the total came from.
//!
//! Each market in a universe runs its own engine from its own initial capital,
//! so "the batch result" is the **sum** of the per-market PnLs. This module
//! builds two views of that:
//!
//! - a **combined cumulative-PnL curve** over calendar time — each market
//!   contributes its mark-to-market PnL from its first snapshot, stepping to its
//!   settled total at the end; the curves are summed on a shared timeline, so
//!   the line is the portfolio's running PnL as markets trade and resolve;
//! - a **per-market attribution** — every market split into realized /
//!   settlement / −fees, the exact decomposition of its contribution.
//!
//! Memory stays bounded: each market's series is downsampled to a small curve
//! right after its run, so the batch holds curves, not full recorders.

use std::path::Path;

use serde::Serialize;

use crate::engine::{BacktestResult, Settlement};

use super::record::TimeSeriesRecorder;
use super::series;

/// Points kept per market before combining — enough to shape each contribution,
/// bounded so a large universe's union timeline stays small.
const PER_MARKET_POINTS: usize = 600;
/// Cap on the combined curve handed to the chart.
const MAX_POINTS: usize = 1_500;

/// A finished market, reduced to what the batch report needs: its scalar
/// result and its downsampled PnL contribution curve (`(ts_us, pnl)`, terminal
/// settlement included). The full recorder is dropped after this is built.
pub struct MarketRun {
    pub id: String,
    pub category: String,
    pub result: BacktestResult,
    pub curve: Vec<(i64, f64)>,
}

impl MarketRun {
    /// Reduce a recorded run to a `MarketRun`, downsampling its equity curve to
    /// a PnL contribution (`equity - initial_capital`).
    pub fn from_recorded(
        id: String,
        category: String,
        result: BacktestResult,
        recorder: &TimeSeriesRecorder,
    ) -> Self {
        let equity = series::equity_curve(recorder.samples(), &result);
        let initial = result.initial_capital;
        let curve = downsample_curve(&equity, initial, PER_MARKET_POINTS);
        Self {
            id,
            category,
            result,
            curve,
        }
    }
}

/// Time-bucket a `(ts, equity)` curve down to `max` points of `(ts, pnl)`,
/// keeping the last (closing) value in each bucket. Preserves the first and
/// last points so the contribution starts and ends where the market did.
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

/// Sum the per-market PnL contributions onto one shared timeline. Before a
/// market's first point it contributes 0; after its last it holds its final
/// PnL (last-observation-carried-forward). The result is the portfolio's
/// cumulative PnL over calendar time.
fn combined_curve(runs: &[MarketRun]) -> Vec<(i64, f64)> {
    let curves: Vec<&Vec<(i64, f64)>> = runs.iter().map(|r| &r.curve).collect();

    // Union of every timestamp, ascending and unique.
    let mut times: Vec<i64> = curves
        .iter()
        .flat_map(|c| c.iter().map(|&(t, _)| t))
        .collect();
    times.sort_unstable();
    times.dedup();

    // A cursor per market, advanced monotonically as the shared time increases.
    let mut cursor = vec![0usize; curves.len()];
    let mut out: Vec<(i64, f64)> = Vec::with_capacity(times.len());
    for &t in &times {
        let mut sum = 0.0;
        for (ci, curve) in curves.iter().enumerate() {
            if curve.is_empty() {
                continue;
            }
            while cursor[ci] + 1 < curve.len() && curve[cursor[ci] + 1].0 <= t {
                cursor[ci] += 1;
            }
            // Zero until this market's first point; its PnL thereafter.
            sum += if t < curve[0].0 {
                0.0
            } else {
                curve[cursor[ci]].1
            };
        }
        out.push((t, sum));
    }

    // Downsample the (already unioned) combined curve to the plot cap.
    if out.len() > MAX_POINTS {
        let step = out.len().div_ceil(MAX_POINTS);
        let last = *out.last().expect("non-empty");
        let mut thinned: Vec<(i64, f64)> = out.iter().step_by(step).copied().collect();
        if thinned.last() != Some(&last) {
            thinned.push(last);
        }
        thinned
    } else {
        out
    }
}

// --- serialized model -------------------------------------------------------

#[derive(Serialize)]
struct CurvePoint {
    /// Timestamp, milliseconds.
    t: i64,
    #[serde(rename = "p")]
    pnl: f64,
}

#[derive(Serialize)]
struct MarketRow {
    id: String,
    short: String,
    category: String,
    total: f64,
    realized: f64,
    settlement: f64,
    fees: f64,
    state: &'static str,
    fills: u64,
}

#[derive(Serialize)]
struct AggOut {
    markets: usize,
    total_pnl: f64,
    realized_pnl: f64,
    settlement_pnl: f64,
    fees: f64,
    fills: u64,
    rejections: u64,
    settled: usize,
    unresolved: usize,
    nothing_held: usize,
}

#[derive(Serialize)]
struct BatchModel<'a> {
    name: &'a str,
    agg: AggOut,
    combined: Vec<CurvePoint>,
    markets: Vec<MarketRow>,
}

/// Shorten a long token id for display, matching the CLI's per-market lines.
fn short_id(id: &str) -> String {
    if id.len() > 12 {
        format!("{}…{}", &id[..4], &id[id.len() - 4..])
    } else {
        id.to_string()
    }
}

fn build_model<'a>(name: &'a str, runs: &[MarketRun]) -> BatchModel<'a> {
    let mut agg = AggOut {
        markets: runs.len(),
        total_pnl: 0.0,
        realized_pnl: 0.0,
        settlement_pnl: 0.0,
        fees: 0.0,
        fills: 0,
        rejections: 0,
        settled: 0,
        unresolved: 0,
        nothing_held: 0,
    };
    let mut markets = Vec::with_capacity(runs.len());
    for run in runs {
        let r = &run.result;
        let (settlement_pnl, state) = match &r.settlement {
            Settlement::Settled { pnl, .. } => {
                agg.settled += 1;
                (*pnl, "settled")
            }
            Settlement::Unresolved => {
                agg.unresolved += 1;
                (0.0, "unresolved")
            }
            Settlement::NothingHeld => {
                agg.nothing_held += 1;
                (0.0, "nothing_held")
            }
        };
        agg.total_pnl += r.total_pnl;
        agg.realized_pnl += r.realized_pnl;
        agg.settlement_pnl += settlement_pnl;
        agg.fees += r.fees;
        agg.fills += r.fills;
        agg.rejections += r.rejections;
        markets.push(MarketRow {
            id: run.id.clone(),
            short: short_id(&run.id),
            category: run.category.clone(),
            total: r.total_pnl,
            realized: r.realized_pnl,
            settlement: settlement_pnl,
            fees: r.fees,
            state,
            fills: r.fills,
        });
    }
    // Biggest contributors first — the attribution reads top-down.
    markets.sort_by(|a, b| {
        b.total
            .partial_cmp(&a.total)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let combined = combined_curve(runs)
        .into_iter()
        .map(|(t, p)| CurvePoint {
            t: t / 1_000,
            pnl: p,
        })
        .collect();

    BatchModel {
        name,
        agg,
        combined,
        markets,
    }
}

const TEMPLATE: &str = include_str!("../../assets/report_batch.html");

/// Build the batch HTML report as a string.
pub fn render(name: &str, runs: &[MarketRun]) -> String {
    let model = build_model(name, runs);
    let json = serde_json::to_string(&model)
        .expect("batch model serializes")
        .replace("</", "<\\/");
    TEMPLATE
        .replace("__DATA__", &json)
        .replace("__TITLE__", &super::html::escape_html(name))
}

/// Write the batch HTML report.
pub fn write_html(
    path: &Path,
    name: &str,
    runs: &[MarketRun],
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, render(name, runs))?;
    Ok(())
}

/// Write the batch model as JSON — the `--record` data contract for a batch.
pub fn write_json(
    path: &Path,
    name: &str,
    runs: &[MarketRun],
) -> Result<(), Box<dyn std::error::Error>> {
    let model = build_model(name, runs);
    let file = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(std::io::BufWriter::new(file), &model)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_at(id: &str, times: &[(i64, f64)], total: f64) -> MarketRun {
        // Fake a curve directly; result only needs total/settlement for agg.
        MarketRun {
            id: id.to_string(),
            category: "other".to_string(),
            result: BacktestResult {
                initial_capital: 10_000.0,
                final_cash: 10_000.0 + total,
                realized_pnl: 0.0,
                total_pnl: total,
                fees: 0.0,
                settlement: Settlement::Settled {
                    quantity: 1.0,
                    redemption: crate::domain::types::Price::from_probability(1.0).unwrap(),
                    pnl: total,
                },
                open_position: None,
                fills: 1,
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
            },
            curve: times.to_vec(),
        }
    }

    #[test]
    fn combined_sums_contributions_with_locf() {
        // Market A: pnl 0→10 over t 0..2. Market B: pnl 0→5 over t 1..3.
        let a = run_at("a", &[(0, 0.0), (2_000_000, 10.0)], 10.0);
        let b = run_at("b", &[(1_000_000, 0.0), (3_000_000, 5.0)], 5.0);
        let combined = combined_curve(&[a, b]);

        // Union times: 0,1e6,2e6,3e6. B contributes 0 before t=1e6.
        assert_eq!(combined[0], (0, 0.0)); // A=0, B not started
        assert_eq!(combined[1], (1_000_000, 0.0)); // A=0 (LOCF), B=0
        assert_eq!(combined[2], (2_000_000, 10.0)); // A=10, B=0 (LOCF)
        assert_eq!(combined[3], (3_000_000, 15.0)); // A=10 (LOCF past end), B=5
    }

    #[test]
    fn model_aggregates_and_sorts_markets() {
        let a = run_at("aaaaaaaaaaaaaa", &[(0, 0.0), (1, 3.0)], 3.0);
        let b = run_at("bbbbbbbbbbbbbb", &[(0, 0.0), (1, 20.0)], 20.0);
        let model = build_model("u", &[a, b]);
        assert_eq!(model.agg.markets, 2);
        assert!((model.agg.total_pnl - 23.0).abs() < 1e-9);
        assert_eq!(model.agg.settled, 2);
        // Sorted by contribution: b (20) before a (3).
        assert_eq!(model.markets[0].id, "bbbbbbbbbbbbbb");
        assert!(model.markets[0].short.contains("…"));
    }
}
