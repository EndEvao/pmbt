//! Turning a backtest into something you can look at.
//!
//! The engine records a run through a [`record::Recorder`]; this module owns the
//! observers it drives ([`record::TimeSeriesRecorder`]) and the serializers that
//! turn a finished run into an artifact. Today that is a JSON dump — the stable
//! data contract every richer view (the HTML report, external tooling) builds
//! on. The engine depends only on the `Recorder` trait; the heavier rendering
//! sits behind it, so a run that isn't being reported pulls none of it in.

pub mod batch;
pub mod blotter;
pub mod compare;
pub mod html;
pub mod orders;
pub mod parquet;
pub mod record;
pub mod series;
pub mod stats;

use std::path::Path;

use serde::Serialize;

use crate::engine::{BacktestResult, Settlement};
use record::TimeSeriesRecorder;

/// A flat, serializable view of a `BacktestResult`. The engine's result carries
/// domain types (`Settlement`, `Position`) that aren't meant to be wire types;
/// this is the report boundary's own shape, so the JSON schema is stable even
/// if the internal result grows.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub initial_capital: f64,
    pub final_cash: f64,
    pub realized_pnl: f64,
    pub total_pnl: f64,
    pub fees: f64,
    /// `"unresolved"`, `"nothing_held"`, or `"settled"`.
    pub settlement: &'static str,
    /// The settlement leg of PnL alone (0.0 unless settled).
    pub settlement_pnl: f64,
    /// Redemption price of the held token when settled ($0/$1), else `None`.
    pub redemption: Option<f64>,
    pub fills: u64,
    /// The maker/taker split of `fills` — the first thing to check on a maker
    /// strategy, and the cross-check on fees (maker fills pay none).
    pub maker_fills: u64,
    pub taker_fills: u64,
    pub rejections: u64,
    pub cancels: u64,
    pub expired: u64,
    /// Resting orders that outlived the feed, and the size they held.
    pub expired_resting: u64,
    pub resting_quantity: f64,
    /// The holding left open on an unresolved run: quantity + average cost.
    pub open_qty: Option<f64>,
    pub open_avg: Option<f64>,
}

impl From<&BacktestResult> for Summary {
    fn from(result: &BacktestResult) -> Self {
        let (settlement, settlement_pnl, redemption) = match &result.settlement {
            Settlement::Unresolved => ("unresolved", 0.0, None),
            Settlement::NothingHeld => ("nothing_held", 0.0, None),
            Settlement::Settled {
                pnl, redemption, ..
            } => ("settled", *pnl, Some(redemption.as_f64())),
        };
        let (open_qty, open_avg) = match &result.open_position {
            Some(position) => (Some(position.quantity()), Some(position.average_price())),
            None => (None, None),
        };
        Self {
            initial_capital: result.initial_capital,
            final_cash: result.final_cash,
            realized_pnl: result.realized_pnl,
            total_pnl: result.total_pnl,
            fees: result.fees,
            settlement,
            settlement_pnl,
            redemption,
            fills: result.fills,
            maker_fills: result.maker_fills,
            taker_fills: result.taker_fills,
            rejections: result.rejections,
            cancels: result.cancels,
            expired: result.expired,
            expired_resting: result.expired_resting,
            resting_quantity: result.resting_quantity,
            open_qty,
            open_avg,
        }
    }
}

/// The full JSON document for one run: the scalar summary, the computed
/// statistics, and the recorded series. Borrows the recorder's buffers so
/// writing copies nothing.
#[derive(Debug, Serialize)]
struct RunDocument<'a> {
    summary: Summary,
    stats: stats::Stats,
    trade_stats: blotter::TradeStats,
    order_stats: orders::OrderStats,
    blotter: Vec<blotter::BlotterRow>,
    samples: &'a [record::Sample],
    fills: &'a [record::FillEvent],
    depth: &'a [record::DepthFrame],
    orders: &'a [record::OrderEvent],
}

/// Write the run's summary, statistics, and full time series to a
/// pretty-printed JSON file — the `--record` output. This is the data contract:
/// the HTML report and any external analysis read exactly this shape.
pub fn write_json(
    path: &Path,
    result: &BacktestResult,
    recorder: &TimeSeriesRecorder,
) -> Result<(), Box<dyn std::error::Error>> {
    let (blotter_rows, trade_stats) = blotter::compute(recorder.fills());
    let document = RunDocument {
        summary: Summary::from(result),
        stats: stats::compute(
            recorder.samples(),
            recorder.fills(),
            result,
            series::choose_grid(recorder.samples()),
        ),
        trade_stats,
        order_stats: orders::compute(recorder.orders(), result.fills),
        blotter: blotter_rows,
        samples: recorder.samples(),
        fills: recorder.fills(),
        depth: recorder.depth(),
        orders: recorder.orders(),
    };
    let file = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(std::io::BufWriter::new(file), &document)?;
    Ok(())
}
