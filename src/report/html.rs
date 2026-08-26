//! The self-contained HTML report.
//!
//! Rust builds a compact JSON model — the scalar summary, the stats block, a
//! *downsampled* price/PnL series, every fill, and thinned depth frames — and
//! injects it into a static template that carries its own inlined CSS and a
//! vanilla-JS SVG renderer. The output is one file with no external requests:
//! it opens offline and renders the same everywhere.
//!
//! Downsampling lives here, not in the recorder: the `--record` JSON keeps full
//! fidelity (the data contract), while the report caps the plotted series so a
//! multi-million-snapshot run still produces a small, fast page.

use std::path::Path;

use serde::Serialize;

use crate::engine::BacktestResult;

use super::blotter;
use super::record::{Sample, TimeSeriesRecorder};
use super::series;
use super::stats;
use super::Summary;

/// The template, inlined at compile time — the report has no runtime asset
/// dependency.
const TEMPLATE: &str = include_str!("../../assets/report.html");

/// Cap on plotted price/PnL points. Fine enough to read intraday moves, small
/// enough that the page stays light regardless of the run's length.
const MAX_POINTS: usize = 1_500;
/// Cap on depth frames handed to the scrubber.
const MAX_DEPTH_FRAMES: usize = 400;

/// One plotted point. Short field names keep the embedded JSON compact; the
/// renderer reads them by these names.
#[derive(Serialize)]
struct Point {
    /// Timestamp, milliseconds (JS `Date`-friendly).
    t: i64,
    #[serde(rename = "y")]
    yes: Option<f64>,
    #[serde(rename = "n")]
    no: Option<f64>,
    #[serde(rename = "b")]
    bid: Option<f64>,
    #[serde(rename = "a")]
    ask: Option<f64>,
    #[serde(rename = "p")]
    pnl: f64,
    /// Passive buy-and-hold PnL at this instant — the benchmark overlay.
    #[serde(rename = "e")]
    bench: f64,
    #[serde(rename = "q")]
    qty: f64,
    /// Where OUR quotes sat — the maker envelope.
    #[serde(rename = "qb")]
    quote_bid: Option<f64>,
    #[serde(rename = "qa")]
    quote_ask: Option<f64>,
}

#[derive(Serialize)]
struct FillOut {
    t: i64,
    side: &'static str,
    price: f64,
    qty: f64,
}

#[derive(Serialize)]
struct DepthOut {
    t: i64,
    /// Bids as `[price, size]` pairs, best-first.
    b: Vec<[f64; 2]>,
    /// Asks as `[price, size]` pairs, best-first.
    a: Vec<[f64; 2]>,
}

#[derive(Serialize)]
struct Model<'a> {
    title: &'a str,
    title_sub: &'a str,
    summary: Summary,
    stats: stats::Stats,
    /// Terminal PnL of the buy-and-hold benchmark — for the alpha tile.
    bench_final_pnl: f64,
    trade_stats: blotter::TradeStats,
    order_stats: super::orders::OrderStats,
    blotter: Vec<blotter::BlotterRow>,
    pts: Vec<Point>,
    fills: Vec<FillOut>,
    depth: Vec<DepthOut>,
}

/// µs → ms, for the JS time axis.
fn ms(us: i64) -> i64 {
    us / 1_000
}

/// Time-bucket the samples down to at most `MAX_POINTS`, keeping the last
/// sample in each bucket (the bucket's closing state). A resolved run gets a
/// terminal point at the settled PnL so the curve shows the redemption jump.
fn downsample_points(
    samples: &[Sample],
    result: &BacktestResult,
    bench: &[f64],
    bench_terminal: f64,
) -> Vec<Point> {
    let mut pts: Vec<Point> = Vec::new();
    if samples.is_empty() {
        return pts;
    }
    let t0 = samples[0].ts;
    let span = (samples[samples.len() - 1].ts - t0).max(1);
    let width = (span as usize / MAX_POINTS).max(1) as i64;
    let mut cur_bucket = i64::MIN;

    for (i, s) in samples.iter().enumerate() {
        let bucket = (s.ts - t0) / width;
        let point = Point {
            t: ms(s.ts),
            yes: s.mid,
            no: s.mid.map(|m| 1.0 - m),
            bid: s.bid,
            ask: s.ask,
            pnl: s.pnl,
            bench: bench.get(i).copied().unwrap_or(0.0),
            qty: s.qty,
            quote_bid: s.quote_bid,
            quote_ask: s.quote_ask,
        };
        if bucket == cur_bucket {
            // Same bucket: overwrite with the later (closing) sample.
            *pts.last_mut().expect("bucket started") = point;
        } else {
            cur_bucket = bucket;
            pts.push(point);
        }
    }

    // Terminal settlement: a vertical step to the net PnL at the final instant —
    // both the strategy and the benchmark redeem here.
    if !matches!(result.settlement, crate::engine::Settlement::Unresolved) {
        if let Some(last) = samples.last() {
            pts.push(Point {
                t: ms(last.ts),
                yes: None,
                no: None,
                bid: None,
                ask: None,
                pnl: result.total_pnl,
                bench: bench_terminal,
                qty: 0.0,
                quote_bid: None,
                quote_ask: None,
            });
        }
    }
    pts
}

/// Stride the depth frames down to at most `MAX_DEPTH_FRAMES`.
fn downsample_depth(recorder: &TimeSeriesRecorder) -> Vec<DepthOut> {
    let frames = recorder.depth();
    let step = (frames.len() / MAX_DEPTH_FRAMES).max(1);
    frames
        .iter()
        .step_by(step)
        .map(|f| DepthOut {
            t: ms(f.ts),
            b: f.bids.iter().map(|l| [l.price, l.size]).collect(),
            a: f.asks.iter().map(|l| [l.price, l.size]).collect(),
        })
        .collect()
}

/// Build the standalone HTML report as a string.
pub fn render(
    title: &str,
    subtitle: &str,
    result: &BacktestResult,
    recorder: &TimeSeriesRecorder,
    resolution: Option<f64>,
) -> String {
    // The return-metric grid is chosen from the run's own span (see
    // [`series::choose_grid`]) so it's never a caller's guess.
    let grid_us = series::choose_grid(recorder.samples());
    let bench = series::benchmark(recorder.samples(), result.initial_capital, resolution);
    let (blotter_rows, trade_stats) = blotter::compute(recorder.fills());
    // The blotter carries µs timestamps (the JSON contract's unit); the JS time
    // axis wants ms, like `Point`/`FillOut`, so convert here for the render.
    let blotter: Vec<blotter::BlotterRow> = blotter_rows
        .into_iter()
        .map(|mut r| {
            r.ts = ms(r.ts);
            r
        })
        .collect();
    let model = Model {
        title,
        title_sub: subtitle,
        summary: Summary::from(result),
        stats: stats::compute(recorder.samples(), recorder.fills(), result, grid_us),
        bench_final_pnl: bench.terminal_pnl,
        trade_stats,
        order_stats: super::orders::compute(recorder.orders(), result.fills),
        blotter,
        pts: downsample_points(recorder.samples(), result, &bench.pnl, bench.terminal_pnl),
        fills: recorder
            .fills()
            .iter()
            .map(|f| FillOut {
                t: ms(f.ts),
                side: f.side,
                price: f.price,
                qty: f.qty,
            })
            .collect(),
        depth: downsample_depth(recorder),
    };

    // Serialize, then neutralize any "</" so the JSON can't close the host
    // <script> element early (defense against a stray sequence in a string).
    let json = serde_json::to_string(&model)
        .expect("report model serializes")
        .replace("</", "<\\/");

    TEMPLATE
        .replace("__DATA__", &json)
        .replace("__TITLE__", &escape_html(title))
}

/// Write the report to a file.
pub fn write_html(
    path: &Path,
    title: &str,
    subtitle: &str,
    result: &BacktestResult,
    recorder: &TimeSeriesRecorder,
    resolution: Option<f64>,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(path, render(title, subtitle, result, recorder, resolution))?;
    Ok(())
}

/// Minimal HTML text escaping for the title, which lands in element text and a
/// `<title>`.
pub(crate) fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::marketstate::{Level, MarketState};
    use crate::domain::types::{MarketId, Price};
    use crate::engine::{Engine, Simulation};
    use crate::fill::fillmodel::NaiveFillModel;
    use crate::io;
    use crate::report::record::RecorderConfig;

    fn snapshot(ts: i64, bid: f64, ask: f64) -> MarketState {
        let level = |p: f64| {
            Box::from([Level {
                price: Price::from_probability(p).unwrap(),
                size: 1_000.0,
            }])
        };
        MarketState::new(
            MarketId::new("token".to_string()),
            ts,
            level(bid),
            level(ask),
        )
        .unwrap()
    }

    #[test]
    fn render_produces_self_contained_html_with_data() {
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let feed = io::fixture(vec![
            snapshot(1_000_000, 0.30, 0.31),
            snapshot(2_000_000, 0.40, 0.41),
        ]);
        let result = Engine::new(10_000.0, Box::new(NaiveFillModel), Simulation::zero())
            .run_recorded(
                feed,
                &mut crate::buyandhold::BuyAndHold::new(100.0),
                Some(Price::from_probability(1.0).unwrap()),
                &mut recorder,
            )
            .unwrap();

        let html = render("Test", "sub", &result, &recorder, Some(1.0));
        // Placeholders are gone and the data is embedded.
        assert!(!html.contains("__DATA__"));
        assert!(!html.contains("__TITLE__"));
        assert!(html.contains("\"summary\""));
        assert!(html.contains("\"pts\""));
        // No external resource references (offline / self-contained): no CDN
        // links or remote script/style sources. (The SVG XML namespace URL is
        // not a fetch, so it's allowed.)
        assert!(!html.contains("<link"));
        assert!(!html.contains("src=\"http"));
        assert!(!html.contains("@import"));
    }
}
