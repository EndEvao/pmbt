//! Standalone research report. Calculations use the full event history;
//! charts retain extrema within bounded time buckets, plus terminal settlement.
use super::record::{BookQuality, Sample, TimeSeriesRecorder};
use super::{blotter, series, stats, Summary};
use crate::engine::{BacktestResult, Settlement};
use serde::Serialize;
use std::path::Path;
const TEMPLATE: &str = include_str!("../../assets/report.html");
const MAX_POINTS: usize = 1500;
const MAX_DEPTH_FRAMES: usize = 400;

/// Supplied by the run command after all CLI/profile overrides are resolved.
/// Unknown metadata stays unknown for library callers.
#[derive(Debug, Default, Serialize)]
pub struct ReportContext {
    pub market_name: Option<String>,
    pub outcome: Option<String>,
    pub settings: Vec<(String, String)>,
    pub queue: Option<String>,
    pub tape_present: Option<bool>,
}

#[derive(Clone, Serialize)]
struct Point {
    t: i64,
    y: Option<f64>,
    n: Option<f64>,
    b: Option<f64>,
    a: Option<f64>,
    p: f64,
    e: f64,
    q: f64,
    dd: f64,
    qb: Option<f64>,
    qa: Option<f64>,
    settlement: bool,
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
    b: Vec<[f64; 2]>,
    a: Vec<[f64; 2]>,
}
#[derive(Serialize)]
struct Analysis {
    ending_equity: f64,
    headline_pnl: f64,
    headline_return: f64,
    unrealized_pnl: f64,
    avg_deployed: Option<f64>,
    peak_deployed: f64,
    peak_inventory: f64,
    recorded_samples: usize,
    fill_records: usize,
    cancels_missed: u64,
    expired_gtd: u64,
    depth_levels: usize,
}
impl Analysis {
    fn compute(samples: &[Sample], result: &BacktestResult, recorder: &TimeSeriesRecorder) -> Self {
        let unresolved = matches!(result.settlement, Settlement::Unresolved);
        let ending_equity = if unresolved {
            samples.last().map_or(result.final_cash, |s| s.equity)
        } else {
            result.final_cash
        };
        let headline_pnl = ending_equity - result.initial_capital;
        let duration = samples
            .first()
            .zip(samples.last())
            .map_or(0, |(a, b)| b.ts - a.ts);
        let deployed_time: f64 = samples
            .windows(2)
            .map(|w| w[0].qty * w[0].avg_cost * (w[1].ts - w[0].ts) as f64)
            .sum();
        Self {
            ending_equity,
            headline_pnl,
            headline_return: headline_pnl / result.initial_capital,
            unrealized_pnl: if unresolved {
                headline_pnl - result.total_pnl
            } else {
                0.0
            },
            avg_deployed: (duration > 0)
                .then(|| deployed_time / duration as f64 / result.initial_capital),
            peak_deployed: samples
                .iter()
                .map(|s| s.qty * s.avg_cost / result.initial_capital)
                .fold(0.0, f64::max),
            peak_inventory: samples.iter().map(|s| s.qty).fold(0.0, f64::max),
            recorded_samples: samples.len(),
            fill_records: recorder.fills().len(),
            cancels_missed: result.cancels_missed,
            expired_gtd: result.expired_gtd,
            depth_levels: recorder.config().depth_levels,
        }
    }
}
#[derive(Serialize)]
struct Model<'a> {
    title: &'a str,
    title_sub: &'a str,
    context: &'a ReportContext,
    summary: Summary,
    stats: stats::Stats,
    analysis: Analysis,
    quality: &'a BookQuality,
    bench_final_pnl: f64,
    trade_stats: blotter::TradeStats,
    order_stats: super::orders::OrderStats,
    blotter: Vec<blotter::BlotterRow>,
    pts: Vec<Point>,
    fills: Vec<FillOut>,
    depth: Vec<DepthOut>,
}
fn ms(us: i64) -> i64 {
    us / 1000
}

fn downsample_points(
    samples: &[Sample],
    result: &BacktestResult,
    bench: &[f64],
    bench_terminal: f64,
) -> Vec<Point> {
    let mut peak = f64::NEG_INFINITY;
    let mut all: Vec<Point> = samples
        .iter()
        .enumerate()
        .map(|(i, s)| {
            peak = peak.max(s.equity);
            Point {
                t: ms(s.ts),
                y: s.mid,
                n: s.mid.map(|p| 1.0 - p),
                b: s.bid,
                a: s.ask,
                p: s.pnl,
                e: bench.get(i).copied().unwrap_or(0.0),
                q: s.qty,
                dd: if peak > 0.0 {
                    (peak - s.equity) / peak
                } else {
                    0.0
                },
                qb: s.quote_bid,
                qa: s.quote_ask,
                settlement: false,
            }
        })
        .collect();
    // At most ten representatives per bucket: endpoints plus extrema of P&L,
    // inventory, price and drawdown. Never compute statistics from this subset.
    if all.len() > MAX_POINTS {
        let buckets = MAX_POINTS / 10;
        let first = samples[0].ts;
        let width = ((samples.last().unwrap().ts - first) / buckets as i64 + 1).max(1);
        let mut selected = Vec::new();
        let mut start = 0;
        while start < all.len() {
            let bucket = (samples[start].ts - first) / width;
            let mut end = start + 1;
            while end < all.len() && (samples[end].ts - first) / width == bucket {
                end += 1;
            }
            let mut indices = vec![start, end - 1];
            for field in [
                |p: &Point| p.p,
                |p: &Point| p.q,
                |p: &Point| p.y.unwrap_or(0.0),
                |p: &Point| p.dd,
            ] {
                indices.push(
                    (start..end)
                        .min_by(|&a, &b| field(&all[a]).total_cmp(&field(&all[b])))
                        .unwrap(),
                );
                indices.push(
                    (start..end)
                        .max_by(|&a, &b| field(&all[a]).total_cmp(&field(&all[b])))
                        .unwrap(),
                );
            }
            indices.sort_unstable();
            indices.dedup();
            selected.extend(indices.into_iter().map(|i| all[i].clone()));
            start = end;
        }
        all = selected;
    }
    if !matches!(result.settlement, Settlement::Unresolved) {
        if let Some(last) = samples.last() {
            peak = peak.max(result.final_cash);
            all.push(Point {
                t: ms(last.ts),
                y: None,
                n: None,
                b: None,
                a: None,
                p: result.total_pnl,
                e: bench_terminal,
                q: 0.0,
                dd: if peak > 0.0 {
                    (peak - result.final_cash) / peak
                } else {
                    0.0
                },
                qb: None,
                qa: None,
                settlement: true,
            });
        }
    }
    all
}
fn downsample_depth(recorder: &TimeSeriesRecorder) -> Vec<DepthOut> {
    let frames = recorder.depth();
    let count = frames.len().min(MAX_DEPTH_FRAMES);
    (0..count)
        .map(|i| {
            let index = if count > 1 {
                i * (frames.len() - 1) / (count - 1)
            } else {
                0
            };
            let f = &frames[index];
            DepthOut {
                t: ms(f.ts),
                b: f.bids.iter().map(|l| [l.price, l.size]).collect(),
                a: f.asks.iter().map(|l| [l.price, l.size]).collect(),
            }
        })
        .collect()
}
pub fn render(
    title: &str,
    subtitle: &str,
    result: &BacktestResult,
    recorder: &TimeSeriesRecorder,
    resolution: Option<f64>,
) -> String {
    render_with_context(
        title,
        subtitle,
        result,
        recorder,
        resolution,
        &ReportContext::default(),
    )
}
pub fn render_with_context(
    title: &str,
    subtitle: &str,
    result: &BacktestResult,
    recorder: &TimeSeriesRecorder,
    resolution: Option<f64>,
    context: &ReportContext,
) -> String {
    let bench = series::benchmark(recorder.samples(), result.initial_capital, resolution);
    let (rows, trade_stats) = blotter::compute(recorder.fills());
    let model = Model {
        title,
        title_sub: subtitle,
        context,
        summary: Summary::from(result),
        stats: stats::compute(
            recorder.samples(),
            recorder.fills(),
            result,
            series::choose_grid(recorder.samples()),
        ),
        analysis: Analysis::compute(recorder.samples(), result, recorder),
        quality: recorder.quality(),
        bench_final_pnl: bench.terminal_pnl,
        trade_stats,
        order_stats: super::orders::compute(recorder.orders(), result.fills),
        blotter: rows
            .into_iter()
            .map(|mut r| {
                r.ts = ms(r.ts);
                r
            })
            .collect(),
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
    let json = serde_json::to_string(&model)
        .expect("report model serializes")
        .replace('<', "\\u003c");
    // Substitute only template tokens, never token-like text inside user metadata.
    let (head, tail) = TEMPLATE.split_once("__DATA__").expect("report data slot");
    format!(
        "{}{}{}",
        head.replace("__TITLE__", &escape_html(title)),
        json,
        tail
    )
}
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
pub fn write_html_with_context(
    path: &Path,
    title: &str,
    subtitle: &str,
    result: &BacktestResult,
    recorder: &TimeSeriesRecorder,
    resolution: Option<f64>,
    context: &ReportContext,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(
        path,
        render_with_context(title, subtitle, result, recorder, resolution, context),
    )?;
    Ok(())
}
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
                &mut crate::test_support::BuyOnce::new(100.0),
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
    fn empty_result() -> BacktestResult {
        Engine::new(10_000.0, Box::new(NaiveFillModel), Simulation::zero())
            .run(
                io::fixture(Vec::<MarketState>::new()),
                &mut crate::test_support::BuyOnce::new(100.0),
                None,
            )
            .unwrap()
    }

    fn sample(ts: i64, pnl: f64, qty: f64, avg_cost: f64) -> Sample {
        Sample {
            ts,
            bid: Some(0.3),
            ask: Some(0.4),
            mid: Some(0.35),
            cash: 10_000.0 + pnl - qty * 0.35,
            qty,
            avg_cost,
            equity: 10_000.0 + pnl,
            pnl,
            quote_bid: None,
            quote_ask: None,
        }
    }

    #[test]
    fn chart_thinning_preserves_extrema_endpoints_and_terminal_drawdown() {
        let mut samples: Vec<_> = (0..6000).map(|i| sample(i * 1000, 0.0, 0.0, 0.0)).collect();
        samples[83] = sample(83_000, 50.0, 0.0, 0.0);
        samples[84] = sample(84_000, -20.0, 0.0, 0.0);
        samples[95].qty = 999.0;
        samples[96].mid = Some(0.99);
        let mut result = empty_result();
        result.settlement = Settlement::NothingHeld;
        result.total_pnl = -500.0;
        result.final_cash = 9500.0;
        let points = downsample_points(&samples, &result, &vec![0.0; samples.len()], -10_000.0);
        assert!(points.len() <= MAX_POINTS + 1);
        assert_eq!(points.first().unwrap().t, 0);
        assert_eq!(points[points.len() - 2].t, 5999);
        assert!(points.iter().any(|p| p.p == 50.0));
        assert!(points.iter().any(|p| p.p == -20.0));
        assert!(points.iter().any(|p| p.q == 999.0));
        assert!(points.iter().any(|p| p.y == Some(0.99)));
        let terminal = points.last().unwrap();
        assert!(terminal.settlement);
        assert_eq!(terminal.q, 0.0);
        assert_eq!(terminal.p, -500.0);
        assert_eq!(terminal.e, -10_000.0);
        assert!((terminal.dd - 550.0 / 10_050.0).abs() < 1e-12);
    }

    #[test]
    fn deployment_is_time_weighted_and_open_inventory_reconciles() {
        let recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let samples = vec![
            sample(0, 0.0, 100.0, 0.25),
            sample(1_000_000, 10.0, 200.0, 0.5),
            sample(4_000_000, 8.0, 20.0, 0.2),
        ];
        let mut result = empty_result();
        result.total_pnl = 3.0;
        let a = Analysis::compute(&samples, &result, &recorder);
        assert!((a.avg_deployed.unwrap() - 0.008125).abs() < 1e-12);
        assert_eq!(a.peak_deployed, 0.01);
        assert_eq!(a.peak_inventory, 200.0);
        assert_eq!(a.ending_equity, 10_008.0);
        assert_eq!(a.headline_pnl, 8.0);
        assert_eq!(a.unrealized_pnl, 5.0);
        assert_eq!(a.headline_pnl, result.total_pnl + a.unrealized_pnl);
        assert!(Analysis::compute(&[], &result, &recorder)
            .avg_deployed
            .is_none());
        assert!(Analysis::compute(&samples[..1], &result, &recorder)
            .avg_deployed
            .is_none());
    }

    #[test]
    fn empty_report_and_untrusted_metadata_are_safe_to_embed() {
        let recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let context = ReportContext {
            market_name: Some("</script><script>alert(1)</script>__TITLE__".into()),
            ..Default::default()
        };
        let html = render_with_context(
            "<Test>__DATA__",
            "source",
            &empty_result(),
            &recorder,
            None,
            &context,
        );
        assert!(html.contains("<title>&lt;Test&gt;"));
        assert!(!html.contains("</script><script>alert"));
        let json = html
            .split("<script id=\"report-data\" type=\"application/json\">")
            .nth(1)
            .unwrap()
            .split("</script>")
            .next()
            .unwrap();
        let data: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(data["context"]["market_name"], context.market_name.unwrap());
        assert_eq!(data["analysis"]["headline_pnl"], 0.0);
        assert_eq!(data["pts"].as_array().unwrap().len(), 0);
        assert!(data["analysis"]["avg_deployed"].is_null());
    }
}
