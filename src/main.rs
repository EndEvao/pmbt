use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process;

use pmbt::accumulator::Accumulator;
use pmbt::buyandhold::BuyAndHold;
use pmbt::buyandsell::BuyAndSell;
use pmbt::convert::convert_capture;
use pmbt::domain::order::TimeInForce;
use pmbt::domain::types::Price;
use pmbt::engine::{BacktestResult, Engine, Settlement, Simulation};
use pmbt::feed::{FileFeed, MergedFeed};
use pmbt::fill::fillmodel::{FillModel, NaiveFillModel};
use pmbt::fill::sweep::SweepFillModel;
use pmbt::latency::LatencyProfile;
use pmbt::quoter::Quoter;
use pmbt::report::{
    self,
    record::{RecorderConfig, TimeSeriesRecorder},
};
use pmbt::resting::QueueModel;
use pmbt::strategy::Strategy;
use pmbt::universe::{self, Category, SimulationConfig, SimulationProfile, Universe};

const INITIAL_CAPITAL: f64 = 10_000.0;

const USAGE: &str = "usage:
  pmbt run <strategy> <book_snapshot_full.parquet>
           [--config <profile.toml>] [--market-name <name>] [--outcome <label>]
           [--resolution 0|1] [--fill naive|sweep]
           [--latency zero|dublin|retail-eu|us-east] [--category <cat>]
           [--perception on|off] [--taker-delay on|off] [--fees on|off]
           [--queue front|proportional|trades-only]
           [--record <out.json>] [--record-dir <dir>] [--report <out.html>]
      backtest a strategy over one token's book; --report writes a
      self-contained HTML report (price/PnL/depth + stats), --record dumps
      the full time series as JSON, --record-dir writes it as Parquet tables
      for pandas (samples/fills/blotter/depth + summary.json — ~50x smaller
      than the JSON); --resolution is what the
      traded token redeemed at ($0 lost / $1 won), omit if unresolved;
      --fill picks the execution model (default naive: full size at the
      touch); --latency delays execution by the preset's feed+transit
      (default zero); --category adds the exchange's taker delay and fee for
      that market type (sports 3s, crypto/finance 250ms; fees per tier);
      --perception also delivers snapshots to the strategy late by the feed
      component (default off); --taker-delay/--fees off drop those exchange
      costs (both default on, but only bite when a category is given);
      --queue models the line ahead of a RESTING order: proportional
      (default) joins behind the displayed size, drained by trades and
      advanced proportionally by cancels; front assumes no queue (upper
      bound); trades-only ignores cancels (lower bound). No effect on
      marketable orders;
      --config reads a [simulation] section (fill/latency/category/perception/
      taker_delay/fees) from any TOML — a profile or a whole universe file —
      so the settings live in one place; CLI flags override it
  pmbt batch <strategy> <universe.toml> [--fill ...] [--latency ...]
             [--perception on|off] [--taker-delay on|off] [--fees on|off]
             [--record <out.json>] [--report <out.html>]
      backtest a strategy over every market in a universe config; --report
      writes a batch HTML report (combined cumulative PnL + per-market
      attribution), --record dumps the batch model as JSON; resolutions and
      categories (hence taker delays and fees) come from the per-market
      entries; flags override the file's [simulation] section, then defaults
  pmbt compare <strategy> <book_snapshot_full.parquet>
               [--axis realism|latency|fees|perception|taker-delay]
               [--config <profile.toml>]
               [--resolution 0|1] [--category <cat>] [--fill ...] [--latency ...]
               [--record <out.json>] [--report <out.html>]
      run the same strategy over one book under a matrix of simulation
      configs and compare them. --axis realism (default) is a cumulative
      ladder (frictionless → +latency → +taker-delay → +fees → +perception)
      whose step-to-step PnL differences are each component's cost; the other
      axes sweep one knob. taker-delay/fees need --category to bite
  pmbt convert <capture.jsonl> <out_dir>
      convert a raw websocket capture to Telonex parquet";

/// The strategy registry: adding a strategy is one new arm.
fn build(name: &str) -> Option<Box<dyn Strategy>> {
    match name {
        "buyandhold" => Some(Box::new(BuyAndHold::new(100.0))),
        "buyandsell" => Some(Box::new(BuyAndSell::new(100.0))),
        // Patient GTD maker: one bid at the touch, retired by the exchange
        // after the shortest life GTD allows (120 s), then re-placed.
        "accumulator" => Some(Box::new(Accumulator::new(
            100.0,
            0,
            TimeInForce::GTD_MIN_LIFE_US,
            500.0,
        ))),
        // Two-sided maker: joins the touch, 100 shares a side, capped at 500.
        "quoter" => Some(Box::new(Quoter::new(100.0, 0, 500.0))),
        // The same quoter stepping INSIDE the spread when there is room — the
        // placement whose front-of-queue assumption is actually defensible.
        "quoter-improve" => Some(Box::new(Quoter::new(100.0, -1, 500.0))),
        _ => None,
    }
}

/// The fill-model registry, mirroring the strategy registry: `naive` fills
/// full size at the touch (the baseline), `sweep` walks the book FOK.
fn build_fill(name: &str) -> Option<Box<dyn FillModel>> {
    match name {
        "naive" => Some(Box::new(NaiveFillModel)),
        "sweep" => Some(Box::new(SweepFillModel)),
        _ => None,
    }
}

/// The latency-preset registry: observer placement relative to the CLOB
/// (AWS eu-west-2). `zero` (the default) reproduces latency-free backtests.
fn build_latency(name: &str) -> Option<LatencyProfile> {
    match name {
        "zero" => Some(LatencyProfile::zero()),
        "dublin" => Some(LatencyProfile::dublin()),
        "retail-eu" => Some(LatencyProfile::retail_eu()),
        "us-east" => Some(LatencyProfile::us_east()),
        _ => None,
    }
}

/// The queue-model registry: how much of the line ahead of a resting order we
/// model. `proportional` is the default — see `resting::QueueModel`.
fn build_queue(name: &str) -> Option<QueueModel> {
    match name {
        "front" => Some(QueueModel::Front),
        "proportional" => Some(QueueModel::Proportional),
        "trades-only" => Some(QueueModel::TradesOnly),
        _ => None,
    }
}

/// Category names for the run verb's `--category` flag (batch reads
/// categories from the universe entries instead).
fn build_category(name: &str) -> Option<Category> {
    match name {
        "politics" => Some(Category::Politics),
        "sports" => Some(Category::Sports),
        "crypto" => Some(Category::Crypto),
        "finance" => Some(Category::Finance),
        "tech" => Some(Category::Tech),
        "culture" => Some(Category::Culture),
        "science" => Some(Category::Science),
        "geopolitics" => Some(Category::Geopolitics),
        "other" => Some(Category::Other),
        _ => None,
    }
}

fn main() {
    if let Err(err) = dispatch() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn dispatch() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("run") => cmd_run(args),
        Some("batch") => cmd_batch(args),
        Some("compare") => cmd_compare(args),
        Some("convert") => cmd_convert(args),
        _ => Err(USAGE.into()),
    }
}

fn cmd_run(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let (Some(name), Some(path)) = (args.next(), args.next()) else {
        return Err(USAGE.into());
    };

    // run-only flags (--resolution, --category) plus the shared simulation
    // flags. --resolution is binary-only for now — this parser is the single
    // place a future 0.5 for voided/50-50 markets unlocks.
    let mut sim_flags = SimFlags::default();
    let mut resolution = None;
    let mut category = None;
    let mut record_path = None;
    let mut report_path = None;
    let mut config_path = None;
    let mut record_dir = None;
    let mut market_name = None;
    let mut outcome = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--market-name" => {
                market_name = Some(args.next().ok_or("--market-name requires a label")?)
            }
            "--outcome" => outcome = Some(args.next().ok_or("--outcome requires a label")?),
            "--config" => match args.next() {
                Some(path) => config_path = Some(path),
                None => return Err("--config takes a TOML profile path".into()),
            },
            "--record" => match args.next() {
                Some(path) => record_path = Some(path),
                None => return Err("--record takes an output JSON path".into()),
            },
            "--record-dir" => match args.next() {
                Some(path) => record_dir = Some(path),
                None => return Err("--record-dir takes an output directory".into()),
            },
            "--report" => match args.next() {
                Some(path) => report_path = Some(path),
                None => return Err("--report takes an output HTML path".into()),
            },
            "--resolution" => match args.next().as_deref() {
                Some(value @ ("0" | "1")) => resolution = Some(Price::parse_decimal(value)?),
                other => {
                    return Err(format!(
                        "--resolution takes 0 (token lost) or 1 (token won), got {other:?}"
                    )
                    .into())
                }
            },
            "--category" => match args.next() {
                Some(value) => {
                    category = Some(build_category(&value).ok_or_else(|| {
                        format!(
                            "unknown category {value:?} \
                             (politics|sports|crypto|finance|tech|culture|science|other)"
                        )
                    })?)
                }
                None => return Err("--category takes a market category name".into()),
            },
            other if parse_sim_flag(&mut sim_flags, other, &mut args)? => {}
            other => return Err(format!("unknown argument {other:?}\n{USAGE}").into()),
        }
    }
    // CLI flags override the profile, which overrides the built-in defaults —
    // the same precedence `batch` applies to a universe's [simulation].
    let profile = load_profile(config_path.as_ref())?;
    let resolved = resolve_simulation(&sim_flags, profile.simulation.as_ref())?;
    let category = category.or_else(|| profile.simulation.as_ref().and_then(|s| s.category));

    let mut strategy = build(&name).ok_or_else(|| format!("unknown strategy {name:?}"))?;
    // One Telonex file is one token's book, so the file choice *is* the
    // outcome choice — no outcome argument.
    let feed = open_feed(&[PathBuf::from(&path)])?;

    let engine = Engine::new(
        INITIAL_CAPITAL,
        resolved.fill()?,
        resolved.simulation(category),
    );

    // Record the full time series only when an output (JSON and/or HTML) was
    // asked for — a plain run stays on the no-op path and pays nothing.
    let result = if let Some(out) = record_dir
        .as_ref()
        .filter(|_| record_path.is_none() && report_path.is_none())
    {
        let mut recorder =
            report::stream::ParquetRecorder::new(Path::new(out), RecorderConfig::default())?;
        let result = engine.run_recorded(feed, strategy.as_mut(), resolution, &mut recorder)?;
        let samples = recorder.finish(&result)?;
        println!("streamed {samples} samples + fills/blotter/depth/orders parquet to {out}/");
        result
    } else if record_path.is_some() || report_path.is_some() || record_dir.is_some() {
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let result = engine.run_recorded(feed, strategy.as_mut(), resolution, &mut recorder)?;
        if let Some(out) = &record_path {
            report::write_json(Path::new(out), &result, &recorder)?;
            println!("recorded {} samples to {out}", recorder.samples().len());
        }
        if let Some(out) = &record_dir {
            report::parquet::write_run(Path::new(out), &result, &recorder)?;
            println!(
                "wrote {} samples + fills/blotter/depth parquet to {out}/",
                recorder.samples().len()
            );
        }
        if let Some(out) = &report_path {
            // Subtitle is the source file's name — the market the report is of.
            let subtitle = Path::new(&path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            let sim = resolved.simulation(category);
            let queue = match sim.queue {
                QueueModel::Front => "front",
                QueueModel::Proportional => "proportional",
                QueueModel::TradesOnly => "trades-only",
            };
            let context = report::html::ReportContext {
                market_name: market_name.clone(),
                outcome: outcome.clone(),
                queue: Some(queue.into()),
                tape_present: Path::new(&path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_suffix("_book_snapshot_full.parquet"))
                    .map(|stem| {
                        Path::new(&path)
                            .with_file_name(format!("{stem}_trades.parquet"))
                            .is_file()
                    }),
                settings: vec![
                    ("Fill model".into(), resolved.fill_name.clone()),
                    ("Queue model".into(), queue.into()),
                    (
                        "Feed delay".into(),
                        format!("{} ms", sim.latency.feed.as_secs_f64() * 1000.0),
                    ),
                    (
                        "Order transit".into(),
                        format!("{} ms", sim.latency.transit.as_secs_f64() * 1000.0),
                    ),
                    (
                        "Taker hold".into(),
                        format!("{} ms", sim.latency.taker_delay.as_secs_f64() * 1000.0),
                    ),
                    (
                        "Perception delay".into(),
                        if sim.perception { "on" } else { "off" }.into(),
                    ),
                    ("Taker fee coefficient".into(), sim.fee_rate.to_string()),
                    (
                        "Category".into(),
                        category.map_or("unspecified".into(), |c| format!("{c:?}").to_lowercase()),
                    ),
                    (
                        "Resolution supplied".into(),
                        resolution.map_or("unresolved".into(), |p| p.to_decimal_string()),
                    ),
                    ("Order size".into(), "100 shares".into()),
                    (
                        "Inventory cap".into(),
                        if matches!(name.as_str(), "quoter" | "quoter-improve" | "accumulator") {
                            "500 shares"
                        } else {
                            "not configured"
                        }
                        .into(),
                    ),
                    ("Engine version".into(), env!("CARGO_PKG_VERSION").into()),
                ],
            };
            report::html::write_html_with_context(
                Path::new(out),
                &name,
                subtitle,
                &result,
                &recorder,
                resolution.map(|p| p.as_f64()),
                &context,
            )?;
            println!("wrote report to {out}");
        }
        result
    } else {
        engine.run(feed, strategy.as_mut(), resolution)?
    };

    print_result(&name, &result, resolved.queue);
    Ok(())
}

/// A run's outcome, formatted for a human: money as money, the P&L broken into
/// the three legs that make it up, and the order flow that produced it. The
/// machine-readable forms are `--record` (JSON) and `--record-dir` (Parquet).
fn print_result(strategy: &str, result: &BacktestResult, queue: QueueModel) {
    let pct = |v: f64| 100.0 * v / result.initial_capital;
    println!("\n{strategy}");
    println!(
        "  capital     ${:>12.2}  ->  ${:<12.2}",
        result.initial_capital, result.final_cash
    );
    println!(
        "  net P&L     ${:>12.2}   ({:+.2}%)",
        result.total_pnl,
        pct(result.total_pnl)
    );

    // The three legs, so the total is never a black box.
    let settlement_pnl = match &result.settlement {
        Settlement::Settled { pnl, .. } => *pnl,
        _ => 0.0,
    };
    println!("    trading   ${:>12.2}", result.realized_pnl);
    println!("    settled   ${:>12.2}", settlement_pnl);
    println!("    fees      ${:>12.2}", -result.fees);

    match &result.settlement {
        Settlement::Settled {
            quantity,
            redemption,
            ..
        } => println!(
            "  settlement  {quantity:.0} shares redeemed at ${:.2}",
            redemption.as_f64()
        ),
        Settlement::NothingHeld => println!("  settlement  flat at the end, nothing to redeem"),
        Settlement::Unresolved => {
            let held = result
                .open_position
                .as_ref()
                .map_or(0.0, |position| position.quantity());
            if held > 0.0 {
                println!(
                    "  settlement  unresolved — {held:.0} shares left open, NOT marked to market"
                );
            }
        }
    }

    println!(
        "  fills       {:<6}  ({} maker, {} taker)",
        result.fills, result.maker_fills, result.taker_fills
    );
    // Only worth the line when the strategy actually used these.
    if result.cancels > 0 || result.expired_resting > 0 || result.resting_quantity > 0.0 {
        println!(
            "  orders      {} cancelled, {} still resting at the end ({:.0} shares held back)",
            result.cancels, result.expired_resting, result.resting_quantity
        );
    }
    if result.expired_gtd > 0 {
        println!(
            "  gtd expiry  {} resting orders retired by the exchange on their timer",
            result.expired_gtd
        );
    }
    if result.cancels_missed > 0 {
        println!(
            "  picked off  {} cancels arrived too late (the order had already filled)",
            result.cancels_missed
        );
    }
    if result.rejections > 0 || result.expired > 0 {
        println!(
            "  rejected    {}   expired in flight  {}",
            result.rejections, result.expired
        );
    }
    if result.maker_fills > 0 {
        // The queue assumption travels with the number it produced.
        let note = match queue {
            QueueModel::Front => {
                "front-of-queue: any flow reaching our price fills us, ignoring\n        the size already queued ahead — an UPPER BOUND on maker fills"
            }
            QueueModel::Proportional => {
                "queue: joined behind the displayed size; trades drain it and\n        cancels advance us proportionally — the neutral estimate"
            }
            QueueModel::TradesOnly => {
                "queue: joined behind the displayed size, drained by trades only\n        (cancels never advance us) — a LOWER BOUND on maker fills"
            }
        };
        println!("\n  note: {note}.");
    }
}

/// Run the *same* strategy over the *same* book under a matrix of simulation
/// configs, so each realism component's cost is a measured difference. One
/// engine per config (fresh strategy + feed — both stateful/consumed per run);
/// the feed is re-opened rather than buffered, keeping memory bounded.
fn cmd_compare(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let (Some(name), Some(path)) = (args.next(), args.next()) else {
        return Err(USAGE.into());
    };

    let mut sim_flags = SimFlags::default();
    let mut resolution = None;
    let mut category = None;
    let mut record_path = None;
    let mut report_path = None;
    let mut config_path = None;
    let mut axis = "realism".to_string();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--axis" => {
                axis = args
                    .next()
                    .ok_or("--axis takes realism|latency|fees|perception|taker-delay")?
            }
            "--resolution" => match args.next().as_deref() {
                Some(value @ ("0" | "1")) => resolution = Some(Price::parse_decimal(value)?),
                other => {
                    return Err(format!(
                        "--resolution takes 0 (token lost) or 1 (token won), got {other:?}"
                    )
                    .into())
                }
            },
            "--category" => match args.next() {
                Some(value) => {
                    category = Some(build_category(&value).ok_or_else(|| {
                        format!(
                            "unknown category {value:?} \
                             (politics|sports|crypto|finance|tech|culture|science|geopolitics|other)"
                        )
                    })?)
                }
                None => return Err("--category takes a market category name".into()),
            },
            "--config" => match args.next() {
                Some(p) => config_path = Some(p),
                None => return Err("--config takes a TOML profile path".into()),
            },
            "--record" => match args.next() {
                Some(p) => record_path = Some(p),
                None => return Err("--record takes an output JSON path".into()),
            },
            "--report" => match args.next() {
                Some(p) => report_path = Some(p),
                None => return Err("--report takes an output HTML path".into()),
            },
            other if parse_sim_flag(&mut sim_flags, other, &mut args)? => {}
            other => return Err(format!("unknown argument {other:?}\n{USAGE}").into()),
        }
    }

    // CLI flags override the profile, which overrides the built-in defaults.
    let profile = load_profile(config_path.as_ref())?;
    let base = resolve_simulation(&sim_flags, profile.simulation.as_ref())?;
    let category = category.or_else(|| profile.simulation.as_ref().and_then(|s| s.category));
    let configs = build_compare_matrix(&axis, &base, &sim_flags)?;
    let ladder = axis == "realism";
    if ladder && category.is_none() {
        eprintln!(
            "note: no --category — the taker-delay and fees rungs are no-ops \
             (both are category-driven); their cost will read as 0"
        );
    }

    println!("comparing {} configs on axis {axis:?}:", configs.len());
    let mut runs: Vec<report::compare::CompareRun> = Vec::with_capacity(configs.len());
    for (label, detail, resolved) in &configs {
        let mut strategy = build(&name).ok_or_else(|| format!("unknown strategy {name:?}"))?;
        let feed = open_feed(&[PathBuf::from(&path)])?;
        let engine = Engine::new(
            INITIAL_CAPITAL,
            resolved.fill()?,
            resolved.simulation(category),
        );
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let result = engine.run_recorded(feed, strategy.as_mut(), resolution, &mut recorder)?;
        println!(
            "  {label:<14} total_pnl {:+.2}  fills {}  fees {:.2}",
            result.total_pnl, result.fills, result.fees
        );
        runs.push(report::compare::CompareRun::from_recorded(
            label.clone(),
            detail.clone(),
            result,
            &recorder,
        ));
    }

    let subtitle = Path::new(&path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    if let Some(out) = &record_path {
        report::compare::write_json(
            Path::new(out),
            &name,
            subtitle,
            ladder,
            category.is_some(),
            &runs,
        )?;
        println!("recorded comparison to {out}");
    }
    if let Some(out) = &report_path {
        report::compare::write_html(
            Path::new(out),
            &name,
            subtitle,
            ladder,
            category.is_some(),
            &runs,
        )?;
        println!("wrote comparison report to {out}");
    }
    Ok(())
}

/// A single labelled config in a comparison matrix.
type CompareConfig = (String, String, ResolvedSim);

/// Build the config matrix for a comparison axis. The base carries the fill
/// model (and any CLI-set fields); each config overrides the realism knobs.
fn build_compare_matrix(
    axis: &str,
    base: &ResolvedSim,
    flags: &SimFlags,
) -> Result<Vec<CompareConfig>, Box<dyn Error>> {
    let zero = LatencyProfile::zero();
    // The non-zero placement preset used by the ladder / perception axis: the
    // CLI's --latency if given, else dublin (the research-sourced realistic one).
    let lat_name = flags.latency.as_deref().unwrap_or("dublin");
    let lat =
        build_latency(lat_name).ok_or_else(|| format!("unknown latency preset {lat_name:?}"))?;

    // Build a variant that reuses the base fill model and overrides the knobs.
    let v = |preset: LatencyProfile, perception: bool, taker_delay: bool, fees: bool| ResolvedSim {
        fill_name: base.fill_name.clone(),
        queue: base.queue,
        preset,
        perception,
        taker_delay,
        fees,
    };

    let matrix = match axis {
        // Cumulative ladder: each rung adds exactly one friction, so consecutive
        // total-PnL deltas attribute cost to that component.
        "realism" => vec![
            (
                "frictionless".into(),
                "latency zero · taker-delay off · fees off · perception off".into(),
                v(zero, false, false, false),
            ),
            (
                "+latency".into(),
                format!("latency {lat_name} · taker-delay off · fees off"),
                v(lat, false, false, false),
            ),
            (
                "+taker-delay".into(),
                format!("latency {lat_name} · taker-delay on · fees off"),
                v(lat, false, true, false),
            ),
            (
                "+fees".into(),
                format!("latency {lat_name} · taker-delay on · fees on"),
                v(lat, false, true, true),
            ),
            (
                "+perception".into(),
                format!("latency {lat_name} · taker-delay on · fees on · perception on"),
                v(lat, true, true, true),
            ),
        ],
        // Placement-latency sweep: only the preset varies; other frictions held.
        "latency" => ["zero", "dublin", "retail-eu", "us-east"]
            .iter()
            .map(|n| {
                let preset = build_latency(n).expect("known preset");
                (
                    (*n).to_string(),
                    format!("latency {n} · taker-delay on · fees on"),
                    v(preset, false, true, true),
                )
            })
            .collect(),
        // A/B toggles: everything else constant, one knob flipped.
        "fees" => vec![
            (
                "fees off".into(),
                "latency zero · fees off".into(),
                v(zero, false, true, false),
            ),
            (
                "fees on".into(),
                "latency zero · fees on".into(),
                v(zero, false, true, true),
            ),
        ],
        // Perception only bites when the feed component is non-zero, so this A/B
        // runs on the dublin preset rather than zero.
        "perception" => vec![
            (
                "perception off".into(),
                format!("latency {lat_name} · perception off"),
                v(lat, false, true, true),
            ),
            (
                "perception on".into(),
                format!("latency {lat_name} · perception on"),
                v(lat, true, true, true),
            ),
        ],
        "taker-delay" => vec![
            (
                "taker-delay off".into(),
                "latency zero · taker-delay off".into(),
                v(zero, false, false, false),
            ),
            (
                "taker-delay on".into(),
                "latency zero · taker-delay on".into(),
                v(zero, false, true, false),
            ),
        ],
        other => {
            return Err(format!(
                "unknown --axis {other:?} (realism|latency|fees|perception|taker-delay)"
            )
            .into())
        }
    };
    Ok(matrix)
}

/// Run one engine per universe entry (fresh strategy + fill model each — both
/// are stateful across a run) and report per-market lines plus the aggregate.
/// Fails fast: the first market that can't load or run aborts the batch — a
/// partial aggregate is a lie.
fn cmd_batch(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let (Some(name), Some(path)) = (args.next(), args.next()) else {
        return Err(USAGE.into());
    };
    // Only simulation flags here (plus --record/--report): resolutions and
    // categories live in the universe's per-market entries.
    let mut sim_flags = SimFlags::default();
    let mut record_path = None;
    let mut report_path = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--record" => match args.next() {
                Some(p) => record_path = Some(p),
                None => return Err("--record takes an output JSON path".into()),
            },
            "--report" => match args.next() {
                Some(p) => report_path = Some(p),
                None => return Err("--report takes an output HTML path".into()),
            },
            other if parse_sim_flag(&mut sim_flags, other, &mut args)? => {}
            other => return Err(format!("unknown argument {other:?}\n{USAGE}").into()),
        }
    }
    let reporting = record_path.is_some() || report_path.is_some();

    let universe = Universe::load(&path)?;
    // CLI flags override the universe's [simulation] section, which overrides
    // built-in defaults.
    let resolved = resolve_simulation(&sim_flags, universe.simulation.as_ref())?;

    let mut results = Vec::new();
    // Only populated when reporting: each market reduced to its PnL curve +
    // result, so a large universe holds curves, not full recorders.
    let mut runs: Vec<report::batch::MarketRun> = Vec::new();
    for entry in &universe.markets {
        let mut strategy = build(&name).ok_or_else(|| format!("unknown strategy {name:?}"))?;
        let feed = open_feed(&entry.data_files(&universe.data_dir)?)?;

        // The preset covers observer placement; the exchange's hold comes
        // from this market's category — the universe layer doing its job.
        let engine = Engine::new(
            INITIAL_CAPITAL,
            resolved.fill()?,
            resolved.simulation(Some(entry.category)),
        );
        let result = if reporting {
            let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
            let result =
                engine.run_recorded(feed, strategy.as_mut(), entry.resolution()?, &mut recorder)?;
            runs.push(report::batch::MarketRun::from_recorded(
                entry.market_id.clone(),
                format!("{:?}", entry.category).to_lowercase(),
                result.clone(),
                &recorder,
            ));
            result
        } else {
            engine.run(feed, strategy.as_mut(), entry.resolution()?)?
        };

        let id = &entry.market_id;
        let short = if id.len() > 12 {
            format!("{}…{}", &id[..4], &id[id.len() - 4..])
        } else {
            id.clone()
        };
        let outcome = match &result.settlement {
            Settlement::Settled { pnl, .. } => format!("settled {pnl:+.2}"),
            Settlement::Unresolved => "unresolved".to_string(),
            Settlement::NothingHeld => "nothing held".to_string(),
        };
        println!(
            "  {short}  {:<8}  total {:+10.2}  fills {:>6}  rej {:>6}  {outcome}",
            format!("{:?}", entry.category).to_lowercase(),
            result.total_pnl,
            result.fills,
            result.rejections,
        );
        results.push((entry.market_id.clone(), result));
    }

    let agg = universe::aggregate(&results);
    println!(
        "universe {:?}: {} markets\n  \
         total_pnl {:+.2} (realized {:+.2}, settlement {:+.2}, fees {:.2})\n  \
         fills {}  rejections {}  settled {}  unresolved {}  nothing_held {}",
        universe.name,
        agg.markets,
        agg.total_pnl,
        agg.realized_pnl,
        agg.settlement_pnl,
        agg.fees,
        agg.fills,
        agg.rejections,
        agg.settled,
        agg.unresolved,
        agg.nothing_held,
    );

    // Batch outputs: the combined cumulative curve + per-market attribution.
    if let Some(out) = &record_path {
        report::batch::write_json(Path::new(out), &universe.name, &runs)?;
        println!("recorded batch to {out}");
    }
    if let Some(out) = &report_path {
        report::batch::write_html(Path::new(out), &universe.name, &runs)?;
        println!("wrote batch report to {out}");
    }
    Ok(())
}

/// Simulation settings from the CLI, all optional — resolved against a
/// universe's `[simulation]` section and built-in defaults by
/// `resolve_simulation` (precedence: CLI > file > defaults).
#[derive(Default)]
struct SimFlags {
    fill: Option<String>,
    latency: Option<String>,
    perception: Option<bool>,
    taker_delay: Option<bool>,
    fees: Option<bool>,
    queue: Option<String>,
}

/// Try to consume one simulation flag. `Ok(true)` = it was one of ours.
fn parse_sim_flag(
    flags: &mut SimFlags,
    flag: &str,
    args: &mut impl Iterator<Item = String>,
) -> Result<bool, Box<dyn Error>> {
    match flag {
        "--fill" => {
            flags.fill = Some(
                args.next()
                    .ok_or("--fill takes a model name (naive|sweep)")?,
            );
        }
        "--latency" => {
            flags.latency = Some(
                args.next()
                    .ok_or("--latency takes a preset (zero|dublin|retail-eu|us-east)")?,
            );
        }
        "--perception" => flags.perception = Some(parse_on_off("--perception", args.next())?),
        "--taker-delay" => flags.taker_delay = Some(parse_on_off("--taker-delay", args.next())?),
        "--fees" => flags.fees = Some(parse_on_off("--fees", args.next())?),
        "--queue" => {
            flags.queue = Some(
                args.next()
                    .ok_or("--queue takes front|proportional|trades-only")?,
            )
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Open one or more of a market's consecutive book files — chained into one
/// continuous feed — together with each day's sibling trades tape.
///
/// A single path is the ordinary one-day backtest (a chain of one). A missing
/// tape is not fatal: the book replays fine without it, and only resting (maker)
/// orders need trades to fill. So we warn per missing day and carry on rather
/// than refusing a marketable backtest over a book with no tape beside it. A
/// malformed tape still fails loudly.
fn open_feed(book_paths: &[PathBuf]) -> Result<FileFeed, Box<dyn Error>> {
    let (feed, missing_tapes) = MergedFeed::from_book_paths(book_paths)?;
    for book_path in &missing_tapes {
        println!(
            "note: no trades file beside {} — replaying the book only \
             (resting orders cannot fill without a tape)",
            book_path.display()
        );
    }
    Ok(feed)
}

/// Load a `--config` simulation profile, if one was given. Any TOML with a
/// `[simulation]` section works — including a full universe file, whose market
/// entries are ignored (loudly: silently dropping them would be a trap).
fn load_profile(path: Option<&String>) -> Result<SimulationProfile, Box<dyn Error>> {
    let Some(path) = path else {
        return Ok(SimulationProfile::default());
    };
    let profile = SimulationProfile::load(path)?;
    let markets = profile.market_count();
    if markets > 0 {
        let plural = if markets == 1 { "entry" } else { "entries" };
        println!("note: {path} has {markets} market {plural} — --config reads only [simulation]");
    }
    if profile.simulation.is_none() {
        println!("note: {path} has no [simulation] section — using defaults");
    }
    Ok(profile)
}

fn parse_on_off(flag: &str, value: Option<String>) -> Result<bool, Box<dyn Error>> {
    match value.as_deref() {
        Some("on") => Ok(true),
        Some("off") => Ok(false),
        other => Err(format!("{flag} takes on|off, got {other:?}").into()),
    }
}

/// The effective simulation settings after precedence resolution. The fill
/// model stays a *name* so batch can build a fresh instance per market.
struct ResolvedSim {
    fill_name: String,
    queue: QueueModel,
    preset: LatencyProfile,
    perception: bool,
    taker_delay: bool,
    fees: bool,
}

impl ResolvedSim {
    /// Compose the per-run engine options for a market of known (or absent)
    /// category: preset + the exchange's hold and taker fee when their
    /// toggles are on. Both are category-driven, so an absent category (a
    /// bare `run` without `--category`) engages neither — the pre-latency,
    /// fee-free behavior.
    fn simulation(&self, category: Option<Category>) -> Simulation {
        let taker = match (self.taker_delay, category) {
            (true, Some(category)) => pmbt::latency::taker_delay(category),
            _ => std::time::Duration::ZERO,
        };
        let fee_rate = match (self.fees, category) {
            (true, Some(category)) => pmbt::fees::fee_rate(category),
            _ => 0.0,
        };
        Simulation {
            latency: self.preset.with_taker_delay(taker),
            perception: self.perception,
            fee_rate,
            queue: self.queue,
        }
    }

    fn fill(&self) -> Result<Box<dyn FillModel>, Box<dyn Error>> {
        build_fill(&self.fill_name)
            .ok_or_else(|| format!("unknown fill model {:?} (naive|sweep)", self.fill_name).into())
    }
}

/// Precedence: CLI flag > universe `[simulation]` section > built-in default
/// (fill naive, latency zero, perception off, taker delay on, fees on).
fn resolve_simulation(
    cli: &SimFlags,
    file: Option<&SimulationConfig>,
) -> Result<ResolvedSim, Box<dyn Error>> {
    let empty = SimulationConfig::default();
    let file = file.unwrap_or(&empty);

    let latency_name = cli
        .latency
        .as_deref()
        .or(file.latency.as_deref())
        .unwrap_or("zero");
    let preset = build_latency(latency_name).ok_or_else(|| {
        format!("unknown latency preset {latency_name:?} (zero|dublin|retail-eu|us-east)")
    })?;

    let queue_name = cli
        .queue
        .as_deref()
        .or(file.queue.as_deref())
        .unwrap_or("proportional");
    let queue = build_queue(queue_name).ok_or_else(|| {
        format!("unknown queue model {queue_name:?} (front|proportional|trades-only)")
    })?;

    Ok(ResolvedSim {
        queue,
        fill_name: cli
            .fill
            .as_deref()
            .or(file.fill.as_deref())
            .unwrap_or("naive")
            .to_string(),
        preset,
        perception: cli.perception.or(file.perception).unwrap_or(false),
        taker_delay: cli.taker_delay.or(file.taker_delay).unwrap_or(true),
        fees: cli.fees.or(file.fees).unwrap_or(true),
    })
}

fn cmd_convert(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let (Some(capture), Some(out_dir)) = (args.next(), args.next()) else {
        return Err(USAGE.into());
    };

    let report = convert_capture(Path::new(&capture), Path::new(&out_dir))?;

    println!(
        "{capture}: {} frames on {} → {} assets into {out_dir}",
        report.frames,
        report.date,
        report.assets.len(),
    );
    for asset in &report.assets {
        println!("  {:>9} rows  {}", asset.rows, asset.asset_id);
    }

    // The trade tape, per asset — empty on thin markets that never printed.
    let trades: u64 = report.trades.iter().map(|a| a.rows).sum();
    if trades > 0 {
        println!(
            "trades: {trades} prints across {} assets",
            report.trades.len()
        );
    } else {
        println!("trades: none in this capture");
    }

    // Gap accounting: the recordings are known to be missing timeframes, and
    // suppressed frames are the loud proof — never let them pass silently.
    let episodes: u64 = report.gaps.iter().map(|g| g.stale_episodes).sum();
    let suppressed: u64 = report.gaps.iter().map(|g| g.suppressed_frames).sum();
    if episodes > 0 || suppressed > 0 || report.clamped_timestamps > 0 {
        println!(
            "gaps: {episodes} stale episodes across {} assets, {suppressed} frames suppressed, \
             {} timestamps clamped",
            report.gaps.len(),
            report.clamped_timestamps,
        );
    } else {
        println!("gaps: none detected");
    }
    Ok(())
}
