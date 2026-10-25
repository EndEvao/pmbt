use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process;

use pmbt::accumulator::Accumulator;
use pmbt::buyandhold::BuyAndHold;
use pmbt::buyandsell::BuyAndSell;
use pmbt::config::{resolve, SimulationConfig, SimulationProfile};
use pmbt::convert::convert_capture;
use pmbt::domain::order::TimeInForce;
use pmbt::domain::types::Price;
use pmbt::engine::{BacktestResult, Settlement};
use pmbt::experiments::{
    comparison_matrix as build_compare_matrix, run_batch, run_comparison, BatchConfig,
    ComparisonConfig, ExperimentOutputs,
};
use pmbt::quoter::Quoter;
use pmbt::resting::QueueModel;
use pmbt::strategy::Strategy;
use pmbt::universe::{Category, Universe};
use pmbt::{
    ArtifactKind, Backtest, BacktestConfig, OutputConfig, RecordingMode, RunDiagnostic,
    StrategyMetadata,
};

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
fn build(name: &str) -> Option<(Box<dyn Strategy>, StrategyMetadata)> {
    let size = 100.0;
    let cap = 500.0;
    let offset = if name == "quoter-improve" { -1 } else { 0 };
    let lifetime = TimeInForce::GTD_MIN_LIFE_US;
    let strategy: Box<dyn Strategy> = match name {
        "buyandhold" => Box::new(BuyAndHold::new(size)),
        "buyandsell" => Box::new(BuyAndSell::new(size)),
        "accumulator" => Box::new(Accumulator::new(size, offset, lifetime, cap)),
        "quoter" | "quoter-improve" => Box::new(Quoter::new(size, offset, cap)),
        _ => return None,
    };
    let mut metadata = StrategyMetadata::new(name);
    metadata
        .parameters
        .push(("Order size".into(), format!("{size} shares")));
    if matches!(name, "quoter" | "quoter-improve" | "accumulator") {
        metadata
            .parameters
            .push(("Inventory cap".into(), format!("{cap} shares")));
        metadata
            .parameters
            .push(("Offset ticks".into(), offset.to_string()));
    }
    if name == "accumulator" {
        metadata
            .parameters
            .push(("GTD lifetime".into(), format!("{lifetime} us")));
    }
    Some((strategy, metadata))
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
    let mut sim_flags = SimulationConfig::default();
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
    sim_flags.category = category;
    let (mut strategy, metadata) =
        build(&name).ok_or_else(|| format!("unknown strategy {name:?}"))?;
    let mut config = BacktestConfig::new(path);
    config.initial_capital = INITIAL_CAPITAL;
    config.simulation = profile.simulation.unwrap_or_default();
    config.resolution = resolution;
    config.market_name = market_name;
    config.outcome = outcome;
    let outcome = Backtest::new(config)
        .overrides(sim_flags)
        .metadata(metadata)
        .outputs(OutputConfig {
            html: report_path.map(PathBuf::from),
            json: record_path.map(PathBuf::from),
            parquet: record_dir.map(PathBuf::from),
        })
        .run(strategy.as_mut())?;
    print_diagnostics(&outcome.diagnostics);
    for artifact in &outcome.artifacts {
        let out = artifact.path.display();
        let samples = outcome.recorded_samples.unwrap_or(0);
        match artifact.kind {
            ArtifactKind::Html => println!("wrote report to {out}"),
            ArtifactKind::Json => println!("recorded {samples} samples to {out}"),
            ArtifactKind::Parquet => {
                let verb = if outcome.recording == RecordingMode::StreamingParquet {
                    "streamed"
                } else {
                    "wrote"
                };
                println!("{verb} {samples} samples + fills/blotter/depth/orders parquet to {out}/");
            }
        }
    }
    print_result(&name, &outcome.result, outcome.simulation.queue);
    Ok(())
}

fn print_diagnostics(diagnostics: &[RunDiagnostic]) {
    for diagnostic in diagnostics {
        match diagnostic {
            RunDiagnostic::MissingTradeTape { book_path } => println!(
                "note: no trades file beside {} — replaying the book only (resting orders cannot fill without a tape)",
                book_path.display()
            ),
        }
    }
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

    let mut sim_flags = SimulationConfig::default();
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
    sim_flags.category = category;
    let base = resolve(profile.simulation.as_ref(), &sim_flags)?;
    let category = base.category;
    let configs = build_compare_matrix(&axis, &base, &sim_flags)?;
    let ladder = axis == "realism";
    if ladder && category.is_none() {
        eprintln!(
            "note: no --category — the taker-delay and fees rungs are no-ops \
             (both are category-driven); their cost will read as 0"
        );
    }

    build(&name).ok_or_else(|| format!("unknown strategy {name:?}"))?;
    println!("comparing {} configs on axis {axis:?}:", configs.len());
    let mut input = BacktestConfig::new(path);
    input.initial_capital = INITIAL_CAPITAL;
    input.resolution = resolution;
    let outcome = run_comparison(
        ComparisonConfig {
            input,
            variants: configs,
            strategy_name: name.clone(),
            ladder,
            outputs: ExperimentOutputs {
                html: report_path.map(PathBuf::from),
                json: record_path.map(PathBuf::from),
            },
        },
        || build(&name).expect("validated strategy").0,
    )?;
    print_diagnostics(&outcome.diagnostics);
    for run in &outcome.runs {
        println!(
            "  {:<14} total_pnl {:+.2}  fills {}  fees {:.2}",
            run.label, run.result.total_pnl, run.result.fills, run.result.fees
        );
    }
    for artifact in outcome.artifacts {
        println!("wrote comparison to {}", artifact.path.display());
    }
    Ok(())
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
    let mut sim_flags = SimulationConfig::default();
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
    let universe = Universe::load(&path)?;
    let universe_name = universe.name.clone();
    build(&name).ok_or_else(|| format!("unknown strategy {name:?}"))?;
    let outcome = run_batch(
        BatchConfig {
            universe,
            overrides: sim_flags,
            initial_capital: INITIAL_CAPITAL,
            outputs: ExperimentOutputs {
                html: report_path.map(PathBuf::from),
                json: record_path.map(PathBuf::from),
            },
        },
        |_| build(&name).expect("validated strategy").0,
    )?;
    print_diagnostics(&outcome.diagnostics);
    for run in &outcome.runs {
        println!(
            "  {}  {:<8}  total {:+10.2}  fills {:>6}  rej {:>6}",
            run.id, run.category, run.result.total_pnl, run.result.fills, run.result.rejections
        );
    }
    let agg = &outcome.aggregate;
    println!(
        "universe {:?}: {} markets\n  \
         total_pnl {:+.2} (realized {:+.2}, settlement {:+.2}, fees {:.2})\n  \
         fills {}  rejections {}  settled {}  unresolved {}  nothing_held {}",
        universe_name,
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

    for artifact in outcome.artifacts {
        println!("wrote batch to {}", artifact.path.display());
    }
    Ok(())
}

/// Try to consume one simulation flag. `Ok(true)` = it was one of ours.
fn parse_sim_flag(
    flags: &mut SimulationConfig,
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

#[cfg(test)]
mod config_compatibility_tests {
    use super::*;
    use pmbt::config::latency_preset;
    use pmbt::latency::LatencyProfile;

    #[test]
    fn comparison_uses_explicit_latency_or_dublin_not_the_profile_preset() {
        let profile = SimulationConfig {
            latency: Some("us-east".into()),
            fill: Some("sweep".into()),
            queue: Some("trades-only".into()),
            category: Some(Category::Sports),
            ..Default::default()
        };
        for (override_name, expected) in [
            (None, LatencyProfile::dublin()),
            (Some("retail-eu"), LatencyProfile::retail_eu()),
        ] {
            let flags = SimulationConfig {
                latency: override_name.map(str::to_owned),
                ..Default::default()
            };
            let base = resolve(Some(&profile), &flags).unwrap();
            if override_name.is_none() {
                assert_eq!(base.preset, LatencyProfile::us_east());
            }
            for axis in ["realism", "perception"] {
                let runs = build_compare_matrix(axis, &base, &flags).unwrap();
                assert_eq!(runs.last().unwrap().2.preset, expected);
                for (_, _, config) in &runs {
                    assert_eq!(config.fill_model, base.fill_model);
                    assert_eq!(config.queue, base.queue);
                    assert_eq!(config.category, Some(Category::Sports));
                }
            }
        }
    }

    #[test]
    fn comparison_axes_preserve_their_existing_friction_settings() {
        let flags = SimulationConfig::default();
        let base = resolve(None, &flags).unwrap();
        let realism = build_compare_matrix("realism", &base, &flags).unwrap();
        let toggles: Vec<_> = realism
            .iter()
            .map(|(_, _, c)| (c.perception, c.taker_delay, c.fees))
            .collect();
        assert_eq!(
            toggles,
            vec![
                (false, false, false),
                (false, false, false),
                (false, true, false),
                (false, true, true),
                (true, true, true)
            ]
        );
        assert_eq!(realism[0].2.preset, LatencyProfile::zero());
        let fees = build_compare_matrix("fees", &base, &flags).unwrap();
        assert!(!fees[0].2.fees);
        assert!(fees[1].2.fees);
        assert!(fees
            .iter()
            .all(|(_, _, c)| c.preset == LatencyProfile::zero()));
        let holds = build_compare_matrix("taker-delay", &base, &flags).unwrap();
        assert!(!holds[0].2.taker_delay);
        assert!(holds[1].2.taker_delay);
        assert!(holds.iter().all(|(_, _, c)| !c.fees));
        let latency = build_compare_matrix("latency", &base, &flags).unwrap();
        assert_eq!(latency.len(), 4);
        for (name, _, config) in latency {
            assert_eq!(config.preset, latency_preset(&name).unwrap());
            assert!(!config.perception);
            assert!(config.fees && config.taker_delay);
        }
    }
}
