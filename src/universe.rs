//! Universe configs: TOML-defined sets of markets for batch backtesting.
//!
//! A universe is the unit of "run this strategy over these markets": each
//! entry names a token, its category, and (if the market has resolved) what
//! the token redeemed at. The batch runner executes one single-market engine
//! per entry — the engine's single-market assumption is never touched — and
//! aggregates the results.
//!
//! This is also where per-market metadata lives as features grow: the
//! `Category` enum exists so the future latency model can map category →
//! Polymarket's taker delay (sports → 3 s / 1 s, crypto & finance → 250 ms,
//! rest → 0) without new plumbing.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::domain::types::Price;
use crate::engine::{BacktestResult, Settlement};

/// Polymarket's stable site-level taxonomy. Variants are distinguished where
/// the exchange treats them differently — taker delay (sports; crypto &
/// finance) and taker fees (per-tier rates, `geopolitics` fee-free). Unknown
/// strings fail parsing — no silent `other` (fail fast; a typo'd category
/// would otherwise silently get the wrong delay or fee later).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    Politics,
    Sports,
    Crypto,
    Finance,
    Tech,
    Culture,
    Science,
    /// World events — the fee-free tier.
    Geopolitics,
    Other,
}

/// One market in the universe. `market_id` is the token id — the same string
/// that keys the ledger and prefixes the data filenames.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEntry {
    pub market_id: String,
    pub category: Category,
    /// What the traded token redeemed at (0 lost / 1 won); absent = the
    /// market is unresolved. Same semantics as the `--resolution` CLI flag.
    resolution: Option<u8>,
    /// Optional explicit data file, overriding discovery in `data_dir`.
    file: Option<PathBuf>,
}

/// Per-universe simulation defaults — the `[simulation]` section. Every
/// field is optional; CLI flags override whatever is set here, and built-in
/// defaults (fill naive, latency zero, perception off, taker delay on, fees
/// on) fill the rest. Names stay strings at this layer — the registries that
/// resolve them live in `main.rs`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationConfig {
    /// Fill model name (`naive` | `sweep`).
    pub fill: Option<String>,
    /// Latency preset name (`zero` | `dublin` | `retail-eu` | `us-east`).
    pub latency: Option<String>,
    /// Deliver snapshots to the strategy late by the feed component.
    pub perception: Option<bool>,
    /// Apply the exchange's category-based taker delay (off = counterfactual
    /// "what if the exchange had no hold").
    pub taker_delay: Option<bool>,
    /// Apply the exchange's category-based taker fee (off = counterfactual
    /// "what if trading were free").
    pub fees: Option<bool>,
    /// Market category — the axis that drives the taker delay (sports 3 s,
    /// crypto/finance 250 ms) and the fee tier. Honoured by `run`/`compare`
    /// through `--config`, where it saves the easily-forgotten `--category`
    /// flag; **ignored by `batch`**, whose markets each carry their own.
    pub category: Option<Category>,
    /// Queue model for resting orders: `front` (optimistic bound),
    /// `proportional` (default, neutral), `trades-only` (pessimistic bound).
    /// Only affects strategies that rest limit orders.
    pub queue: Option<String>,
}

/// A standalone simulation profile: any TOML carrying a `[simulation]` section.
/// Deliberately lenient — unlike [`Universe`], unknown top-level keys are
/// *ignored*, so a full universe file doubles as a profile and one run can
/// reuse a universe's exact realism settings.
#[derive(Debug, Default, Deserialize)]
pub struct SimulationProfile {
    #[serde(default)]
    pub simulation: Option<SimulationConfig>,
    /// Present only so a caller can tell the user their market entries are
    /// being ignored — `--config` reads nothing but `[simulation]`.
    #[serde(default)]
    pub markets: Option<Vec<toml::Value>>,
}

impl SimulationProfile {
    /// Load a profile from a TOML file. A file with no `[simulation]` section
    /// is valid and simply changes nothing.
    pub fn load(path: impl AsRef<Path>) -> Result<SimulationProfile, UniverseError> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }

    /// How many market entries this file carries (0 for a pure profile).
    pub fn market_count(&self) -> usize {
        self.markets.as_ref().map_or(0, Vec::len)
    }
}

/// A parsed, validated universe config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Universe {
    pub name: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub simulation: Option<SimulationConfig>,
    pub markets: Vec<MarketEntry>,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data/telonex")
}

impl Universe {
    /// Load and validate a universe TOML file. All config failures surface
    /// here, at startup — before any engine runs.
    pub fn load(path: impl AsRef<Path>) -> Result<Universe, UniverseError> {
        Self::parse(&std::fs::read_to_string(path)?)
    }

    fn parse(text: &str) -> Result<Universe, UniverseError> {
        let universe: Universe = toml::from_str(text)?;
        if universe.markets.is_empty() {
            return Err(UniverseError::EmptyUniverse);
        }
        let mut seen = HashSet::new();
        for entry in &universe.markets {
            if !seen.insert(entry.market_id.as_str()) {
                return Err(UniverseError::DuplicateMarket(entry.market_id.clone()));
            }
            entry.resolution()?; // validate 0|1 up front, not mid-batch
        }
        Ok(universe)
    }
}

impl MarketEntry {
    /// The entry's redemption price, if the market has resolved.
    pub fn resolution(&self) -> Result<Option<Price>, UniverseError> {
        match self.resolution {
            None => Ok(None),
            Some(0) => Ok(Some(Price::new(0).expect("0 ticks is in range"))),
            Some(1) => Ok(Some(
                Price::new(Price::TICKS_PER_UNIT).expect("1.0 is in range"),
            )),
            Some(other) => Err(UniverseError::BadResolution(other)),
        }
    }

    /// Locate the entry's `book_snapshot_full` parquet file(s), in chronological
    /// order: the explicit `file` override (as a one-element list) if given, else
    /// every `{market_id}_*_book_snapshot_full.parquet` under `data_dir`, sorted.
    /// Filenames embed a zero-padded ISO date, so a lexicographic sort *is*
    /// chronological — the order a chained feed must replay them in. Zero matches
    /// is a loud error; several is the normal multi-day case (chained into one
    /// continuous feed), not an error.
    pub fn data_files(&self, data_dir: &Path) -> Result<Vec<PathBuf>, UniverseError> {
        if let Some(file) = &self.file {
            return Ok(vec![file.clone()]);
        }
        let mut matches: Vec<PathBuf> = std::fs::read_dir(data_dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name().and_then(|n| n.to_str()).is_some_and(|name| {
                    name.starts_with(&format!("{}_", self.market_id))
                        && name.ends_with("_book_snapshot_full.parquet")
                })
            })
            .collect();
        if matches.is_empty() {
            return Err(UniverseError::DataFileNotFound(self.market_id.clone()));
        }
        matches.sort(); // filename date is zero-padded ISO ⇒ lexicographic = chronological
        Ok(matches)
    }
}

/// Anything that can go wrong turning a universe file into runnable markets.
#[derive(Debug, Error)]
pub enum UniverseError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("resolution takes 0 (token lost) or 1 (token won), got {0}")]
    BadResolution(u8),
    #[error("market {0:?} appears more than once")]
    DuplicateMarket(String),
    #[error("universe has no markets")]
    EmptyUniverse,
    #[error("no book_snapshot_full parquet for market {0:?} in the data dir")]
    DataFileNotFound(String),
}

/// The universe-level rollup: exact sums over the per-market results. Cash
/// figures stay per-market (each run starts from its own initial capital);
/// P&L and counts add.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub markets: usize,
    pub total_pnl: f64,
    pub realized_pnl: f64,
    pub settlement_pnl: f64,
    pub fees: f64,
    pub fills: u64,
    pub rejections: u64,
    pub settled: usize,
    pub unresolved: usize,
    pub nothing_held: usize,
}

pub fn aggregate(results: &[(String, BacktestResult)]) -> Aggregate {
    let mut agg = Aggregate {
        markets: results.len(),
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
    for (_, result) in results {
        agg.total_pnl += result.total_pnl;
        agg.realized_pnl += result.realized_pnl;
        agg.fees += result.fees;
        agg.fills += result.fills;
        agg.rejections += result.rejections;
        match &result.settlement {
            Settlement::Settled { pnl, .. } => {
                agg.settlement_pnl += pnl;
                agg.settled += 1;
            }
            Settlement::Unresolved => agg.unresolved += 1,
            Settlement::NothingHeld => agg.nothing_held += 1,
        }
    }
    agg
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
        name = "test"
        data_dir = "somewhere/else"

        [[markets]]
        market_id = "111"
        category = "sports"
        resolution = 1

        [[markets]]
        market_id = "222"
        category = "politics"

        [[markets]]
        market_id = "333"
        category = "crypto"
        resolution = 0
        file = "custom/path.parquet"
    "#;

    // --- simulation profiles (`--config`) ---------------------------------

    #[test]
    fn profile_parses_a_bare_simulation_section() {
        let profile: SimulationProfile = toml::from_str(
            r#"
            [simulation]
            fill = "sweep"
            latency = "dublin"
            category = "sports"
            fees = false
        "#,
        )
        .unwrap();
        assert_eq!(profile.market_count(), 0);
        let sim = profile.simulation.unwrap();
        assert_eq!(sim.fill.as_deref(), Some("sweep"));
        assert_eq!(sim.latency.as_deref(), Some("dublin"));
        assert_eq!(sim.category, Some(Category::Sports));
        assert_eq!(sim.fees, Some(false));
    }

    #[test]
    fn a_universe_file_doubles_as_a_profile() {
        // The lenient parse ignores name/data_dir/markets, so one run can
        // reuse a universe's realism settings — but still reports the markets.
        let profile: SimulationProfile = toml::from_str(FULL).unwrap();
        assert!(profile.simulation.is_none());
        assert_eq!(profile.market_count(), 3);
    }

    #[test]
    fn profile_without_a_simulation_section_is_valid_and_empty() {
        let profile: SimulationProfile = toml::from_str("name = \"nothing\"").unwrap();
        assert!(profile.simulation.is_none());
        assert_eq!(profile.market_count(), 0);
    }

    #[test]
    fn unknown_key_inside_simulation_still_fails_loudly() {
        let err = toml::from_str::<SimulationProfile>(
            r#"
            [simulation]
            fil = "sweep"
        "#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn misspelled_category_fails_loudly() {
        let err = toml::from_str::<SimulationProfile>(
            r#"
            [simulation]
            category = "sprots"
        "#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn parses_a_full_universe() {
        let u = Universe::parse(FULL).unwrap();
        assert_eq!(u.name, "test");
        assert_eq!(u.data_dir, PathBuf::from("somewhere/else"));
        assert_eq!(u.markets.len(), 3);

        assert_eq!(u.markets[0].category, Category::Sports);
        assert_eq!(
            u.markets[0].resolution().unwrap(),
            Some(Price::new(Price::TICKS_PER_UNIT).unwrap())
        );
        assert_eq!(u.markets[1].resolution().unwrap(), None); // unresolved
        assert_eq!(
            u.markets[2].resolution().unwrap(),
            Some(Price::new(0).unwrap())
        );
        assert_eq!(
            u.markets[2].data_files(Path::new("ignored")).unwrap(),
            vec![PathBuf::from("custom/path.parquet")]
        );
    }

    #[test]
    fn data_dir_defaults() {
        let u = Universe::parse(
            r#"
            name = "d"
            [[markets]]
            market_id = "1"
            category = "other"
            "#,
        )
        .unwrap();
        assert_eq!(u.data_dir, PathBuf::from("data/telonex"));
    }

    #[test]
    fn every_category_parses_and_unknown_fails() {
        for (text, want) in [
            ("politics", Category::Politics),
            ("sports", Category::Sports),
            ("crypto", Category::Crypto),
            ("finance", Category::Finance),
            ("tech", Category::Tech),
            ("culture", Category::Culture),
            ("science", Category::Science),
            ("other", Category::Other),
        ] {
            let toml =
                format!("name = \"c\"\n[[markets]]\nmarket_id = \"1\"\ncategory = \"{text}\"");
            assert_eq!(Universe::parse(&toml).unwrap().markets[0].category, want);
        }
        let bad = "name = \"c\"\n[[markets]]\nmarket_id = \"1\"\ncategory = \"esports\"";
        assert!(matches!(Universe::parse(bad), Err(UniverseError::Toml(_))));
    }

    #[test]
    fn simulation_section_parses_fully_partially_or_absent() {
        let full = r#"
            name = "s"
            [simulation]
            fill = "sweep"
            latency = "dublin"
            perception = true
            taker_delay = false
            [[markets]]
            market_id = "1"
            category = "other"
        "#;
        let u = Universe::parse(full).unwrap();
        let sim = u.simulation.unwrap();
        assert_eq!(sim.fill.as_deref(), Some("sweep"));
        assert_eq!(sim.latency.as_deref(), Some("dublin"));
        assert_eq!(sim.perception, Some(true));
        assert_eq!(sim.taker_delay, Some(false));

        let partial = r#"
            name = "s"
            [simulation]
            latency = "dublin"
            [[markets]]
            market_id = "1"
            category = "other"
        "#;
        let sim = Universe::parse(partial).unwrap().simulation.unwrap();
        assert_eq!(sim.latency.as_deref(), Some("dublin"));
        assert_eq!(sim.fill, None);

        assert!(Universe::parse(FULL).unwrap().simulation.is_none());

        let unknown = r#"
            name = "s"
            [simulation]
            jitters = true
            [[markets]]
            market_id = "1"
            category = "other"
        "#;
        assert!(matches!(
            Universe::parse(unknown),
            Err(UniverseError::Toml(_))
        ));
    }

    #[test]
    fn rejects_bad_resolution() {
        let bad =
            "name = \"r\"\n[[markets]]\nmarket_id = \"1\"\ncategory = \"other\"\nresolution = 2";
        assert!(matches!(
            Universe::parse(bad),
            Err(UniverseError::BadResolution(2))
        ));
    }

    #[test]
    fn rejects_duplicates_and_empty() {
        let dup = r#"
            name = "dup"
            [[markets]]
            market_id = "1"
            category = "other"
            [[markets]]
            market_id = "1"
            category = "sports"
        "#;
        assert!(matches!(
            Universe::parse(dup),
            Err(UniverseError::DuplicateMarket(id)) if id == "1"
        ));
        assert!(matches!(
            Universe::parse("name = \"e\"\nmarkets = []"),
            Err(UniverseError::EmptyUniverse)
        ));
    }

    #[test]
    fn discovery_finds_all_days_in_chronological_order() {
        let dir = std::env::temp_dir().join(format!("pmbt_universe_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Discovery is pure name-matching: empty marker files suffice.
        for name in [
            "42_2026-06-13_book_snapshot_full.parquet",
            "42_2026-06-13_trades.parquet", // wrong channel: ignored
            "421_2026-06-13_book_snapshot_full.parquet", // different token: ignored
        ] {
            std::fs::write(dir.join(name), b"").unwrap();
        }

        let entry = |id: &str| MarketEntry {
            market_id: id.to_string(),
            category: Category::Other,
            resolution: None,
            file: None,
        };

        // One day: a single-element list.
        assert_eq!(
            entry("42").data_files(&dir).unwrap(),
            vec![dir.join("42_2026-06-13_book_snapshot_full.parquet")]
        );
        // No matching token: a loud error.
        assert!(matches!(
            entry("999").data_files(&dir),
            Err(UniverseError::DataFileNotFound(id)) if id == "999"
        ));

        // A second day is chained, not an error, and returned in chronological
        // (lexicographic) order — the order a chained feed must replay.
        std::fs::write(dir.join("42_2026-06-14_book_snapshot_full.parquet"), b"").unwrap();
        assert_eq!(
            entry("42").data_files(&dir).unwrap(),
            vec![
                dir.join("42_2026-06-13_book_snapshot_full.parquet"),
                dir.join("42_2026-06-14_book_snapshot_full.parquet"),
            ]
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    #[ignore = "runs the real sample data; run with `cargo test -- --ignored`"]
    fn example_universe_reproduces_the_known_single_run() {
        use crate::buyandhold::BuyAndHold;
        use crate::engine::Engine;
        use crate::feed::MergedFeed;
        use crate::fill::sweep::SweepFillModel;

        let universe = Universe::load("universes/example.toml").unwrap();
        let mut results = Vec::new();
        for entry in &universe.markets {
            let (feed, _) =
                MergedFeed::from_book_paths(&entry.data_files(&universe.data_dir).unwrap())
                    .unwrap();
            let result = Engine::new(
                10_000.0,
                Box::new(SweepFillModel),
                crate::engine::Simulation::zero(),
            )
            .run(
                feed,
                &mut BuyAndHold::new(100.0),
                entry.resolution().unwrap(),
            )
            .unwrap();
            results.push((entry.market_id.clone(), result));
        }

        // The known single-run numbers for the sample market (resolved No →
        // the Yes token redeems $0): buy 100 @ 0.085, settle at -8.50.
        assert_eq!(results.len(), 1);
        let result = &results[0].1;
        assert!((result.final_cash - 9_991.5).abs() < 1e-9);
        assert!((result.total_pnl + 8.5).abs() < 1e-9);

        let agg = aggregate(&results);
        assert_eq!(agg.markets, 1);
        assert!((agg.total_pnl + 8.5).abs() < 1e-9);
        assert!((agg.settlement_pnl + 8.5).abs() < 1e-9);
        assert_eq!((agg.settled, agg.unresolved, agg.nothing_held), (1, 0, 0));
    }

    #[test]
    fn aggregate_sums_exactly() {
        let result = |total: f64, realized: f64, fees: f64, settlement: Settlement, fills: u64| {
            BacktestResult {
                initial_capital: 10_000.0,
                final_cash: 10_000.0 + total,
                realized_pnl: realized,
                total_pnl: total,
                fees,
                settlement,
                open_position: None,
                fills,
                rejections: 1,
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
        };
        let results = vec![
            (
                "a".to_string(),
                result(
                    91.5,
                    0.0,
                    1.25,
                    Settlement::Settled {
                        quantity: 100.0,
                        redemption: Price::new(Price::TICKS_PER_UNIT).unwrap(),
                        pnl: 91.5,
                    },
                    1,
                ),
            ),
            (
                "b".to_string(),
                result(-2.0, -2.0, 0.5, Settlement::Unresolved, 4),
            ),
            (
                "c".to_string(),
                result(-0.5, -0.5, 0.0, Settlement::NothingHeld, 2),
            ),
        ];

        let agg = aggregate(&results);
        assert_eq!(agg.markets, 3);
        assert!((agg.total_pnl - 89.0).abs() < 1e-9);
        assert!((agg.realized_pnl - (-2.5)).abs() < 1e-9);
        assert!((agg.settlement_pnl - 91.5).abs() < 1e-9);
        assert!((agg.fees - 1.75).abs() < 1e-9);
        assert_eq!(agg.fills, 7);
        assert_eq!(agg.rejections, 3);
        assert_eq!((agg.settled, agg.unresolved, agg.nothing_held), (1, 1, 1));
    }
}
