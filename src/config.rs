//! Shared simulation configuration for library callers and the CLI.
//!
//! Loading preserves optional fields. [`resolve`] applies explicit overrides,
//! then profile values, then defaults, validating only the effective values.
//! It does not open market data, print diagnostics, or run the engine.
//!
//! ```
//! use pmbt::config::{resolve, SimulationConfig};
//! let overrides = SimulationConfig {
//!     fill: Some("sweep".into()),
//!     latency: Some("dublin".into()),
//!     ..Default::default()
//! };
//! let settings = resolve(None, &overrides)?;
//! let simulation = settings.simulation(None);
//! let fill_model = settings.fill();
//! # Ok::<(), pmbt::config::ConfigError>(())
//! ```

use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use crate::engine::Simulation;
use crate::fill::fillmodel::{FillModel, NaiveFillModel};
use crate::fill::sweep::SweepFillModel;
use crate::latency::LatencyProfile;
use crate::resting::QueueModel;
use crate::universe::{Category, UniverseError};

/// Optional simulation settings — the `[simulation]` section. Every
/// field is optional; CLI flags override whatever is set here, and built-in
/// defaults (fill naive, latency zero, perception off, taker delay on, fees
/// on) fill the rest. Names stay strings at this layer; [`resolve`] validates the effective
/// values after merging.
#[derive(Debug, Clone, Default, Deserialize)]
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
/// Deliberately lenient — unlike [`crate::universe::Universe`], unknown top-level keys are
/// *ignored*, so a full universe file doubles as a profile and one run can
/// reuse a universe's exact realism settings.
#[derive(Debug, Clone, Default, Deserialize)]
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
    /// is valid and simply changes nothing. The existing `UniverseError` return
    /// type is retained for source compatibility; resolving uses [`ConfigError`].
    pub fn load(path: impl AsRef<Path>) -> Result<SimulationProfile, UniverseError> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }

    /// How many market entries this file carries (0 for a pure profile).
    pub fn market_count(&self) -> usize {
        self.markets.as_ref().map_or(0, Vec::len)
    }
}

/// An invalid effective simulation setting. File/TOML loading errors remain
/// available through [`SimulationProfile::load`] and its existing error type.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigError {
    #[error("unknown fill model {0:?} (naive|sweep)")]
    UnknownFill(String),
    #[error("unknown latency preset {0:?} (zero|dublin|retail-eu|us-east)")]
    UnknownLatency(String),
    #[error("unknown queue model {0:?} (front|proportional|trades-only)")]
    UnknownQueue(String),
}

/// Validated built-in fill model selection. Custom models can still be passed
/// directly to [`crate::engine::Engine::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillModelKind {
    Naive,
    Sweep,
}

impl FromStr for FillModelKind {
    type Err = ConfigError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "naive" => Ok(Self::Naive),
            "sweep" => Ok(Self::Sweep),
            other => Err(ConfigError::UnknownFill(other.into())),
        }
    }
}

impl FillModelKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Naive => "naive",
            Self::Sweep => "sweep",
        }
    }

    /// Construct a fresh fill model for each run.
    pub fn build(self) -> Box<dyn FillModel> {
        match self {
            Self::Naive => Box::new(NaiveFillModel),
            Self::Sweep => Box::new(SweepFillModel),
        }
    }
}

/// Resolve a placement-latency preset. Shared with comparison-matrix builders,
/// whose choice of preset may intentionally differ from the base profile.
pub fn latency_preset(name: &str) -> Result<LatencyProfile, ConfigError> {
    match name {
        "zero" => Ok(LatencyProfile::zero()),
        "dublin" => Ok(LatencyProfile::dublin()),
        "retail-eu" => Ok(LatencyProfile::retail_eu()),
        "us-east" => Ok(LatencyProfile::us_east()),
        other => Err(ConfigError::UnknownLatency(other.into())),
    }
}

fn queue_model(name: &str) -> Result<QueueModel, ConfigError> {
    match name {
        "front" => Ok(QueueModel::Front),
        "proportional" => Ok(QueueModel::Proportional),
        "trades-only" => Ok(QueueModel::TradesOnly),
        other => Err(ConfigError::UnknownQueue(other.into())),
    }
}

/// Complete simulation settings. String selections have been validated; a
/// fill model can now be built without another fallible name lookup.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedSimulation {
    pub fill_model: FillModelKind,
    pub queue: QueueModel,
    pub preset: LatencyProfile,
    pub perception: bool,
    pub taker_delay: bool,
    pub fees: bool,
    /// Default category from explicit overrides or the profile. A batch uses
    /// each market's category instead, via [`Self::simulation`].
    pub category: Option<Category>,
}

impl ResolvedSimulation {
    /// Compose engine options. A supplied market category takes precedence over
    /// the resolved default. Without either, category-based fees and holds are
    /// zero, matching a bare CLI run. Fee-free categories also remain fee-free.
    pub fn simulation(&self, market_category: Option<Category>) -> Simulation {
        let category = market_category.or(self.category);
        let taker = match (self.taker_delay, category) {
            (true, Some(category)) => crate::latency::taker_delay(category),
            _ => Duration::ZERO,
        };
        let fee_rate = match (self.fees, category) {
            (true, Some(category)) => crate::fees::fee_rate(category),
            _ => 0.0,
        };
        Simulation {
            latency: self.preset.with_taker_delay(taker),
            perception: self.perception,
            fee_rate,
            queue: self.queue,
        }
    }

    pub fn fill(&self) -> Box<dyn FillModel> {
        self.fill_model.build()
    }
}

/// Pure precedence resolution: explicit overrides > profile > built-in defaults.
/// `Some(false)` is an explicit override; `None` means unspecified. Unknown
/// names fail here, before market data is opened. Shadowed names are not used
/// or validated. The input configurations are left unchanged.
pub fn resolve(
    profile: Option<&SimulationConfig>,
    overrides: &SimulationConfig,
) -> Result<ResolvedSimulation, ConfigError> {
    let empty = SimulationConfig::default();
    let profile = profile.unwrap_or(&empty);
    let preset = latency_preset(
        overrides
            .latency
            .as_deref()
            .or(profile.latency.as_deref())
            .unwrap_or("zero"),
    )?;
    let queue = queue_model(
        overrides
            .queue
            .as_deref()
            .or(profile.queue.as_deref())
            .unwrap_or("proportional"),
    )?;
    let fill_model = overrides
        .fill
        .as_deref()
        .or(profile.fill.as_deref())
        .unwrap_or("naive")
        .parse()?;
    Ok(ResolvedSimulation {
        fill_model,
        queue,
        preset,
        perception: overrides.perception.or(profile.perception).unwrap_or(false),
        taker_delay: overrides
            .taker_delay
            .or(profile.taker_delay)
            .unwrap_or(true),
        fees: overrides.fees.or(profile.fees).unwrap_or(true),
        category: overrides.category.or(profile.category),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    #[test]
    fn resolved_fill_selection_drives_the_expected_execution_model() {
        use crate::domain::marketstate::{Level, MarketState};
        use crate::domain::types::{MarketId, Price};
        let level = |price, size| Level {
            price: Price::from_probability(price).unwrap(),
            size,
        };
        for (name, expected_cash) in [("naive", 1012.0), ("sweep", 1011.0)] {
            let settings = resolve(
                None,
                &SimulationConfig {
                    fill: Some(name.into()),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(settings.fill_model.name(), name);
            let state = MarketState::new(
                MarketId::new("token".into()),
                1_000_000,
                Box::from([level(0.3, 100.0)]),
                Box::from([level(0.4, 10.0), level(0.5, 10.0)]),
            )
            .unwrap();
            let result = Engine::new(1000.0, settings.fill(), settings.simulation(None))
                .run(
                    crate::io::fixture(vec![state]),
                    &mut crate::buyandhold::BuyAndHold::new(20.0),
                    Some(Price::from_probability(1.0).unwrap()),
                )
                .unwrap();
            assert!((result.final_cash - expected_cash).abs() < 1e-9);
        }
    }
}
