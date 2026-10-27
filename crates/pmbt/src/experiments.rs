//! Batch and comparison workflows for caller-owned strategies. Factories are
//! called once per run; no strategy state is shared by the orchestration.
//! Batch capital is independent per market, never carried between markets.
use crate::{
    backtest::{open_input, validate_outputs},
    config::{latency_preset, resolve, ConfigError, ResolvedSimulation, SimulationConfig},
    engine::{BacktestResult, Engine, RunError},
    latency::LatencyProfile,
    report::{
        self,
        record::{RecorderConfig, TimeSeriesRecorder},
    },
    strategy::Strategy,
    universe::{self, Aggregate, MarketEntry, Universe, UniverseError},
    ArtifactKind, BacktestConfig, BacktestError, OutputConfig, RunArtifact, RunDiagnostic,
};
use std::path::PathBuf;
use thiserror::Error;

/// Aggregate reports support HTML and JSON. They retain reduced curves, dropping
/// each full history before the next run. Destinations follow `OutputConfig` rules.
#[derive(Debug, Clone, Default)]
pub struct ExperimentOutputs {
    pub html: Option<PathBuf>,
    pub json: Option<PathBuf>,
}
impl ExperimentOutputs {
    fn outputs(&self) -> OutputConfig {
        OutputConfig {
            html: self.html.clone(),
            json: self.json.clone(),
            parquet: None,
        }
    }
    fn requested(&self) -> bool {
        self.html.is_some() || self.json.is_some()
    }
}

#[derive(Debug, Error)]
pub enum ExperimentError {
    #[error(transparent)]
    Configuration(#[from] ConfigError),
    #[error(transparent)]
    Universe(#[from] UniverseError),
    #[error(transparent)]
    Backtest(#[from] BacktestError),
    #[error("unknown --axis {0:?} (realism|latency|fees|perception|taker-delay)")]
    UnknownAxis(String),
    #[error("comparison requires at least one variant")]
    EmptyComparison,
    #[error("run {label:?} failed: {source}")]
    Run {
        label: String,
        #[source]
        source: BacktestError,
    },
}

#[derive(Debug)]
pub struct BatchConfig {
    pub universe: Universe,
    pub overrides: SimulationConfig,
    pub initial_capital: f64,
    pub outputs: ExperimentOutputs,
}
impl BatchConfig {
    pub fn new(universe: Universe) -> Self {
        Self {
            universe,
            overrides: SimulationConfig::default(),
            initial_capital: 10_000.0,
            outputs: ExperimentOutputs::default(),
        }
    }
}
/// Curves are empty when no aggregate output was requested (no history captured).
/// `simulations` is in the same order as `runs` and includes per-market categories.
pub struct BatchOutcome {
    pub runs: Vec<report::batch::MarketRun>,
    pub aggregate: Aggregate,
    pub simulations: Vec<ResolvedSimulation>,
    pub diagnostics: Vec<RunDiagnostic>,
    pub artifacts: Vec<RunArtifact>,
}

pub fn run_batch(
    config: BatchConfig,
    mut factory: impl FnMut(&MarketEntry) -> Box<dyn Strategy>,
) -> Result<BatchOutcome, ExperimentError> {
    let universe = &config.universe;
    let base = resolve(universe.simulation.as_ref(), &config.overrides)?;
    if universe.markets.is_empty() {
        return Err(UniverseError::EmptyUniverse.into());
    }
    let mut ids = std::collections::HashSet::new();
    for entry in &universe.markets {
        if !ids.insert(&entry.market_id) {
            return Err(UniverseError::DuplicateMarket(entry.market_id.clone()).into());
        }
        entry.resolution()?;
    }
    validate_outputs(&config.outputs.outputs())?;
    let mut runs = Vec::new();
    let mut simulations = Vec::new();
    let mut diagnostics = Vec::new();
    for entry in &universe.markets {
        let paths = entry.data_files(&universe.data_dir)?;
        let mut input = BacktestConfig::new(&paths[0]);
        input.book_paths = paths;
        input.initial_capital = config.initial_capital;
        input.resolution = entry.resolution()?;
        let resolved = ResolvedSimulation {
            category: Some(entry.category),
            ..base
        };
        let (feed, notes) = open_input(&input).map_err(|source| ExperimentError::Run {
            label: entry.market_id.clone(),
            source,
        })?;
        let mut strategy = factory(entry);
        let (result, recorder) = execute(
            feed,
            &input,
            resolved,
            strategy.as_mut(),
            config.outputs.requested(),
        )
        .map_err(|source| ExperimentError::Run {
            label: entry.market_id.clone(),
            source,
        })?;
        let category = format!("{:?}", entry.category).to_lowercase();
        runs.push(match recorder {
            Some(recorder) => report::batch::MarketRun::from_recorded(
                entry.market_id.clone(),
                category,
                result,
                &recorder,
            ),
            None => report::batch::MarketRun {
                id: entry.market_id.clone(),
                category,
                result,
                curve: Vec::new(),
            },
        });
        simulations.push(resolved);
        diagnostics.extend(notes);
    }
    let aggregate = universe::aggregate(
        &runs
            .iter()
            .map(|r| (r.id.clone(), r.result.clone()))
            .collect::<Vec<_>>(),
    );
    if let Some(path) = &config.outputs.json {
        report::batch::write_json(path, &universe.name, &runs)
            .map_err(|e| BacktestError::output(ArtifactKind::Json, path, e))?;
    }
    if let Some(path) = &config.outputs.html {
        report::batch::write_html(path, &universe.name, &runs)
            .map_err(|e| BacktestError::output(ArtifactKind::Html, path, e))?;
    }
    Ok(BatchOutcome {
        runs,
        aggregate,
        simulations,
        diagnostics,
        artifacts: config.outputs.outputs().artifacts(),
    })
}

/// Label, explanatory detail, and effective simulation options. A custom matrix
/// may be supplied directly; built-in axes come from `comparison_matrix`.
pub type CompareConfig = (String, String, ResolvedSimulation);
#[derive(Debug)]
pub struct ComparisonConfig {
    pub input: BacktestConfig,
    /// Each variant supplies its simulation settings; input.simulation is ignored.
    /// input.category, if present, overrides the category on every variant.
    pub variants: Vec<CompareConfig>,
    pub strategy_name: String,
    /// Whether consecutive differences should be displayed as a friction ladder.
    pub ladder: bool,
    pub outputs: ExperimentOutputs,
}
pub struct ComparisonOutcome {
    pub runs: Vec<report::compare::CompareRun>,
    pub variants: Vec<CompareConfig>,
    pub diagnostics: Vec<RunDiagnostic>,
    pub artifacts: Vec<RunArtifact>,
}

pub fn run_comparison(
    mut config: ComparisonConfig,
    mut factory: impl FnMut() -> Box<dyn Strategy>,
) -> Result<ComparisonOutcome, ExperimentError> {
    if config.variants.is_empty() {
        return Err(ExperimentError::EmptyComparison);
    }
    validate_outputs(&config.outputs.outputs())?;
    let mut runs = Vec::new();
    let mut diagnostics = Vec::new();
    for (label, detail, resolved) in &mut config.variants {
        resolved.category = config.input.category.or(resolved.category);
        let (feed, notes) = open_input(&config.input).map_err(|source| ExperimentError::Run {
            label: label.clone(),
            source,
        })?;
        let mut strategy = factory();
        let (result, recorder) = execute(feed, &config.input, *resolved, strategy.as_mut(), true)
            .map_err(|source| ExperimentError::Run {
            label: label.clone(),
            source,
        })?;
        runs.push(report::compare::CompareRun::from_recorded(
            label.clone(),
            detail.clone(),
            result,
            &recorder.expect("comparison records history"),
        ));
        // Every variant uses the same input; report coverage once.
        if diagnostics.is_empty() {
            diagnostics = notes;
        }
    }
    let subtitle = config
        .input
        .book_paths
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    if let Some(path) = &config.outputs.json {
        report::compare::write_json(
            path,
            &config.strategy_name,
            &subtitle,
            config.ladder,
            config.variants.iter().all(|(_, _, s)| s.category.is_some()),
            &runs,
        )
        .map_err(|e| BacktestError::output(ArtifactKind::Json, path, e))?;
    }
    if let Some(path) = &config.outputs.html {
        report::compare::write_html(
            path,
            &config.strategy_name,
            &subtitle,
            config.ladder,
            config.variants.iter().all(|(_, _, s)| s.category.is_some()),
            &runs,
        )
        .map_err(|e| BacktestError::output(ArtifactKind::Html, path, e))?;
    }
    Ok(ComparisonOutcome {
        runs,
        variants: config.variants,
        diagnostics,
        artifacts: config.outputs.outputs().artifacts(),
    })
}

fn execute(
    feed: crate::backtest::SingleMarketFeed,
    input: &BacktestConfig,
    resolved: ResolvedSimulation,
    strategy: &mut dyn Strategy,
    record: bool,
) -> Result<(BacktestResult, Option<TimeSeriesRecorder>), BacktestError> {
    let engine = Engine::new(
        input.initial_capital,
        resolved.fill(),
        resolved.simulation(None),
    );
    let mut recorder = record.then(|| TimeSeriesRecorder::new(RecorderConfig::default()));
    let result = match &mut recorder {
        Some(recorder) => engine.run_recorded(feed, strategy, input.resolution, recorder),
        None => engine.run(feed, strategy, input.resolution),
    }
    .map_err(|e| match e {
        RunError::Feed(e) => e,
        // Neither NullRecorder nor TimeSeriesRecorder performs fallible I/O.
        RunError::Recorder(_) => unreachable!("in-memory recording cannot fail"),
    })?;
    Ok((result, recorder))
}

/// Existing CLI axes, with identical historical preset semantics. For realism
/// and perception, the explicit `flags.latency` wins; otherwise use Dublin,
/// even if the base profile names another preset. Other axes override their
/// documented frictions rather than preserving every base toggle.
pub fn comparison_matrix(
    axis: &str,
    base: &ResolvedSimulation,
    flags: &SimulationConfig,
) -> Result<Vec<CompareConfig>, ExperimentError> {
    let zero = LatencyProfile::zero();
    // The non-zero placement preset used by the ladder / perception axis: the
    // CLI's --latency if given, else dublin (the research-sourced realistic one).
    let lat_name = flags.latency.as_deref().unwrap_or("dublin");
    let lat = latency_preset(lat_name)?;

    // Build a variant that reuses the base fill model and overrides the knobs.
    let v = |preset: LatencyProfile, perception: bool, taker_delay: bool, fees: bool| {
        ResolvedSimulation {
            fill_model: base.fill_model,
            category: base.category,
            queue: base.queue,
            preset,
            perception,
            taker_delay,
            fees,
        }
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
                let preset = latency_preset(n).expect("known preset");
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
            return Err(ExperimentError::UnknownAxis(other.into()));
        }
    };
    Ok(matrix)
}
