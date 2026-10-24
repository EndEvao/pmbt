//! Single-market, file-backed backtests without CLI setup code.
//!
//! Inputs are chronological Telonex `*_book_snapshot_full.parquet` files for
//! one token. Their sibling trade tapes are optional. The runner returns
//! diagnostics instead of printing; strategy state belongs to the caller.
//!
//! ```no_run
//! use pmbt::{Backtest, BacktestConfig, OutputConfig, StrategyMetadata};
//! let mut config = BacktestConfig::new("data/token_book_snapshot_full.parquet");
//! config.simulation.fill = Some("sweep".into());
//! let mut strategy = pmbt::buyandhold::BuyAndHold::new(25.0);
//! let outcome = Backtest::new(config)
//!     .metadata(StrategyMetadata::new("My strategy"))
//!     .outputs(OutputConfig::html("report.html"))
//!     .run(&mut strategy)?;
//! assert!(outcome.artifacts[0].path.is_file());
//! # Ok::<(), pmbt::BacktestError>(())
//! ```
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::config::{resolve, ConfigError, ResolvedSimulation, SimulationConfig};
use crate::domain::types::{MarketId, Price};
use crate::engine::{BacktestResult, Engine, RunError};
use crate::feed::{Event, EventSource, FileFeed};
use crate::io::LoadError;
use crate::report::{
    self,
    html::ReportContext,
    record::{RecorderConfig, TimeSeriesRecorder},
};
use crate::resting::QueueModel;
use crate::strategy::Strategy;
use crate::universe::Category;

/// Settings for one run. Paths are relative to the process working directory.
/// Load profiles with [`crate::config::SimulationProfile`] and assign their
/// optional settings to `simulation`; no additional file format is introduced.
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub book_paths: Vec<PathBuf>,
    /// Finite and strictly positive; defaults to $10,000.
    pub initial_capital: f64,
    pub simulation: SimulationConfig,
    /// Overrides the category in `simulation` when supplied.
    pub category: Option<Category>,
    /// Token-relative redemption price. `None` leaves inventory unresolved.
    pub resolution: Option<Price>,
    pub market_name: Option<String>,
    pub outcome: Option<String>,
}
impl BacktestConfig {
    pub fn new(book_path: impl Into<PathBuf>) -> Self {
        Self {
            book_paths: vec![book_path.into()],
            initial_capital: 10_000.0,
            simulation: SimulationConfig::default(),
            category: None,
            resolution: None,
            market_name: None,
            outcome: None,
        }
    }
}

/// Caller-supplied display metadata, never a strategy registry lookup.
#[derive(Debug, Clone)]
pub struct StrategyMetadata {
    pub name: String,
    pub parameters: Vec<(String, String)>,
}
impl StrategyMetadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            parameters: Vec::new(),
        }
    }
}
impl Default for StrategyMetadata {
    fn default() -> Self {
        Self::new("Custom strategy")
    }
}

/// Optional outputs. No outputs means no history is retained. Parquet alone
/// streams; requesting HTML or JSON buffers history for all requested outputs.
///
/// File destinations must not exist. Parquet destinations must be absent or
/// empty directories. Parent directories must exist. Outputs must not overlap.
/// Writes are not transactional: an error can leave incomplete files/directories;
/// callers should use a fresh destination for a retry.
#[derive(Debug, Clone, Default)]
pub struct OutputConfig {
    pub html: Option<PathBuf>,
    pub json: Option<PathBuf>,
    pub parquet: Option<PathBuf>,
}
impl OutputConfig {
    pub fn html(path: impl Into<PathBuf>) -> Self {
        Self {
            html: Some(path.into()),
            ..Self::default()
        }
    }
    pub fn json(path: impl Into<PathBuf>) -> Self {
        Self {
            json: Some(path.into()),
            ..Self::default()
        }
    }
    pub fn parquet(path: impl Into<PathBuf>) -> Self {
        Self {
            parquet: Some(path.into()),
            ..Self::default()
        }
    }
    pub fn recording_mode(&self) -> RecordingMode {
        if self.html.is_some() || self.json.is_some() {
            RecordingMode::Buffered
        } else if self.parquet.is_some() {
            RecordingMode::StreamingParquet
        } else {
            RecordingMode::None
        }
    }
    fn artifacts(&self) -> Vec<RunArtifact> {
        [
            (ArtifactKind::Html, &self.html),
            (ArtifactKind::Json, &self.json),
            (ArtifactKind::Parquet, &self.parquet),
        ]
        .into_iter()
        .filter_map(|(kind, path)| {
            path.as_ref().map(|path| RunArtifact {
                kind,
                path: path.clone(),
            })
        })
        .collect()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordingMode {
    None,
    Buffered,
    StreamingParquet,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    Html,
    Json,
    Parquet,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArtifact {
    pub kind: ArtifactKind,
    /// HTML/JSON file, or a directory containing the Parquet table set.
    pub path: PathBuf,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunDiagnostic {
    /// The named book file was replayed without its sibling trade tape.
    MissingTradeTape { book_path: PathBuf },
}
/// Returned only when the run and every requested writer completed successfully.
#[derive(Debug)]
pub struct RunOutcome {
    pub result: BacktestResult,
    pub config: BacktestConfig,
    /// Resolved settings including the effective market category.
    pub simulation: ResolvedSimulation,
    pub strategy: StrategyMetadata,
    pub diagnostics: Vec<RunDiagnostic>,
    pub artifacts: Vec<RunArtifact>,
    pub recording: RecordingMode,
    /// `None` for unrecorded runs; `Some(0)` for a recorded empty feed.
    pub recorded_samples: Option<u64>,
}
#[derive(Debug, Error)]
pub enum BacktestError {
    #[error(transparent)]
    Configuration(#[from] ConfigError),
    #[error("initial capital must be finite and positive, got {0}")]
    InvalidCapital(f64),
    #[error("at least one book file is required")]
    NoInputs,
    #[error("expected a *_book_snapshot_full.parquet input, got {0}")]
    InvalidBookPath(PathBuf),
    #[error("could not open input files {paths:?}: {source}")]
    Input {
        paths: Vec<PathBuf>,
        #[source]
        source: LoadError,
    },
    #[error("feed failed during simulation: {0}")]
    Simulation(#[source] LoadError),
    #[error("single-market run contains multiple tokens: {expected} and {found}")]
    MixedMarkets { expected: String, found: String },
    #[error("invalid output destination {path}: {reason}")]
    InvalidOutput { path: PathBuf, reason: &'static str },
    #[error("{kind:?} output failed at {path}: {source}")]
    Output {
        kind: ArtifactKind,
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error>,
    },
}
impl BacktestError {
    fn output(
        kind: ArtifactKind,
        path: &Path,
        source: impl Into<Box<dyn std::error::Error>>,
    ) -> Self {
        Self::Output {
            kind,
            path: path.to_path_buf(),
            source: source.into(),
        }
    }
}

/// Consumed by one run. Pass a fresh strategy for independent experiments.
/// Custom feeds/recorders/fill models remain available through [`Engine`].
#[derive(Debug)]
pub struct Backtest {
    config: BacktestConfig,
    outputs: OutputConfig,
    metadata: StrategyMetadata,
}
impl Backtest {
    pub fn new(config: BacktestConfig) -> Self {
        Self {
            config,
            outputs: OutputConfig::default(),
            metadata: StrategyMetadata::default(),
        }
    }
    pub fn outputs(mut self, outputs: OutputConfig) -> Self {
        self.outputs = outputs;
        self
    }
    pub fn metadata(mut self, metadata: StrategyMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn run(self, strategy: &mut dyn Strategy) -> Result<RunOutcome, BacktestError> {
        if !self.config.initial_capital.is_finite() || self.config.initial_capital <= 0.0 {
            return Err(BacktestError::InvalidCapital(self.config.initial_capital));
        }
        if self.config.book_paths.is_empty() {
            return Err(BacktestError::NoInputs);
        }
        for path in &self.config.book_paths {
            if !path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.ends_with("_book_snapshot_full.parquet"))
            {
                return Err(BacktestError::InvalidBookPath(path.clone()));
            }
        }
        let overrides = SimulationConfig {
            category: self.config.category,
            ..Default::default()
        };
        let simulation = resolve(Some(&self.config.simulation), &overrides)?;
        let (feed, missing) =
            FileFeed::from_book_paths(&self.config.book_paths).map_err(|source| {
                BacktestError::Input {
                    paths: self.config.book_paths.clone(),
                    source,
                }
            })?;
        validate_outputs(&self.outputs)?;
        let diagnostics = missing
            .iter()
            .map(|p| RunDiagnostic::MissingTradeTape {
                book_path: p.clone(),
            })
            .collect();
        let feed = SingleMarketFeed {
            inner: feed,
            market: None,
        };
        let engine = Engine::new(
            self.config.initial_capital,
            simulation.fill(),
            simulation.simulation(None),
        );
        let recording = self.outputs.recording_mode();
        let resolution = self.config.resolution;
        let (result, recorded_samples) = match recording {
            RecordingMode::None => (
                engine
                    .run(feed, strategy, resolution)
                    .map_err(|e| self.run_error(e))?,
                None,
            ),
            RecordingMode::StreamingParquet => {
                let path = self
                    .outputs
                    .parquet
                    .as_ref()
                    .expect("mode requires Parquet");
                let output_error = |e| BacktestError::output(ArtifactKind::Parquet, path, e);
                let mut recorder =
                    report::stream::ParquetRecorder::new(path, RecorderConfig::default())
                        .map_err(output_error)?;
                let result = engine
                    .run_recorded(feed, strategy, resolution, &mut recorder)
                    .map_err(|e| self.run_error(e))?;
                let count = recorder.finish(&result).map_err(output_error)?;
                (result, Some(count))
            }
            RecordingMode::Buffered => {
                let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
                let result = engine
                    .run_recorded(feed, strategy, resolution, &mut recorder)
                    .map_err(|e| self.run_error(e))?;
                if let Some(path) = &self.outputs.json {
                    report::write_json(path, &result, &recorder)
                        .map_err(|e| BacktestError::output(ArtifactKind::Json, path, e))?;
                }
                if let Some(path) = &self.outputs.parquet {
                    report::parquet::write_run(path, &result, &recorder)
                        .map_err(|e| BacktestError::output(ArtifactKind::Parquet, path, e))?;
                }
                if let Some(path) = &self.outputs.html {
                    let context = self.report_context(&simulation, missing.len());
                    let source = self
                        .config
                        .book_paths
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join("\n");
                    report::html::write_html_with_context(
                        path,
                        &self.metadata.name,
                        &source,
                        &result,
                        &recorder,
                        resolution.map(|p| p.as_f64()),
                        &context,
                    )
                    .map_err(|e| BacktestError::output(ArtifactKind::Html, path, e))?;
                }
                (result, Some(recorder.samples().len() as u64))
            }
        };
        Ok(RunOutcome {
            result,
            config: self.config,
            simulation,
            strategy: self.metadata,
            diagnostics,
            artifacts: self.outputs.artifacts(),
            recording,
            recorded_samples,
        })
    }

    fn run_error(&self, error: RunError<BacktestError>) -> BacktestError {
        match error {
            RunError::Feed(error) => error,
            RunError::Recorder(source) => BacktestError::output(
                ArtifactKind::Parquet,
                self.outputs
                    .parquet
                    .as_deref()
                    .expect("only streaming recorder is fallible"),
                source,
            ),
        }
    }

    fn report_context(&self, resolved: &ResolvedSimulation, missing_tapes: usize) -> ReportContext {
        let sim = resolved.simulation(None);
        let queue = match sim.queue {
            QueueModel::Front => "front",
            QueueModel::Proportional => "proportional",
            QueueModel::TradesOnly => "trades-only",
        };
        let mut settings = vec![
            ("Fill model".into(), resolved.fill_model.name().into()),
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
                resolved
                    .category
                    .map_or("unspecified".into(), |c| format!("{c:?}").to_lowercase()),
            ),
            (
                "Resolution supplied".into(),
                self.config
                    .resolution
                    .map_or("unresolved".into(), |p| p.to_decimal_string()),
            ),
            (
                "Trade-tape coverage".into(),
                format!(
                    "{} / {} files",
                    self.config.book_paths.len() - missing_tapes,
                    self.config.book_paths.len()
                ),
            ),
            ("Engine version".into(), env!("CARGO_PKG_VERSION").into()),
        ];
        settings.extend(
            self.metadata
                .parameters
                .iter()
                .map(|(k, v)| (format!("Strategy / {k}"), v.clone())),
        );
        ReportContext {
            market_name: self.config.market_name.clone(),
            outcome: self.config.outcome.clone(),
            settings,
            queue: Some(queue.into()),
            // Existing report field is boolean; mixed coverage is shown in settings.
            tape_present: if missing_tapes == 0 {
                Some(true)
            } else if missing_tapes == self.config.book_paths.len() {
                Some(false)
            } else {
                None
            },
        }
    }
}

// Keep the single-token assumption enforced without changing the reusable feed.
struct SingleMarketFeed {
    inner: FileFeed,
    market: Option<MarketId>,
}
impl EventSource for SingleMarketFeed {
    type Error = BacktestError;
    fn next_event(&mut self, deadline: Option<i64>) -> Option<Result<Event, Self::Error>> {
        self.inner.next_event(deadline).map(|event| {
            let event = event.map_err(BacktestError::Simulation)?;
            if let Event::Book(state) = &event {
                if let Some(market) = &self.market {
                    if market != &state.market_id {
                        return Err(BacktestError::MixedMarkets {
                            expected: market.as_str().to_owned(),
                            found: state.market_id.as_str().to_owned(),
                        });
                    }
                } else {
                    self.market = Some(state.market_id.clone());
                }
            }
            Ok(event)
        })
    }
}

fn validate_outputs(outputs: &OutputConfig) -> Result<(), BacktestError> {
    let mut destinations: Vec<PathBuf> = Vec::new();
    for artifact in outputs.artifacts() {
        let path = &artifact.path;
        let invalid = |reason| BacktestError::InvalidOutput {
            path: path.clone(),
            reason,
        };
        if path.file_name().is_none() {
            return Err(invalid("destination must have a file or directory name"));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = parent
            .canonicalize()
            .map_err(|e| BacktestError::output(artifact.kind, path, e))?;
        if !parent.is_dir() {
            return Err(invalid("parent must be a directory"));
        }
        let destination = parent.join(path.file_name().expect("checked above"));
        if destinations
            .iter()
            .any(|p| p.starts_with(&destination) || destination.starts_with(p))
        {
            return Err(invalid("output destinations overlap"));
        }
        destinations.push(destination);
        // symlink_metadata also rejects dangling symlinks instead of following
        // them and inadvertently writing outside the requested destination.
        match path.symlink_metadata() {
            Ok(meta) if artifact.kind == ArtifactKind::Parquet && meta.is_dir() => {
                let mut entries = path
                    .read_dir()
                    .map_err(|e| BacktestError::output(artifact.kind, path, e))?;
                if let Some(entry) = entries.next() {
                    entry.map_err(|e| BacktestError::output(artifact.kind, path, e))?;
                    return Err(invalid("Parquet directory must be empty"));
                }
            }
            Ok(_) => return Err(invalid("destination already exists")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BacktestError::output(artifact.kind, path, e)),
        }
    }
    Ok(())
}
