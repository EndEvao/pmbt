pub mod backtest;
pub mod clob;
pub mod config;
pub mod convert;
pub mod domain;
pub mod engine;
pub mod experiments;
pub mod feed;
pub mod fees;
pub mod fill;
pub mod io;
pub mod latency;
pub mod portfolio;
pub mod report;
pub mod reservation;
pub mod residual;
pub mod resting;
pub mod strategy;
pub mod telonex;
pub mod universe;

#[cfg(test)]
mod engine_regressions;

// Common entry points for file-backed library runs.
pub use backtest::{
    ArtifactKind, Backtest, BacktestConfig, BacktestError, OutputConfig, RecordingMode,
    RunArtifact, RunDiagnostic, RunOutcome, RunProvenance, SourceRevision, StrategyMetadata,
};

#[cfg(test)]
mod test_support;
