// Example strategies live in the top-level `strategies/` directory, one level
// up from `src/`, so they read as a distinct layer built *on* the engine rather
// than part of its internals. `#[path]` keeps them compiled into the crate with
// their module names unchanged (`crate::quoter::Quoter`, etc.), so nothing that
// uses them had to move. Adding one: drop a file in `strategies/`, add a `mod`
// line here, and one match arm in `build()` (`src/main.rs`).
#[path = "../strategies/accumulator.rs"]
pub mod accumulator;
#[path = "../strategies/buyandhold.rs"]
pub mod buyandhold;
#[path = "../strategies/buyandsell.rs"]
pub mod buyandsell;
#[path = "../strategies/quoter.rs"]
pub mod quoter;

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
    RunArtifact, RunDiagnostic, RunOutcome, StrategyMetadata,
};
