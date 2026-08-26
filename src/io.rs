use thiserror::Error;

use crate::domain::marketstate::{BookError, MarketState};
use crate::domain::types::PriceError;
use crate::feed::Snapshot;

/// Anything that can go wrong turning at-rest data into a `MarketState`. All
/// the data-boundary failures live here, so the engine downstream only ever
/// sees already-valid `MarketState`s.
#[derive(Debug, Error)]
pub enum LoadError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow decode error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("column {0:?} missing or of unexpected type")]
    MissingColumn(&'static str),
    #[error("invalid price: {0}")]
    Price(#[from] PriceError),
    #[error("size {0:?} is not a decimal number")]
    InvalidSize(String),
    #[error("trade side {0:?} is not \"buy\" or \"sell\"")]
    InvalidSide(String),
    #[error("invalid book: {0}")]
    Book(#[from] BookError),
    #[error("timestamps out of order: previous {previous} µs, got {got} µs")]
    OutOfOrder { previous: i64, got: i64 },
}

/// An in-memory source: hands back pre-built `MarketState`s with the same
/// `Result` item type as a file source. Lets the engine (and tests) run with
/// no file at all.
///
/// Each state is wrapped in a trade-free [`Snapshot`], which is what a
/// marketable-order test wants — no resting orders, so nothing reads the tape.
/// Tests that need trades build [`Snapshot`]s directly.
pub fn fixture(states: Vec<MarketState>) -> impl crate::feed::EventSource {
    replay(
        states
            .into_iter()
            .map(|state| {
                Ok(Snapshot {
                    state,
                    trades: Box::from([]),
                })
            })
            .collect(),
    )
}

/// The same, for snapshots that already carry trades.
pub fn replay(snapshots: Vec<Result<Snapshot, LoadError>>) -> impl crate::feed::EventSource {
    crate::feed::Replay::new(snapshots.into_iter())
}
