//! Latency modeling: where time goes between "the exchange's book changed"
//! and "our order interacts with it".
//!
//! Components are stored separately because they answer to different owners:
//! `feed` and `transit` are properties of *our* placement relative to the
//! exchange (Polymarket's CLOB runs in AWS eu-west-2, London); `taker_delay`
//! is the exchange's deliberate per-market-type hold (see
//! [`taker_delay`]). Market orders use feed + transit + taker delay; limits and
//! cancels use feed + transit. Perception optionally delays book delivery.
//! The event clock executes deadlines against the latest available book,
//! including on otherwise quiet intervals. Components are constant per run.
//!
//! Constant delays for now. Preset numbers are estimates from research
//! (AWS inter-region latency tables; Telonex's own capture infrastructure
//! measured a 7–13 ms feed floor from a well-placed collector), not probes —
//! refine when a real measurement exists.

use std::time::Duration;

use crate::universe::Category;

/// A constant decision-to-execution latency model. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencyProfile {
    /// Exchange event → we know about it (websocket path to us).
    pub feed: Duration,
    /// Our order's trip to the matching engine (REST + signing + network).
    pub transit: Duration,
    /// Polymarket's deliberate marketable-order hold (0 / 250 ms / 1 s / 3 s
    /// depending on market type); the order is re-validated when it expires.
    pub taker_delay: Duration,
}

impl LatencyProfile {
    /// No latency at all: orders execute against the snapshot that triggered
    /// them. The default — reproduces pre-latency backtests exactly.
    pub fn zero() -> Self {
        Self::observer(Duration::ZERO, Duration::ZERO)
    }

    /// A well-placed bot: AWS eu-west-1 (Dublin), one region hop from the
    /// CLOB's eu-west-2. ~2 ms one-way network + exchange-internal publish
    /// (~7 ms floor observed by Telonex's collectors).
    pub fn dublin() -> Self {
        Self::observer(Duration::from_millis(9), Duration::from_millis(4))
    }

    /// A US East Coast box: ~65 ms one-way across the Atlantic each
    /// direction.
    pub fn us_east() -> Self {
        Self::observer(Duration::from_millis(70), Duration::from_millis(65))
    }

    /// A European home connection: consumer routing, no colocation.
    pub fn retail_eu() -> Self {
        Self::observer(Duration::from_millis(30), Duration::from_millis(20))
    }

    fn observer(feed: Duration, transit: Duration) -> Self {
        Self {
            feed,
            transit,
            taker_delay: Duration::ZERO,
        }
    }

    /// Attach the exchange's hold for the market being traded (from
    /// [`taker_delay`] when the category is known).
    pub fn with_taker_delay(mut self, taker_delay: Duration) -> Self {
        self.taker_delay = taker_delay;
        self
    }

    /// The total decision-to-execution offset in engine clock units (µs) —
    /// the only number v1 execution needs.
    pub fn execution_delay_us(&self) -> i64 {
        (self.feed + self.transit + self.taker_delay).as_micros() as i64
    }

    /// Time for an order merely to *reach* the book — feed plus transit, with
    /// no exchange hold. The taker delay is a brake on orders that **aggress**;
    /// an order that will rest as a maker adds liquidity rather than taking it,
    /// so it is posted as soon as it arrives.
    ///
    /// (A limit order that crosses on arrival does aggress, and strictly should
    /// serve the hold on that portion. Modelling it would mean splitting one
    /// order across two arrival times; we accept the small understatement and
    /// note it here.)
    pub fn placement_delay_us(&self) -> i64 {
        (self.feed + self.transit).as_micros() as i64
    }
}

/// Polymarket's deliberate taker delay by market type: live sports markets
/// hold marketable orders 3 s; crypto & finance up/down markets 250 ms;
/// everything else executes immediately. (A 1 s delay is being piloted on
/// NBA/MLB specifically — finer granularity than `Category` carries; when it
/// matters, per-league values become a config concern, not a code change
/// here.) The historical per-day value is unrecoverable from the exchange,
/// which is why this is modeled config rather than looked-up data.
pub fn taker_delay(category: Category) -> Duration {
    match category {
        Category::Sports => Duration::from_secs(3),
        Category::Crypto | Category::Finance => Duration::from_millis(250),
        Category::Politics
        | Category::Tech
        | Category::Culture
        | Category::Science
        | Category::Geopolitics
        | Category::Other => Duration::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_delay_sums_all_components() {
        let profile = LatencyProfile {
            feed: Duration::from_millis(9),
            transit: Duration::from_millis(4),
            taker_delay: Duration::from_secs(3),
        };
        assert_eq!(profile.execution_delay_us(), 3_013_000);
        assert_eq!(LatencyProfile::zero().execution_delay_us(), 0);
    }

    #[test]
    fn with_taker_delay_composes_onto_presets() {
        let profile = LatencyProfile::dublin().with_taker_delay(taker_delay(Category::Sports));
        assert_eq!(profile.execution_delay_us(), 3_013_000);
        // The observer components are untouched by the exchange's hold.
        assert_eq!(profile.feed, Duration::from_millis(9));
        assert_eq!(profile.transit, Duration::from_millis(4));
    }

    #[test]
    fn taker_delay_maps_categories() {
        assert_eq!(taker_delay(Category::Sports), Duration::from_secs(3));
        assert_eq!(taker_delay(Category::Crypto), Duration::from_millis(250));
        assert_eq!(taker_delay(Category::Finance), Duration::from_millis(250));
        for quiet in [
            Category::Politics,
            Category::Tech,
            Category::Culture,
            Category::Science,
            Category::Other,
        ] {
            assert_eq!(taker_delay(quiet), Duration::ZERO);
        }
    }

    #[test]
    fn presets_are_ordered_by_distance() {
        let (z, d, r, u) = (
            LatencyProfile::zero().execution_delay_us(),
            LatencyProfile::dublin().execution_delay_us(),
            LatencyProfile::retail_eu().execution_delay_us(),
            LatencyProfile::us_east().execution_delay_us(),
        );
        assert!(z < d && d < r && r < u);
    }
}
