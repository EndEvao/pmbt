use crate::domain::fill::Fill;
use crate::domain::marketstate::MarketState;

/// The book as this snapshot's own executions have left it.
///
/// Several orders can come due at one timestamp — routine under latency, where
/// a batch lands together, and again because `execute_due` runs twice per
/// snapshot (before and after the strategy decides). Walking the pristine
/// snapshot each time would let every one of them spend the *same* depth, so a
/// strategy could buy a 500-share level ten times over at the touch. The
/// residual is the fix: each execution deducts what it took, and the next one
/// walks what is left.
///
/// Scope is exactly one snapshot. See [`MarketState::consume`] for why this is
/// self-consistency rather than market impact.
pub struct ResidualBook<'a> {
    pristine: &'a MarketState,
    /// `None` until something is actually consumed — the overwhelming majority
    /// of snapshots, which then cost nothing but the borrow.
    depleted: Option<MarketState>,
}

impl<'a> ResidualBook<'a> {
    pub fn new(pristine: &'a MarketState) -> Self {
        Self {
            pristine,
            depleted: None,
        }
    }

    /// The untouched snapshot. Its timestamp is the clock — consuming depth
    /// leaves prices and time alone, so this is what the engine reads for "now".
    ///
    /// Returns the `'a` borrow, not a `&self` one: reading "now" must not lock
    /// the residual, or the caller could not go on to `consume` into it.
    pub fn pristine(&self) -> &'a MarketState {
        self.pristine
    }

    /// What an order executing now actually trades against.
    pub fn current(&self) -> &MarketState {
        self.depleted.as_ref().unwrap_or(self.pristine)
    }

    pub fn consume(&mut self, fills: &[Fill]) {
        if !fills.is_empty() {
            self.depleted = Some(self.current().consume(fills));
        }
    }
}
