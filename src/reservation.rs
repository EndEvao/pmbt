use std::collections::HashMap;

use crate::domain::types::MarketId;

/// Resources a resting order has committed but not yet spent — a *hold*, kept
/// **parallel to** the [`Portfolio`](crate::portfolio::Portfolio) money ledger
/// rather than inside it. A resting buy holds cash (global, since cash is
/// global); a resting sell holds shares of one market.
///
/// Like the portfolio, this ledger **never rejects**: `reserve_*`/`release_*`
/// are unconditional. The engine gates *before* reserving, comparing a **free**
/// balance against what the order needs — exactly as `can_afford` guards a
/// marketable buy:
///
/// - resting buy: `portfolio.cash() - reserved_cash() >= walked_cost`
/// - resting sell: `portfolio.position(m).quantity() - reserved_shares(m) >= qty`
///
/// Reserving never touches `cash`: the money is still the account's until the
/// order actually fills, so `final_cash = initial + total_pnl` still holds.
///
/// Amounts are scalar totals; the engine's open-orders book remembers how much
/// each order reserved and passes it back to `release_*` on cancel/fill.
#[derive(Debug, Clone, Default)]
pub struct ReservationLedger {
    reserved_cash: f64,
    reserved_shares: HashMap<MarketId, f64>,
}

impl ReservationLedger {
    /// A ledger holding nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Total cash held by resting buys.
    pub fn reserved_cash(&self) -> f64 {
        self.reserved_cash
    }

    /// Shares of `market_id` held by resting sells; `0.0` if none — a query
    /// never inserts.
    pub fn reserved_shares(&self, market_id: &MarketId) -> f64 {
        self.reserved_shares.get(market_id).copied().unwrap_or(0.0)
    }

    /// Hold `amount` of cash for a resting buy. Never rejects — the engine
    /// checks the free balance first.
    pub fn reserve_cash(&mut self, amount: f64) {
        debug_assert!(amount.is_finite() && amount > 0.0);
        self.reserved_cash += amount;
    }

    /// Release cash a resting buy no longer holds (it filled or was cancelled).
    /// Clamped at zero so float residue can't leak a negative hold — which would
    /// *inflate* the free balance and let a later order over-commit.
    pub fn release_cash(&mut self, amount: f64) {
        debug_assert!(amount.is_finite() && amount > 0.0);
        debug_assert!(
            self.reserved_cash + 1e-6 >= amount,
            "released more cash than reserved"
        );
        self.reserved_cash = (self.reserved_cash - amount).max(0.0);
    }

    /// Hold `qty` shares of `market_id` for a resting sell.
    pub fn reserve_shares(&mut self, market_id: &MarketId, qty: f64) {
        debug_assert!(qty.is_finite() && qty > 0.0);
        *self.reserved_shares.entry(market_id.clone()).or_insert(0.0) += qty;
    }

    /// Release shares a resting sell no longer holds. Clamped at zero, as with
    /// [`release_cash`](Self::release_cash).
    pub fn release_shares(&mut self, market_id: &MarketId, qty: f64) {
        debug_assert!(qty.is_finite() && qty > 0.0);
        let held = self.reserved_shares.entry(market_id.clone()).or_insert(0.0);
        debug_assert!(*held + 1e-6 >= qty, "released more shares than reserved");
        *held = (*held - qty).max(0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mkt(id: &str) -> MarketId {
        MarketId::new(id.to_string())
    }

    #[test]
    fn reserve_then_release_cash_returns_to_zero() {
        let mut r = ReservationLedger::new();
        r.reserve_cash(60.0);
        assert_eq!(r.reserved_cash(), 60.0);
        r.release_cash(60.0);
        assert_eq!(r.reserved_cash(), 0.0);
    }

    #[test]
    fn cash_reservations_accumulate_and_release_partially() {
        let mut r = ReservationLedger::new();
        r.reserve_cash(60.0);
        r.reserve_cash(40.0);
        assert_eq!(r.reserved_cash(), 100.0);
        r.release_cash(60.0);
        assert_eq!(r.reserved_cash(), 40.0);
    }

    #[test]
    fn share_reservations_are_per_market() {
        let (yes, no) = (mkt("yes"), mkt("no"));
        let mut r = ReservationLedger::new();
        r.reserve_shares(&yes, 100.0);
        r.reserve_shares(&no, 50.0);
        assert_eq!(r.reserved_shares(&yes), 100.0);
        assert_eq!(r.reserved_shares(&no), 50.0);

        r.release_shares(&yes, 100.0);
        assert_eq!(r.reserved_shares(&yes), 0.0);
        assert_eq!(r.reserved_shares(&no), 50.0); // untouched
    }

    #[test]
    fn absent_market_reserves_nothing() {
        let r = ReservationLedger::new();
        assert_eq!(r.reserved_shares(&mkt("never")), 0.0);
    }

    #[test]
    #[should_panic(expected = "released more cash than reserved")]
    fn over_releasing_cash_trips_the_assert() {
        let mut r = ReservationLedger::new();
        r.reserve_cash(10.0);
        r.release_cash(20.0);
    }
}
