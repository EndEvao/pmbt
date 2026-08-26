use crate::domain::fill::Fill;

/// A net holding in one market: signed share `quantity` plus the `average_price`
/// (dollars / probability, `[0,1]`) of the *currently held* shares. The market it
/// belongs to is the key it's stored under in the portfolio — a position doesn't
/// carry its own id. Flat is `quantity == 0.0`, where `average_price` is
/// meaningless and parked at 0.0.
///
/// Long-only for now: `quantity` never goes negative.
///
/// Quantity and cost basis are `f64`, unlike [`Price`](crate::domain::types::Price)
/// which is fixed-point. Deliberate and standard for a backtest, but a known
/// edge for the live path: a process running for days folds millions of fills,
/// where `f64` cash can accumulate rounding the fixed-point price model was
/// built to avoid. Revisit if live P&L drifts from an independent tally.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Position {
    quantity: f64,
    average_price: f64,
}

impl Position {
    /// A flat position. Pre-created so a query always returns something, even if
    /// no trades happen during the backtest.
    pub fn new() -> Self {
        Self {
            quantity: 0.0,
            average_price: 0.0,
        }
    }

    pub fn is_flat(&self) -> bool {
        self.quantity == 0.0
    }

    pub fn quantity(&self) -> f64 {
        self.quantity
    }

    pub fn average_price(&self) -> f64 {
        self.average_price
    }

    /// Fold a fill into this position and return the **realized P&L** in dollars
    /// (0.0 for a buy).
    ///
    /// - **Buy:** add to (or open) the long and re-average the cost basis,
    ///   weighted by size. No P&L is realized.
    /// - **Sell:** reduce the long; the average cost is *unchanged*, and P&L is
    ///   realized on the shares closed: `(sell_price - average) * sold`.
    ///
    /// Overselling (which would go short) is rejected — the engine's
    /// `can_execute` is responsible for never letting such a fill reach here.
    pub fn apply(&mut self, fill: &Fill) -> f64 {
        let fill_qty = fill.signed_quantity();
        let fill_price = fill.price.as_f64();

        if fill_qty >= 0.0 {
            // BUY: weighted re-average. `new_quantity > 0` (order quantities are
            // validated positive), so the division is safe.
            let new_quantity = self.quantity + fill_qty;
            self.average_price =
                (self.quantity * self.average_price + fill_qty * fill_price) / new_quantity;
            self.quantity = new_quantity;
            0.0
        } else {
            // SELL: average stays put; realize P&L on the closed shares.
            let sell_qty = -fill_qty;
            debug_assert!(
                sell_qty <= self.quantity,
                "oversell: long-only position can't go short; engine must reject via can_execute"
            );
            let realized = (fill_price - self.average_price) * sell_qty;
            self.quantity -= sell_qty;
            if self.quantity == 0.0 {
                self.average_price = 0.0; // back to flat
            }
            realized
        }
    }

    /// Redeem the entire holding at the resolution price and return the
    /// realized P&L: `(redemption − average) × quantity`. Flattens the
    /// position. Settlement is not a trade — no `Fill` is involved, on
    /// purpose: redemption bypasses the order pipeline entirely.
    pub(crate) fn settle(&mut self, redemption: f64) -> f64 {
        let realized = (redemption - self.average_price) * self.quantity;
        self.quantity = 0.0;
        self.average_price = 0.0;
        realized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long(quantity: f64, average_price: f64) -> Position {
        Position {
            quantity,
            average_price,
        }
    }

    #[test]
    fn settle_realizes_redemption_minus_cost_and_flattens() {
        let mut winner = long(100.0, 0.085);
        assert!((winner.settle(1.0) - 91.5).abs() < 1e-9);
        assert!(winner.is_flat());
        assert_eq!(winner.average_price(), 0.0);

        let mut loser = long(100.0, 0.085);
        assert!((loser.settle(0.0) - (-8.5)).abs() < 1e-9);
        assert!(loser.is_flat());
    }

    #[test]
    fn settling_flat_is_a_no_op() {
        let mut flat = Position::new();
        assert_eq!(flat.settle(1.0), 0.0);
        assert!(flat.is_flat());
    }
}
