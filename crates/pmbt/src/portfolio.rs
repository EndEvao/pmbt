use std::collections::HashMap;

use crate::domain::fill::Fill;
use crate::domain::position::Position;
use crate::domain::types::{MarketId, Price};

/// The ledger: cash plus a net position per market. Positions are created
/// lazily — a market gets a (flat) `Position` the first time it's traded.
#[derive(Debug, Clone)]
pub struct Portfolio {
    initial_capital: f64,
    cash: f64,
    open_positions: HashMap<MarketId, Position>,
}

impl Portfolio {
    /// A fresh portfolio: all cash, no positions. The map starts empty;
    /// positions appear on first trade.
    pub fn new(initial_capital: f64) -> Self {
        Self {
            initial_capital,
            cash: initial_capital,
            open_positions: HashMap::new(),
        }
    }

    pub fn initial_capital(&self) -> f64 {
        self.initial_capital
    }

    pub fn cash(&self) -> f64 {
        self.cash
    }

    /// Read-only view of a market's position. Returns a **flat** position if the
    /// market has never been traded, so callers never handle "absent" — and it
    /// does *not* insert (a query must not mutate the ledger).
    pub fn position(&self, market_id: &MarketId) -> Position {
        self.open_positions
            .get(market_id)
            .copied()
            .unwrap_or_else(Position::new)
    }

    /// Every market currently holding shares, in no particular order. Filters
    /// out flat entries (a fully closed position stays in the map), so callers
    /// only ever see real holdings.
    pub fn open_positions(&self) -> impl Iterator<Item = (&MarketId, &Position)> {
        self.open_positions.iter().filter(|(_, p)| !p.is_flat())
    }

    /// The live position for a market, created flat on first touch. The only
    /// place a position enters the map.
    fn position_entry(&mut self, market_id: MarketId) -> &mut Position {
        self.open_positions.entry(market_id).or_default()
    }

    /// Apply a fill: move cash by the transaction value, fold the fill into the
    /// market's position, and return the realized P&L it produced.
    pub fn apply(&mut self, market_id: &MarketId, fill: &Fill) -> f64 {
        // Cash flow = signed shares × price: a buy (+qty) spends cash, a sell
        // (−qty) brings it in.
        self.cash -= fill.signed_quantity() * fill.price.as_f64();
        self.position_entry(market_id.clone()).apply(fill)
    }

    /// Deduct a taker fee from cash. A real money movement (exchange rake, not
    /// a trade — it touches no position), and like every ledger action it
    /// never rejects: the engine's `can_afford` is what guards against a buy
    /// it can't cover. The fee amount is the engine's to compute.
    pub fn charge_fee(&mut self, fee: f64) {
        self.cash -= fee;
    }

    /// Redeem a market's open position at the resolution price (the traded
    /// token's terminal value): cash receives `quantity × redemption`, the
    /// position flattens, and the realized settlement P&L is returned. A pure
    /// ledger action — settlement is not a trade, so it never touches the
    /// order pipeline, and like every ledger action it never rejects.
    pub fn settle(&mut self, market_id: &MarketId, redemption: Price) -> f64 {
        let position = self.position_entry(market_id.clone());
        let proceeds = position.quantity() * redemption.as_f64();
        let realized = position.settle(redemption.as_f64());
        self.cash += proceeds;
        realized
    }
}
