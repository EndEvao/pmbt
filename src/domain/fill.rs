use crate::domain::order::OrderSide;
use crate::domain::types::Price;

/// Which side of the match a fill was: did it *remove* resting liquidity
/// (taker, pays the fee) or *add* it (maker, pays nothing)? Set by which engine
/// path produced the fill, not by a heuristic — so one order can yield both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liquidity {
    Maker,
    Taker,
}

/// The outcome of an order executing against the book — *what actually filled*.
/// Produced by a `FillModel`, consumed by the portfolio. Pure value, no behavior.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Fill {
    pub(crate) side: OrderSide,
    pub(crate) price: Price,
    pub(crate) quantity: f64,
    /// Timestamp of the snapshot this executed against (execution time, *not*
    /// the order's submission time — they differ under latency).
    pub(crate) timestamp: i64,
    pub(crate) liquidity: Liquidity,
}

impl Fill {
    /// A **taker** fill — the order crossed the spread and removed liquidity.
    /// The fill models produce these; a resting order hit by someone else's
    /// aggressing trade uses [`Fill::maker`] instead.
    pub(crate) fn new(side: OrderSide, price: Price, quantity: f64, timestamp: i64) -> Self {
        Self {
            side,
            price,
            quantity,
            timestamp,
            liquidity: Liquidity::Taker,
        }
    }

    /// A **maker** fill — a resting order that was hit by someone else's
    /// aggressing trade. `price` is the order's own resting price (the maker
    /// keeps their posted price; the price improvement goes to the taker), and
    /// `timestamp` is the trade that filled it.
    pub(crate) fn maker(side: OrderSide, price: Price, quantity: f64, timestamp: i64) -> Self {
        Self {
            side,
            price,
            quantity,
            timestamp,
            liquidity: Liquidity::Maker,
        }
    }

    /// Signed size in shares: `+` for a buy, `-` for a sell. Lets a `Position`
    /// fold a fill in without re-deriving the direction.
    pub(crate) fn signed_quantity(&self) -> f64 {
        match self.side {
            OrderSide::Buy => self.quantity,
            OrderSide::Sell => -self.quantity,
        }
    }
}
