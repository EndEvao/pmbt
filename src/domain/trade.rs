use crate::domain::order::OrderSide;
use crate::domain::types::{MarketId, Price};

/// One executed trade off the tape — a *print*, not a book state. The optimistic
/// resting-order matcher (a later phase) fills a resting order when a trade
/// prints at its price.
///
/// `side` is the **aggressor's** side: a `Sell` lifted the bid, a `Buy` lifted
/// the ask (verified against book deltas in the feed tests before anything
/// relies on it). `origin_asset_id` records which token's book the trade
/// originated on — carried for future complement-aware attribution, but unused
/// in the single-book optimistic v1, where any trade at a resting order's price
/// is treated as a fill trigger.
///
/// Fields are `pub(crate)` (read directly by the in-crate feed/matcher, like
/// [`Fill`](crate::domain::fill::Fill)); not `Copy` because `MarketId` owns a
/// `String`.
#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub(crate) timestamp: i64,
    pub(crate) price: Price,
    pub(crate) size: f64,
    pub(crate) side: OrderSide,
    pub(crate) origin_asset_id: MarketId,
}

impl Trade {
    pub(crate) fn new(
        timestamp: i64,
        price: Price,
        size: f64,
        side: OrderSide,
        origin_asset_id: MarketId,
    ) -> Self {
        debug_assert!(size.is_finite() && size > 0.0);
        Self {
            timestamp,
            price,
            size,
            side,
            origin_asset_id,
        }
    }
}
