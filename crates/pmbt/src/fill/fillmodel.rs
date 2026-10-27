use crate::domain::fill::Fill;
use crate::domain::marketstate::MarketState;
use crate::domain::order::{Order, OrderSide};

/// Simulates how an order would execute against a market snapshot.
///
/// This is the swappable seam: different implementations model different
/// amounts of realism (depth limits, fees, slippage, latency). The engine
/// holds *some* `FillModel` and never knows which concrete one.
pub trait FillModel {
    /// Simulate an order against the snapshot: what would match **right now**,
    /// one fill per price level consumed, at that level's exact tick price.
    ///
    /// A limit order's `limit_price` bounds the walk — a buy never pays above
    /// it, a sell never accepts below it — so the result may be **partial**, or
    /// empty when nothing crosses. Reporting the partial is the point: the
    /// engine decides what happens to the unmatched remainder, because that is
    /// time-in-force *policy* (FOK kills, FAK drops, GTC/GTD rest), not matching.
    ///
    /// Fill models are portfolio-blind: they model the exchange's matching,
    /// not the trader's wallet. Affordability is the engine's check, made
    /// against the returned fills' actual cost.
    fn fill(&self, order: &Order, market: &MarketState) -> Box<[Fill]>;
}

/// The simplest model: fills the full requested size at the touch, with no
/// depth limits, no fees, no slippage. Buys lift the ask; sells hit the bid.
/// Deliberately unrealistic — the baseline every realistic model is compared
/// against. Stateless — hence a unit struct.
pub struct NaiveFillModel;

impl FillModel for NaiveFillModel {
    fn fill(&self, order: &Order, market: &MarketState) -> Box<[Fill]> {
        let side = order.side();
        // A marketable order is gated on its side being present, but a limit
        // order may rest against an empty side — so an absent touch is simply
        // "nothing matches", not a panic.
        let Some(price) = (match side {
            OrderSide::Buy => market.ask().map(|level| level.price),
            OrderSide::Sell => market.bid().map(|level| level.price),
        }) else {
            return Box::from([]);
        };
        // Beyond the limit, nothing crosses — the order rests instead.
        if !order.accepts_price(price) {
            return Box::from([]);
        }
        Box::from([Fill::new(side, price, order.quantity(), market.timestamp)])
    }
}
