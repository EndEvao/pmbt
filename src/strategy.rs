use crate::domain::marketstate::MarketState;
use crate::domain::order::{OrderRequest, OrderSide};
use crate::domain::types::{OrderId, Price};
use crate::portfolio::Portfolio;

/// What a strategy asks the engine to do on one snapshot.
///
/// Returned as an **ordered** list, so a cancel followed by a place is how a
/// strategy re-prices a quote — Polymarket has no atomic replace, so modelling
/// one would be less faithful than the two steps it really takes.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Submit a new order. Intent only — it still faces every gate, and the
    /// engine assigns its [`OrderId`].
    Place(OrderRequest),
    /// Withdraw a live order of ours, whether it is still in flight or already
    /// resting. Cancelling an id that has since filled or expired is a normal
    /// race (the strategy acted on the previous tick's view) and is ignored.
    Cancel(OrderId),
    /// Ask to be called back at this exchange timestamp, even if the market has
    /// said nothing by then.
    ///
    /// Without this a strategy can only ever act when a book arrives, so on a
    /// quiet market anything it wanted to do on a schedule simply never
    /// happens. A request in the past, or one already passed by the time it
    /// would fire, is dropped rather than firing late.
    WakeAt(i64),
}

/// Where one of our live orders is: still travelling to the exchange, or on the
/// book as a maker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    /// Accepted, but still inside its latency window — not yet on the book, so
    /// it cannot fill. Always empty under zero latency.
    Pending,
    /// Live on the book, fillable by incoming trades.
    Resting,
}

/// A read-only look at one of our own live orders. The strategy sees these to
/// avoid re-placing what it already sent and to decide what to cancel.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenOrder {
    pub id: OrderId,
    pub side: OrderSide,
    /// Size still unfilled.
    pub quantity: f64,
    /// `None` for a marketable order in flight.
    pub limit_price: Option<Price>,
    pub state: OrderState,
}

/// A trading strategy: reacts to market snapshots, one call per snapshot, in
/// time order.
///
/// The one method does both jobs — fold the snapshot into whatever internal
/// state the model keeps (hence `&mut self`), then maybe act. Implementations
/// hold only their own config and derived memory; the world (market, ledger,
/// open orders) arrives borrowed and must not be copied out. The portfolio is
/// read-only here by construction: mutating it needs `&mut`, which the engine
/// keeps for itself.
///
/// Returned actions are intent, not done deals — the engine may reject a place
/// (insufficient funds, overselling), so never assume it filled.
///
/// `orders` lists our live orders, each tagged [`OrderState`]. A strategy
/// learns the [`OrderId`] of an order it placed by finding it here on a *later*
/// snapshot, exactly as a real exchange acknowledges asynchronously — an order
/// is not addressable until it exists. Unlike the market snapshot, this view is
/// never delayed by perception: order state arrives on the private channel, not
/// the public market feed.
pub trait Strategy {
    fn on_market_state(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        orders: &[OpenOrder],
    ) -> Vec<Action>;

    /// Time reached `state.timestamp` without the market saying anything new.
    ///
    /// Fires on every clock appointment — an order of ours expiring, a cancel
    /// landing — and on any [`Action::WakeAt`] the strategy asked for. The
    /// default does nothing, so a purely reactive strategy is unaffected.
    ///
    /// `state` is the **last book this strategy has been shown**, restamped to
    /// now. That is not a lesser thing than the snapshot `on_market_state`
    /// gets: between updates it is exactly what a live trader has, since a book
    /// that has not changed is still the book. Under perception delay it is the
    /// last *delivered* book, never the newer one being withheld.
    ///
    /// This is separate from `on_market_state` because the two mean different
    /// things: that one says "the market moved", this one says "time passed".
    /// Merging them would have every strategy re-react to a snapshot it had
    /// already handled.
    fn on_time(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        orders: &[OpenOrder],
    ) -> Vec<Action> {
        let _ = (state, portfolio, orders);
        Vec::new()
    }
}
