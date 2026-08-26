//! A patient accumulator: rest one bid at the touch and let the exchange retire
//! it, instead of cancelling and repricing.
//!
//! This is the strategy shape GTD is actually for. The quoter chases the touch
//! and so must cancel constantly — which is what exposed it to in-flight cancel
//! risk and, before that was accounted for, to stacking duplicate orders. Here
//! the timer does the retiring: the order either fills, or the exchange pulls it
//! and the next snapshot places a fresh one at wherever the market now is.
//!
//! Two consequences worth being explicit about, because they are the trade:
//!
//! - **No cancels at all**, so none of the cancel-race machinery applies. The
//!   strategy needs no set of orders-being-cancelled and no in-flight exposure
//!   accounting, because it holds at most one order by construction.
//! - **Coarse repricing.** GTD cannot expire sooner than
//!   [`TimeInForce::GTD_MIN_LIFE_US`] (120 s), so between placements the quote
//!   is stale by construction. On a fast-moving book that is real adverse
//!   selection — the order sits at a price the market has left. That cost is the
//!   point of running it, not a flaw to tune away.

use crate::domain::marketstate::MarketState;
use crate::domain::order::{OrderRequest, OrderSide, TimeInForce};
use crate::domain::types::Price;
use crate::portfolio::Portfolio;
use crate::strategy::{Action, OpenOrder, Strategy};

/// Micro-ticks in one cent — mirrors the constant in `quoter`; every market in
/// the dev data is on the cent grid (see the note on `OrderRequest`).
const TICK: i64 = Price::TICKS_PER_UNIT / 100;

/// Rests a single GTD bid, replacing it only once the exchange has retired it.
pub struct Accumulator {
    /// Shares per order.
    size: f64,
    /// Ticks below the bid to rest: 0 joins the touch, positive sits behind it.
    depth_ticks: i64,
    /// How long each order should really live, in microseconds. Under the
    /// exchange minimum every order is rejected — surfaced, not clamped.
    life_us: i64,
    /// Stop bidding at this position — the same risk limit the quoter has.
    max_inventory: f64,
}

impl Accumulator {
    pub fn new(size: f64, depth_ticks: i64, life_us: i64, max_inventory: f64) -> Self {
        debug_assert!(size > 0.0 && max_inventory >= size);
        Self {
            size,
            depth_ticks,
            life_us,
            max_inventory,
        }
    }
}

impl Strategy for Accumulator {
    fn on_market_state(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        orders: &[OpenOrder],
    ) -> Vec<Action> {
        self.quote(state, portfolio, orders)
    }

    /// The same decision on a clock appointment. This is what makes the timer a
    /// real cycle rather than a one-way ratchet: the order the exchange just
    /// retired is replaced at that instant, instead of leaving the strategy
    /// out of the market until the book next happens to move — which on a quiet
    /// day is most of the day.
    fn on_time(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        orders: &[OpenOrder],
    ) -> Vec<Action> {
        self.quote(state, portfolio, orders)
    }
}

impl Accumulator {
    fn quote(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        orders: &[OpenOrder],
    ) -> Vec<Action> {
        // One order at a time. `orders` covers both in flight and resting, so
        // this also stops a second placement during the latency window — the
        // hole that let the quoter breach its own inventory limit. Here it is
        // closed by the shape of the strategy rather than by bookkeeping.
        if orders.iter().any(|o| o.side == OrderSide::Buy) {
            return Vec::new();
        }

        let Some(bid) = state.bid() else {
            return Vec::new();
        };
        // Below the grid there is simply no quote to make.
        let Ok(want) = Price::new(bid.price.ticks() - self.depth_ticks * TICK) else {
            return Vec::new();
        };

        // Nothing is working (checked above), so the filled position is the
        // whole exposure — no in-flight term is needed.
        let room = self.max_inventory - portfolio.position(&state.market_id).quantity();
        let quantity = self.size.min(room);
        if quantity <= 0.0 {
            return Vec::new();
        }

        vec![Action::Place(OrderRequest::limit(
            state.market_id.clone(),
            OrderSide::Buy,
            quantity,
            want,
            TimeInForce::gtd_after(state.timestamp, self.life_us),
        ))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fill::Fill;
    use crate::domain::marketstate::Level;
    use crate::domain::types::MarketId;
    use crate::strategy::OrderState;

    fn book(bid: f64, ask: f64) -> MarketState {
        let level = |p: f64| {
            Box::from([Level {
                price: Price::from_probability(p).unwrap(),
                size: 500.0,
            }])
        };
        MarketState::new(MarketId::new("t".to_string()), 0, level(bid), level(ask)).unwrap()
    }

    fn live_bid() -> OpenOrder {
        OpenOrder {
            id: crate::domain::types::OrderId::new(1),
            side: OrderSide::Buy,
            quantity: 100.0,
            limit_price: Some(Price::from_probability(0.35).unwrap()),
            state: OrderState::Resting,
        }
    }

    #[test]
    fn places_a_gtd_that_lives_exactly_as_long_as_asked() {
        let mut s = Accumulator::new(100.0, 0, TimeInForce::GTD_MIN_LIFE_US, 500.0);
        let actions = s.on_market_state(&book(0.35, 0.36), &Portfolio::new(10_000.0), &[]);
        let Some(Action::Place(request)) = actions.first() else {
            panic!("expected one placement, got {actions:?}");
        };
        // Joins the touch, and the stated expiry is buffer + life.
        assert_eq!(request.limit_price(), Price::new(350_000).ok());
        let stated = TimeInForce::GTD_SECURITY_BUFFER_US + TimeInForce::GTD_MIN_LIFE_US;
        assert_eq!(request.tif(), TimeInForce::Gtd { expires_at: stated });
        // And it clears the exchange floor by exactly nothing to spare.
        assert!(request.tif().horizon_ok(0));
        assert_eq!(
            request.tif().effective_expiry(),
            Some(TimeInForce::GTD_MIN_LIFE_US)
        );
    }

    #[test]
    fn holds_off_while_an_order_is_still_live() {
        let mut s = Accumulator::new(100.0, 0, TimeInForce::GTD_MIN_LIFE_US, 500.0);
        let actions =
            s.on_market_state(&book(0.35, 0.36), &Portfolio::new(10_000.0), &[live_bid()]);
        assert!(actions.is_empty(), "one order at a time");
    }

    #[test]
    fn stops_bidding_once_the_inventory_limit_is_reached() {
        let market = MarketId::new("t".to_string());
        let mut s = Accumulator::new(100.0, 0, TimeInForce::GTD_MIN_LIFE_US, 100.0);
        let mut portfolio = Portfolio::new(10_000.0);
        // Empty book position: it bids.
        assert_eq!(
            s.on_market_state(&book(0.35, 0.36), &portfolio, &[]).len(),
            1,
            "with room, it places"
        );
        // Fill it up to the limit; now there is no room left.
        portfolio.apply(
            &market,
            &Fill::new(
                OrderSide::Buy,
                Price::from_probability(0.35).unwrap(),
                100.0,
                0,
            ),
        );
        assert!(
            s.on_market_state(&book(0.35, 0.36), &portfolio, &[])
                .is_empty(),
            "at the limit, it stops"
        );
    }
}
