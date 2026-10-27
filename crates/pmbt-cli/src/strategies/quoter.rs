//! A two-sided quoting strategy — the worked example of maker trading.
//!
//! Posts a bid, and once it has inventory, offers that inventory back out,
//! cancelling and re-posting as the touch moves. The cycle is
//! **bid → filled → offer → flat → repeat**, and it is shaped by one hard
//! constraint of this engine: it is **long-only**. A real market maker quotes
//! both sides from flat and goes short when lifted; here the ask side only
//! exists once the bid side has actually filled, so the strategy is an
//! inventory cycle rather than a symmetric quote.
//!
//! # Where it quotes, and why that matters
//!
//! `offset_ticks` places both quotes relative to the touch:
//!
//! | offset | bid | meaning |
//! |---|---|---|
//! | `0` | at the best bid | join the queue at the touch |
//! | `-1` | one tick better | step *inside* the spread — alone at a new level |
//! | `1` | one tick worse | sit behind the touch |
//!
//! This is the knob that decides how much the optimistic fill model is
//! flattering us. Joining a queue assumes we are at its front when we are
//! really behind whatever already rests there ([`pmbt::resting`] explains the
//! consequence). **Improving** is different: a price level nobody else occupies
//! makes front-of-queue nearly true, so those fills are the defensible ones.
//!
//! Improving needs room. Measured on the dev data, the pre-game days are 100%
//! one-tick-wide — a `-1` offset can't be quoted at all there — while game day
//! is 2+ ticks in 13.6% of snapshots. So the same strategy is near-idle on a
//! quiet market and genuinely active on a moving one, which is the honest
//! result, not a bug.
//!
//! # What it refuses to do
//!
//! It quotes only on a sane two-sided book (both sides present, bid < ask).
//! Real captures contain locked, crossed and one-sided books — mostly transient
//! feed artifacts that [`MarketState`] deliberately passes through — and a
//! quote derived from a crossed book is nonsense. Existing quotes are left
//! resting through those snapshots rather than churned.

use std::collections::HashSet;

use pmbt::domain::marketstate::MarketState;
use pmbt::domain::order::{OrderRequest, OrderSide, TimeInForce};
use pmbt::domain::types::{OrderId, Price};
use pmbt::portfolio::Portfolio;
use pmbt::strategy::{Action, OpenOrder, Strategy};

/// Micro-ticks in one cent. Polymarket's tick is per-market (0.01 down to
/// 0.0001); we don't carry that metadata yet, and every market in the dev data
/// is on the cent grid — see the note on `OrderRequest`.
const TICK: i64 = Price::TICKS_PER_UNIT / 100;

/// Quotes both sides around the touch, cycling inventory.
pub struct Quoter {
    /// Shares per quote.
    size: f64,
    /// Ticks away from the touch: 0 joins, negative improves, positive sits back.
    offset_ticks: i64,
    /// Stop bidding once the position reaches this — the risk limit that stops
    /// a one-way market from turning the quoter into a buy-and-hold.
    max_inventory: f64,
    /// Orders we have already asked to cancel. A cancel takes time to reach the
    /// exchange, so the order keeps appearing in the view until it lands —
    /// without this the quoter would see its own stale quote, cancel it again
    /// and place another replacement *every snapshot*, stacking up duplicate
    /// bids that all fill. Pruned each call to ids still live.
    cancelling: HashSet<OrderId>,
}

impl Quoter {
    pub fn new(size: f64, offset_ticks: i64, max_inventory: f64) -> Self {
        debug_assert!(size > 0.0 && max_inventory >= size);
        Self {
            size,
            offset_ticks,
            max_inventory,
            cancelling: HashSet::new(),
        }
    }

    /// Our current quote on `side`: the first order we have *not* already asked
    /// to cancel. Orders with a cancel in flight are deliberately invisible
    /// here — they are being retired, and treating one as the live quote is
    /// what makes the quoter re-cancel it forever.
    fn live<'a>(
        orders: &'a [OpenOrder],
        cancelling: &HashSet<OrderId>,
        side: OrderSide,
    ) -> Option<&'a OpenOrder> {
        orders
            .iter()
            .find(|o| o.side == side && !cancelling.contains(&o.id))
    }

    /// Size still working on `side` that we intend to keep — orders being
    /// retired (`cancelling`, plus anything cancelled in this same batch) are
    /// excluded.
    ///
    /// This is the quantity that must count against the inventory limit:
    /// sizing on the *filled* position alone ignores the quotes already
    /// travelling toward the exchange, and under latency they all fill. Note
    /// the honest gap — an order whose cancel has not landed can still fill, so
    /// excluding it understates worst-case exposure. Counting it would be
    /// stricter but would stall quoting for a full round trip after every
    /// reprice, which is the larger distortion.
    fn working(orders: &[OpenOrder], cancelling: &HashSet<OrderId>, side: OrderSide) -> f64 {
        orders
            .iter()
            .filter(|o| o.side == side && !cancelling.contains(&o.id))
            .map(|o| o.quantity)
            .sum()
    }
}

impl Strategy for Quoter {
    fn on_market_state(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        orders: &[OpenOrder],
    ) -> Vec<Action> {
        // Only a sane two-sided book yields a meaningful quote.
        let (Some(bid), Some(ask)) = (state.bid(), state.ask()) else {
            return Vec::new();
        };
        if bid.price() >= ask.price() {
            return Vec::new();
        }

        // Step both quotes off the touch. Out-of-range prices (near 0 or 1)
        // simply mean there is no quote to make.
        let (Ok(want_bid), Ok(want_ask)) = (
            Price::new(bid.price().ticks() - self.offset_ticks * TICK),
            Price::new(ask.price().ticks() + self.offset_ticks * TICK),
        ) else {
            return Vec::new();
        };
        // Improving by more than the spread would cross our own quotes — that
        // is the offset asking for room the market isn't giving.
        if want_bid >= want_ask {
            return Vec::new();
        }

        // Forget cancels that have landed: the order is gone from the view.
        self.cancelling
            .retain(|id| orders.iter().any(|o| o.id == *id));

        let position = portfolio.position(state.market_id()).quantity();
        let live_bid = Self::live(orders, &self.cancelling, OrderSide::Buy);
        let live_ask = Self::live(orders, &self.cancelling, OrderSide::Sell);
        let priced_at =
            |o: Option<&OpenOrder>, p: Price| o.is_some_and(|o| o.limit_price == Some(p));

        let mut actions = Vec::new();

        // Cancels first: the list is ordered, so a stale quote releases its
        // hold before the replacement asks for one. This is a reprice.
        let mut retiring = Vec::new();
        if let Some(stale) = live_bid.filter(|_| !priced_at(live_bid, want_bid)) {
            actions.push(Action::Cancel(stale.id));
            retiring.push(stale.id);
        }
        if let Some(stale) = live_ask.filter(|_| !priced_at(live_ask, want_ask)) {
            actions.push(Action::Cancel(stale.id));
            retiring.push(stale.id);
        }
        self.cancelling.extend(retiring);

        // Bid: top up toward the inventory limit, counting what is already
        // working so in-flight quotes cannot breach it.
        if !priced_at(live_bid, want_bid) {
            let room = self.max_inventory
                - position
                - Self::working(orders, &self.cancelling, OrderSide::Buy);
            let quantity = self.size.min(room);
            if quantity > 0.0 {
                actions.push(Action::Place(OrderRequest::limit(
                    state.market_id().clone(),
                    OrderSide::Buy,
                    quantity,
                    want_bid,
                    TimeInForce::Gtc,
                )));
            }
        }

        // Ask: offer out what we actually hold — long-only, so an empty
        // position means no offer. Shares already promised by a working ask are
        // not free to promise again.
        if !priced_at(live_ask, want_ask) {
            let free = position - Self::working(orders, &self.cancelling, OrderSide::Sell);
            let quantity = self.size.min(free);
            if quantity > 0.0 {
                actions.push(Action::Place(OrderRequest::limit(
                    state.market_id().clone(),
                    OrderSide::Sell,
                    quantity,
                    want_ask,
                    TimeInForce::Gtc,
                )));
            }
        }

        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::test_helpers::buy;
    use pmbt::domain::marketstate::Level;
    use pmbt::domain::types::MarketId;
    use pmbt::strategy::OrderState;

    fn price(p: f64) -> Price {
        Price::from_probability(p).unwrap()
    }

    fn book(bid: f64, ask: f64) -> MarketState {
        MarketState::new(
            MarketId::new("token".to_string()),
            1,
            Box::from([Level::new(price(bid), 1_000.0).unwrap()]),
            Box::from([Level::new(price(ask), 1_000.0).unwrap()]),
        )
        .unwrap()
    }

    fn one_sided(bid: Option<f64>) -> MarketState {
        let level = |p: f64| Box::from([Level::new(price(p), 10.0).unwrap()]);
        MarketState::new(
            MarketId::new("token".to_string()),
            1,
            bid.map_or(Box::from([]), level),
            Box::from([]),
        )
        .unwrap()
    }

    fn placed(actions: &[Action]) -> Vec<(OrderSide, f64, f64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Place(r) => {
                    Some((r.side(), r.limit_price().unwrap().as_f64(), r.quantity()))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn joining_quotes_at_the_touch() {
        let mut q = Quoter::new(100.0, 0, 500.0);
        let actions = q.on_market_state(&book(0.35, 0.36), &Portfolio::new(10_000.0), &[]);
        // Flat, so only a bid — there is nothing to offer.
        assert_eq!(placed(&actions), vec![(OrderSide::Buy, 0.35, 100.0)]);
    }

    #[test]
    fn improving_steps_inside_a_wide_spread() {
        let mut q = Quoter::new(100.0, -1, 500.0);
        let actions = q.on_market_state(&book(0.30, 0.36), &Portfolio::new(10_000.0), &[]);
        assert_eq!(placed(&actions), vec![(OrderSide::Buy, 0.31, 100.0)]);
    }

    #[test]
    fn improving_is_impossible_on_a_one_tick_spread() {
        // 0.35/0.36 improved by a tick each way inverts — no quote to make.
        let mut q = Quoter::new(100.0, -1, 500.0);
        let actions = q.on_market_state(&book(0.35, 0.36), &Portfolio::new(10_000.0), &[]);
        assert!(actions.is_empty());
    }

    #[test]
    fn offers_inventory_once_it_holds_some() {
        let mut q = Quoter::new(100.0, 0, 500.0);
        let mut portfolio = Portfolio::new(10_000.0);
        buy(&mut portfolio, "token", price(0.35), 60.0);
        let actions = q.on_market_state(&book(0.35, 0.36), &portfolio, &[]);
        // Bids for more, and offers the 60 it actually has (not the full size).
        assert_eq!(
            placed(&actions),
            vec![(OrderSide::Buy, 0.35, 100.0), (OrderSide::Sell, 0.36, 60.0)]
        );
    }

    #[test]
    fn a_stale_quote_is_cancelled_before_the_replacement() {
        let mut q = Quoter::new(100.0, 0, 500.0);
        let stale = OpenOrder {
            id: pmbt::domain::types::OrderId::new(7),
            side: OrderSide::Buy,
            quantity: 100.0,
            limit_price: Some(price(0.30)),
            state: OrderState::Resting,
        };
        let actions = q.on_market_state(&book(0.35, 0.36), &Portfolio::new(10_000.0), &[stale]);
        // Cancel must come first so its hold is released for the new order.
        assert!(matches!(actions[0], Action::Cancel(_)));
        assert_eq!(placed(&actions), vec![(OrderSide::Buy, 0.35, 100.0)]);
    }

    #[test]
    fn a_correctly_priced_quote_is_left_alone() {
        let mut q = Quoter::new(100.0, 0, 500.0);
        let good = OpenOrder {
            id: pmbt::domain::types::OrderId::new(7),
            side: OrderSide::Buy,
            quantity: 100.0,
            limit_price: Some(price(0.35)),
            state: OrderState::Resting,
        };
        let actions = q.on_market_state(&book(0.35, 0.36), &Portfolio::new(10_000.0), &[good]);
        assert!(
            actions.is_empty(),
            "no churn when the quote is already right"
        );
    }

    #[test]
    fn odd_books_are_skipped() {
        let mut q = Quoter::new(100.0, 0, 500.0);
        let portfolio = Portfolio::new(10_000.0);
        // Crossed, locked, and one-sided all yield no quote.
        assert!(q
            .on_market_state(&book(0.40, 0.36), &portfolio, &[])
            .is_empty());
        assert!(q
            .on_market_state(&book(0.36, 0.36), &portfolio, &[])
            .is_empty());
        assert!(q
            .on_market_state(&one_sided(Some(0.35)), &portfolio, &[])
            .is_empty());
        assert!(q
            .on_market_state(&one_sided(None), &portfolio, &[])
            .is_empty());
    }

    #[test]
    fn the_inventory_limit_stops_the_bid() {
        let mut q = Quoter::new(100.0, 0, 100.0);
        let mut portfolio = Portfolio::new(10_000.0);
        buy(&mut portfolio, "token", price(0.35), 100.0);
        let actions = q.on_market_state(&book(0.35, 0.36), &portfolio, &[]);
        // At the limit: offer only, no more bidding.
        assert_eq!(placed(&actions), vec![(OrderSide::Sell, 0.36, 100.0)]);
    }
}
