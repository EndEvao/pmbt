//! Resting orders matched against the trade tape with configurable queue
//! assumptions. `Front` is an optimistic upper bound; `Proportional` starts
//! behind displayed depth and attributes unexplained shrinkage proportionally
//! to cancels; `TradesOnly` advances only on observed trades. L2 data cannot
//! identify individual queue positions, so results remain model-dependent.
//!
//! # The matching rule
//!
//! `side` on a [`Trade`] is the **aggressor's** side (verified against book
//! deltas in `feed`), so:
//!
//! - a resting **buy** at `L` is filled by a **sell** trade at `price <= L`
//! - a resting **sell** at `L` is filled by a **buy** trade at `price >= L`
//!
//! `<=`/`>=` rather than `==` because a sell printing *below* our bid is
//! impossible while our bid is live — the exchange must match the highest bid
//! first. Such a print means the seller swept **through** our level, which
//! would have consumed us on the way down. Requiring an exact match would
//! silently skip those sweeps — precisely the *adverse* fills — and flatter
//! maker P&L.
//!
//! Fills are capped by the trade's size (we can never receive more shares than
//! actually changed hands) and priced at the order's own resting price.

use crate::domain::fill::Fill;
use crate::domain::marketstate::MarketState;
use crate::domain::order::OrderSide;
use crate::domain::trade::Trade;
use crate::domain::types::{MarketId, OrderId, Price};

/// How much of the line ahead of a resting order we model.
///
/// The book aggregates by price, so the displayed size at a level **is** the
/// queue waiting there — an order joining that price starts behind exactly that
/// many shares, by the exchange's own price-time priority. What we cannot
/// observe is how that line clears: when depth shrinks without a trade, we have
/// no way to tell whether the cancels came from ahead of us or behind. That
/// single unknown is what these modes bracket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueueModel {
    /// No queue at all — any flow reaching our price fills us. Asserts we are
    /// first in a line we actually joined the back of, so it is an **upper
    /// bound** on maker fills, not an estimate.
    Front,
    /// Join the back; trades drain the line; cancellations advance us in
    /// proportion to how much of the level they removed. Proportional
    /// attribution claims nothing about *who* cancelled, which is exactly the
    /// state of our knowledge — the neutral choice.
    #[default]
    Proportional,
    /// Join the back; only trades drain the line. A **lower bound**: on a venue
    /// where ~80% of depletion is cancellation, ignoring cancels means a joined
    /// order essentially never fills.
    TradesOnly,
}

/// One of our orders resting on the book as a maker.
#[derive(Debug, Clone, PartialEq)]
pub struct RestingOrder {
    pub id: OrderId,
    pub market_id: MarketId,
    pub side: OrderSide,
    /// The price we are posted at — also the price any fill executes at.
    pub price: Price,
    /// Shares still unfilled.
    pub remaining: f64,
    /// When the order reached the book. Drives time priority among our own
    /// orders, and gates eligibility: a trade at or before this instant printed
    /// before we existed, so it cannot fill us.
    pub placed_at: i64,
    /// Resources still held for this order: cash for a buy (`price x remaining`),
    /// shares for a sell. Released as it fills, cancels or expires.
    pub reserved: f64,
    /// Shares ahead of us in the line at our price. Seeded from the displayed
    /// size when we joined; drained by trades and (depending on the model) by
    /// cancellations. Zero means we are at the front — true by construction
    /// when we posted at a price nobody else occupied.
    pub queue_ahead: f64,
    /// Displayed size at our price when we last looked, so a later snapshot can
    /// tell how much the level shrank — the signal for cancellations.
    pub last_displayed: f64,
    /// When the exchange pulls this order on its own, for a GTD order — already
    /// net of the security buffer, so it is the moment the order really goes.
    /// `None` for GTC, which only ever leaves by filling or being cancelled.
    pub expires_at: Option<i64>,
}

impl RestingOrder {
    /// Whether `trade` can fill this order: it must aggress the opposite way,
    /// cross our price, and have printed strictly after we reached the book.
    fn matched_by(&self, trade: &Trade) -> bool {
        if trade.timestamp <= self.placed_at {
            return false;
        }
        match self.side {
            // Our bid is lifted by a seller at or below our price.
            OrderSide::Buy => trade.side == OrderSide::Sell && trade.price <= self.price,
            // Our offer is lifted by a buyer at or above our price.
            OrderSide::Sell => trade.side == OrderSide::Buy && trade.price >= self.price,
        }
    }
}

/// One maker fill produced by the matcher, with the bookkeeping the engine
/// needs to settle it.
#[derive(Debug, Clone, PartialEq)]
pub struct MakerFill {
    pub order_id: OrderId,
    pub market_id: MarketId,
    pub fill: Fill,
    /// Reservation freed by this fill — cash for a buy, shares for a sell.
    pub released: f64,
    /// Whether this fill completed the order (it is no longer resting).
    pub complete: bool,
}

/// The book of our own resting orders, kept in **price-then-time** priority so
/// a trade consumes them in the order a real exchange would.
///
/// A `Vec` with linear `insert`/scan: `match_trade` walks every order per
/// trade, and `insert` scans for the priority slot. Both are O(n) in the number
/// of resting orders, which is fine at the scale the strategies here run (a
/// handful live at once); a strategy resting thousands would want a keyed
/// structure instead.
#[derive(Debug, Default)]
pub struct OpenOrders {
    orders: Vec<RestingOrder>,
    /// Volume that has traded at each of our own prices since the last book we
    /// reconciled against, so [`OpenOrders::observe`] can subtract it before
    /// blaming shrinkage on cancellations.
    ///
    /// This has to survive across events, not just within one. Trades arrive on
    /// clock ticks too, and a tick carries the *previous* book forward
    /// unchanged — so there is no shrinkage to explain yet, and a tally that
    /// reset there would be thrown away. The book showing the level eaten
    /// arrives later, by which time the volume that explains it is gone and the
    /// whole drop gets attributed to cancellations. Under `Proportional` that
    /// advances our queue position for free, always optimistically.
    traded: Vec<(Price, OrderSide, f64)>,
}

impl OpenOrders {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// Total unfilled shares across every resting order.
    pub fn total_remaining(&self) -> f64 {
        // Folded from +0.0: `Sum` for floats starts at -0.0, which would make
        // an empty book report "-0.0".
        self.orders.iter().fold(0.0, |total, o| total + o.remaining)
    }

    /// Total resources still held by resting orders (cash for buys, shares for
    /// sells — summed as one figure for the run summary).
    pub fn total_reserved(&self) -> f64 {
        self.orders.iter().fold(0.0, |total, o| total + o.reserved)
    }

    pub fn iter(&self) -> impl Iterator<Item = &RestingOrder> {
        self.orders.iter()
    }

    /// The best price we are posted at on `side` — the front of our own book,
    /// which price-time ordering puts first. This is what the report draws as
    /// the quote envelope.
    pub fn best(&self, side: OrderSide) -> Option<Price> {
        self.orders.iter().find(|o| o.side == side).map(|o| o.price)
    }

    /// Add an order to the book, keeping price-then-time priority: better
    /// prices first (higher for buys, lower for sells), ties broken by who
    /// arrived earlier. Buys and sells are held in one list — they can never
    /// match the same trade, so they never compete for priority.
    pub fn insert(&mut self, order: RestingOrder) {
        let position = self
            .orders
            .iter()
            .position(|existing| {
                if existing.side != order.side {
                    return false;
                }
                let better_price = match order.side {
                    OrderSide::Buy => order.price > existing.price,
                    OrderSide::Sell => order.price < existing.price,
                };
                better_price
                    || (order.price == existing.price && order.placed_at < existing.placed_at)
            })
            .unwrap_or(self.orders.len());
        self.orders.insert(position, order);
    }

    /// Remove an order (a cancellation), returning it so the caller can release
    /// what it still held.
    pub fn remove(&mut self, id: OrderId) -> Option<RestingOrder> {
        let position = self.orders.iter().position(|o| o.id == id)?;
        Some(self.orders.remove(position))
    }

    /// Remove every order whose expiry has passed by `now`, returning them so
    /// the caller can release what they held.
    ///
    /// A GTD expiry is the *exchange* acting on a schedule it agreed to at
    /// placement — unlike a cancel, nothing is in flight and nothing can lose a
    /// race.
    ///
    /// The engine schedules the earliest expiry independently of book updates.
    /// At an equal timestamp, its current tie policy processes the trade first.
    pub fn expire(&mut self, now: i64) -> Vec<RestingOrder> {
        let mut gone = Vec::new();
        self.orders.retain(|order| {
            if order.expires_at.is_some_and(|at| at <= now) {
                gone.push(order.clone());
                false
            } else {
                true
            }
        });
        gone
    }

    /// Take every resting order, emptying the book — used at feed end, where
    /// whatever is still open expires unfilled.
    pub fn drain(&mut self) -> Vec<RestingOrder> {
        std::mem::take(&mut self.orders)
    }

    /// Fill resting orders against `trades`, and remember how much traded at
    /// our own prices.
    ///
    /// Separate from [`OpenOrders::observe`] because the two answer to
    /// different events: a trade is a print, a book is a picture. Bundling them
    /// only works while every trade arrives attached to a book — which stopped
    /// being true when the clock started delivering trades on ticks.
    pub fn fill_from(&mut self, trades: &[Trade]) -> Vec<MakerFill> {
        let mut fills = Vec::new();
        for trade in trades {
            for order in self.orders.iter().filter(|o| o.matched_by(trade)) {
                let key = (order.price, order.side);
                match self.traded.iter_mut().find(|(p, s, _)| (*p, *s) == key) {
                    Some((_, _, total)) => *total += trade.size,
                    None => self.traded.push((key.0, key.1, trade.size)),
                }
            }
            fills.extend(self.match_trade(trade));
        }
        self.orders.retain(|o| o.remaining > 0.0);
        fills
    }

    /// Fill against trades and reconcile against a book, in one call.
    ///
    /// Test-only, and it is still a real shape: it is exactly what a `Book`
    /// event does, since a book arrives with the trades of its own window. The
    /// split matters for the *other* case — trades delivered on a clock tick,
    /// with no new book behind them — which the engine tests cover.
    #[cfg(test)]
    fn advance(
        &mut self,
        trades: &[Trade],
        state: &MarketState,
        model: QueueModel,
    ) -> Vec<MakerFill> {
        let fills = self.fill_from(trades);
        self.observe(state, model);
        fills
    }

    /// Reconcile against a **new** book: shrinkage that the trades since the
    /// last book do not explain is attributed to cancellations. Clears the
    /// tally, which is why it must only ever be called with a book the market
    /// actually printed — never with one carried forward to a clock tick.
    pub fn observe(&mut self, state: &MarketState, model: QueueModel) {
        self.observe_book(state, model);
        self.traded.clear();
    }

    /// Match one trade, draining each order's queue before it can fill.
    ///
    /// A trade sweeps a price level from the front, so `min(trade.size,
    /// queue_ahead)` is exactly how much of *that* order's line it consumed —
    /// each `queue_ahead` is measured from the start of the line, so they are
    /// computed independently. Only `available`, the volume left to actually
    /// fill with, is shared: we can never receive more shares than traded.
    ///
    /// Known limitation, inherent to L2: two of *our own* orders at the same
    /// price both seed `queue_ahead` from the public displayed size, which does
    /// not include the other — so the second under-counts the line ahead of it
    /// by the first's size and fills slightly too eagerly. The engine could
    /// correct this (it knows its own orders), but no current strategy rests
    /// two orders at one price, so it is a **deliberate deferral**, not an
    /// oversight. It is also not a one-liner: the own-orders term must be
    /// tracked *separately* from `queue_ahead`, because `observe_book` scales
    /// `queue_ahead` by public-book shrinkage to attribute cancellations, and
    /// our own orders neither sit in the public book nor cancel through it —
    /// folding them in would let that attribution shrink them wrongly.
    fn match_trade(&mut self, trade: &Trade) -> Vec<MakerFill> {
        let mut available = trade.size;
        let mut fills = Vec::new();

        for order in self.orders.iter_mut() {
            if !order.matched_by(trade) {
                continue;
            }
            // The trade eats the line ahead of us first.
            let eaten = trade.size.min(order.queue_ahead);
            order.queue_ahead -= eaten;
            if order.queue_ahead > 0.0 {
                continue; // still queued — this trade never reached us
            }

            // Only what is left of the trade after clearing our queue can fill
            // us, and only as much volume as no earlier order already took.
            let quantity = (trade.size - eaten).min(order.remaining).min(available);
            if quantity <= 0.0 {
                continue;
            }

            // Release the share of the hold this fill consumes, proportional to
            // the quantity filled — cash for a buy, shares for a sell.
            let released = if order.remaining > 0.0 {
                order.reserved * (quantity / order.remaining)
            } else {
                0.0
            };
            order.reserved -= released;
            order.remaining -= quantity;
            available -= quantity;

            fills.push(MakerFill {
                order_id: order.id,
                market_id: order.market_id.clone(),
                fill: Fill::maker(order.side, order.price, quantity, trade.timestamp),
                released,
                complete: order.remaining <= 0.0,
            });
        }
        fills
    }

    /// Attribute unexplained depth shrinkage to cancellations ahead of us.
    ///
    /// Depth falling by more than traded means orders were pulled. Whether they
    /// sat ahead of us or behind is unknowable from aggregate data, so
    /// [`QueueModel::Proportional`] scales the line by how much of the level
    /// survived — asserting nothing about *who* cancelled. Growth never applies:
    /// new arrivals join behind us.
    fn observe_book(&mut self, state: &MarketState, model: QueueModel) {
        for order in self.orders.iter_mut() {
            let displayed = state.size_at(order.side, order.price);
            if model == QueueModel::Proportional {
                let volume = self
                    .traded
                    .iter()
                    .find(|(p, s, _)| *p == order.price && *s == order.side)
                    .map_or(0.0, |(_, _, v)| *v);
                // What the level would show if trading were the only cause.
                let expected = (order.last_displayed - volume).max(0.0);
                if displayed < expected && expected > 0.0 {
                    order.queue_ahead *= displayed / expected;
                }
            }
            order.last_displayed = displayed;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::marketstate::Level;

    fn mkt() -> MarketId {
        MarketId::new("token".to_string())
    }

    fn price(p: &str) -> Price {
        Price::parse_decimal(p).unwrap()
    }

    fn resting(id: u64, side: OrderSide, p: &str, qty: f64, placed_at: i64) -> RestingOrder {
        let price = price(p);
        let reserved = match side {
            OrderSide::Buy => price.as_f64() * qty,
            OrderSide::Sell => qty,
        };
        RestingOrder {
            expires_at: None,
            queue_ahead: 0.0,
            last_displayed: 0.0,
            id: OrderId::new(id),
            market_id: mkt(),
            side,
            price,
            remaining: qty,
            placed_at,
            reserved,
        }
    }

    fn trade(side: OrderSide, p: &str, size: f64, ts: i64) -> Trade {
        Trade::new(ts, price(p), size, side, mkt())
    }

    /// A book showing `levels` on both sides — what `advance` inspects for
    /// cancellations after the trades are applied.
    fn book_state(levels: &[(&str, f64)]) -> MarketState {
        let side: Box<[Level]> = levels
            .iter()
            .map(|(p, size)| Level {
                price: price(p),
                size: *size,
            })
            .collect();
        MarketState::new(mkt(), 0, side.clone(), side).unwrap()
    }

    /// Push one trade through the real entry point. The empty book means no
    /// cancellation signal, so the trade is the only force acting.
    fn feed(book: &mut OpenOrders, t: &Trade) -> Vec<MakerFill> {
        book.advance(std::slice::from_ref(t), &book_state(&[]), QueueModel::Front)
    }

    #[test]
    fn a_sell_trade_at_our_bid_fills_a_resting_buy() {
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 100.0, 0));

        let fills = feed(&mut book, &trade(OrderSide::Sell, "0.35", 100.0, 10));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill.quantity, 100.0);
        // Filled at OUR price, and flagged maker (so no taker fee).
        assert_eq!(fills[0].fill.price, price("0.35"));
        assert!(fills[0].complete);
        assert!(book.is_empty(), "a fully filled order leaves the book");
    }

    #[test]
    fn a_sweep_below_our_bid_still_fills_us() {
        // A seller printing under our bid must have swept through our level.
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 50.0, 0));

        let fills = feed(&mut book, &trade(OrderSide::Sell, "0.33", 50.0, 10));
        assert_eq!(fills.len(), 1);
        // Still our price, not the trade's.
        assert_eq!(fills[0].fill.price, price("0.35"));
    }

    #[test]
    fn a_trade_above_our_bid_does_not_fill_us() {
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 50.0, 0));
        assert!(book
            .match_trade(&trade(OrderSide::Sell, "0.36", 50.0, 10))
            .is_empty());
        assert_eq!(book.len(), 1);
    }

    #[test]
    fn a_buy_trade_never_fills_a_resting_buy() {
        // Only the opposite aggressor can lift us.
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 50.0, 0));
        assert!(book
            .match_trade(&trade(OrderSide::Buy, "0.35", 50.0, 10))
            .is_empty());
    }

    #[test]
    fn resting_sells_fill_on_buy_trades_at_or_above_their_price() {
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Sell, "0.40", 30.0, 0));

        assert!(book
            .match_trade(&trade(OrderSide::Buy, "0.39", 30.0, 10))
            .is_empty());
        let fills = feed(&mut book, &trade(OrderSide::Buy, "0.41", 30.0, 20));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill.price, price("0.40"));
    }

    #[test]
    fn fill_is_capped_by_the_trade_size() {
        // 10 shares traded can never fill 100 of ours.
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 100.0, 0));

        let fills = feed(&mut book, &trade(OrderSide::Sell, "0.35", 10.0, 10));
        assert_eq!(fills[0].fill.quantity, 10.0);
        assert!(!fills[0].complete);
        assert_eq!(book.len(), 1, "the remainder keeps resting");
        assert_eq!(book.total_remaining(), 90.0);
    }

    #[test]
    fn partial_fills_release_the_hold_proportionally() {
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.50", 100.0, 0));
        // Reserved 0.50 * 100 = 50.0; filling 40 releases 20.0.
        let fills = feed(&mut book, &trade(OrderSide::Sell, "0.50", 40.0, 10));
        assert!((fills[0].released - 20.0).abs() < 1e-9);
        assert!((book.total_reserved() - 30.0).abs() < 1e-9);
    }

    #[test]
    fn a_trade_walks_our_orders_in_price_then_time_priority() {
        let mut book = OpenOrders::new();
        // Same price, different arrival: the earlier one fills first.
        book.insert(resting(1, OrderSide::Buy, "0.35", 30.0, 5));
        book.insert(resting(2, OrderSide::Buy, "0.35", 40.0, 1));
        // A better price jumps ahead of both.
        book.insert(resting(3, OrderSide::Buy, "0.36", 10.0, 9));

        let fills = feed(&mut book, &trade(OrderSide::Sell, "0.30", 50.0, 20));
        let order: Vec<u64> = fills
            .iter()
            .map(|f| f.order_id.to_string().parse().unwrap())
            .collect();
        // 50 shares: 10 to #3 (best price), 40 to #2 (same price, earlier),
        // and nothing left for #1 — the trade size is exhausted.
        assert_eq!(order, vec![3, 2], "best price, then earliest");
        assert_eq!(fills[0].fill.quantity, 10.0);
        assert_eq!(fills[1].fill.quantity, 40.0);
        assert_eq!(book.total_remaining(), 30.0, "#1 is untouched");
    }

    #[test]
    fn a_trade_at_or_before_placement_cannot_fill_us() {
        // No lookahead: the order did not exist when this trade printed.
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 50.0, 100));
        assert!(book
            .match_trade(&trade(OrderSide::Sell, "0.35", 50.0, 100))
            .is_empty());
        assert!(book
            .match_trade(&trade(OrderSide::Sell, "0.35", 50.0, 99))
            .is_empty());
        assert_eq!(
            feed(&mut book, &trade(OrderSide::Sell, "0.35", 50.0, 101)).len(),
            1
        );
    }

    #[test]
    fn remove_and_drain_return_orders_with_their_holds() {
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.50", 100.0, 0));
        book.insert(resting(2, OrderSide::Sell, "0.60", 20.0, 0));

        let cancelled = book.remove(OrderId::new(1)).unwrap();
        assert!((cancelled.reserved - 50.0).abs() < 1e-9);
        assert!(book.remove(OrderId::new(99)).is_none());

        let left = book.drain();
        assert_eq!(left.len(), 1);
        assert!(book.is_empty());
    }

    // --- queue model -------------------------------------------------------

    /// An order joining a level starts behind everything already displayed
    /// there — the book's aggregate size IS the line.
    fn queued(id: u64, side: OrderSide, p: &str, qty: f64, ahead: f64) -> RestingOrder {
        let mut o = resting(id, side, p, qty, 0);
        o.queue_ahead = ahead;
        o.last_displayed = ahead;
        o
    }

    #[test]
    fn a_trade_smaller_than_the_queue_does_not_fill_us() {
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 1_000.0));

        let fills = book.advance(
            &[trade(OrderSide::Sell, "0.35", 300.0, 10)],
            &book_state(&[("0.35", 700.0)]),
            QueueModel::Proportional,
        );
        assert!(fills.is_empty(), "300 traded against 1000 ahead: no fill");
        assert_eq!(book.iter().next().unwrap().queue_ahead, 700.0);
    }

    #[test]
    fn the_leftover_after_the_queue_clears_fills_us() {
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 500.0));

        // 600 traded: 500 clears the line, the leftover 100 reaches us.
        let fills = book.advance(
            &[trade(OrderSide::Sell, "0.35", 600.0, 10)],
            &book_state(&[]),
            QueueModel::Proportional,
        );
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill.quantity, 100.0);
        assert!(book.is_empty());
    }

    #[test]
    fn cancellations_advance_the_queue_proportionally() {
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 800.0));

        // No trades; depth 800 -> 600 is pure cancellation, so the line ahead
        // shrinks by the same fraction: 800 * 600/800 = 600.
        book.advance(
            &[],
            &book_state(&[("0.35", 600.0)]),
            QueueModel::Proportional,
        );
        assert!((book.iter().next().unwrap().queue_ahead - 600.0).abs() < 1e-9);
    }

    #[test]
    fn trade_volume_is_not_counted_twice_as_cancellation() {
        // THE double-count guard: a 200-share trade drops depth 1000 -> 800.
        // The trade already drained 200 from the queue; attributing the same
        // 200 again as a cancel would advance us twice for one event.
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 1_000.0));

        book.advance(
            &[trade(OrderSide::Sell, "0.35", 200.0, 10)],
            &book_state(&[("0.35", 800.0)]),
            QueueModel::Proportional,
        );
        assert!(
            (book.iter().next().unwrap().queue_ahead - 800.0).abs() < 1e-9,
            "queue should be 800 (trade only), not 640 (trade + phantom cancel)"
        );
    }

    #[test]
    fn trades_only_ignores_cancellations() {
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 800.0));

        book.advance(&[], &book_state(&[("0.35", 100.0)]), QueueModel::TradesOnly);
        assert_eq!(
            book.iter().next().unwrap().queue_ahead,
            800.0,
            "the pessimistic bound: only trades move the line"
        );
    }

    #[test]
    fn growth_never_pushes_us_back() {
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 500.0));

        // New arrivals join BEHIND us, so a deeper level changes nothing.
        book.advance(
            &[],
            &book_state(&[("0.35", 5_000.0)]),
            QueueModel::Proportional,
        );
        assert_eq!(book.iter().next().unwrap().queue_ahead, 500.0);
    }

    #[test]
    fn one_trade_sweeping_two_of_our_orders_reconciles_exactly() {
        // Line: [X 1000][A 100][Y 50][B 200]; a 1200 trade should fill A 100
        // and B 50 — 1000+100+50+50 = 1200 accounted for exactly.
        let mut book = OpenOrders::new();
        book.insert(queued(1, OrderSide::Buy, "0.35", 100.0, 1_000.0)); // A
        let mut b = queued(2, OrderSide::Buy, "0.35", 200.0, 1_150.0); // B
        b.placed_at = 1; // later arrival -> lower time priority
        book.insert(b);

        let fills = book.advance(
            &[trade(OrderSide::Sell, "0.35", 1_200.0, 10)],
            &book_state(&[]),
            QueueModel::Proportional,
        );
        let total: f64 = fills.iter().map(|f| f.fill.quantity).sum();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].fill.quantity, 100.0);
        assert_eq!(fills[1].fill.quantity, 50.0);
        assert_eq!(total, 150.0);
    }

    #[test]
    fn front_mode_fills_immediately_regardless_of_depth() {
        // The optimistic bound: seeded at 0, so a deep level is irrelevant.
        let mut book = OpenOrders::new();
        book.insert(resting(1, OrderSide::Buy, "0.35", 100.0, 0));
        let fills = book.advance(
            &[trade(OrderSide::Sell, "0.35", 100.0, 10)],
            &book_state(&[("0.35", 1_000_000.0)]),
            QueueModel::Front,
        );
        assert_eq!(fills.len(), 1);
    }
}
