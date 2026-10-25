use thiserror::Error;

use crate::domain::fill::Fill;
use crate::domain::order::OrderSide;
use crate::domain::types::{MarketId, Price};

/// One resting level of the order book: a price and the size available there.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Level {
    pub(crate) price: Price,
    pub(crate) size: f64,
}

impl Level {
    /// Price at this level.
    pub fn price(&self) -> Price {
        self.price
    }
    /// Displayed quantity at this level.
    pub fn size(&self) -> f64 {
        self.size
    }
}

/// A validated snapshot of one market's order book at an instant.
///
/// The ladders are best-first — bids strictly descending, asks strictly
/// ascending (an aggregated book has one level per price). `new` is the
/// checked door enforcing that, so downstream code never sees a malformed
/// book; the sides are private because reaching past the accessors could not
/// preserve those invariants.
///
/// The touch is *not* required to be un-crossed: real Telonex captures contain
/// transient locked (`bid == ask`) and even crossed (`bid > ask`) snapshots
/// (~0.02% of a sample day) — states the matching engine resolved a moment
/// later. The feed reports what the exchange recorded; whether an order should
/// realistically fill against such a book is the fill layer's concern.
///
/// A snapshot is born finished: decoded whole at the data boundary and never
/// resized after. `Box<[Level]>` makes that compiler-enforced — the grow/shrink
/// operations don't exist. Top-of-book sources participate with one-element
/// ladders; there is no separate "quote" representation.
///
/// Sides may be empty — real books go one-sided near resolution — so the touch
/// accessors return `Option`.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketState {
    pub(crate) market_id: MarketId,
    /// Exchange time in microseconds.
    pub(crate) timestamp: i64,
    bids: Box<[Level]>,
    asks: Box<[Level]>,
}

impl MarketState {
    /// The token whose book this snapshot describes.
    pub fn market_id(&self) -> &MarketId {
        &self.market_id
    }
    /// Exchange time in microseconds (the delivered view under perception delay).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Validate a book into existence: each side strictly sorted best-first.
    /// Empty sides pass — they are vacuously sorted.
    pub fn new(
        market_id: MarketId,
        timestamp: i64,
        bids: Box<[Level]>,
        asks: Box<[Level]>,
    ) -> Result<Self, BookError> {
        for (index, pair) in bids.windows(2).enumerate() {
            if pair[1].price >= pair[0].price {
                return Err(BookError::UnsortedBids { index: index + 1 });
            }
        }
        for (index, pair) in asks.windows(2).enumerate() {
            if pair[1].price <= pair[0].price {
                return Err(BookError::UnsortedAsks { index: index + 1 });
            }
        }

        Ok(Self {
            market_id,
            timestamp,
            bids,
            asks,
        })
    }

    /// The full bid ladder, best (highest) first.
    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    /// The full ask ladder, best (lowest) first.
    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    /// Best bid — the touch. `None` when the bid side is empty.
    pub fn bid(&self) -> Option<&Level> {
        self.bids.first()
    }

    /// Best ask — the touch. `None` when the ask side is empty.
    pub fn ask(&self) -> Option<&Level> {
        self.asks.first()
    }

    /// Midpoint of the touch; `None` if either side is empty. Floors on an odd
    /// tick sum; cannot occur with grid-aligned inputs.
    pub fn mid(&self) -> Option<Price> {
        Some(self.bid()?.price.midpoint(self.ask()?.price))
    }

    /// Touch spread in micro-ticks; `None` if either side is empty.
    pub fn spread(&self) -> Option<i64> {
        Some(self.ask()?.price.difference(self.bid()?.price))
    }

    /// Displayed size resting at exactly `price` on `side`; `0.0` when no level
    /// sits there.
    ///
    /// This is the quantity a queue model needs: because the book aggregates by
    /// price, the size at a level **is** the line waiting there, so an order
    /// joining that price starts behind exactly this much. A price nobody
    /// occupies returns `0.0` — there is genuinely no queue to join.
    pub fn size_at(&self, side: OrderSide, price: Price) -> f64 {
        let ladder = match side {
            OrderSide::Buy => &self.bids,
            OrderSide::Sell => &self.asks,
        };
        ladder
            .iter()
            .find(|level| level.price == price)
            .map_or(0.0, |level| level.size)
    }

    /// This same book, restamped to `timestamp`.
    ///
    /// For the event clock: when time advances to an appointment with no new
    /// book — a GTD expiring, a cancel landing — the engine still needs a book
    /// to act against, and the right one is the last one the market showed,
    /// carried forward. Restamping it means `state.timestamp` is always "now"
    /// inside the engine loop, so nothing downstream needs a separate clock
    /// parameter. The ladders are untouched, so no invariant can move.
    pub fn at(&self, timestamp: i64) -> MarketState {
        MarketState {
            timestamp,
            ..self.clone()
        }
    }

    /// This book with `fills` removed from the side they took liquidity from —
    /// a buy consumes asks, a sell consumes bids. Levels drained to nothing are
    /// dropped; the other side is carried over untouched.
    ///
    /// This is what stops two orders executing at the *same instant* from each
    /// spending the same depth: the engine walks the residual book rather than
    /// the pristine snapshot. It is deliberately **not** market impact — the
    /// residual lives for one snapshot and then the feed's next book is ground
    /// truth again, because that book already records what the real market did.
    /// Carrying our depletion forward would double-count against it.
    ///
    /// Prices never change and levels only disappear, so sortedness survives by
    /// construction — it still goes back through [`MarketState::new`], the one
    /// door that decides what a valid book is.
    pub fn consume(&self, fills: &[Fill]) -> MarketState {
        let Some(first) = fills.first() else {
            return self.clone();
        };
        let taken = match first.side {
            OrderSide::Buy => &self.asks,
            OrderSide::Sell => &self.bids,
        };
        let mut left = Vec::with_capacity(taken.len());
        for level in taken {
            let eaten: f64 = fills
                .iter()
                .filter(|fill| fill.price == level.price)
                .map(|fill| fill.quantity)
                .sum();
            // A fully-consumed level lands on exactly 0.0 (the walk takes
            // `min(remaining, size)`), so `> 0.0` drops it without an epsilon.
            let size = level.size - eaten;
            if size > 0.0 {
                left.push(Level {
                    price: level.price,
                    size,
                });
            }
        }
        let left = left.into_boxed_slice();
        let (bids, asks) = match first.side {
            OrderSide::Buy => (self.bids.clone(), left),
            OrderSide::Sell => (left, self.asks.clone()),
        };
        MarketState::new(self.market_id.clone(), self.timestamp, bids, asks)
            .expect("consuming a valid book leaves prices and their order intact")
    }
}

#[cfg(test)]
mod consume_tests {
    use super::*;
    use crate::domain::fill::Fill;

    fn level(price: f64, size: f64) -> Level {
        Level {
            price: Price::from_probability(price).unwrap(),
            size,
        }
    }

    /// bids 0.40x10, asks 0.50x10 then 0.60x20.
    fn book() -> MarketState {
        MarketState::new(
            MarketId::new("t".to_string()),
            1,
            Box::from([level(0.40, 10.0)]),
            Box::from([level(0.50, 10.0), level(0.60, 20.0)]),
        )
        .unwrap()
    }

    fn fill(side: OrderSide, price: f64, quantity: f64) -> Fill {
        Fill::new(side, Price::from_probability(price).unwrap(), quantity, 1)
    }

    #[test]
    fn a_buy_consumes_asks_and_drops_drained_levels() {
        // Sweeps all of 0.50 and part of 0.60.
        let left = book().consume(&[
            fill(OrderSide::Buy, 0.50, 10.0),
            fill(OrderSide::Buy, 0.60, 5.0),
        ]);
        assert_eq!(left.asks().len(), 1);
        assert_eq!(left.ask().unwrap().price.as_f64(), 0.60);
        assert_eq!(left.ask().unwrap().size, 15.0);
        // The side that gave up nothing is untouched.
        assert_eq!(left.bids(), book().bids());
    }

    #[test]
    fn a_sell_consumes_bids() {
        let left = book().consume(&[fill(OrderSide::Sell, 0.40, 4.0)]);
        assert_eq!(left.bid().unwrap().size, 6.0);
        assert_eq!(left.asks(), book().asks());
    }

    #[test]
    fn consuming_a_whole_side_empties_it() {
        let left = book().consume(&[fill(OrderSide::Sell, 0.40, 10.0)]);
        assert!(left.bids().is_empty());
        assert!(left.bid().is_none());
        // An empty side is still a valid book, and the ask survives.
        assert!(left.ask().is_some());
    }

    #[test]
    fn no_fills_leaves_the_book_alone() {
        let left = book().consume(&[]);
        assert_eq!(left.bids(), book().bids());
        assert_eq!(left.asks(), book().asks());
    }
}

/// Why a book couldn't be built — a ladder invariant was violated.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum BookError {
    #[error("bids not strictly descending at index {index}")]
    UnsortedBids { index: usize },
    #[error("asks not strictly ascending at index {index}")]
    UnsortedAsks { index: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: f64, size: f64) -> Level {
        Level {
            price: Price::from_probability(price).unwrap(),
            size,
        }
    }

    fn ladder(prices: &[f64]) -> Box<[Level]> {
        prices.iter().map(|&p| level(p, 10.0)).collect()
    }

    fn book(bids: &[f64], asks: &[f64]) -> Result<MarketState, BookError> {
        MarketState::new(
            MarketId::new("m".to_string()),
            0,
            ladder(bids),
            ladder(asks),
        )
    }

    #[test]
    fn accepts_sorted_uncrossed_book_and_exposes_touch() {
        let state = book(&[0.08, 0.079, 0.078], &[0.085, 0.093]).unwrap();

        assert_eq!(
            state.bid().unwrap().price,
            Price::from_probability(0.08).unwrap()
        );
        assert_eq!(
            state.ask().unwrap().price,
            Price::from_probability(0.085).unwrap()
        );
        assert_eq!(state.bids().len(), 3);
        assert_eq!(state.asks().len(), 2);
        assert_eq!(state.spread(), Some(5_000));
    }

    #[test]
    fn rejects_unsorted_bids() {
        assert_eq!(
            book(&[0.08, 0.081], &[0.09]),
            Err(BookError::UnsortedBids { index: 1 })
        );
        // Duplicate levels are unsorted too — an aggregated book has one level per price.
        assert_eq!(
            book(&[0.08, 0.08], &[0.09]),
            Err(BookError::UnsortedBids { index: 1 })
        );
    }

    #[test]
    fn rejects_unsorted_asks() {
        assert_eq!(
            book(&[0.05], &[0.09, 0.085]),
            Err(BookError::UnsortedAsks { index: 1 })
        );
    }

    #[test]
    fn passes_through_locked_and_crossed_books() {
        // Real captures contain transient locked/crossed snapshots — the feed
        // reports them; execution realism is the fill layer's concern.
        assert!(book(&[0.085], &[0.085]).is_ok());
        assert!(book(&[0.09], &[0.085]).is_ok());
    }

    #[test]
    fn allows_empty_sides() {
        let no_bids = book(&[], &[0.9]).unwrap();
        assert!(no_bids.bid().is_none());
        assert_eq!(no_bids.mid(), None);
        assert_eq!(no_bids.spread(), None);

        let no_asks = book(&[0.9], &[]).unwrap();
        assert!(no_asks.ask().is_none());

        assert!(book(&[], &[]).is_ok());
    }

    #[test]
    fn mid_is_touch_midpoint() {
        let state = book(&[0.08], &[0.09]).unwrap();
        assert_eq!(state.mid(), Some(Price::from_probability(0.085).unwrap()));
    }
}
