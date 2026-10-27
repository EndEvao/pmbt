//! Depth-aware execution: a marketable order sweeping the book.
//!
//! Reports **what the book can match right now** — partial included. Killing
//! or resting the unmatched remainder is time-in-force *policy*, which belongs
//! to the engine (FOK kills the whole order, FAK drops the rest, GTC rests it
//! as a maker order). A limit price bounds the walk. Resting orders are filled
//! elsewhere entirely, against the trades tape — see `crate::resting`.

use crate::domain::fill::Fill;
use crate::domain::marketstate::MarketState;
use crate::domain::order::{Order, OrderSide};
use crate::fill::fillmodel::FillModel;

/// Walks the book best-first, consuming liquidity level by level: buys sweep
/// up the asks, sells sweep down the bids. Emits one `Fill` per level
/// consumed, at that level's exact tick price — the record matches what the
/// exchange would report for a multi-level match. A limit order stops at its
/// price; running out of ladder (or crossing the limit) simply means a smaller
/// result. Stateless — hence a unit struct.
pub struct SweepFillModel;

impl FillModel for SweepFillModel {
    fn fill(&self, order: &Order, market: &MarketState) -> Box<[Fill]> {
        let side = order.side();
        let ladder = match side {
            OrderSide::Buy => market.asks(),
            OrderSide::Sell => market.bids(),
        };

        let mut remaining = order.quantity();
        let mut fills = Vec::new();
        for level in ladder {
            if remaining <= 0.0 {
                break;
            }
            // A limit order stops walking once the ladder passes its price:
            // levels beyond it are matches the order refuses to make.
            if !order.accepts_price(level.price) {
                break;
            }
            // The final take equals `remaining` exactly (min then subtract),
            // so full consumption lands on exactly 0.0. Intermediate
            // `remaining -= size` can carry float dust into a microscopic
            // next-level fill — harmless, and recorded faithfully rather
            // than epsilon-patched.
            let take = remaining.min(level.size);
            fills.push(Fill::new(side, level.price, take, market.timestamp));
            remaining -= take;
        }

        // Whatever matched is reported, partial included — the engine's
        // time-in-force policy decides the remainder's fate (FOK kills the
        // whole order, FAK drops it, GTC rests it).
        fills.into_boxed_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::marketstate::Level;
    use crate::domain::types::{MarketId, OrderId, Price};

    fn ladder(levels: &[(f64, f64)]) -> Box<[Level]> {
        levels
            .iter()
            .map(|&(price, size)| Level {
                price: Price::from_probability(price).unwrap(),
                size,
            })
            .collect()
    }

    /// Bids 0.080×20, 0.079×50; asks 0.085×100, 0.090×150, 0.100×500.
    fn snapshot() -> MarketState {
        MarketState::new(
            MarketId::new("token".to_string()),
            1,
            ladder(&[(0.08, 20.0), (0.079, 50.0)]),
            ladder(&[(0.085, 100.0), (0.09, 150.0), (0.1, 500.0)]),
        )
        .unwrap()
    }

    fn order(side: OrderSide, quantity: f64) -> Order {
        Order::new(OrderId::new(1), 1, quantity, side, None)
    }

    /// A limit order: the same, bounded by a price it refuses to cross.
    fn limit_order(side: OrderSide, quantity: f64, limit: f64) -> Order {
        Order::new(
            OrderId::new(1),
            1,
            quantity,
            side,
            Some(Price::from_probability(limit).unwrap()),
        )
    }

    fn assert_fill(fill: &Fill, price: f64, quantity: f64) {
        assert_eq!(fill.price, Price::from_probability(price).unwrap());
        assert!((fill.quantity - quantity).abs() < 1e-9);
    }

    #[test]
    fn buy_sweeps_up_the_asks_one_fill_per_level() {
        let fills = SweepFillModel.fill(&order(OrderSide::Buy, 300.0), &snapshot());

        assert_eq!(fills.len(), 3);
        assert_fill(&fills[0], 0.085, 100.0);
        assert_fill(&fills[1], 0.09, 150.0);
        assert_fill(&fills[2], 0.1, 50.0);
    }

    #[test]
    fn touch_deep_enough_means_single_fill() {
        let fills = SweepFillModel.fill(&order(OrderSide::Buy, 100.0), &snapshot());
        assert_eq!(fills.len(), 1);
        assert_fill(&fills[0], 0.085, 100.0);
    }

    #[test]
    fn order_equal_to_total_depth_consumes_the_ladder() {
        let fills = SweepFillModel.fill(&order(OrderSide::Sell, 70.0), &snapshot());
        assert_eq!(fills.len(), 2);
        assert_fill(&fills[0], 0.08, 20.0);
        assert_fill(&fills[1], 0.079, 50.0);
    }

    #[test]
    fn order_exceeding_depth_reports_the_partial() {
        // The walk reports what the ladder can match; killing the remainder is
        // the engine's time-in-force policy, not the fill model's call.
        let fills = SweepFillModel.fill(&order(OrderSide::Sell, 70.1), &snapshot());
        let matched: f64 = fills.iter().map(|f| f.quantity).sum();
        assert!(
            (matched - 70.0).abs() < 1e-9,
            "the whole book, not the order"
        );
    }

    #[test]
    fn a_limit_stops_walking_past_its_price() {
        // Buy 300 with a 0.09 cap: takes 0.085 and 0.09, refuses the 0.1 level.
        let fills = SweepFillModel.fill(&limit_order(OrderSide::Buy, 300.0, 0.09), &snapshot());
        assert_eq!(fills.len(), 2);
        assert_fill(&fills[0], 0.085, 100.0);
        assert_fill(&fills[1], 0.09, 150.0);
    }

    #[test]
    fn a_non_marketable_limit_matches_nothing() {
        // Bidding 0.08 when the best ask is 0.085: nothing crosses, so the
        // order rests instead of trading.
        let fills = SweepFillModel.fill(&limit_order(OrderSide::Buy, 100.0, 0.08), &snapshot());
        assert!(fills.is_empty());
    }

    #[test]
    fn empty_side_matches_nothing() {
        let one_sided = MarketState::new(
            MarketId::new("token".to_string()),
            1,
            ladder(&[]),
            ladder(&[(0.9, 10.0)]),
        )
        .unwrap();
        assert!(SweepFillModel
            .fill(&order(OrderSide::Sell, 1.0), &one_sided)
            .is_empty());
    }
}
