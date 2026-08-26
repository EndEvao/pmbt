//! Maker metrics, computed from the recorded order lifecycle.
//!
//! Fills alone can't judge a maker strategy: the orders that *didn't* fill are
//! half the story, and how long they waited is the other half. Folding the
//! [`OrderEvent`] stream into per-order histories answers the three questions a
//! maker actually asks — did my quotes get hit, how long did they sit, and how
//! much did I churn to keep them there.
//!
//! [`fill_ratio`](OrderStats::fill_ratio) is also the sharpest read on how much
//! the optimistic matcher is flattering us: under front-of-queue assumptions it
//! runs far higher than a real queue would allow.

use serde::Serialize;

use super::record::{OrderEvent, OrderEventKind};

/// How our orders fared, over the whole run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct OrderStats {
    /// Orders submitted.
    pub placed: u64,
    /// Orders that reached the book as makers (the rest were marketable, or
    /// died before landing).
    pub rested: u64,
    pub cancelled: u64,
    /// Still resting when the feed ended.
    pub expired: u64,
    /// Fraction of resting orders that left the book by filling rather than by
    /// being cancelled or expiring. The headline maker-efficiency number —
    /// and, under the optimistic matcher, an upper bound.
    pub fill_ratio: f64,
    /// Cancels per fill: how much quote churn each execution cost.
    pub cancel_rate: f64,
    /// Mean time an order spent resting before it left the book, microseconds.
    /// Averaged over orders that actually finished (cancelled or expired) —
    /// the direct input to a faithful queue model later.
    pub mean_time_in_queue_us: f64,
    /// The longest any single order rested.
    pub max_time_in_queue_us: i64,
}

/// Fold the event stream into per-order outcomes.
///
/// `fills` is the run's fill count, which the events don't carry (a fill is
/// reported through `on_fill`, not `on_order`) — so it is passed in.
pub fn compute(events: &[OrderEvent], fills: u64) -> OrderStats {
    let count = |kind: OrderEventKind| events.iter().filter(|e| e.kind == kind).count() as u64;
    let placed = count(OrderEventKind::Placed);
    let rested = count(OrderEventKind::Rested);
    let cancelled = count(OrderEventKind::Cancelled);
    let expired = count(OrderEventKind::Expired);

    // Time in queue: pair each `Rested` with the event that ended it. Orders
    // that filled away entirely have no ending event, so they are excluded —
    // the average is over orders whose full lifetime we can see.
    let mut total_us = 0i64;
    let mut max_us = 0i64;
    let mut finished = 0u64;
    for start in events.iter().filter(|e| e.kind == OrderEventKind::Rested) {
        let ended = events.iter().find(|e| {
            e.id == start.id
                && e.ts >= start.ts
                && matches!(e.kind, OrderEventKind::Cancelled | OrderEventKind::Expired)
        });
        if let Some(end) = ended {
            let waited = end.ts - start.ts;
            total_us += waited;
            max_us = max_us.max(waited);
            finished += 1;
        }
    }

    // An order leaves the book by filling, being cancelled, or expiring. What
    // rested but never ended must have filled away.
    let filled_away = rested.saturating_sub(cancelled + expired);

    OrderStats {
        placed,
        rested,
        cancelled,
        expired,
        fill_ratio: if rested > 0 {
            filled_away as f64 / rested as f64
        } else {
            0.0
        },
        cancel_rate: if fills > 0 {
            cancelled as f64 / fills as f64
        } else {
            0.0
        },
        mean_time_in_queue_us: if finished > 0 {
            total_us as f64 / finished as f64
        } else {
            0.0
        },
        max_time_in_queue_us: max_us,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(ts: i64, id: u64, kind: OrderEventKind) -> OrderEvent {
        OrderEvent {
            ts,
            id,
            kind,
            side: "buy",
            price: Some(0.35),
            quantity: 100.0,
        }
    }

    #[test]
    fn counts_each_stage() {
        let events = vec![
            event(0, 1, OrderEventKind::Placed),
            event(1, 1, OrderEventKind::Rested),
            event(5, 1, OrderEventKind::Cancelled),
            event(0, 2, OrderEventKind::Placed),
            event(1, 2, OrderEventKind::Rested),
        ];
        let stats = compute(&events, 1);
        assert_eq!(stats.placed, 2);
        assert_eq!(stats.rested, 2);
        assert_eq!(stats.cancelled, 1);
        // Order 2 rested and never ended → it filled away: 1 of 2.
        assert!((stats.fill_ratio - 0.5).abs() < 1e-12);
        assert!((stats.cancel_rate - 1.0).abs() < 1e-12);
    }

    #[test]
    fn times_orders_from_resting_to_removal() {
        let events = vec![
            event(100, 1, OrderEventKind::Rested),
            event(400, 1, OrderEventKind::Cancelled), // waited 300
            event(100, 2, OrderEventKind::Rested),
            event(200, 2, OrderEventKind::Expired), // waited 100
        ];
        let stats = compute(&events, 0);
        assert!((stats.mean_time_in_queue_us - 200.0).abs() < 1e-9);
        assert_eq!(stats.max_time_in_queue_us, 300);
    }

    #[test]
    fn an_empty_run_is_all_zeros_not_nan() {
        let stats = compute(&[], 0);
        assert_eq!(stats.fill_ratio, 0.0);
        assert_eq!(stats.cancel_rate, 0.0);
        assert_eq!(stats.mean_time_in_queue_us, 0.0);
    }
}
