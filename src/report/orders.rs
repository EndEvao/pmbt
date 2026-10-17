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
    let mut acc = Accumulator::default();
    for event in events {
        acc.push(event);
    }
    acc.stats(fills)
}

/// Live resting orders only; completed orders leave this map immediately.
#[derive(Default)]
pub(crate) struct Accumulator {
    starts: std::collections::HashMap<u64, i64>,
    placed: u64,
    rested: u64,
    cancelled: u64,
    expired: u64,
    total_us: i64,
    max_us: i64,
    finished: u64,
}
impl Accumulator {
    pub fn filled(&mut self, id: u64) {
        self.starts.remove(&id);
    }
    pub fn push(&mut self, event: &OrderEvent) {
        match event.kind {
            OrderEventKind::Placed => self.placed += 1,
            OrderEventKind::Rested => {
                self.rested += 1;
                self.starts.insert(event.id, event.ts);
            }
            OrderEventKind::Cancelled | OrderEventKind::Expired => {
                if event.kind == OrderEventKind::Cancelled {
                    self.cancelled += 1;
                } else {
                    self.expired += 1;
                }
                if let Some(start) = self.starts.remove(&event.id) {
                    let waited = event.ts - start;
                    self.total_us += waited;
                    self.max_us = self.max_us.max(waited);
                    self.finished += 1;
                }
            }
        }
    }
    pub fn stats(&self, fills: u64) -> OrderStats {
        OrderStats {
            placed: self.placed,
            rested: self.rested,
            cancelled: self.cancelled,
            expired: self.expired,
            fill_ratio: if self.rested > 0 {
                self.rested.saturating_sub(self.cancelled + self.expired) as f64
                    / self.rested as f64
            } else {
                0.0
            },
            cancel_rate: if fills > 0 {
                self.cancelled as f64 / fills as f64
            } else {
                0.0
            },
            mean_time_in_queue_us: if self.finished > 0 {
                self.total_us as f64 / self.finished as f64
            } else {
                0.0
            },
            max_time_in_queue_us: self.max_us,
        }
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
    fn completed_makers_release_streaming_history() {
        let mut acc = Accumulator::default();
        for id in 0..10_000 {
            acc.push(&event(0, id, OrderEventKind::Placed));
            acc.push(&event(1, id, OrderEventKind::Rested));
            acc.filled(id);
            assert!(acc.starts.is_empty());
        }
        assert_eq!(acc.stats(10_000).fill_ratio, 1.0);
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
