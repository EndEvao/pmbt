//! Per-trade analytics: fold the recorded fills back through a domain
//! [`Position`] so realized P&L, win rate and holding periods are computed with
//! the **exact** average-cost logic the engine uses (buy = re-average, sell =
//! realize `(price − avg) × sold`). Reconstructing real [`Fill`]s and replaying
//! them means there is no second copy of the accounting to drift from the live
//! path — the recorded `f64` prices round-trip losslessly back to on-grid
//! [`Price`]s (they came from `Price::as_f64`).

use serde::Serialize;

use crate::domain::fill::Fill;
use crate::domain::order::OrderSide;
use crate::domain::position::Position;
use crate::domain::types::Price;

use super::record::FillEvent;

/// One executed fill enriched with the running position and any P&L it realized
/// — a trading blotter row.
#[derive(Debug, Clone, Serialize)]
pub struct BlotterRow {
    pub ts: i64,
    pub side: &'static str,
    pub price: f64,
    pub qty: f64,
    /// Net position in shares immediately after this fill.
    pub position_after: f64,
    /// P&L realized by this fill: 0 for a buy; a sell realizes on closed shares.
    pub realized: f64,
}

/// Round-trip summary over the run's fills. Every figure is over *realizing
/// sells*, so a strategy that only ever buys (e.g. buy-and-hold, which exits via
/// settlement rather than a sell) has `round_trips == 0` and all fields zero.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TradeStats {
    /// Number of sells that closed inventory.
    pub round_trips: usize,
    /// Fraction of round trips with strictly positive realized P&L.
    pub win_rate: f64,
    /// Mean realized P&L among winning round trips (0 if none).
    pub avg_win: f64,
    /// Mean realized P&L among losing round trips (≤ 0; 0 if none).
    pub avg_loss: f64,
    /// Quantity-weighted holding time of closed shares, microseconds.
    pub avg_hold_us: f64,
}

/// Incremental accounting; memory is independent of the number of fills.
#[derive(Default)]
pub(crate) struct Accumulator {
    position: Position,
    acq_time: f64,
    sells: usize,
    wins: usize,
    losses: usize,
    win_sum: f64,
    loss_sum: f64,
    hold_num: f64,
    hold_den: f64,
}
impl Accumulator {
    pub fn push(&mut self, fe: &FillEvent) -> BlotterRow {
        let side = if fe.side == "sell" {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };
        let fill = Fill::new(
            side,
            Price::from_probability(fe.price).expect("recorded price"),
            fe.qty,
            fe.ts,
        );
        let held = self.position.quantity();
        if side == OrderSide::Buy {
            self.acq_time = (held * self.acq_time + fe.qty * fe.ts as f64) / (held + fe.qty);
        }
        let realized = self.position.apply(&fill);
        if side == OrderSide::Sell {
            self.sells += 1;
            let closed = fe.qty.min(held);
            self.hold_num += (fe.ts as f64 - self.acq_time) * closed;
            self.hold_den += closed;
            if realized > 0.0 {
                self.wins += 1;
                self.win_sum += realized;
            }
            if realized < 0.0 {
                self.losses += 1;
                self.loss_sum += realized;
            }
        }
        BlotterRow {
            ts: fe.ts,
            side: fe.side,
            price: fe.price,
            qty: fe.qty,
            position_after: self.position.quantity(),
            realized,
        }
    }
    pub fn stats(&self) -> TradeStats {
        let ratio = |sum: f64, n: f64| if n > 0.0 { sum / n } else { 0.0 };
        TradeStats {
            round_trips: self.sells,
            win_rate: ratio(self.wins as f64, self.sells as f64),
            avg_win: ratio(self.win_sum, self.wins as f64),
            avg_loss: ratio(self.loss_sum, self.losses as f64),
            avg_hold_us: ratio(self.hold_num, self.hold_den),
        }
    }
}

/// Build the blotter and summary through the same incremental accounting.
pub fn compute(fills: &[FillEvent]) -> (Vec<BlotterRow>, TradeStats) {
    let mut acc = Accumulator::default();
    let rows = fills.iter().map(|f| acc.push(f)).collect();
    (rows, acc.stats())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fe(ts: i64, side: &'static str, price: f64, qty: f64) -> FillEvent {
        FillEvent {
            ts,
            side,
            price,
            qty,
        }
    }

    #[test]
    fn buy_only_has_no_round_trips() {
        let (rows, stats) = compute(&[fe(1, "buy", 0.40, 100.0)]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].position_after, 100.0);
        assert_eq!(rows[0].realized, 0.0);
        assert_eq!(stats.round_trips, 0);
        assert_eq!(stats.win_rate, 0.0);
        assert_eq!(stats.avg_hold_us, 0.0);
    }

    #[test]
    fn round_trip_realizes_and_times_the_hold() {
        // Buy 100 @ 0.40 at t=0, sell 100 @ 0.55 at t=2s. Realized = 0.15*100.
        let (rows, stats) = compute(&[
            fe(0, "buy", 0.40, 100.0),
            fe(2_000_000, "sell", 0.55, 100.0),
        ]);
        assert!((rows[1].realized - 15.0).abs() < 1e-9);
        assert_eq!(rows[1].position_after, 0.0);
        assert_eq!(stats.round_trips, 1);
        assert_eq!(stats.win_rate, 1.0);
        assert!((stats.avg_win - 15.0).abs() < 1e-9);
        assert_eq!(stats.avg_loss, 0.0);
        assert!((stats.avg_hold_us - 2_000_000.0).abs() < 1e-6);
    }

    #[test]
    fn averages_split_wins_and_losses() {
        // Two round trips: +$10 win, −$4 loss → win rate 0.5.
        let (_rows, stats) = compute(&[
            fe(0, "buy", 0.50, 100.0),
            fe(1, "sell", 0.60, 100.0), // +10
            fe(2, "buy", 0.60, 100.0),
            fe(3, "sell", 0.56, 100.0), // −4
        ]);
        assert_eq!(stats.round_trips, 2);
        assert!((stats.win_rate - 0.5).abs() < 1e-12);
        assert!((stats.avg_win - 10.0).abs() < 1e-9);
        assert!((stats.avg_loss + 4.0).abs() < 1e-9);
    }
}
