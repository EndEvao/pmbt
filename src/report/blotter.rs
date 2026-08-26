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

/// Build the blotter rows and the round-trip summary from the recorded fills.
pub fn compute(fills: &[FillEvent]) -> (Vec<BlotterRow>, TradeStats) {
    let mut position = Position::new();
    // Quantity-weighted acquisition time of the shares currently held, so a sell
    // can attribute a holding period back to when the inventory was bought.
    let mut acq_time = 0.0;
    let mut rows = Vec::with_capacity(fills.len());

    let mut sells = 0usize;
    let mut wins: Vec<f64> = Vec::new();
    let mut losses: Vec<f64> = Vec::new();
    let mut hold_num = 0.0; // Σ hold_us · closed_qty
    let mut hold_den = 0.0; // Σ closed_qty

    for fe in fills {
        let side = match fe.side {
            "sell" => OrderSide::Sell,
            _ => OrderSide::Buy,
        };
        let price = Price::from_probability(fe.price).expect("recorded fill price is on-grid");
        let fill = Fill::new(side, price, fe.qty, fe.ts);
        let held = position.quantity();

        let realized = match side {
            OrderSide::Buy => {
                // Re-average the acquisition time alongside the cost basis.
                let new_qty = held + fe.qty;
                if new_qty > 0.0 {
                    acq_time = (held * acq_time + fe.qty * fe.ts as f64) / new_qty;
                }
                position.apply(&fill)
            }
            OrderSide::Sell => {
                let r = position.apply(&fill);
                sells += 1;
                let closed = fe.qty.min(held);
                hold_num += (fe.ts as f64 - acq_time) * closed;
                hold_den += closed;
                if r > 0.0 {
                    wins.push(r);
                } else if r < 0.0 {
                    losses.push(r);
                }
                r
            }
        };

        rows.push(BlotterRow {
            ts: fe.ts,
            side: fe.side,
            price: fe.price,
            qty: fe.qty,
            position_after: position.quantity(),
            realized,
        });
    }

    let mean = |xs: &[f64]| {
        if xs.is_empty() {
            0.0
        } else {
            xs.iter().sum::<f64>() / xs.len() as f64
        }
    };
    let stats = TradeStats {
        round_trips: sells,
        win_rate: if sells > 0 {
            wins.len() as f64 / sells as f64
        } else {
            0.0
        },
        avg_win: mean(&wins),
        avg_loss: mean(&losses),
        avg_hold_us: if hold_den > 0.0 {
            hold_num / hold_den
        } else {
            0.0
        },
    };
    (rows, stats)
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
