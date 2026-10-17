//! The recording seam: a read-only observer the engine drives during a run.
//!
//! The engine emits only a scalar `BacktestResult`; everything a visual needs —
//! the price path, the mark-to-market equity curve, the resting book over time,
//! the discrete fills — lives *inside* the run loop and is otherwise discarded.
//! A `Recorder` is how it gets out without the engine growing a dependency on
//! how the data is later plotted: the engine calls two methods, both handed
//! borrowed, read-only state, and the recorder decides what to keep.
//!
//! The default methods are no-ops, so [`NullRecorder`] — what a plain `run`
//! uses — is byte-identical to a run with no recorder at all. Sampling *policy*
//! (how often to snapshot depth, how to mark equity) lives here in
//! [`TimeSeriesRecorder`], never in the engine, which stays a dumb emitter.

use serde::Serialize;

use crate::domain::fill::Fill;
use crate::domain::marketstate::MarketState;
use crate::domain::order::OrderSide;
use crate::portfolio::Portfolio;

/// A read-only observer of a backtest. The engine calls it; it never influences
/// the run — every argument arrives borrowed and immutable. Both methods
/// default to no-ops so a non-recording run pays nothing and stays identical.
pub trait Recorder {
    /// One book snapshot has been fully processed: any orders due at this
    /// timestamp have executed and the strategy has decided. `state` is the
    /// *true* book at `state.timestamp`; `portfolio` reflects every fill up to
    /// and including this instant.
    fn on_snapshot(&mut self, _state: &MarketState, _portfolio: &Portfolio, _quotes: Quotes) {}

    /// A non-book event has finished. The book is carried forward at the
    /// event's timestamp; sample the portfolio without publishing new depth.
    fn on_event(&mut self, _state: &MarketState, _portfolio: &Portfolio, _quotes: Quotes) {}

    /// A single fill just booked into the portfolio, inside the execute loop.
    /// `ts` is the execution timestamp (the snapshot it filled against, which
    /// under latency is later than the order's submission).
    fn on_fill(&mut self, _ts: i64, _side: OrderSide, _fill: &Fill) {}

    /// A point in an order's life other than a fill: accepted, reached the
    /// book, cancelled, expired. Together with `on_fill` these reconstruct the
    /// whole lifecycle, which is what maker metrics (fill ratio, time in
    /// queue) are computed from.
    fn on_order(&mut self, _event: &OrderEvent) {}
}

/// Where our own resting quotes sat at one instant: the best price we were
/// posted at on each side. Copy and tiny — sampled every snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Quotes {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
}

/// What happened to an order, other than filling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderEventKind {
    /// Accepted by the engine and sent — still inside its latency window.
    Placed,
    /// Reached the book as a maker order and began resting.
    Rested,
    /// Withdrawn by the strategy.
    Cancelled,
    /// Removed by the exchange rather than by us: a GTD order reaching its
    /// timer, or anything still open when the feed ended.
    Expired,
}

/// One moment in an order's life.
#[derive(Debug, Clone, Serialize)]
pub struct OrderEvent {
    pub ts: i64,
    /// The engine's order id, so events can be joined into per-order histories.
    pub id: u64,
    pub kind: OrderEventKind,
    pub side: &'static str,
    /// Limit price; `None` for a marketable order.
    pub price: Option<f64>,
    /// Size at this moment — the full order when placed, the unfilled
    /// remainder when cancelled or expired.
    pub quantity: f64,
}

/// The default recorder: keeps nothing, does nothing. What `Engine::run` uses,
/// so an un-recorded run is provably free of any observer effect.
pub struct NullRecorder;

impl Recorder for NullRecorder {}

/// One sampled instant of the run — a row in the price/equity time series. All
/// prices are probabilities in `[0,1]` (dollars per share); the domain's
/// fixed-point `Price` is flattened to `f64` here so the report types stay
/// plain, serializable, and decoupled from the engine's numeric model.
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// Exchange time, microseconds.
    pub ts: i64,
    /// Best bid / ask / mid of the traded token; `None` when that side is empty
    /// (books go one-sided near resolution).
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub mid: Option<f64>,
    /// Ledger cash at this instant.
    pub cash: f64,
    /// Net position in the traded token (long-only, so `>= 0`).
    pub qty: f64,
    /// Average cost of the held shares; `0.0` when flat.
    pub avg_cost: f64,
    /// Mark-to-market equity: `cash + qty * mark`, where `mark` is the best
    /// price available (mid, else last-known — see [`TimeSeriesRecorder`]). This
    /// is the continuous line the PnL curve is drawn from; it moves with the
    /// book even on snapshots where nothing traded.
    pub equity: f64,
    /// Cumulative mark-to-market PnL: `equity - initial_capital`.
    pub pnl: f64,
    /// Where OUR resting quotes sat at this instant — the best price we were
    /// posted at on each side, `None` when we had no quote there. Sampled
    /// rather than reconstructed from events so it lines up with the price
    /// path exactly; two series instead of one mark per order.
    pub quote_bid: Option<f64>,
    pub quote_ask: Option<f64>,
}

/// A discrete execution, marked on the price line.
#[derive(Debug, Clone, Serialize)]
pub struct FillEvent {
    pub ts: i64,
    /// `"buy"` or `"sell"` — serialized as a string for the report consumer.
    pub side: &'static str,
    pub price: f64,
    pub qty: f64,
}

/// One price level of a captured book, size in shares.
#[derive(Debug, Clone, Serialize)]
pub struct DepthLevel {
    pub price: f64,
    pub size: f64,
}

/// A downsampled book snapshot for the depth visual: the top-N levels of each
/// side at one instant. Captured at most once per `depth_interval_us` — a full
/// ladder every snapshot would balloon the output on a busy market.
#[derive(Debug, Clone, Serialize)]
pub struct DepthFrame {
    pub ts: i64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

/// How much of the book to keep and how often. Kept small and explicit so the
/// sampling policy is one obvious knob, not scattered magic numbers.
#[derive(Debug, Clone, Copy)]
pub struct RecorderConfig {
    /// Levels kept per side in each [`DepthFrame`].
    pub depth_levels: usize,
    /// Minimum microseconds between captured depth frames.
    pub depth_interval_us: i64,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            depth_levels: 10,
            depth_interval_us: 1_000_000, // 1 s
        }
    }
}

/// The working recorder: accumulates the full price/equity series (one
/// [`Sample`] per snapshot), every [`FillEvent`] exactly, and time-thinned
/// [`DepthFrame`]s. Marking is continuous: when a snapshot has no mid (a
/// one-sided book), equity is marked against the last price seen, so the curve
/// never gaps or teleports to zero while a position is held.
pub struct TimeSeriesRecorder {
    cfg: RecorderConfig,
    /// Captured once, from the first snapshot's portfolio.
    initial_capital: Option<f64>,
    /// Last price used to mark the book; carries a mark across one-sided
    /// snapshots so held inventory is never valued at zero by accident.
    last_mark: Option<f64>,
    last_depth_ts: Option<i64>,
    samples: Vec<Sample>,
    fills: Vec<FillEvent>,
    depth: Vec<DepthFrame>,
    orders: Vec<OrderEvent>,
}

impl TimeSeriesRecorder {
    pub fn new(cfg: RecorderConfig) -> Self {
        Self {
            cfg,
            initial_capital: None,
            last_mark: None,
            last_depth_ts: None,
            samples: Vec::new(),
            fills: Vec::new(),
            depth: Vec::new(),
            orders: Vec::new(),
        }
    }

    pub fn initial_capital(&self) -> f64 {
        self.initial_capital.unwrap_or(0.0)
    }

    pub fn samples(&self) -> &[Sample] {
        &self.samples
    }

    pub fn fills(&self) -> &[FillEvent] {
        &self.fills
    }

    pub fn depth(&self) -> &[DepthFrame] {
        &self.depth
    }

    pub fn orders(&self) -> &[OrderEvent] {
        &self.orders
    }

    /// Capture the top-N levels of a side into plain `f64` pairs.
    fn levels(&self, book: &[crate::domain::marketstate::Level]) -> Vec<DepthLevel> {
        book.iter()
            .take(self.cfg.depth_levels)
            .map(|level| DepthLevel {
                price: level.price.as_f64(),
                size: level.size,
            })
            .collect()
    }

    fn sample(&mut self, state: &MarketState, portfolio: &Portfolio, quotes: Quotes) {
        let initial = *self
            .initial_capital
            .get_or_insert_with(|| portfolio.initial_capital());

        let position = portfolio.position(&state.market_id);
        let qty = position.quantity();
        let cash = portfolio.cash();

        let bid = state.bid().map(|level| level.price.as_f64());
        let ask = state.ask().map(|level| level.price.as_f64());
        let mid = state.mid().map(|price| price.as_f64());

        // Mark against the freshest price available, remembering it so a later
        // one-sided snapshot can still value inventory. `mid` first (both sides
        // present), then whichever touch exists, then the carried mark.
        let mark = mid.or(bid).or(ask).or(self.last_mark);
        if let Some(mark) = mark {
            self.last_mark = Some(mark);
        }
        let equity = cash + qty * mark.unwrap_or(0.0);

        self.samples.push(Sample {
            ts: state.timestamp,
            bid,
            ask,
            mid,
            cash,
            qty,
            avg_cost: position.average_price(),
            equity,
            pnl: equity - initial,
            quote_bid: quotes.bid,
            quote_ask: quotes.ask,
        });
    }
}

impl Recorder for TimeSeriesRecorder {
    fn on_event(&mut self, state: &MarketState, portfolio: &Portfolio, quotes: Quotes) {
        self.sample(state, portfolio, quotes);
    }

    fn on_snapshot(&mut self, state: &MarketState, portfolio: &Portfolio, quotes: Quotes) {
        self.sample(state, portfolio, quotes);
        // Thin the depth capture to at most one frame per interval.
        let due = self
            .last_depth_ts
            .is_none_or(|last| state.timestamp - last >= self.cfg.depth_interval_us);
        if due {
            self.last_depth_ts = Some(state.timestamp);
            self.depth.push(DepthFrame {
                ts: state.timestamp,
                bids: self.levels(state.bids()),
                asks: self.levels(state.asks()),
            });
        }
    }

    fn on_fill(&mut self, ts: i64, side: OrderSide, fill: &Fill) {
        self.fills.push(FillEvent {
            ts,
            side: match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            price: fill.price.as_f64(),
            qty: fill.quantity,
        });
    }

    fn on_order(&mut self, event: &OrderEvent) {
        self.orders.push(event.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::marketstate::Level;
    use crate::domain::types::{MarketId, Price};

    fn snapshot(ts: i64, bid: f64, ask: f64) -> MarketState {
        let level = |p: f64| {
            Box::from([Level {
                price: Price::from_probability(p).unwrap(),
                size: 1_000.0,
            }])
        };
        MarketState::new(
            MarketId::new("token".to_string()),
            ts,
            level(bid),
            level(ask),
        )
        .unwrap()
    }

    #[test]
    fn samples_one_row_per_snapshot_with_flat_equity() {
        let mut rec = TimeSeriesRecorder::new(RecorderConfig::default());
        let portfolio = Portfolio::new(10_000.0);
        rec.on_snapshot(&snapshot(1, 0.08, 0.09), &portfolio, Quotes::default());
        rec.on_snapshot(&snapshot(2, 0.10, 0.12), &portfolio, Quotes::default());

        assert_eq!(rec.samples().len(), 2);
        // Flat: equity is just cash, PnL is zero regardless of price moves.
        assert!((rec.samples()[0].mid.unwrap() - 0.085).abs() < 1e-9);
        assert_eq!(rec.samples()[1].equity, 10_000.0);
        assert_eq!(rec.samples()[1].pnl, 0.0);
    }

    #[test]
    fn depth_is_thinned_to_the_interval() {
        let cfg = RecorderConfig {
            depth_levels: 10,
            depth_interval_us: 1_000_000,
        };
        let mut rec = TimeSeriesRecorder::new(cfg);
        let portfolio = Portfolio::new(10_000.0);
        // Three snapshots 0.5 s apart: frames at t=0 and t=1_000_000 only.
        rec.on_snapshot(&snapshot(0, 0.08, 0.09), &portfolio, Quotes::default());
        rec.on_snapshot(
            &snapshot(500_000, 0.08, 0.09),
            &portfolio,
            Quotes::default(),
        );
        rec.on_snapshot(
            &snapshot(1_000_000, 0.08, 0.09),
            &portfolio,
            Quotes::default(),
        );

        assert_eq!(rec.depth().len(), 2);
        assert_eq!(rec.depth()[0].ts, 0);
        assert_eq!(rec.depth()[1].ts, 1_000_000);
    }

    #[test]
    fn null_recorder_keeps_nothing() {
        let mut rec = NullRecorder;
        rec.on_snapshot(
            &snapshot(1, 0.08, 0.09),
            &Portfolio::new(10_000.0),
            Quotes::default(),
        );
        // Nothing to assert but the call compiling and being a no-op; the type
        // has no state. This documents the default-impl contract.
    }
}
