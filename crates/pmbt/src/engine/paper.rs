//! Serial multi-asset paper execution with one cash and reservation ledger.
//!
//! Instrument contexts are moved into the existing execution kernel for a step.
//! Only book/order/timer state is switched; portfolio, reservations and order IDs
//! always have exactly one owner. Historical and paper fills use the same code.
use super::*;
use serde::Serialize;

/// Recorder whose callbacks can be attributed to the active outcome.
pub trait PaperRecorder: Recorder {
    fn on_asset(&mut self, asset: &MarketId);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{domain::marketstate::Level, fill::sweep::SweepFillModel};

    struct Once {
        done: bool,
        limit: bool,
        target: Option<&'static str>,
    }
    impl Strategy for Once {
        fn on_market_state(
            &mut self,
            b: &MarketState,
            _: &Portfolio,
            _: &[OpenOrder],
        ) -> Vec<Action> {
            if self.done {
                return vec![];
            }
            self.done = true;
            let id = self
                .target
                .map(|s| MarketId::new(s.into()))
                .unwrap_or_else(|| b.market_id().clone());
            vec![Action::Place(if self.limit {
                OrderRequest::limit(
                    id,
                    OrderSide::Buy,
                    10.,
                    Price::from_probability(0.4).unwrap(),
                    TimeInForce::Gtc,
                )
            } else {
                OrderRequest::market(id, OrderSide::Buy, 10., TimeInForce::Fok)
            })]
        }
    }
    #[derive(Default)]
    struct Log {
        asset: String,
        fills: Vec<(String, f64)>,
        orders: Vec<u64>,
    }
    impl Recorder for Log {
        fn on_fill(&mut self, _: i64, _: OrderSide, f: &Fill) {
            self.fills.push((self.asset.clone(), f.quantity()));
        }
        fn on_order(&mut self, o: &OrderEvent) {
            if matches!(o.kind, OrderEventKind::Placed) {
                self.orders.push(o.id);
            }
        }
    }
    impl PaperRecorder for Log {
        fn on_asset(&mut self, id: &MarketId) {
            self.asset = id.as_str().into();
        }
    }
    fn book(id: &str, at: i64, depth: f64) -> MarketState {
        MarketState::new(
            MarketId::new(id.into()),
            at,
            vec![Level::new(Price::from_probability(0.4).unwrap(), depth).unwrap()]
                .into_boxed_slice(),
            vec![Level::new(Price::from_probability(0.5).unwrap(), 100.).unwrap()]
                .into_boxed_slice(),
        )
        .unwrap()
    }
    fn pair(at: i64, depth: f64) -> [MarketState; 2] {
        [book("yes", at, depth), book("no", at, depth)]
    }
    fn trade(id: &str, at: i64, qty: f64) -> Event {
        Event::Trade(
            Trade::try_new(
                at,
                Price::from_probability(0.4).unwrap(),
                qty,
                OrderSide::Sell,
                MarketId::new(id.into()),
            )
            .unwrap(),
        )
    }
    fn session(capital: f64, limit: bool, transit: u64) -> PaperSession {
        let mut sim = Simulation::zero();
        sim.latency.transit = std::time::Duration::from_micros(transit);
        PaperSession::new(
            capital,
            Box::new(SweepFillModel),
            sim,
            ["yes", "no"]
                .into_iter()
                .map(|id| {
                    (
                        MarketId::new(id.into()),
                        Box::new(Once {
                            done: false,
                            limit,
                            target: None,
                        }) as Box<dyn Strategy>,
                    )
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn both_outcomes_use_one_cash_ledger_and_unique_ids() {
        let mut s = session(10., false, 0);
        let mut log = Log::default();
        s.synchronize(pair(10, 10.), &mut log).unwrap();
        let status = s.status();
        assert_eq!(status.cash, 0.);
        assert_eq!(
            status.assets.iter().map(|a| a.quantity).collect::<Vec<_>>(),
            vec![10., 10.]
        );
        assert_eq!(log.fills, vec![("yes".into(), 10.), ("no".into(), 10.)]);
        assert_ne!(log.orders[0], log.orders[1]);
        let mut poor = session(6., false, 0);
        poor.synchronize(pair(10, 10.), &mut Log::default())
            .unwrap();
        assert_eq!(poor.status().cash, 1.);
        assert_eq!(poor.status().assets[1].quantity, 0.);
        assert_eq!(poor.status().assets[1].rejections, 1);
    }

    #[test]
    fn reservations_prevent_the_other_outcome_spending_committed_cash() {
        let mut s = session(5., true, 100);
        let mut log = Log::default();
        s.synchronize(pair(10, 10.), &mut log).unwrap();
        assert_eq!(s.status().reserved_cash, 4.);
        assert_eq!(s.status().assets[1].rejections, 1);
        s.advance_to(110, &mut log).unwrap();
        assert_eq!(s.status().reserved_cash, 4.);
    }

    #[test]
    fn trades_cannot_fill_orders_on_the_other_outcome() {
        let mut s = session(20., true, 0);
        let mut log = Log::default();
        s.synchronize(pair(10, 0.), &mut log).unwrap();
        s.process(trade("no", 20, 10.), &mut log).unwrap();
        assert_eq!(s.status().assets[0].quantity, 0.);
        assert_eq!(s.status().assets[1].quantity, 10.);
        assert_eq!(log.fills, vec![("no".into(), 10.)]);
    }

    #[test]
    fn gap_retains_orders_cash_and_positions_but_reseeds_queues() {
        let mut s = session(20., true, 0);
        let mut log = Log::default();
        s.synchronize(pair(10, 10.), &mut log).unwrap();
        s.process(trade("yes", 20, 5.), &mut log).unwrap();
        let before = s.status();
        s.suspend(30).unwrap();
        s.process(trade("yes", 40, 100.), &mut log).unwrap();
        s.advance_to(50, &mut log).unwrap();
        assert_eq!(s.status().cash, before.cash);
        assert_eq!(s.status().reserved_cash, before.reserved_cash);
        assert_eq!(s.status().equity, None);
        s.synchronize(pair(60, 20.), &mut log).unwrap();
        s.process(trade("yes", 70, 15.), &mut log).unwrap();
        assert!(log.fills.is_empty());
        s.process(trade("yes", 80, 10.), &mut log).unwrap();
        assert_eq!(log.fills, vec![("yes".into(), 5.)]);
        assert_eq!(s.status().assets[0].quantity, 5.);
        assert_eq!(s.status().gaps, 1);
    }

    #[test]
    fn delayed_orders_execute_on_quiet_market_and_freeze_during_gap() {
        let mut s = session(10., false, 100);
        let mut log = Log::default();
        s.synchronize(pair(10, 0.), &mut log).unwrap();
        assert_eq!(s.next_deadline(), Some(110));
        s.advance_to(109, &mut log).unwrap();
        assert!(log.fills.is_empty());
        s.suspend(109).unwrap();
        s.advance_to(500, &mut log).unwrap();
        assert!(log.fills.is_empty());
        s.synchronize(pair(600, 0.), &mut log).unwrap();
        assert_eq!(log.fills.len(), 2);
        assert_eq!(s.status().cash, 0.);
    }

    #[test]
    fn backwards_time_and_foreign_assets_are_rejected() {
        let mut s = session(10., false, 100);
        let mut log = Log::default();
        s.synchronize(pair(10, 0.), &mut log).unwrap();
        assert!(s.process(trade("third", 11, 10.), &mut log).is_err());
        assert!(s.advance_to(9, &mut log).is_err());
        assert_eq!(s.status().cash, 10.);
    }

    #[test]
    fn pending_gtd_that_expires_during_gap_never_fills_after_resync() {
        struct Expiring(bool);
        impl Strategy for Expiring {
            fn on_market_state(
                &mut self,
                b: &MarketState,
                _: &Portfolio,
                _: &[OpenOrder],
            ) -> Vec<Action> {
                if self.0 {
                    return vec![];
                }
                self.0 = true;
                vec![Action::Place(OrderRequest::limit(
                    b.market_id().clone(),
                    OrderSide::Buy,
                    10.,
                    Price::from_probability(0.6).unwrap(),
                    TimeInForce::Gtd {
                        expires_at: b.timestamp() + 180_000_000,
                    },
                ))]
            }
        }
        let mut s = session(20., false, 1_000_000);
        let mut log = Log::default();
        s.instruments[0].strategy = Box::new(Expiring(false));
        s.synchronize(pair(10, 0.), &mut log).unwrap();
        assert_eq!(s.status().reserved_cash, 6.);
        s.suspend(20).unwrap();
        s.synchronize(pair(200_000_000, 0.), &mut log).unwrap();
        assert_eq!(s.status().assets[0].quantity, 0.);
        assert_eq!(s.status().assets[0].open_orders, 0);
        assert_eq!(s.status().reserved_cash, 0.);
        assert_eq!(s.instruments[0].run.tally.expired_gtd, 1);
        assert_eq!(log.fills, vec![("no".into(), 10.)]);
    }

    #[test]
    fn strategy_cannot_price_another_outcome_against_current_book() {
        let mut s = session(10., false, 0);
        s.instruments[0].strategy = Box::new(Once {
            done: false,
            limit: false,
            target: Some("no"),
        });
        s.synchronize(pair(10, 0.), &mut Log::default()).unwrap();
        assert_eq!(s.status().assets[0].rejections, 1);
        assert_eq!(s.status().assets[0].quantity, 0.);
        assert_eq!(s.status().cash, 5.);
    }
}

struct Instrument {
    id: MarketId,
    strategy: Box<dyn Strategy>,
    run: RunState,
    resting: OpenOrders,
    cancels: VecDeque<(OrderId, i64)>,
    wakeups: BTreeSet<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AssetStatus {
    pub asset_id: String,
    pub quantity: f64,
    pub cost_basis: f64,
    pub mark: Option<f64>,
    pub mark_timestamp_us: Option<i64>,
    pub open_orders: usize,
    pub fills: u64,
    pub rejections: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PaperStatus {
    pub timestamp_us: i64,
    pub ready: bool,
    pub data_valid: bool,
    pub initial_capital: f64,
    pub cash: f64,
    pub reserved_cash: f64,
    pub realized_pnl: f64,
    pub fees: f64,
    /// None during gaps or when any held asset cannot be marked.
    pub equity: Option<f64>,
    pub total_pnl: Option<f64>,
    pub gaps: u64,
    pub assets: Vec<AssetStatus>,
}

/// Live observation clock: timestamps are local normalized receive times.
/// Artificial feed/perception delay is forbidden to avoid charging it twice.
pub struct PaperSession {
    core: Engine,
    instruments: Vec<Instrument>,
    now: i64,
    ready: bool,
    data_valid: bool,
    gaps: u64,
}

impl PaperSession {
    pub fn new(
        capital: f64,
        fill: Box<dyn FillModel>,
        sim: Simulation,
        strategies: Vec<(MarketId, Box<dyn Strategy>)>,
    ) -> Result<Self, String> {
        if !capital.is_finite() || capital <= 0.0 {
            return Err("capital must be finite and positive".into());
        }
        if sim.perception || !sim.latency.feed.is_zero() {
            return Err("live observation requires perception=false and feed latency=0".into());
        }
        if !sim.fee_rate.is_finite() || sim.fee_rate < 0.0 {
            return Err("fee rate must be finite and nonnegative".into());
        }
        if strategies.len() != 2 || strategies[0].0 == strategies[1].0 {
            return Err("paper trading requires distinct YES and NO assets".into());
        }
        let instruments = strategies
            .into_iter()
            .map(|(id, strategy)| Instrument {
                id,
                strategy,
                run: RunState::default(),
                resting: OpenOrders::new(),
                cancels: VecDeque::new(),
                wakeups: BTreeSet::new(),
            })
            .collect();
        Ok(Self {
            core: Engine::new(capital, fill, sim),
            instruments,
            now: 0,
            ready: false,
            data_valid: false,
            gaps: 0,
        })
    }

    fn with_instrument<T>(
        &mut self,
        index: usize,
        f: impl FnOnce(&mut Engine, &mut Instrument) -> T,
    ) -> T {
        let i = &mut self.instruments[index];
        std::mem::swap(&mut self.core.resting, &mut i.resting);
        std::mem::swap(&mut self.core.pending_cancels, &mut i.cancels);
        std::mem::swap(&mut self.core.wakeups, &mut i.wakeups);
        let result = f(&mut self.core, i);
        std::mem::swap(&mut self.core.resting, &mut i.resting);
        std::mem::swap(&mut self.core.pending_cancels, &mut i.cancels);
        std::mem::swap(&mut self.core.wakeups, &mut i.wakeups);
        result
    }

    fn clock(&mut self, now: i64) -> std::io::Result<()> {
        if now < self.now {
            return Err(std::io::Error::other("paper clock moved backwards"));
        }
        self.now = now;
        Ok(())
    }

    fn step(
        &mut self,
        index: usize,
        event: Event,
        recorder: &mut dyn PaperRecorder,
    ) -> std::io::Result<()> {
        self.with_instrument(index, |core, i| {
            recorder.on_asset(&i.id);
            core.step(&mut i.run, event, i.strategy.as_mut(), recorder)
        })
    }

    /// Freeze executions, retaining positions, strategy memory and working orders.
    pub fn suspend(&mut self, now: i64) -> std::io::Result<()> {
        self.clock(now)?;
        if self.ready {
            self.gaps += 1;
        }
        self.ready = false;
        self.data_valid = false;
        Ok(())
    }

    /// End observation without manufacturing a connection gap or settlement.
    pub fn stop(&mut self, now: i64) -> std::io::Result<()> {
        self.clock(now)?;
        self.ready = false;
        Ok(())
    }

    /// Both fresh books must be supplied together before resuming either asset.
    /// No fills are invented for the gap. Queue estimates restart behind depth;
    /// overdue cancels/expiries are applied by the normal engine before execution.
    pub fn synchronize(
        &mut self,
        books: [MarketState; 2],
        recorder: &mut dyn PaperRecorder,
    ) -> std::io::Result<()> {
        let now = books[0].timestamp;
        if books[1].timestamp != now
            || books
                .iter()
                .zip(&self.instruments)
                .any(|(b, i)| b.market_id != i.id)
        {
            return Err(std::io::Error::other(
                "synchronize requires ordered YES/NO books at one time",
            ));
        }
        self.clock(now)?;
        self.ready = false;
        self.data_valid = false;
        for (index, book) in books.into_iter().enumerate() {
            self.instruments[index]
                .resting
                .reseed(&book, self.core.sim.queue);
            // Missed strategy wakeups are collapsed into the fresh book callback.
            self.instruments[index].wakeups.retain(|&at| at > now);
            self.step(index, Event::Book(book), recorder)?;
        }
        self.ready = true;
        self.data_valid = true;
        Ok(())
    }

    /// Earliest pending deadline; ties are stable in configured YES/NO order.
    pub fn next_deadline(&mut self) -> Option<i64> {
        if !self.ready {
            return None;
        }
        self.deadline().map(|(at, _)| at)
    }

    fn deadline(&mut self) -> Option<(i64, usize)> {
        (0..self.instruments.len())
            .filter_map(|index| {
                self.with_instrument(index, |core, i| {
                    core.next_deadline(&i.run.pending, &i.run.unseen, 0, i.run.last_ts)
                })
                .map(|at| (at, index))
            })
            .min()
    }

    /// Market input wins a same-time tie against the affected asset's timers.
    pub fn process(
        &mut self,
        event: Event,
        recorder: &mut dyn PaperRecorder,
    ) -> std::io::Result<()> {
        let (now, asset) = match &event {
            Event::Book(b) => (b.timestamp, &b.market_id),
            Event::Trade(t) => (t.timestamp, &t.origin_asset_id),
            Event::Time(now) => return self.advance_to(*now, recorder),
        };
        if now < self.now {
            return Err(std::io::Error::other("paper clock moved backwards"));
        }
        let index = self
            .instruments
            .iter()
            .position(|i| &i.id == asset)
            .ok_or_else(|| std::io::Error::other("input for unsubscribed asset"))?;
        if !self.ready {
            self.clock(now)?;
            return Ok(());
        }
        self.advance(now, false, recorder)?;
        self.clock(now)?;
        self.step(index, event, recorder)
    }

    fn advance(
        &mut self,
        now: i64,
        inclusive: bool,
        recorder: &mut dyn PaperRecorder,
    ) -> std::io::Result<()> {
        while self.ready {
            let Some((at, index)) = self.deadline() else {
                break;
            };
            if at > now || (!inclusive && at == now) {
                break;
            }
            self.step(index, Event::Time(at), recorder)?;
        }
        Ok(())
    }

    pub fn advance_to(
        &mut self,
        now: i64,
        recorder: &mut dyn PaperRecorder,
    ) -> std::io::Result<()> {
        if now < self.now {
            return Err(std::io::Error::other("paper clock moved backwards"));
        }
        self.advance(now, true, recorder)?;
        self.clock(now)
    }

    pub fn status(&self) -> PaperStatus {
        let mut equity = self.data_valid.then_some(self.core.portfolio.cash());
        let assets = self
            .instruments
            .iter()
            .map(|i| {
                let position = self.core.portfolio.position(&i.id);
                let mark = i
                    .run
                    .last_book
                    .as_ref()
                    .and_then(MarketState::mid)
                    .map(|p| p.as_f64());
                if position.quantity() > 0.0 {
                    equity = equity.zip(mark).map(|(e, m)| e + position.quantity() * m);
                }
                AssetStatus {
                    asset_id: i.id.as_str().to_owned(),
                    quantity: position.quantity(),
                    cost_basis: position.quantity() * position.average_price(),
                    mark: if self.data_valid { mark } else { None },
                    mark_timestamp_us: i.run.last_book.as_ref().map(MarketState::timestamp),
                    open_orders: i.run.pending.len() + i.resting.len(),
                    fills: i.run.tally.fills,
                    rejections: i.run.tally.rejections,
                }
            })
            .collect();
        PaperStatus {
            timestamp_us: self.now,
            ready: self.ready,
            data_valid: self.data_valid,
            initial_capital: self.core.portfolio.initial_capital(),
            cash: self.core.portfolio.cash(),
            reserved_cash: self.core.reservations.reserved_cash(),
            realized_pnl: self
                .instruments
                .iter()
                .map(|i| i.run.tally.realized_pnl)
                .sum(),
            fees: self.instruments.iter().map(|i| i.run.tally.fees).sum(),
            equity,
            total_pnl: equity.map(|e| e - self.core.portfolio.initial_capital()),
            gaps: self.gaps,
            assets,
        }
    }
}
