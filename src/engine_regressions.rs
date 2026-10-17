use crate::{
    domain::{
        marketstate::{Level, MarketState},
        order::{OrderRequest, OrderSide, TimeInForce},
        types::{MarketId, Price},
    },
    engine::{Engine, Simulation},
    feed::MergedFeed,
    fill::sweep::SweepFillModel,
    portfolio::Portfolio,
    strategy::{Action, OpenOrder, Strategy},
};
use std::collections::BTreeMap;
fn id() -> MarketId {
    MarketId::new("token".into())
}
fn p(x: f64) -> Price {
    Price::from_probability(x).unwrap()
}
fn book(ts: i64, size: f64) -> MarketState {
    MarketState::new(
        id(),
        ts,
        Box::from([Level {
            price: p(0.4),
            size: 100.,
        }]),
        Box::from([Level {
            price: p(0.5),
            size,
        }]),
    )
    .unwrap()
}
fn buy(q: f64) -> Action {
    Action::Place(OrderRequest::market(
        id(),
        OrderSide::Buy,
        q,
        TimeInForce::Fok,
    ))
}
#[derive(Default)]
struct Script {
    books: BTreeMap<i64, Vec<Action>>,
    ticks: BTreeMap<i64, Vec<Action>>,
    woke: Vec<i64>,
}
impl Strategy for Script {
    fn on_market_state(&mut self, s: &MarketState, _: &Portfolio, _: &[OpenOrder]) -> Vec<Action> {
        self.books.remove(&s.timestamp).unwrap_or_default()
    }
    fn on_time(&mut self, s: &MarketState, _: &Portfolio, _: &[OpenOrder]) -> Vec<Action> {
        self.woke.push(s.timestamp);
        self.ticks.remove(&s.timestamp).unwrap_or_default()
    }
}
#[test]
fn clock_tick_must_not_replenish_consumed_depth() {
    let mut s = Script::default();
    s.books.insert(0, vec![buy(10.), Action::WakeAt(1)]);
    s.ticks.insert(1, vec![buy(10.)]);
    s.books.insert(10, vec![buy(10.)]);
    let feed = MergedFeed::new(
        vec![Ok(book(0, 10.)), Ok(book(10, 10.))].into_iter(),
        std::iter::empty(),
    );
    let r = Engine::new(100., Box::new(SweepFillModel), Simulation::zero())
        .run(feed, &mut s, None)
        .unwrap();
    assert_eq!(r.open_position.unwrap().quantity(), 20.);
    assert_eq!(r.rejections, 1);
}

use crate::{domain::trade::Trade, resting::QueueModel};
#[test]
fn partially_crossing_limit_must_keep_remainder_funded_after_fees() {
    let mut s = Script::default();
    s.books.insert(
        0,
        vec![Action::Place(OrderRequest::limit(
            id(),
            OrderSide::Buy,
            10.,
            p(0.5),
            TimeInForce::Gtc,
        ))],
    );
    let mut sim = Simulation::zero();
    sim.fee_rate = 0.05;
    sim.queue = QueueModel::Front;
    let trade = Trade {
        origin_asset_id: id(),
        timestamp: 1,
        side: OrderSide::Sell,
        price: p(0.5),
        size: 6.,
    };
    let feed = MergedFeed::new(
        vec![Ok(book(0, 4.)), Ok(book(2, 4.))].into_iter(),
        vec![Ok(trade)].into_iter(),
    );
    let r = Engine::new(5., Box::new(SweepFillModel), sim)
        .run(feed, &mut s, None)
        .unwrap();
    assert!(r.final_cash >= 0., "cash became {}", r.final_cash);
}

#[test]
fn funded_partial_limit_still_fills_and_rests() {
    for tif in [
        TimeInForce::Gtc,
        TimeInForce::Gtd {
            expires_at: 180_000_000,
        },
    ] {
        let mut s = Script::default();
        s.books.insert(
            0,
            vec![Action::Place(OrderRequest::limit(
                id(),
                OrderSide::Buy,
                10.,
                p(0.5),
                tif,
            ))],
        );
        let mut sim = Simulation::zero();
        sim.fee_rate = 0.05;
        let feed = MergedFeed::new(
            vec![Ok(book(0, 4.)), Ok(book(2, 4.))].into_iter(),
            std::iter::empty(),
        );
        let r = Engine::new(5.05, Box::new(SweepFillModel), sim)
            .run(feed, &mut s, None)
            .unwrap();
        assert_eq!(r.taker_fills, 1);
        assert_eq!(r.resting_quantity, 6.);
        assert!((r.final_cash - r.resting_reserved).abs() < 1e-12);
    }
}

use crate::report::record::{RecorderConfig, TimeSeriesRecorder};
use std::time::Duration;
#[test]
fn faster_limit_order_must_not_wait_behind_delayed_market_order() {
    let mut s = Script::default();
    s.books.insert(
        0,
        vec![
            buy(1.),
            Action::Place(OrderRequest::limit(
                id(),
                OrderSide::Buy,
                1.,
                p(0.5),
                TimeInForce::Fok,
            )),
        ],
    );
    let mut sim = Simulation::zero();
    sim.latency.taker_delay = Duration::from_secs(3);
    let feed = MergedFeed::new(
        vec![Ok(book(0, 100.)), Ok(book(4_000_000, 100.))].into_iter(),
        std::iter::empty(),
    );
    let mut rec = TimeSeriesRecorder::new(RecorderConfig::default());
    Engine::new(100., Box::new(SweepFillModel), sim)
        .run_recorded(feed, &mut s, None, &mut rec)
        .unwrap();
    assert_eq!(
        rec.fills().iter().map(|f| f.ts).collect::<Vec<_>>(),
        vec![0, 3_000_000]
    );
}

#[test]
fn equal_execution_deadlines_preserve_submission_order() {
    let mut s = Script::default();
    s.books.insert(0, vec![buy(3.), buy(7.)]);
    let mut sim = Simulation::zero();
    sim.latency.transit = Duration::from_micros(1);
    let feed = MergedFeed::new(
        vec![Ok(book(0, 10.)), Ok(book(2, 10.))].into_iter(),
        std::iter::empty(),
    );
    let mut rec = TimeSeriesRecorder::new(RecorderConfig::default());
    Engine::new(100., Box::new(SweepFillModel), sim)
        .run_recorded(feed, &mut s, None, &mut rec)
        .unwrap();
    assert_eq!(
        rec.fills()
            .iter()
            .map(|f| (f.ts, f.qty))
            .collect::<Vec<_>>(),
        vec![(1, 3.), (1, 7.)]
    );
}

#[test]
fn wakeup_on_book_timestamp_must_not_block_later_wakeups() {
    let mut s = Script::default();
    s.books
        .insert(0, vec![Action::WakeAt(100), Action::WakeAt(200)]);
    let feed = MergedFeed::new(
        vec![Ok(book(0, 10.)), Ok(book(100, 10.)), Ok(book(300, 10.))].into_iter(),
        std::iter::empty(),
    );
    Engine::new(100., Box::new(SweepFillModel), Simulation::zero())
        .run(feed, &mut s, None)
        .unwrap();
    assert!(
        s.woke.contains(&200),
        "wakeups actually delivered: {:?}",
        s.woke
    );
}

#[test]
fn coincident_wakeups_fire_once_with_and_without_perception() {
    for perception in [false, true] {
        let mut s = Script::default();
        s.books
            .insert(0, vec![Action::WakeAt(100), Action::WakeAt(200)]);
        let mut sim = Simulation::zero();
        sim.perception = perception;
        if perception {
            sim.latency.feed = Duration::from_micros(10);
        }
        let feed = MergedFeed::new(
            vec![Ok(book(0, 10.)), Ok(book(100, 10.)), Ok(book(300, 10.))].into_iter(),
            std::iter::empty(),
        );
        Engine::new(100., Box::new(SweepFillModel), sim)
            .run(feed, &mut s, None)
            .unwrap();
        for timestamp in [100, 200] {
            assert_eq!(s.woke.iter().filter(|&&t| t == timestamp).count(), 1);
        }
    }
}
