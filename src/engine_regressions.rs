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
