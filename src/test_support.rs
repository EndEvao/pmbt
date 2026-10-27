//! Deterministic actions for engine/ledger regression tests only.
use crate::domain::marketstate::MarketState;
use crate::domain::order::{OrderRequest, OrderSide, TimeInForce};
use crate::portfolio::Portfolio;
use crate::strategy::{Action, OpenOrder, Strategy};

pub struct BuyOnce {
    size: f64,
    done: bool,
}

impl BuyOnce {
    pub fn new(size: f64) -> Self {
        Self { size, done: false }
    }
}

impl Strategy for BuyOnce {
    fn on_market_state(
        &mut self,
        state: &MarketState,
        _portfolio: &Portfolio,
        _orders: &[OpenOrder],
    ) -> Vec<Action> {
        if self.done {
            return Vec::new();
        }
        self.done = true;
        vec![Action::Place(OrderRequest::market(
            state.market_id.clone(),
            OrderSide::Buy,
            self.size,
            TimeInForce::Fok,
        ))]
    }
}

pub struct AlternateSides {
    size: f64,
    done: bool,
}

impl AlternateSides {
    pub fn new(size: f64) -> Self {
        Self { size, done: false }
    }
}

impl Strategy for AlternateSides {
    fn on_market_state(
        &mut self,
        state: &MarketState,
        _portfolio: &Portfolio,
        _orders: &[OpenOrder],
    ) -> Vec<Action> {
        let side = if self.done {
            self.done = false;
            OrderSide::Sell
        } else {
            self.done = true;
            OrderSide::Buy
        };
        vec![Action::Place(OrderRequest::market(
            state.market_id.clone(),
            side,
            self.size,
            TimeInForce::Fok,
        ))]
    }
}
