use crate::domain::marketstate::MarketState;
use crate::domain::order::{OrderRequest, OrderSide, TimeInForce};
use crate::portfolio::Portfolio;
use crate::strategy::{Action, OpenOrder, Strategy};

pub struct BuyAndSell {
    size: f64,
    done: bool,
}

impl BuyAndSell {
    pub fn new(size: f64) -> Self {
        Self { size, done: false }
    }
}

impl Strategy for BuyAndSell {
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
