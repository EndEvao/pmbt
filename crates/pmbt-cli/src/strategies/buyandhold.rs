use pmbt::domain::marketstate::MarketState;
use pmbt::domain::order::{OrderRequest, OrderSide, TimeInForce};
use pmbt::portfolio::Portfolio;
use pmbt::strategy::{Action, OpenOrder, Strategy};

/// Buys `size` shares on the very first snapshot, then holds forever.
///
/// The baseline strategy: exactly one order at a known time and size, so a
/// run's cash, position, and P&L can be checked by hand — and every real
/// strategy has this to beat.
pub struct BuyAndHold {
    size: f64,
    done: bool,
}

impl BuyAndHold {
    pub fn new(size: f64) -> Self {
        Self { size, done: false }
    }
}

impl Strategy for BuyAndHold {
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
            state.market_id().clone(),
            OrderSide::Buy,
            self.size,
            TimeInForce::Fok,
        ))]
    }
}
