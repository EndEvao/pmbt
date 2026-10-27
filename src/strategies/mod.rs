//! Bundled CLI demonstrations. The engine accepts caller-owned strategies.
pub mod accumulator;
pub mod buyandhold;
pub mod buyandsell;
pub mod quoter;

#[cfg(test)]
mod test_helpers {
    use pmbt::{
        domain::{
            marketstate::{Level, MarketState},
            order::{Order, OrderSide},
            types::{MarketId, OrderId, Price},
        },
        fill::fillmodel::{FillModel, NaiveFillModel},
        portfolio::Portfolio,
    };
    // Seed inventory through the public fill-model/ledger APIs, just as an
    // external strategy test would. No direct construction of private fills.
    pub fn buy(portfolio: &mut Portfolio, token: &str, price: Price, size: f64) {
        let id = MarketId::new(token.into());
        let book = MarketState::new(
            id.clone(),
            1,
            Box::from([]),
            Box::from([Level::new(price, size).unwrap()]),
        )
        .unwrap();
        let order = Order::new(OrderId::new(0), 1, size, OrderSide::Buy, None);
        for fill in NaiveFillModel.fill(&order, &book).iter() {
            portfolio.apply(&id, fill);
        }
    }
}
