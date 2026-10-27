//! Public construction boundary used by custom feeds and external strategy tests.
use pmbt::domain::{
    marketstate::{BookError, Level, MarketState},
    types::{MarketId, Price},
};

#[test]
fn external_levels_reject_invalid_sizes_and_form_usable_books() {
    let price = Price::parse_decimal("0.5").unwrap();
    for size in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(
            Level::new(price, size),
            Err(BookError::InvalidLevelSize(_))
        ));
    }
    let empty = Level::new(price, 0.0).unwrap();
    assert_eq!(empty.size(), 0.0);
    let book = MarketState::new(
        MarketId::new("external".into()),
        123,
        Box::from([]),
        Box::from([Level::new(price, 25.0).unwrap()]),
    )
    .unwrap();
    assert_eq!(book.ask().unwrap().price(), price);
    assert_eq!(book.ask().unwrap().size(), 25.0);
    assert_eq!(book.market_id().as_str(), "external");
    assert_eq!(book.timestamp(), 123);
}
