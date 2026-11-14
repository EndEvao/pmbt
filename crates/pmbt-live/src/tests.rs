use super::*;
use crate::record::{replay, Capture};
use pmbt::{
    domain::{
        order::{OrderRequest, TimeInForce},
        types::MarketId,
    },
    portfolio::Portfolio,
    report::record::Recorder,
    strategy::{Action, OpenOrder},
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) fn config() -> Config {
    Config {
        yes_asset: "yes".into(),
        no_asset: "no".into(),
        strategy: "buyandhold".into(),
        capital: 100.,
        size: 10.,
        max_inventory: 50.,
        fee_rate: 0.,
        transit_ms: 0,
        taker_delay_ms: 0,
    }
}
struct Once(bool);
impl Strategy for Once {
    fn on_market_state(&mut self, b: &MarketState, _: &Portfolio, _: &[OpenOrder]) -> Vec<Action> {
        if self.0 {
            return vec![];
        }
        self.0 = true;
        vec![Action::Place(OrderRequest::market(
            b.market_id().clone(),
            OrderSide::Buy,
            10.,
            TimeInForce::Fok,
        ))]
    }
}
pub(crate) fn driver() -> Driver {
    Driver::new(config(), [Box::new(Once(false)), Box::new(Once(false))]).unwrap()
}
pub(crate) fn book(asset: &str, condition: &str) -> serde_json::Value {
    serde_json::json!({"event_type":"book","market":condition,"asset_id":asset,"timestamp":"1000",
        "bids":[{"price":"0.4","size":"20"}],"asks":[{"price":"0.5","size":"100"}]})
}
pub(crate) fn books() -> String {
    serde_json::json!([book("yes", "condition"), book("no", "condition")]).to_string()
}
pub(crate) fn temp() -> PathBuf {
    static ID: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "pmbt-paper-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        ID.fetch_add(1, Ordering::SeqCst)
    ))
}
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn tls_crypto_provider_is_available_for_secure_websockets() {
    let _ = rustls::ClientConfig::builder();
}
#[derive(Default)]
struct Null;
impl Recorder for Null {}
impl PaperRecorder for Null {
    fn on_asset(&mut self, _: &MarketId) {}
}

#[test]
fn waits_for_both_outcomes_and_rejects_a_different_condition() {
    let mut d = driver();
    let mut r = Null;
    d.apply(&Input::Connected { at_us: 1 }, &mut r).unwrap();
    d.apply(
        &Input::Frame {
            at_us: 2,
            received_us: 2,
            raw: book("yes", "condition").to_string(),
        },
        &mut r,
    )
    .unwrap();
    assert!(!d.status().ready);
    assert_eq!(d.status().cash, 100.);
    assert!(d
        .apply(
            &Input::Frame {
                at_us: 3,
                received_us: 3,
                raw: book("no", "different").to_string()
            },
            &mut r
        )
        .is_err());
    assert_eq!(d.status().cash, 100.);
}

#[test]
fn normalized_capture_replays_identical_fills_and_status_across_gap() {
    let a = temp();
    let b = temp();
    let mut d = driver();
    let mut out = Capture::create(&a, &config()).unwrap();
    let inputs = vec![
        Input::Connected { at_us: 1 },
        Input::Frame {
            at_us: 2,
            received_us: 2,
            raw: books(),
        },
        Input::Gap {
            at_us: 3,
            reason: "test disconnect".into(),
        },
        Input::Tick { at_us: 4 },
        Input::Connected { at_us: 5 },
        Input::Frame {
            at_us: 6,
            received_us: 6,
            raw: books(),
        },
        Input::Stop { at_us: 7 },
    ];
    for i in inputs {
        out.input(&i).unwrap();
        d.apply(&i, &mut out).unwrap();
    }
    let expected = d.status();
    out.finish(&expected, "stopped").unwrap();
    assert_eq!(expected.cash, 90.);
    assert_eq!(expected.gaps, 1);
    let mut copy = Capture::create(&b, &config()).unwrap();
    let actual = replay(&a, &mut driver(), &mut copy).unwrap();
    assert_eq!(actual, expected);
    copy.finish(&actual, "stopped").unwrap();
    assert_eq!(
        fs::read(a.join("events-000000.jsonl")).unwrap(),
        fs::read(b.join("events-000000.jsonl")).unwrap()
    );
    fs::remove_dir_all(a).unwrap();
    fs::remove_dir_all(b).unwrap();
}

#[test]
fn active_or_incomplete_capture_is_not_misrepresented_as_recoverable() {
    let a = temp();
    let b = temp();
    let _active = Capture::create(&a, &config()).unwrap();
    assert!(Capture::create(&a, &config()).is_err());
    let mut out = Capture::create(&b, &config()).unwrap();
    assert!(replay(&a, &mut driver(), &mut out).is_err());
    fs::remove_dir_all(a).unwrap();
    fs::remove_dir_all(b).unwrap();
}

#[test]
fn untrusted_book_requests_resync_and_freezes_execution() {
    let mut d = driver();
    let mut r = Null;
    d.apply(
        &Input::Frame {
            at_us: 1,
            received_us: 1,
            raw: books(),
        },
        &mut r,
    )
    .unwrap();
    let raw=serde_json::json!({"event_type":"best_bid_ask","market":"condition","asset_id":"yes","timestamp":"1001","best_bid":"0.2","best_ask":"0.5"}).to_string();
    let error = d
        .apply(
            &Input::Frame {
                at_us: 2,
                received_us: 2,
                raw,
            },
            &mut r,
        )
        .unwrap_err();
    assert!(error.downcast_ref::<ResyncRequired>().is_some());
    assert!(!d.status().ready);
    assert_eq!(d.status().cash, 90.);
}
