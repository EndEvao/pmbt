//! The Polymarket CLOB websocket wire spec, as types.
//!
//! One websocket frame is either a single event object or an array of them
//! (arrays appear on subscribe, carrying the initial `book` snapshots).
//! [`decode_frame`] accepts both and is the exact entry point a live socket
//! adapter will call on raw frames; recorded captures reach it through
//! [`CaptureRecord`].
//!
//! Decode is *structure only*: every price/size/timestamp stays the string the
//! exchange sent. Numeric conversion (and its validation) happens downstream —
//! prices through the strict `Price::parse_decimal` door.
//!
//! An unknown `event_type` is a loud [`DecodeError`], not a skipped message:
//! a new event on the wire is information we want to hear about, not lose.

use serde::Deserialize;
use thiserror::Error;

/// One line of a recorded capture file: the receive-side envelope our
/// recorder wraps around each raw websocket frame. Replay-only — a live
/// adapter has the frame in hand and calls [`decode_frame`] directly.
#[derive(Debug, Clone, Deserialize)]
pub struct CaptureRecord {
    /// Local receive time, float seconds since the epoch.
    pub recv_t: f64,
    /// Local receive time, ISO-8601 (`"2026-06-17T20:15:41Z"`).
    pub recv_iso: String,
    /// The websocket frame, verbatim.
    pub raw: String,
}

/// One CLOB websocket event, tagged by its `event_type` field.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum ClobEvent {
    /// Full book snapshot for one asset — sent on subscribe and as periodic
    /// resyncs. Replaces any book state built so far.
    Book(BookEvent),
    /// Level deltas; the only other event that mutates a book. One event can
    /// carry changes for several assets of the same market.
    PriceChange(PriceChangeEvent),
    /// The exchange's own statement of an asset's touch. Book-neutral, but
    /// precious: it is the oracle the book builder verifies itself against.
    BestBidAsk(BestBidAskEvent),
    /// A trade printed. Book-neutral (resting liquidity already updated via
    /// `price_change`); decoded for completeness and future trades use.
    LastTradePrice(LastTradePriceEvent),
    /// The market's price grid changed. Book-neutral.
    TickSizeChange(TickSizeChangeEvent),
}

/// One resting level as it appears on the wire. Ladders in `book` events
/// arrive sorted *away* from the touch — order is not meaningful here.
#[derive(Debug, Clone, Deserialize)]
pub struct WireLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BookEvent {
    /// The `0x…` condition id.
    pub market: String,
    /// The token id — the instrument this book belongs to.
    pub asset_id: String,
    /// Exchange time, integer milliseconds as a string.
    pub timestamp: String,
    pub bids: Vec<WireLevel>,
    pub asks: Vec<WireLevel>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceChangeEvent {
    pub market: String,
    pub timestamp: String,
    pub price_changes: Vec<PriceLevelChange>,
}

/// One level delta: `size` is the new *absolute* size at `price` (`"0"`
/// removes the level). `best_bid`/`best_ask` state the touch *after* this
/// change — the inline oracle.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceLevelChange {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: Side,
    /// `""` when the side is empty.
    pub best_bid: String,
    /// `""` when the side is empty.
    pub best_ask: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BestBidAskEvent {
    pub market: String,
    pub asset_id: String,
    /// `""` when the side is empty.
    pub best_bid: String,
    /// `""` when the side is empty.
    pub best_ask: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LastTradePriceEvent {
    pub market: String,
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: Side,
    pub timestamp: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TickSizeChangeEvent {
    pub market: String,
    pub asset_id: String,
    pub old_tick_size: String,
    pub new_tick_size: String,
    pub timestamp: String,
}

/// Why a frame couldn't be decoded.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("frame is not valid CLOB JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// Decode one websocket frame into its events — a single object or an array
/// of them (the subscribe-time shape).
pub fn decode_frame(raw: &str) -> Result<Vec<ClobEvent>, DecodeError> {
    let trimmed = raw.trim_start();
    if trimmed.starts_with('[') {
        Ok(serde_json::from_str(trimmed)?)
    } else {
        Ok(vec![serde_json::from_str(trimmed)?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Field-for-field shapes of real captured events (ids shortened).
    const BOOK: &str = r#"{"market":"0xb02c","asset_id":"1049","timestamp":"1781726403407","hash":"77f6","bids":[{"price":"0.08","size":"20"}],"asks":[{"price":"0.999","size":"65"},{"price":"0.95","size":"5"}],"tick_size":"0.01","event_type":"book","last_trade_price":""}"#;
    const PRICE_CHANGE: &str = r#"{"market":"0xb361","price_changes":[{"asset_id":"2413","price":"0.42","size":"1271039.8","side":"BUY","hash":"5301","best_bid":"0.42","best_ask":"0.43"},{"asset_id":"9262","price":"0.58","size":"0","side":"SELL","hash":"aa00","best_bid":"0.57","best_ask":""}],"timestamp":"1781722981400","event_type":"price_change"}"#;
    const BEST_BID_ASK: &str = r#"{"market":"0xb361","asset_id":"4551","best_bid":"0.57","best_ask":"0.58","spread":"0.01","timestamp":"1781722981400","event_type":"best_bid_ask"}"#;
    const LAST_TRADE: &str = r#"{"market":"0xb361","asset_id":"4551","price":"0.58","size":"3.206895","fee_rate_bps":"0","side":"BUY","timestamp":"1781722981407","event_type":"last_trade_price","transaction_hash":"0xabc"}"#;
    const TICK_SIZE: &str = r#"{"market":"0xb361","asset_id":"4551","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1781733118545","event_type":"tick_size_change"}"#;

    #[test]
    fn decodes_every_event_type() {
        let book = &decode_frame(BOOK).unwrap()[0];
        let ClobEvent::Book(b) = book else {
            panic!("expected Book, got {book:?}")
        };
        assert_eq!(b.asset_id, "1049");
        assert_eq!(b.timestamp, "1781726403407");
        assert_eq!(b.bids.len(), 1);
        assert_eq!(b.asks[1].size, "5");

        let pc = &decode_frame(PRICE_CHANGE).unwrap()[0];
        let ClobEvent::PriceChange(pc) = pc else {
            panic!("expected PriceChange, got {pc:?}")
        };
        assert_eq!(pc.price_changes.len(), 2);
        assert_eq!(pc.price_changes[0].side, Side::Buy);
        assert_eq!(pc.price_changes[1].size, "0");
        assert_eq!(pc.price_changes[1].best_ask, ""); // empty side

        assert!(matches!(
            &decode_frame(BEST_BID_ASK).unwrap()[0],
            ClobEvent::BestBidAsk(e) if e.best_bid == "0.57"
        ));
        assert!(matches!(
            &decode_frame(LAST_TRADE).unwrap()[0],
            ClobEvent::LastTradePrice(e) if e.side == Side::Buy
        ));
        assert!(matches!(
            &decode_frame(TICK_SIZE).unwrap()[0],
            ClobEvent::TickSizeChange(e) if e.new_tick_size == "0.001"
        ));
    }

    #[test]
    fn decodes_array_frames() {
        let frame = format!("[{BOOK},{BEST_BID_ASK}]");
        let events = decode_frame(&frame).unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], ClobEvent::Book(_)));
        assert!(matches!(events[1], ClobEvent::BestBidAsk(_)));
    }

    #[test]
    fn unknown_event_type_is_loud() {
        let unknown = r#"{"event_type":"mystery_event","asset_id":"1"}"#;
        assert!(matches!(decode_frame(unknown), Err(DecodeError::Json(_))));
    }

    #[test]
    fn malformed_json_is_loud() {
        assert!(decode_frame("{not json").is_err());
        assert!(decode_frame("").is_err());
    }

    #[test]
    fn parses_capture_envelope() {
        let line = r#"{"recv_t": 1781727341.561, "recv_iso": "2026-06-17T20:15:41Z", "raw": "[]"}"#;
        let record: CaptureRecord = serde_json::from_str(line).unwrap();
        assert!((record.recv_t - 1781727341.561).abs() < 1e-9);
        assert_eq!(&record.recv_iso[..10], "2026-06-17");
        assert_eq!(record.raw, "[]");
    }
}
