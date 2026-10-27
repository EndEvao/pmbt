//! Book reconstruction from CLOB events.
//!
//! The websocket speaks in deltas: a `book` event carries a full ladder, then
//! `price_change` events mutate it one level at a time. [`BookBuilder`] keeps
//! a live working book per asset and, per frame, emits frozen
//! [`BookSnapshot`]s — a whole frame is folded before anything is emitted, so
//! no snapshot captures a half-applied state that never existed on the
//! exchange.
//!
//! ## Gap tolerance
//!
//! Recorded captures are missing timeframes (the recorder dropped out
//! occasionally), and deltas folded across an unrecorded gap yield silently
//! wrong books. The stream carries its own defense: every `price_change`
//! entry and every `best_bid_ask` event states the exchange's own view of the
//! touch. The builder checks itself against that oracle inline; on mismatch
//! the asset is marked **stale** — nothing is emitted for it — until the next
//! `book` event resyncs it. Assets are also born stale until their first
//! `book`. Staleness is accounted, never silent: see [`BookBuilder::gap_stats`].

use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

use crate::clob::decode::{ClobEvent, Side, WireLevel};
use crate::domain::marketstate::Level;
use crate::domain::types::{Price, PriceError};

/// A frozen per-asset book state at one frame, ladders best-first — exactly
/// one Telonex `book_snapshot_full` row. A future live adapter turns this
/// into a `MarketState` through the existing checked door.
#[derive(Debug, Clone, PartialEq)]
pub struct BookSnapshot {
    /// Token id — the instrument this book belongs to.
    pub asset_id: String,
    /// The `0x…` condition id.
    pub market: String,
    /// Exchange time, µs.
    pub timestamp_us: i64,
    /// Receive time of the frame that produced this snapshot, µs.
    pub local_timestamp_us: i64,
    /// Best-first: descending price.
    pub bids: Vec<Level>,
    /// Best-first: ascending price.
    pub asks: Vec<Level>,
}

/// One asset's live working book. `BTreeMap` keeps each side sorted at all
/// times, so emission is a straight ordered collect.
struct AssetBook {
    market: String,
    bids: BTreeMap<Price, f64>,
    asks: BTreeMap<Price, f64>,
    /// Exchange timestamp (µs) of the last event applied in the current frame.
    frame_ts_us: i64,
    /// Exchange timestamp (µs) of the last emitted snapshot — the
    /// monotonicity fence.
    last_emitted_ts_us: i64,
    /// True whenever this book cannot be trusted: before the first `book`
    /// event, and after the touch oracle caught a recording gap.
    stale: bool,
    stale_episodes: u64,
    suppressed_frames: u64,
    oracle_checks: u64,
    clamped_timestamps: u64,
}

impl AssetBook {
    fn new(market: String) -> Self {
        Self {
            market,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            frame_ts_us: i64::MIN,
            last_emitted_ts_us: i64::MIN,
            stale: true, // untrusted until the first full `book` snapshot
            stale_episodes: 0,
            suppressed_frames: 0,
            oracle_checks: 0,
            clamped_timestamps: 0,
        }
    }

    fn best_bid(&self) -> Option<Price> {
        self.bids.last_key_value().map(|(p, _)| *p)
    }

    fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }
}

/// Per-asset gap accounting, so callers can report missing timeframes loudly.
#[derive(Debug, Clone, PartialEq)]
pub struct GapReport {
    pub asset_id: String,
    /// Times the touch oracle caught the book drifting (recording gaps).
    pub stale_episodes: u64,
    /// Frames that touched the asset but were suppressed while stale.
    pub suppressed_frames: u64,
}

/// Folds CLOB events into per-asset working books and emits per-frame
/// snapshots. See the module docs for the emission and staleness rules.
#[derive(Default)]
pub struct BookBuilder {
    books: HashMap<String, AssetBook>,
}

impl BookBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one websocket frame's events and emit a snapshot per asset whose
    /// book changed (and is trusted). `local_timestamp_us` is when the caller
    /// received the frame — `recv_t` on replay, "now" on a live socket.
    pub fn apply_frame(
        &mut self,
        events: &[ClobEvent],
        local_timestamp_us: i64,
    ) -> Result<Vec<BookSnapshot>, ClobError> {
        // Assets whose book mutated this frame, in first-touch order.
        let mut touched: Vec<String> = Vec::new();
        let touch = |touched: &mut Vec<String>, asset: &str| {
            if !touched.iter().any(|a| a == asset) {
                touched.push(asset.to_string());
            }
        };

        for event in events {
            match event {
                ClobEvent::Book(book) => {
                    let ts = timestamp_us(&book.timestamp)?;
                    let entry = self
                        .books
                        .entry(book.asset_id.clone())
                        .or_insert_with(|| AssetBook::new(book.market.clone()));
                    entry.bids = ladder(&book.bids)?;
                    entry.asks = ladder(&book.asks)?;
                    entry.stale = false; // a full snapshot is ground truth
                    entry.frame_ts_us = ts;
                    touch(&mut touched, &book.asset_id);
                }
                ClobEvent::PriceChange(change) => {
                    let ts = timestamp_us(&change.timestamp)?;
                    for delta in &change.price_changes {
                        let entry = self
                            .books
                            .entry(delta.asset_id.clone())
                            .or_insert_with(|| AssetBook::new(change.market.clone()));
                        let price = Price::parse_decimal(&delta.price)?;
                        let size = parse_size(&delta.size)?;
                        let side = match delta.side {
                            Side::Buy => &mut entry.bids,
                            Side::Sell => &mut entry.asks,
                        };
                        if size == 0.0 {
                            side.remove(&price);
                        } else {
                            side.insert(price, size);
                        }
                        // Inline oracle: the delta states the touch after this
                        // change. Disagreement means events were lost — a
                        // recording gap — and the book can't be trusted until
                        // the next full snapshot. (Skipped while already
                        // stale: unverifiable either way.)
                        if !entry.stale {
                            entry.oracle_checks += 1;
                            let bid_ok = touch_matches(entry.best_bid(), &delta.best_bid)?;
                            let ask_ok = touch_matches(entry.best_ask(), &delta.best_ask)?;
                            if !(bid_ok && ask_ok) {
                                entry.stale = true;
                                entry.stale_episodes += 1;
                            }
                        }
                        entry.frame_ts_us = ts;
                        touch(&mut touched, &delta.asset_id);
                    }
                }
                // Book-neutral, but still an oracle: a touch statement that
                // disagrees with a "fresh" book reveals a gap the deltas
                // haven't exposed yet.
                ClobEvent::BestBidAsk(quote) => {
                    if let Some(entry) = self.books.get_mut(&quote.asset_id) {
                        if !entry.stale {
                            entry.oracle_checks += 1;
                            let bid_ok = touch_matches(entry.best_bid(), &quote.best_bid)?;
                            let ask_ok = touch_matches(entry.best_ask(), &quote.best_ask)?;
                            if !(bid_ok && ask_ok) {
                                entry.stale = true;
                                entry.stale_episodes += 1;
                            }
                        }
                    }
                }
                ClobEvent::LastTradePrice(_) | ClobEvent::TickSizeChange(_) => {}
            }
        }

        let mut snapshots = Vec::new();
        for asset_id in touched {
            let book = self.books.get_mut(&asset_id).expect("touched ⇒ present");
            if book.stale {
                book.suppressed_frames += 1;
                continue;
            }
            // Real captures show rare (~0.01%) per-asset stamp inversions,
            // almost all `book` resyncs delivered just after deltas bearing
            // newer stamps (observed 1–375 ms). The content is authoritative
            // — the resync is the trust anchor — so keep it and clamp the
            // label to the monotone fence, counted, never silent.
            let ts = if book.frame_ts_us < book.last_emitted_ts_us {
                book.clamped_timestamps += 1;
                book.last_emitted_ts_us
            } else {
                book.frame_ts_us
            };
            book.last_emitted_ts_us = ts;
            snapshots.push(BookSnapshot {
                asset_id,
                market: book.market.clone(),
                timestamp_us: ts,
                local_timestamp_us,
                bids: book
                    .bids
                    .iter()
                    .rev() // descending == bids best-first
                    .map(|(&price, &size)| Level { price, size })
                    .collect(),
                asks: book
                    .asks
                    .iter()
                    .map(|(&price, &size)| Level { price, size })
                    .collect(),
            });
        }
        Ok(snapshots)
    }

    /// Whether an asset's book is currently trusted. `None` if never seen.
    pub fn is_fresh(&self, asset_id: &str) -> Option<bool> {
        self.books.get(asset_id).map(|b| !b.stale)
    }

    /// Gap accounting for every asset that ever went stale after its first
    /// sync (or never got one), sorted worst-first by suppressed frames.
    pub fn gap_stats(&self) -> Vec<GapReport> {
        let mut reports: Vec<GapReport> = self
            .books
            .iter()
            .filter(|(_, b)| b.stale_episodes > 0 || b.suppressed_frames > 0)
            .map(|(asset_id, b)| GapReport {
                asset_id: asset_id.clone(),
                stale_episodes: b.stale_episodes,
                suppressed_frames: b.suppressed_frames,
            })
            .collect();
        reports.sort_by_key(|report| std::cmp::Reverse(report.suppressed_frames));
        reports
    }

    /// Total inline oracle checks performed on trusted books (across assets).
    pub fn oracle_checks(&self) -> u64 {
        self.books.values().map(|b| b.oracle_checks).sum()
    }

    /// Total snapshots whose exchange timestamp was clamped to the monotone
    /// fence (rare resync stamp inversions — see `apply_frame`).
    pub fn clamped_timestamps(&self) -> u64 {
        self.books.values().map(|b| b.clamped_timestamps).sum()
    }
}

/// Build one side's map from a wire ladder (arrival order is irrelevant —
/// the map sorts).
fn ladder(levels: &[WireLevel]) -> Result<BTreeMap<Price, f64>, ClobError> {
    levels
        .iter()
        .map(|level| {
            Ok((
                Price::parse_decimal(&level.price)?,
                parse_size(&level.size)?,
            ))
        })
        .collect()
}

fn parse_size(size: &str) -> Result<f64, ClobError> {
    size.parse()
        .map_err(|_| ClobError::InvalidSize(size.to_string()))
}

/// Exchange `timestamp` fields are integer milliseconds as strings; the
/// engine clock is µs.
fn timestamp_us(ms: &str) -> Result<i64, ClobError> {
    ms.parse::<i64>()
        .map(|ms| ms * 1_000)
        .map_err(|_| ClobError::InvalidTimestamp(ms.to_string()))
}

/// Does our touch agree with the exchange's stated one? `""` means "side is
/// empty" on the wire.
fn touch_matches(ours: Option<Price>, stated: &str) -> Result<bool, ClobError> {
    match (ours, stated) {
        (None, "") => Ok(true),
        (Some(_), "") | (None, _) => Ok(false),
        (Some(price), stated) => Ok(price == Price::parse_decimal(stated)?),
    }
}

/// Anything that can go wrong folding events into books.
#[derive(Debug, Error)]
pub enum ClobError {
    #[error("invalid price: {0}")]
    Price(#[from] PriceError),
    #[error("size {0:?} is not a decimal number")]
    InvalidSize(String),
    #[error("timestamp {0:?} is not integer milliseconds")]
    InvalidTimestamp(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clob::decode::decode_frame;

    /// Shorthand: build the frame JSON for a full book event.
    fn book_frame(asset: &str, ts: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
        let side = |levels: &[(&str, &str)]| {
            levels
                .iter()
                .map(|(p, s)| format!(r#"{{"price":"{p}","size":"{s}"}}"#))
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            r#"{{"event_type":"book","market":"0xm","asset_id":"{asset}","timestamp":"{ts}","bids":[{}],"asks":[{}]}}"#,
            side(bids),
            side(asks),
        )
    }

    /// Shorthand: a single-delta price_change frame with its oracle fields.
    #[allow(clippy::too_many_arguments)]
    fn delta_frame(
        asset: &str,
        ts: i64,
        side: &str,
        price: &str,
        size: &str,
        best_bid: &str,
        best_ask: &str,
    ) -> String {
        format!(
            r#"{{"event_type":"price_change","market":"0xm","timestamp":"{ts}","price_changes":[{{"asset_id":"{asset}","price":"{price}","size":"{size}","side":"{side}","best_bid":"{best_bid}","best_ask":"{best_ask}"}}]}}"#
        )
    }

    fn apply(builder: &mut BookBuilder, frame: &str) -> Vec<BookSnapshot> {
        builder
            .apply_frame(&decode_frame(frame).unwrap(), 7_000)
            .unwrap()
    }

    fn prices(levels: &[Level]) -> Vec<String> {
        levels.iter().map(|l| l.price.to_decimal_string()).collect()
    }

    #[test]
    fn book_event_syncs_and_away_from_touch_ladders_come_out_best_first() {
        let mut builder = BookBuilder::new();
        // Real captures list ladders away from the touch (asks descending).
        let snaps = apply(
            &mut builder,
            &book_frame(
                "a",
                1_000,
                &[("0.001", "79"), ("0.08", "20"), ("0.079", "54.99")],
                &[("0.999", "65"), ("0.95", "5"), ("0.085", "623.6")],
            ),
        );

        assert_eq!(snaps.len(), 1);
        let snap = &snaps[0];
        assert_eq!(snap.timestamp_us, 1_000_000); // ms → µs
        assert_eq!(snap.local_timestamp_us, 7_000);
        assert_eq!(prices(&snap.bids), ["0.08", "0.079", "0.001"]);
        assert_eq!(prices(&snap.asks), ["0.085", "0.95", "0.999"]);
    }

    #[test]
    fn deltas_set_overwrite_and_remove_levels() {
        let mut builder = BookBuilder::new();
        apply(
            &mut builder,
            &book_frame("a", 1_000, &[("0.08", "20")], &[("0.085", "10")]),
        );

        // Overwrite the bid size, add a deeper ask, then remove the best ask.
        let snap = &apply(
            &mut builder,
            &delta_frame("a", 2_000, "BUY", "0.08", "25", "0.08", "0.085"),
        )[0];
        assert_eq!(snap.bids[0].size, 25.0);

        apply(
            &mut builder,
            &delta_frame("a", 3_000, "SELL", "0.09", "40", "0.08", "0.085"),
        );
        let snap = &apply(
            &mut builder,
            &delta_frame("a", 4_000, "SELL", "0.085", "0", "0.08", "0.09"),
        )[0];
        assert_eq!(prices(&snap.asks), ["0.09"]);
        assert_eq!(snap.timestamp_us, 4_000_000);
    }

    #[test]
    fn born_stale_until_first_book() {
        let mut builder = BookBuilder::new();
        // Deltas for an asset we've never seen a full book for: applied but
        // untrusted — nothing comes out.
        let snaps = apply(
            &mut builder,
            &delta_frame("a", 1_000, "BUY", "0.5", "10", "0.5", ""),
        );
        assert!(snaps.is_empty());
        assert_eq!(builder.is_fresh("a"), Some(false));

        // The first full book unlocks emission.
        let snaps = apply(&mut builder, &book_frame("a", 2_000, &[("0.5", "10")], &[]));
        assert_eq!(snaps.len(), 1);
        assert_eq!(builder.is_fresh("a"), Some(true));
    }

    #[test]
    fn oracle_mismatch_quarantines_until_resync() {
        let mut builder = BookBuilder::new();
        apply(
            &mut builder,
            &book_frame("a", 1_000, &[("0.08", "20")], &[("0.085", "10")]),
        );

        // The exchange says the best bid is 0.079 — our book says 0.08, so
        // events were lost somewhere: quarantine.
        let snaps = builder
            .apply_frame(
                &decode_frame(&delta_frame(
                    "a", 2_000, "SELL", "0.09", "5", "0.079", "0.085",
                ))
                .unwrap(),
                7_000,
            )
            .unwrap();
        assert!(snaps.is_empty());
        assert_eq!(builder.is_fresh("a"), Some(false));

        // Still suppressed while stale…
        assert!(apply(
            &mut builder,
            &delta_frame("a", 3_000, "BUY", "0.07", "5", "0.079", "0.085"),
        )
        .is_empty());

        // …until a book event resyncs, and emission resumes.
        let snaps = apply(
            &mut builder,
            &book_frame("a", 4_000, &[("0.079", "9")], &[("0.085", "10")]),
        );
        assert_eq!(snaps.len(), 1);
        assert_eq!(prices(&snaps[0].bids), ["0.079"]);

        let stats = builder.gap_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].stale_episodes, 1);
        assert_eq!(stats[0].suppressed_frames, 2);
    }

    #[test]
    fn best_bid_ask_event_is_an_oracle_too() {
        let mut builder = BookBuilder::new();
        apply(
            &mut builder,
            &book_frame("a", 1_000, &[("0.08", "20")], &[("0.085", "10")]),
        );

        let quote = r#"{"event_type":"best_bid_ask","market":"0xm","asset_id":"a","best_bid":"0.07","best_ask":"0.085","spread":"0.015","timestamp":"2000"}"#;
        let snaps = builder
            .apply_frame(&decode_frame(quote).unwrap(), 7_000)
            .unwrap();
        assert!(snaps.is_empty()); // book-neutral: no emission either way
        assert_eq!(builder.is_fresh("a"), Some(false)); // but the gap is caught
    }

    #[test]
    fn multi_asset_frame_emits_one_snapshot_per_touched_asset() {
        let mut builder = BookBuilder::new();
        let initial = format!(
            "[{},{}]",
            book_frame("a", 1_000, &[("0.08", "20")], &[("0.085", "10")]),
            book_frame("b", 1_000, &[("0.4", "5")], &[("0.6", "5")]),
        );
        let snaps = apply(&mut builder, &initial);
        assert_eq!(snaps.len(), 2);

        // One price_change event carrying deltas for both assets.
        let both = r#"{"event_type":"price_change","market":"0xm","timestamp":"2000","price_changes":[
            {"asset_id":"a","price":"0.08","size":"30","side":"BUY","best_bid":"0.08","best_ask":"0.085"},
            {"asset_id":"b","price":"0.5","size":"7","side":"BUY","best_bid":"0.5","best_ask":"0.6"}]}"#;
        let snaps = builder
            .apply_frame(&decode_frame(both).unwrap(), 7_000)
            .unwrap();
        assert_eq!(snaps.len(), 2);
        assert!(snaps.iter().any(|s| s.asset_id == "a"));
        assert!(snaps.iter().any(|s| s.asset_id == "b"));
    }

    #[test]
    fn backwards_timestamps_clamp_to_monotone() {
        let mut builder = BookBuilder::new();
        apply(
            &mut builder,
            &book_frame("a", 2_000, &[("0.08", "20")], &[]),
        );

        // Same timestamp: fine (frames within a millisecond), no clamp.
        let snaps = apply(
            &mut builder,
            &delta_frame("a", 2_000, "BUY", "0.07", "5", "0.08", ""),
        );
        assert_eq!(snaps[0].timestamp_us, 2_000_000);
        assert_eq!(builder.clamped_timestamps(), 0);

        // A resync stamped 1 ms earlier than what we already emitted: content
        // is kept, the label is clamped to the fence, and it's counted.
        let snaps = apply(
            &mut builder,
            &book_frame("a", 1_999, &[("0.09", "20")], &[]),
        );
        assert_eq!(snaps[0].timestamp_us, 2_000_000);
        assert_eq!(prices(&snaps[0].bids), ["0.09"]);
        assert_eq!(builder.clamped_timestamps(), 1);
    }

    #[test]
    #[ignore = "replays a real capture file; run with `cargo test -- --ignored`"]
    fn oracle_replay_over_real_capture() {
        use crate::clob::decode::CaptureRecord;
        use std::io::BufRead;

        // The largest real capture — the most events, the most oracle checks.
        let path = std::fs::read_dir(crate::test_support::data_path("data/england_croatia"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
            .max_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .expect("a capture file");

        let file = std::io::BufReader::new(std::fs::File::open(&path).unwrap());
        let mut builder = BookBuilder::new();
        let mut frames = 0u64;
        let mut emitted = 0u64;
        for line in file.lines() {
            let line = line.unwrap();
            if line.trim().is_empty() {
                continue;
            }
            let record: CaptureRecord = serde_json::from_str(&line).unwrap();
            let events = decode_frame(&record.raw).unwrap();
            let local_us = (record.recv_t * 1e6).round() as i64;
            // The invariant under test: apply_frame never errors on real
            // data, and (by construction, asserted inside) snapshots are
            // only ever emitted from books that just passed the oracle.
            let snaps = builder.apply_frame(&events, local_us).unwrap();
            for snap in &snaps {
                assert_eq!(
                    builder.is_fresh(&snap.asset_id),
                    Some(true),
                    "emitted from a stale book"
                );
            }
            emitted += snaps.len() as u64;
            frames += 1;
        }

        let stats = builder.gap_stats();
        let episodes: u64 = stats.iter().map(|g| g.stale_episodes).sum();
        let suppressed: u64 = stats.iter().map(|g| g.suppressed_frames).sum();
        println!(
            "{}: {frames} frames, {} oracle checks, {emitted} snapshots emitted, \
             {episodes} gap episodes, {suppressed} frames suppressed, {} clamped stamps",
            path.display(),
            builder.oracle_checks(),
            builder.clamped_timestamps(),
        );
        // Guard against a vacuous pass: real captures must exercise the
        // oracle heavily.
        assert!(builder.oracle_checks() > 1_000, "oracle barely exercised");
        assert!(emitted > 0);
    }
}
