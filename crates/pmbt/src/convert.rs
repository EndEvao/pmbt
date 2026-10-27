//! Capture → Telonex converter: normalize-at-rest for our own recordings.
//!
//! Streams a raw websocket capture file (lines of `{recv_t, recv_iso, raw}`)
//! through `clob::decode` + `clob::BookBuilder` and writes, per asset, both
//! Telonex channels the engine consumes — a `book_snapshot_full` file and its
//! sibling `trades` file — **indistinguishable from purchased Telonex data**:
//! the same columns, the same string-typed prices/sizes, both clocks
//! (`timestamp_us` = exchange, `local_timestamp_us` = recorder receive time),
//! SNAPPY-compressed. `TelonexBookSource`/`TelonexTradesSource` and everything
//! above them never learn the files had a different origin.
//!
//! The book comes from the builder (deltas folded into snapshots); the tape
//! comes from `last_trade_price` events, which the builder itself ignores as
//! book-neutral. Trades are ground truth prints and are emitted **independently
//! of book continuity** — a print during an asset's gap quarantine is still
//! recorded, because a missed `price_change` is not a missed trade.
//!
//! Honest fields only: `outcome` and `trade_id` are written empty — nothing in
//! the market stream carries them (the `last_trade_price` event has no outcome,
//! and `decode` does not retain the transaction hash). `origin_asset_id` is the
//! trade's own token: one file is one token, so a print originated on it. Sizes
//! are re-rendered via `f64` Display (shortest round-trip), which normalizes
//! `"623.60"` → `"623.6"`; prices go through the integer-exact
//! `Price::to_decimal_string`, which also validates them on-grid at convert
//! time rather than deferring corruption to read time.
//!
//! Conversion is fail-fast on structure (a bad line aborts — same philosophy
//! as the engine) but **gap-tolerant on continuity**: the recordings are
//! known to be missing timeframes, and the builder's touch oracle quarantines
//! affected books until resync. The [`ConvertReport`] carries that accounting
//! so gaps are visible, never silent.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use thiserror::Error;

use crate::clob::book::{BookBuilder, BookSnapshot, ClobError, GapReport};
use crate::clob::decode::{
    decode_frame, CaptureRecord, ClobEvent, DecodeError, LastTradePriceEvent, Side,
};
use crate::domain::marketstate::Level;
use crate::domain::types::Price;

/// Which Telonex channel a per-asset sink receives. One capture asset yields up
/// to two files — its book and its tape — opened lazily on first use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Book,
    Trades,
}

impl Channel {
    /// The Telonex filename infix for this channel.
    fn infix(self) -> &'static str {
        match self {
            Channel::Book => "book_snapshot_full",
            Channel::Trades => "trades",
        }
    }
}

/// Buffered rows per asset before a record batch is flushed to its writer —
/// bounds memory on large captures without churning tiny row groups.
const FLUSH_ROWS: usize = 8_192;

/// Where one converted capture ended up.
#[derive(Debug)]
pub struct ConvertReport {
    /// Capture date (from the first line's `recv_iso`), used in filenames.
    pub date: String,
    pub frames: u64,
    /// Book rows written per asset, largest first.
    pub assets: Vec<AssetRows>,
    /// Trade prints written per asset, largest first. Empty when a capture
    /// carried no `last_trade_price` events (thin markets often do).
    pub trades: Vec<AssetRows>,
    /// Per-asset staleness accounting from the builder's touch oracle.
    pub gaps: Vec<GapReport>,
    /// Snapshots whose exchange stamp was clamped to the monotone fence.
    pub clamped_timestamps: u64,
}

#[derive(Debug)]
pub struct AssetRows {
    pub asset_id: String,
    pub rows: u64,
}

/// Convert one capture file into per-asset Telonex Parquet files in `out_dir`
/// (created if missing). File names follow the Telonex convention:
/// `{asset_id}_{date}_book_snapshot_full.parquet`.
pub fn convert_capture(capture: &Path, out_dir: &Path) -> Result<ConvertReport, ConvertError> {
    let slug = capture
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("capture")
        .to_string();
    std::fs::create_dir_all(out_dir)?;
    let reader = BufReader::new(File::open(capture)?);
    convert(reader, &slug, |asset_id, date, channel| {
        let name = format!("{asset_id}_{date}_{}.parquet", channel.infix());
        Ok(File::create(out_dir.join(name))?)
    })
}

/// The generic core: stream capture lines from any reader, open one sink per
/// asset through `open_sink(asset_id, date)`. Tests pass in-memory readers
/// and sinks; `convert_capture` passes files.
pub fn convert<R, W, F>(
    reader: R,
    slug: &str,
    mut open_sink: F,
) -> Result<ConvertReport, ConvertError>
where
    R: BufRead,
    W: Write + Send,
    F: FnMut(&str, &str, Channel) -> Result<W, ConvertError>,
{
    let book_schema = telonex_book_schema();
    let trades_schema = telonex_trades_schema();
    let mut builder = BookBuilder::new();
    let mut books: HashMap<String, AssetWriter<W, BookSnapshot>> = HashMap::new();
    let mut trades: HashMap<String, AssetWriter<W, TradeRow>> = HashMap::new();
    let mut date: Option<String> = None;
    let mut frames = 0u64;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: CaptureRecord = serde_json::from_str(&line)?;
        let date = date.get_or_insert_with(|| {
            record
                .recv_iso
                .get(..10)
                .unwrap_or("unknown-date")
                .to_string()
        });
        let events = decode_frame(&record.raw)?;
        let local_timestamp_us = (record.recv_t * 1e6).round() as i64;

        // The book: deltas folded into snapshots by the builder.
        for snapshot in builder.apply_frame(&events, local_timestamp_us)? {
            let asset = snapshot.asset_id.clone();
            let writer = match books.entry(asset.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let sink = open_sink(&asset, date, Channel::Book)?;
                    entry.insert(AssetWriter::new(ArrowWriter::try_new(
                        sink,
                        book_schema.clone(),
                        Some(snappy()),
                    )?))
                }
            };
            writer.buffer.push(snapshot);
            if writer.buffer.len() >= FLUSH_ROWS {
                writer.flush(&book_schema, slug, to_book_batch)?;
            }
        }

        // The tape: `last_trade_price` events, book-neutral and emitted
        // regardless of any gap quarantine on the asset's book.
        for event in &events {
            let ClobEvent::LastTradePrice(t) = event else {
                continue;
            };
            let row = trade_row(t, local_timestamp_us)?;
            let writer = match trades.entry(t.asset_id.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let sink = open_sink(&t.asset_id, date, Channel::Trades)?;
                    entry.insert(AssetWriter::new(ArrowWriter::try_new(
                        sink,
                        trades_schema.clone(),
                        Some(snappy()),
                    )?))
                }
            };
            writer.buffer.push(row);
            if writer.buffer.len() >= FLUSH_ROWS {
                writer.flush(&trades_schema, slug, to_trades_batch)?;
            }
        }
        frames += 1;
    }

    let assets = close_all(books, &book_schema, slug, to_book_batch)?;
    let trades = close_all(trades, &trades_schema, slug, to_trades_batch)?;

    Ok(ConvertReport {
        date: date.unwrap_or_else(|| "unknown-date".to_string()),
        frames,
        assets,
        trades,
        gaps: builder.gap_stats(),
        clamped_timestamps: builder.clamped_timestamps(),
    })
}

/// SNAPPY properties, shared by every writer (Telonex parity).
fn snappy() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

/// Flush and close every writer, returning per-asset row counts largest-first.
fn close_all<W: Write + Send, Row>(
    writers: HashMap<String, AssetWriter<W, Row>>,
    schema: &Arc<Schema>,
    slug: &str,
    to_batch: impl Fn(&Arc<Schema>, &str, &[Row]) -> Result<RecordBatch, ConvertError>,
) -> Result<Vec<AssetRows>, ConvertError> {
    let mut assets = Vec::new();
    for (asset_id, mut writer) in writers {
        writer.flush(schema, slug, &to_batch)?;
        writer.writer.close()?;
        assets.push(AssetRows {
            asset_id,
            rows: writer.rows,
        });
    }
    assets.sort_by_key(|a| std::cmp::Reverse(a.rows));
    Ok(assets)
}

/// One buffered writer, generic over its row type so the book and trade
/// channels share the same batching and flush machinery.
struct AssetWriter<W: Write + Send, Row> {
    writer: ArrowWriter<W>,
    buffer: Vec<Row>,
    rows: u64,
}

impl<W: Write + Send, Row> AssetWriter<W, Row> {
    fn new(writer: ArrowWriter<W>) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
            rows: 0,
        }
    }

    fn flush(
        &mut self,
        schema: &Arc<Schema>,
        slug: &str,
        to_batch: impl Fn(&Arc<Schema>, &str, &[Row]) -> Result<RecordBatch, ConvertError>,
    ) -> Result<(), ConvertError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = to_batch(schema, slug, &self.buffer)?;
        self.writer.write(&batch)?;
        self.rows += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

/// A trade print, parsed and validated at convert time: price on-grid, size a
/// finite number, timestamp integer ms. `origin_asset_id` is the trade's own
/// token — one file, one token.
struct TradeRow {
    timestamp_us: i64,
    local_timestamp_us: i64,
    market: String,
    asset_id: String,
    price: Price,
    size: f64,
    side: Side,
}

fn trade_row(t: &LastTradePriceEvent, local_timestamp_us: i64) -> Result<TradeRow, ConvertError> {
    let ms: i64 = t
        .timestamp
        .parse()
        .map_err(|_| ConvertError::Trade(format!("bad trade timestamp {:?}", t.timestamp)))?;
    let price = Price::parse_decimal(&t.price)
        .map_err(|_| ConvertError::Trade(format!("bad trade price {:?}", t.price)))?;
    let size: f64 = t
        .size
        .parse()
        .map_err(|_| ConvertError::Trade(format!("bad trade size {:?}", t.size)))?;
    Ok(TradeRow {
        timestamp_us: ms * 1_000,
        local_timestamp_us,
        market: t.market.clone(),
        asset_id: t.asset_id.clone(),
        price,
        size,
        side: t.side,
    })
}

fn level_fields() -> Fields {
    Fields::from(vec![
        Field::new("price", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
    ])
}

/// The nine-column Telonex `book_snapshot_full` schema.
fn telonex_book_schema() -> Arc<Schema> {
    let ladder = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(level_fields()),
        true,
    )));
    Arc::new(Schema::new(vec![
        Field::new("timestamp_us", DataType::Int64, false),
        Field::new("local_timestamp_us", DataType::Int64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("market_id", DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, false),
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("outcome", DataType::Utf8, false),
        Field::new("bids", ladder.clone(), true),
        Field::new("asks", ladder, true),
    ]))
}

fn ladder_array(sides: impl Iterator<Item = Vec<Level>>) -> arrow_array::ListArray {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(level_fields(), 8));
    for side in sides {
        for level in side {
            let entry = builder.values();
            entry
                .field_builder::<StringBuilder>(0)
                .expect("price is field 0")
                .append_value(level.price.to_decimal_string());
            entry
                .field_builder::<StringBuilder>(1)
                .expect("size is field 1")
                .append_value(level.size.to_string());
            entry.append(true);
        }
        builder.append(true);
    }
    builder.finish()
}

fn to_book_batch(
    schema: &Arc<Schema>,
    slug: &str,
    rows: &[BookSnapshot],
) -> Result<RecordBatch, ConvertError> {
    let timestamps: Int64Array = rows.iter().map(|r| r.timestamp_us).collect();
    let locals: Int64Array = rows.iter().map(|r| r.local_timestamp_us).collect();
    let constant = |value: &str| -> StringArray { rows.iter().map(|_| Some(value)).collect() };
    let markets: StringArray = rows.iter().map(|r| Some(r.market.as_str())).collect();
    let assets: StringArray = rows.iter().map(|r| Some(r.asset_id.as_str())).collect();
    let bids = ladder_array(rows.iter().map(|r| r.bids.clone()));
    let asks = ladder_array(rows.iter().map(|r| r.asks.clone()));

    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(timestamps),
            Arc::new(locals),
            Arc::new(constant("polymarket")),
            Arc::new(markets),
            Arc::new(constant(slug)),
            Arc::new(assets),
            Arc::new(constant("")), // outcome: unknowable from the stream
            Arc::new(bids),
            Arc::new(asks),
        ],
    )?)
}

/// The twelve-column Telonex `trades` schema. `TelonexTradesSource` reads five
/// of these (`timestamp_us, price, size, side, origin_asset_id`); the rest are
/// written for byte-parity with a purchased tape.
fn telonex_trades_schema() -> Arc<Schema> {
    let s = |name: &str| Field::new(name, DataType::Utf8, false);
    Arc::new(Schema::new(vec![
        Field::new("timestamp_us", DataType::Int64, false),
        Field::new("local_timestamp_us", DataType::Int64, false),
        s("exchange"),
        s("market_id"),
        s("slug"),
        s("asset_id"),
        s("outcome"),
        s("price"),
        s("size"),
        s("side"),
        s("trade_id"),
        s("origin_asset_id"),
    ]))
}

fn to_trades_batch(
    schema: &Arc<Schema>,
    slug: &str,
    rows: &[TradeRow],
) -> Result<RecordBatch, ConvertError> {
    let timestamps: Int64Array = rows.iter().map(|r| r.timestamp_us).collect();
    let locals: Int64Array = rows.iter().map(|r| r.local_timestamp_us).collect();
    let constant = |value: &str| -> StringArray { rows.iter().map(|_| Some(value)).collect() };
    let markets: StringArray = rows.iter().map(|r| Some(r.market.as_str())).collect();
    let assets: StringArray = rows.iter().map(|r| Some(r.asset_id.as_str())).collect();
    // origin_asset_id: one file is one token, so a print originated on it.
    let origins: StringArray = rows.iter().map(|r| Some(r.asset_id.as_str())).collect();
    let prices: StringArray = rows
        .iter()
        .map(|r| Some(r.price.to_decimal_string()))
        .collect();
    let sizes: StringArray = rows.iter().map(|r| Some(r.size.to_string())).collect();
    let sides: StringArray = rows
        .iter()
        .map(|r| {
            Some(match r.side {
                Side::Buy => "buy",
                Side::Sell => "sell",
            })
        })
        .collect();

    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(timestamps),
            Arc::new(locals),
            Arc::new(constant("polymarket")),
            Arc::new(markets),
            Arc::new(constant(slug)),
            Arc::new(assets),
            Arc::new(constant("")), // outcome: unknowable from the stream
            Arc::new(prices),
            Arc::new(sizes),
            Arc::new(sides),
            Arc::new(constant("")), // trade_id: decode drops the transaction hash
            Arc::new(origins),
        ],
    )?)
}

/// Anything that can go wrong converting a capture.
#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("capture line is not a valid envelope: {0}")]
    Envelope(#[from] serde_json::Error),
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Book(#[from] ClobError),
    #[error("malformed trade: {0}")]
    Trade(String),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;

    use super::*;
    use crate::domain::order::OrderSide;
    use crate::io::LoadError;
    use crate::telonex::{TelonexBookSource, TelonexTradesSource};

    /// An in-memory sink the test can read back after the writer is dropped.
    #[derive(Clone, Default)]
    struct SharedVec(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedVec {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn envelope(recv_t: f64, raw: &str) -> String {
        format!(
            r#"{{"recv_t": {recv_t}, "recv_iso": "2026-06-17T20:15:41Z", "raw": {}}}"#,
            serde_json::to_string(raw).unwrap()
        )
    }

    #[test]
    fn converted_capture_reads_back_through_telonex_source() {
        // Two assets: initial books arrive as one array frame (the subscribe
        // shape), then deltas — including a level removal — as single-object
        // frames.
        let initial = r#"[
            {"event_type":"book","market":"0xm","asset_id":"yes","timestamp":"1000","bids":[{"price":"0.001","size":"79"},{"price":"0.08","size":"20"}],"asks":[{"price":"0.999","size":"65"},{"price":"0.085","size":"623.6"}]},
            {"event_type":"book","market":"0xm","asset_id":"no","timestamp":"1000","bids":[{"price":"0.4","size":"5"}],"asks":[{"price":"0.6","size":"5"}]}]"#
            .replace('\n', " ");
        let delta_yes = r#"{"event_type":"price_change","market":"0xm","timestamp":"2000","price_changes":[{"asset_id":"yes","price":"0.085","size":"0","side":"SELL","best_bid":"0.08","best_ask":"0.999"}]}"#;
        let delta_no = r#"{"event_type":"price_change","market":"0xm","timestamp":"3000","price_changes":[{"asset_id":"no","price":"0.5","size":"7.25","side":"BUY","best_bid":"0.5","best_ask":"0.6"}]}"#;
        // A print on "yes" — the tape the converter must now emit. Note the
        // deliberately un-normalized size "12.50" and price "0.0800".
        let trade_yes = r#"{"event_type":"last_trade_price","market":"0xm","asset_id":"yes","price":"0.0800","size":"12.50","side":"BUY","timestamp":"2500"}"#;

        let capture = [
            envelope(10.0, &initial),
            envelope(10.5, delta_yes),
            envelope(10.7, trade_yes),
            envelope(11.0, delta_no),
        ]
        .join("\n");

        // Sinks keyed by (asset, channel) — each asset now yields a book file
        // and, if it traded, a tape file.
        let mut sinks: Vec<(String, Channel, SharedVec)> = Vec::new();
        let report = convert(
            capture.as_bytes(),
            "test-slug",
            |asset_id, date, channel| {
                assert_eq!(date, "2026-06-17");
                let sink = SharedVec::default();
                sinks.push((asset_id.to_string(), channel, sink.clone()));
                Ok(sink)
            },
        )
        .unwrap();

        assert_eq!(report.frames, 4);
        assert_eq!(report.assets.len(), 2);
        // Only "yes" traded, so exactly one tape file.
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].asset_id, "yes");
        assert_eq!(report.trades[0].rows, 1);
        assert!(report.gaps.is_empty());
        assert_eq!(report.clamped_timestamps, 0);

        // Every converted file must stream through the engine's own sources —
        // that is the whole point of the converter.
        for (asset_id, channel, sink) in sinks {
            let bytes = Bytes::from(sink.0.lock().unwrap().clone());
            match channel {
                Channel::Book => {
                    let states: Result<Vec<_>, LoadError> =
                        TelonexBookSource::from_chunk_reader(bytes)
                            .unwrap()
                            .collect();
                    let states = states.unwrap();
                    assert_eq!(states.len(), 2, "{asset_id}: initial book + one delta");
                    assert_eq!(states[0].timestamp, 1_000_000); // ms → µs
                    if asset_id == "yes" {
                        let first = &states[0];
                        assert_eq!(first.bid().unwrap().price.to_decimal_string(), "0.08");
                        assert_eq!(first.ask().unwrap().price.to_decimal_string(), "0.085");
                        let second = &states[1];
                        assert_eq!(second.ask().unwrap().price.to_decimal_string(), "0.999");
                        assert!((second.ask().unwrap().size - 65.0).abs() < 1e-9);
                    }
                }
                Channel::Trades => {
                    assert_eq!(asset_id, "yes", "only yes traded");
                    let trades: Result<Vec<_>, LoadError> =
                        TelonexTradesSource::from_chunk_reader(bytes)
                            .unwrap()
                            .collect();
                    let trades = trades.unwrap();
                    assert_eq!(trades.len(), 1);
                    let t = &trades[0];
                    // ms → µs, price/size normalized on the way out, side lowercased.
                    assert_eq!(t.timestamp, 2_500_000);
                    assert_eq!(t.price.to_decimal_string(), "0.08");
                    assert!((t.size - 12.5).abs() < 1e-9);
                    assert_eq!(t.side, OrderSide::Buy);
                }
            }
        }
    }

    #[test]
    fn structural_corruption_aborts() {
        let capture = envelope(10.0, r#"{"event_type":"mystery"}"#);
        let result = convert(capture.as_bytes(), "slug", |_, _, _| {
            Ok(SharedVec::default())
        });
        assert!(matches!(result, Err(ConvertError::Decode(_))));

        let result = convert(b"not json".as_slice(), "slug", |_, _, _| {
            Ok(SharedVec::default())
        });
        assert!(matches!(result, Err(ConvertError::Envelope(_))));
    }

    #[test]
    #[ignore = "converts a real capture; run with `cargo test -- --ignored`"]
    fn real_capture_converts_and_streams_back() {
        // A small-but-nonempty real capture keeps this test quick.
        let capture = std::fs::read_dir(crate::test_support::data_path("data/england_croatia"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) > 1_000_000)
            .min_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(u64::MAX))
            .expect("a capture over 1 MB");

        let out = std::env::temp_dir().join(format!("pmbt-convert-test-{}", std::process::id()));
        let report = convert_capture(&capture, &out).unwrap();
        let trade_prints: u64 = report.trades.iter().map(|a| a.rows).sum();
        println!(
            "{}: {} frames → {} book assets, {} trade prints, {} gap-affected, {} clamped stamps",
            capture.display(),
            report.frames,
            report.assets.len(),
            trade_prints,
            report.gaps.len(),
            report.clamped_timestamps,
        );

        // Each output must stream to exhaustion through the source for its
        // channel — book files as books, tape files as trades — with the
        // reader re-checking sortedness/monotonicity on the way. The filename
        // infix routes it, exactly as `MergedFeed::from_book_path` pairs them.
        let (mut book_rows, mut trade_rows) = (0u64, 0u64);
        for entry in std::fs::read_dir(&out).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap();
            if name.contains("_book_snapshot_full") {
                let s: Result<Vec<_>, _> = TelonexBookSource::from_path(&path).unwrap().collect();
                book_rows += s.unwrap().len() as u64;
            } else if name.contains("_trades") {
                let s: Result<Vec<_>, _> = TelonexTradesSource::from_path(&path).unwrap().collect();
                trade_rows += s.unwrap().len() as u64;
            } else {
                panic!("unexpected output file: {name}");
            }
        }
        assert_eq!(book_rows, report.assets.iter().map(|a| a.rows).sum::<u64>());
        assert_eq!(trade_rows, trade_prints);
        assert!(book_rows > 0);

        std::fs::remove_dir_all(&out).unwrap();
    }
}
