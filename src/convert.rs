//! Capture → Telonex converter: normalize-at-rest for our own recordings.
//!
//! Streams a raw websocket capture file (lines of `{recv_t, recv_iso, raw}`)
//! through `clob::decode` + `clob::BookBuilder` and writes one
//! `book_snapshot_full` Parquet file per asset, **indistinguishable from
//! purchased Telonex data**: same nine columns, same string-typed
//! prices/sizes, both clocks (`timestamp_us` = exchange, `local_timestamp_us`
//! = recorder receive time — the same "whoever recorded this" semantics
//! Telonex's own column has), SNAPPY-compressed. `TelonexBookSource` and
//! everything above it never learn the file had a different origin.
//!
//! Honest fields only: `outcome` is written empty — nothing in the websocket
//! stream says which side of the market a token is. Sizes are re-rendered via
//! `f64` Display (shortest round-trip), which normalizes `"623.60"` →
//! `"623.6"`; prices go through the integer-exact `Price::to_decimal_string`.
//!
//! Conversion is fail-fast on structure (a bad line aborts — same philosophy
//! as the engine) but **gap-tolerant on continuity**: the recordings are
//! known to be missing timeframes, and the builder's touch oracle quarantines
//! affected books until resync. The [`ConvertReport`] carries that accounting
//! so gaps are visible, never silent.

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
use crate::clob::decode::{decode_frame, CaptureRecord, DecodeError};
use crate::domain::marketstate::Level;

/// Buffered rows per asset before a record batch is flushed to its writer —
/// bounds memory on large captures without churning tiny row groups.
const FLUSH_ROWS: usize = 8_192;

/// Where one converted capture ended up.
#[derive(Debug)]
pub struct ConvertReport {
    /// Capture date (from the first line's `recv_iso`), used in filenames.
    pub date: String,
    pub frames: u64,
    /// Rows written per asset, largest first.
    pub assets: Vec<AssetRows>,
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
    convert(reader, &slug, |asset_id, date| {
        let name = format!("{asset_id}_{date}_book_snapshot_full.parquet");
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
    F: FnMut(&str, &str) -> Result<W, ConvertError>,
{
    let schema = telonex_book_schema();
    let mut builder = BookBuilder::new();
    let mut writers: std::collections::HashMap<String, AssetWriter<W>> =
        std::collections::HashMap::new();
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

        for snapshot in builder.apply_frame(&events, local_timestamp_us)? {
            let writer = match writers.entry(snapshot.asset_id.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let sink = open_sink(&snapshot.asset_id, date)?;
                    let props = WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .build();
                    entry.insert(AssetWriter {
                        writer: ArrowWriter::try_new(sink, schema.clone(), Some(props))?,
                        buffer: Vec::new(),
                        rows: 0,
                    })
                }
            };
            writer.buffer.push(snapshot);
            if writer.buffer.len() >= FLUSH_ROWS {
                writer.flush(&schema, slug)?;
            }
        }
        frames += 1;
    }

    let mut assets = Vec::new();
    for (asset_id, mut writer) in writers {
        writer.flush(&schema, slug)?;
        writer.writer.close()?;
        assets.push(AssetRows {
            asset_id,
            rows: writer.rows,
        });
    }
    assets.sort_by_key(|a| std::cmp::Reverse(a.rows));

    Ok(ConvertReport {
        date: date.unwrap_or_else(|| "unknown-date".to_string()),
        frames,
        assets,
        gaps: builder.gap_stats(),
        clamped_timestamps: builder.clamped_timestamps(),
    })
}

struct AssetWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    buffer: Vec<BookSnapshot>,
    rows: u64,
}

impl<W: Write + Send> AssetWriter<W> {
    fn flush(&mut self, schema: &Arc<Schema>, slug: &str) -> Result<(), ConvertError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = to_record_batch(schema, slug, &self.buffer)?;
        self.writer.write(&batch)?;
        self.rows += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
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

fn to_record_batch(
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
    use crate::io::LoadError;
    use crate::telonex::TelonexBookSource;

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

        let capture = [
            envelope(10.0, &initial),
            envelope(10.5, delta_yes),
            envelope(11.0, delta_no),
        ]
        .join("\n");

        let mut sinks: Vec<(String, SharedVec)> = Vec::new();
        let report = convert(capture.as_bytes(), "test-slug", |asset_id, date| {
            assert_eq!(date, "2026-06-17");
            let sink = SharedVec::default();
            sinks.push((asset_id.to_string(), sink.clone()));
            Ok(sink)
        })
        .unwrap();

        assert_eq!(report.frames, 3);
        assert_eq!(report.assets.len(), 2);
        assert!(report.gaps.is_empty());
        assert_eq!(report.clamped_timestamps, 0);

        // Every converted file must stream through the engine's one and only
        // historical source — that is the whole point of the converter.
        for (asset_id, sink) in sinks {
            let bytes = Bytes::from(sink.0.lock().unwrap().clone());
            let states: Result<Vec<_>, LoadError> = TelonexBookSource::from_chunk_reader(bytes)
                .unwrap()
                .collect();
            let states = states.unwrap();
            assert_eq!(states.len(), 2, "{asset_id}: initial book + one delta");
            assert_eq!(states[0].timestamp, 1_000_000); // ms → µs

            if asset_id == "yes" {
                // Ladder came in away-from-touch and with a removed level; it
                // must read back best-first and without the removed ask.
                let first = &states[0];
                assert_eq!(first.bid().unwrap().price.to_decimal_string(), "0.08");
                assert_eq!(first.ask().unwrap().price.to_decimal_string(), "0.085");
                let second = &states[1];
                assert_eq!(second.ask().unwrap().price.to_decimal_string(), "0.999");
                assert!((second.ask().unwrap().size - 65.0).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn structural_corruption_aborts() {
        let capture = envelope(10.0, r#"{"event_type":"mystery"}"#);
        let result = convert(capture.as_bytes(), "slug", |_, _| Ok(SharedVec::default()));
        assert!(matches!(result, Err(ConvertError::Decode(_))));

        let result = convert(b"not json".as_slice(), "slug", |_, _| {
            Ok(SharedVec::default())
        });
        assert!(matches!(result, Err(ConvertError::Envelope(_))));
    }

    #[test]
    #[ignore = "converts a real capture; run with `cargo test -- --ignored`"]
    fn real_capture_converts_and_streams_back() {
        // A small-but-nonempty real capture keeps this test quick.
        let capture = std::fs::read_dir("data/england_croatia")
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) > 1_000_000)
            .min_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(u64::MAX))
            .expect("a capture over 1 MB");

        let out = std::env::temp_dir().join(format!("pmbt-convert-test-{}", std::process::id()));
        let report = convert_capture(&capture, &out).unwrap();
        println!(
            "{}: {} frames → {} assets, {} gap-affected, {} clamped stamps",
            capture.display(),
            report.frames,
            report.assets.len(),
            report.gaps.len(),
            report.clamped_timestamps,
        );

        // Every output must stream to exhaustion through TelonexBookSource —
        // sortedness and monotonicity re-checked by the reader on the way.
        let mut total_rows = 0u64;
        for entry in std::fs::read_dir(&out).unwrap() {
            let path = entry.unwrap().path();
            let states: Result<Vec<_>, _> = TelonexBookSource::from_path(&path).unwrap().collect();
            total_rows += states.unwrap().len() as u64;
        }
        let written: u64 = report.assets.iter().map(|a| a.rows).sum();
        assert_eq!(total_rows, written);
        assert!(total_rows > 0);

        std::fs::remove_dir_all(&out).unwrap();
    }
}
