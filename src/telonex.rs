//! Historical feed adapter for the Telonex at-rest format.
//!
//! Streams a `book_snapshot_full` Parquet file — one market outcome's full
//! order book over time — as `MarketState`s, batch by batch (constant memory,
//! no whole-file load). Schema facts this reader relies on:
//!
//! - one file == one token: every row carries the same `asset_id`;
//! - prices and sizes are decimal *strings* (parsed through the strict
//!   `Price::parse_decimal` door);
//! - `timestamp_us` is exchange time in microseconds, monotonic in practice
//!   (still guarded here — an inversion is corruption, not a hiccup);
//! - `bids`/`asks` are `list<struct<price,size>>` ladders, best-first.

use std::fs::File;
use std::path::Path;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ListArray, RecordBatch};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;

use crate::domain::marketstate::{Level, MarketState};
use crate::domain::order::OrderSide;
use crate::domain::trade::Trade;
use crate::domain::types::{MarketId, Price};
use crate::io::LoadError;

/// The only columns the engine needs; everything else in the file
/// (`local_timestamp_us`, `exchange`, `slug`, …) is never decoded.
const COLUMNS: [&str; 4] = ["timestamp_us", "asset_id", "bids", "asks"];

/// Streams a Telonex `book_snapshot_full` Parquet file as `MarketState`s.
///
/// The `asset_id` (token id) becomes the `MarketId`: the token is the
/// instrument actually owned and traded — two outcomes of one market must not
/// share a ledger key.
pub struct TelonexBookSource {
    batches: ParquetRecordBatchReader,
    current: Option<RecordBatch>,
    row: usize,
    last_timestamp: i64,
}

impl TelonexBookSource {
    /// Open a `book_snapshot_full` Parquet file for streaming.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, LoadError> {
        Self::from_chunk_reader(File::open(path)?)
    }

    /// Build from anything the parquet crate can read chunks of — a `File` in
    /// production, `bytes::Bytes` in tests (the in-memory analogue of the old
    /// `&[u8]` JSONL fixtures).
    pub fn from_chunk_reader<R>(reader: R) -> Result<Self, LoadError>
    where
        R: parquet::file::reader::ChunkReader + 'static,
    {
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;

        // Project down to the columns we decode; missing ones fail here, at
        // open time, rather than as a confusing decode error mid-stream.
        let schema = builder.parquet_schema();
        let indices = COLUMNS
            .iter()
            .map(|name| {
                (0..schema.root_schema().get_fields().len())
                    .find(|&i| schema.root_schema().get_fields()[i].name() == *name)
                    .ok_or(LoadError::MissingColumn(name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mask = ProjectionMask::roots(schema, indices);

        Ok(Self {
            batches: builder.with_projection(mask).build()?,
            current: None,
            row: 0,
            last_timestamp: i64::MIN,
        })
    }

    /// Decode one row of a projected batch into a `MarketState`.
    fn parse_row(batch: &RecordBatch, row: usize) -> Result<MarketState, LoadError> {
        let column = |name: &'static str| {
            batch
                .column_by_name(name)
                .ok_or(LoadError::MissingColumn(name))
        };

        let timestamp = column("timestamp_us")?
            .as_primitive_opt::<Int64Type>()
            .ok_or(LoadError::MissingColumn("timestamp_us"))?
            .value(row);
        let asset_id = column("asset_id")?
            .as_string_opt::<i32>()
            .ok_or(LoadError::MissingColumn("asset_id"))?
            .value(row)
            .to_string();

        let side = |name: &'static str| -> Result<Box<[Level]>, LoadError> {
            let ladder = column(name)?
                .as_list_opt::<i32>()
                .ok_or(LoadError::MissingColumn(name))?;
            decode_ladder(ladder, row)
        };

        Ok(MarketState::new(
            MarketId::new(asset_id),
            timestamp,
            side("bids")?,
            side("asks")?,
        )?)
    }
}

/// Decode one row's `list<struct<price,size>>` into a ladder. Prices go
/// through the strict decimal door; sizes are plain decimals with no grid, so
/// a float parse is enough.
fn decode_ladder(ladder: &ListArray, row: usize) -> Result<Box<[Level]>, LoadError> {
    let levels = ladder.value(row);
    let levels = levels
        .as_struct_opt()
        .ok_or(LoadError::MissingColumn("bids/asks levels"))?;
    let field = |name: &'static str| {
        levels
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
            .ok_or(LoadError::MissingColumn(name))
    };
    let (prices, sizes) = (field("price")?, field("size")?);

    (0..levels.len())
        .map(|i| {
            Ok(Level {
                price: Price::parse_decimal(prices.value(i))?,
                size: sizes
                    .value(i)
                    .parse()
                    .map_err(|_| LoadError::InvalidSize(sizes.value(i).to_string()))?,
            })
        })
        .collect()
}

impl Iterator for TelonexBookSource {
    type Item = Result<MarketState, LoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Advance within the current batch, or pull the next one.
            let batch = match &self.current {
                Some(batch) if self.row < batch.num_rows() => batch.clone(),
                _ => match self.batches.next()? {
                    Ok(batch) => {
                        self.current = Some(batch);
                        self.row = 0;
                        continue;
                    }
                    Err(err) => return Some(Err(err.into())),
                },
            };
            let row = self.row;
            self.row += 1;

            return Some(Self::parse_row(&batch, row).and_then(|state| {
                if state.timestamp < self.last_timestamp {
                    return Err(LoadError::OutOfOrder {
                        previous: self.last_timestamp,
                        got: state.timestamp,
                    });
                }
                self.last_timestamp = state.timestamp;
                Ok(state)
            }));
        }
    }
}

/// The columns a trade row needs; the rest of the file (`local_timestamp_us`,
/// `exchange`, `slug`, `trade_id`, …) is never decoded.
const TRADE_COLUMNS: [&str; 5] = ["timestamp_us", "price", "size", "side", "origin_asset_id"];

/// Streams a Telonex `trades` Parquet file as [`Trade`]s — the sibling of
/// [`TelonexBookSource`], built the same way (project, stream batch-by-batch,
/// parse strings through the strict doors, guard monotonic time). One print per
/// row; `side` is the aggressor's side.
pub struct TelonexTradesSource {
    batches: ParquetRecordBatchReader,
    current: Option<RecordBatch>,
    row: usize,
    last_timestamp: i64,
}

impl TelonexTradesSource {
    /// Open a `trades` Parquet file for streaming.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, LoadError> {
        Self::from_chunk_reader(File::open(path)?)
    }

    /// Build from anything the parquet crate can read chunks of — a `File` in
    /// production, `bytes::Bytes` in tests.
    pub fn from_chunk_reader<R>(reader: R) -> Result<Self, LoadError>
    where
        R: parquet::file::reader::ChunkReader + 'static,
    {
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;

        let schema = builder.parquet_schema();
        let indices = TRADE_COLUMNS
            .iter()
            .map(|name| {
                (0..schema.root_schema().get_fields().len())
                    .find(|&i| schema.root_schema().get_fields()[i].name() == *name)
                    .ok_or(LoadError::MissingColumn(name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mask = ProjectionMask::roots(schema, indices);

        Ok(Self {
            batches: builder.with_projection(mask).build()?,
            current: None,
            row: 0,
            last_timestamp: i64::MIN,
        })
    }

    /// Decode one row of a projected batch into a [`Trade`].
    fn parse_row(batch: &RecordBatch, row: usize) -> Result<Trade, LoadError> {
        let column = |name: &'static str| {
            batch
                .column_by_name(name)
                .ok_or(LoadError::MissingColumn(name))
        };
        let string = |name: &'static str| -> Result<&str, LoadError> {
            Ok(column(name)?
                .as_string_opt::<i32>()
                .ok_or(LoadError::MissingColumn(name))?
                .value(row))
        };

        let timestamp = column("timestamp_us")?
            .as_primitive_opt::<Int64Type>()
            .ok_or(LoadError::MissingColumn("timestamp_us"))?
            .value(row);
        let price = Price::parse_decimal(string("price")?)?;
        let size_str = string("size")?;
        let size = size_str
            .parse()
            .map_err(|_| LoadError::InvalidSize(size_str.to_string()))?;
        let side = parse_side(string("side")?)?;
        let origin = MarketId::new(string("origin_asset_id")?.to_string());

        Ok(Trade::new(timestamp, price, size, side, origin))
    }
}

/// The trades `side` column is the aggressor's side; anything else is corruption.
fn parse_side(side: &str) -> Result<OrderSide, LoadError> {
    match side {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(LoadError::InvalidSide(other.to_string())),
    }
}

impl Iterator for TelonexTradesSource {
    type Item = Result<Trade, LoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = match &self.current {
                Some(batch) if self.row < batch.num_rows() => batch.clone(),
                _ => match self.batches.next()? {
                    Ok(batch) => {
                        self.current = Some(batch);
                        self.row = 0;
                        continue;
                    }
                    Err(err) => return Some(Err(err.into())),
                },
            };
            let row = self.row;
            self.row += 1;

            return Some(Self::parse_row(&batch, row).and_then(|trade| {
                if trade.timestamp < self.last_timestamp {
                    return Err(LoadError::OutOfOrder {
                        previous: self.last_timestamp,
                        got: trade.timestamp,
                    });
                }
                self.last_timestamp = trade.timestamp;
                Ok(trade)
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder};
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::domain::marketstate::BookError;

    /// A snapshot row for the fixture writer: (timestamp_us, bids, asks) with
    /// levels as (price, size) strings.
    type Row<'a> = (i64, Vec<(&'a str, &'a str)>, Vec<(&'a str, &'a str)>);

    fn level_fields() -> Fields {
        Fields::from(vec![
            Field::new("price", DataType::Utf8, true),
            Field::new("size", DataType::Utf8, true),
        ])
    }

    fn ladder_array(sides: &[&[(&str, &str)]]) -> ListArray {
        let mut builder = ListBuilder::new(StructBuilder::from_fields(level_fields(), 4));
        for side in sides {
            for (price, size) in *side {
                let level = builder.values();
                level
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(price);
                level
                    .field_builder::<StringBuilder>(1)
                    .unwrap()
                    .append_value(size);
                level.append(true);
            }
            builder.append(true);
        }
        builder.finish()
    }

    /// Write rows as an in-memory `book_snapshot_full` Parquet file.
    fn book_file(rows: &[Row]) -> Bytes {
        let ladder_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(level_fields()),
            true,
        )));
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("asset_id", DataType::Utf8, false),
            Field::new("bids", ladder_type.clone(), true),
            Field::new("asks", ladder_type, true),
        ]));

        let timestamps: Int64Array = rows.iter().map(|r| r.0).collect();
        let assets: StringArray = rows.iter().map(|_| Some("token-0")).collect();
        let bids = ladder_array(&rows.iter().map(|r| r.1.as_slice()).collect::<Vec<_>>());
        let asks = ladder_array(&rows.iter().map(|r| r.2.as_slice()).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(timestamps),
                Arc::new(assets),
                Arc::new(bids),
                Arc::new(asks),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn source(rows: &[Row]) -> TelonexBookSource {
        TelonexBookSource::from_chunk_reader(book_file(rows)).unwrap()
    }

    #[test]
    fn streams_snapshots_in_order() {
        let mut src = source(&[
            (
                1_000,
                vec![("0.08", "20"), ("0.079", "54.99")],
                vec![("0.085", "623.6")],
            ),
            (2_000, vec![("0.08", "25")], vec![("0.085", "600")]),
        ]);

        let first = src.next().unwrap().unwrap();
        assert_eq!(first.market_id, MarketId::new("token-0".to_string()));
        assert_eq!(first.timestamp, 1_000);
        assert_eq!(first.bids().len(), 2);
        assert_eq!(
            first.bid().unwrap().price,
            Price::parse_decimal("0.08").unwrap()
        );
        assert!((first.ask().unwrap().size - 623.6).abs() < 1e-9);

        let second = src.next().unwrap().unwrap();
        assert_eq!(second.timestamp, 2_000);
        assert!(src.next().is_none());
    }

    #[test]
    fn empty_side_is_a_valid_book() {
        let mut src = source(&[(1_000, vec![], vec![("0.99", "10")])]);
        let state = src.next().unwrap().unwrap();
        assert!(state.bid().is_none());
        assert!(state.ask().is_some());
    }

    #[test]
    fn off_grid_price_fails_fast() {
        let mut src = source(&[(1_000, vec![("0.0850001", "20")], vec![])]);
        assert!(matches!(src.next().unwrap(), Err(LoadError::Price(_))));
    }

    #[test]
    fn bad_size_fails_fast() {
        let mut src = source(&[(1_000, vec![("0.08", "lots")], vec![])]);
        assert!(matches!(
            src.next().unwrap(),
            Err(LoadError::InvalidSize(_))
        ));
    }

    #[test]
    fn unsorted_ladder_fails_fast() {
        let mut src = source(&[(1_000, vec![("0.08", "1"), ("0.081", "1")], vec![])]);
        assert!(matches!(
            src.next().unwrap(),
            Err(LoadError::Book(BookError::UnsortedBids { index: 1 }))
        ));
    }

    #[test]
    fn out_of_order_timestamps_fail_fast() {
        let mut src = source(&[
            (2_000, vec![("0.08", "1")], vec![("0.09", "1")]),
            (1_000, vec![("0.08", "1")], vec![("0.09", "1")]),
        ]);
        assert!(src.next().unwrap().is_ok());
        assert!(matches!(
            src.next().unwrap(),
            Err(LoadError::OutOfOrder { .. })
        ));
    }

    #[test]
    fn missing_column_fails_at_open() {
        // A file with no `asks` column at all.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_us",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(matches!(
            TelonexBookSource::from_chunk_reader(Bytes::from(buf)),
            Err(LoadError::MissingColumn(_))
        ));
    }

    #[test]
    #[ignore = "reads the real data file; run with `cargo test -- --ignored`"]
    fn streams_real_file() {
        let path = std::fs::read_dir("data/telonex_example")
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("_book_snapshot_full.parquet"))
            })
            .expect("a book_snapshot_full file in data/telonex_example");

        let source = TelonexBookSource::from_path(path).unwrap();
        // The first handful of real snapshots should parse cleanly (which
        // exercises sortedness, crossing, grid and monotonicity checks).
        let ok = source.take(100).filter(Result::is_ok).count();
        assert_eq!(ok, 100);
    }

    // --- trades reader ------------------------------------------------------

    /// Write rows `(timestamp_us, price, size, side)` as an in-memory `trades`
    /// Parquet file (origin_asset_id filled with a constant).
    fn trades_file(rows: &[(i64, &str, &str, &str)]) -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("price", DataType::Utf8, false),
            Field::new("size", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("origin_asset_id", DataType::Utf8, false),
        ]));
        let ts: Int64Array = rows.iter().map(|r| r.0).collect();
        let price: StringArray = rows.iter().map(|r| Some(r.1)).collect();
        let size: StringArray = rows.iter().map(|r| Some(r.2)).collect();
        let side: StringArray = rows.iter().map(|r| Some(r.3)).collect();
        let origin: StringArray = rows.iter().map(|_| Some("origin-0")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts),
                Arc::new(price),
                Arc::new(size),
                Arc::new(side),
                Arc::new(origin),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn trades_source(rows: &[(i64, &str, &str, &str)]) -> TelonexTradesSource {
        TelonexTradesSource::from_chunk_reader(trades_file(rows)).unwrap()
    }

    #[test]
    fn streams_trades_in_order() {
        let mut src = trades_source(&[
            (1_000, "0.36", "13.5", "buy"),
            (2_000, "0.35", "100", "sell"),
        ]);

        let t0 = src.next().unwrap().unwrap();
        assert_eq!(t0.timestamp, 1_000);
        assert_eq!(t0.price, Price::parse_decimal("0.36").unwrap());
        assert!((t0.size - 13.5).abs() < 1e-9);
        assert_eq!(t0.side, OrderSide::Buy);

        let t1 = src.next().unwrap().unwrap();
        assert_eq!(t1.side, OrderSide::Sell);
        assert!(src.next().is_none());
    }

    #[test]
    fn bad_trade_side_fails_fast() {
        let mut src = trades_source(&[(1_000, "0.36", "1", "lift")]);
        assert!(matches!(
            src.next().unwrap(),
            Err(LoadError::InvalidSide(_))
        ));
    }

    #[test]
    fn trades_out_of_order_fail_fast() {
        let mut src = trades_source(&[(2_000, "0.36", "1", "buy"), (1_000, "0.36", "1", "buy")]);
        assert!(src.next().unwrap().is_ok());
        assert!(matches!(
            src.next().unwrap(),
            Err(LoadError::OutOfOrder { .. })
        ));
    }

    #[test]
    fn trades_missing_column_fails_at_open() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_us",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(matches!(
            TelonexTradesSource::from_chunk_reader(Bytes::from(buf)),
            Err(LoadError::MissingColumn(_))
        ));
    }
}
