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
    // An empty side is a valid (empty) ladder — MarketState allows empty sides.
    // When EVERY row of a file has a given side empty (e.g. a book that has
    // collapsed to one side by the settlement day), Arrow can't infer the inner
    // struct type and types the whole column `list<null>`, so the struct cast
    // below fails. Short-circuit the empty row before the cast.
    if levels.is_empty() {
        return Ok(Box::new([]));
    }
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

/// The columns every trade row needs; the rest of the file (`local_timestamp_us`,
/// `exchange`, `slug`, `trade_id`, …) is never decoded. The origin token is a
/// sixth, projected separately because Telonex names it inconsistently — see
/// [`TelonexTradesSource::from_chunk_reader`].
const TRADE_COLUMNS: [&str; 4] = ["timestamp_us", "price", "size", "side"];

/// Streams a Telonex `trades` Parquet file as [`Trade`]s — the sibling of
/// [`TelonexBookSource`], built the same way (project, stream batch-by-batch,
/// parse strings through the strict doors, guard monotonic time). One print per
/// row; `side` is the aggressor's side.
pub struct TelonexTradesSource {
    batches: ParquetRecordBatchReader,
    current: Option<RecordBatch>,
    row: usize,
    last_timestamp: i64,
    /// Which projected column carries the trade origin: `origin_asset_id` when
    /// the file has it, else `asset_id` (this file's own token) as the fallback.
    origin_col: &'static str,
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
        let fields = schema.root_schema().get_fields();

        // Telonex ships `origin_asset_id` (which token's book a print originated
        // on) inconsistently — ~1/3 of real `trades` files omit it. When it's
        // absent, this file's own `asset_id` IS the origin (one file = one
        // token), so we project that instead. The field is unused in v1 (carried
        // for future complement-aware attribution), and files that DO carry the
        // column still read it — so their decode stays byte-identical.
        let origin_col: &'static str = if fields.iter().any(|f| f.name() == "origin_asset_id") {
            "origin_asset_id"
        } else {
            "asset_id"
        };

        let indices = TRADE_COLUMNS
            .iter()
            .copied()
            .chain(std::iter::once(origin_col))
            .map(|name| {
                fields
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or(LoadError::MissingColumn(name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mask = ProjectionMask::roots(schema, indices);

        Ok(Self {
            batches: builder.with_projection(mask).build()?,
            current: None,
            row: 0,
            last_timestamp: i64::MIN,
            origin_col,
        })
    }

    /// Decode one row of a projected batch into a [`Trade`]. `origin_col` names
    /// the projected origin column (`origin_asset_id` or the `asset_id` fallback).
    fn parse_row(
        batch: &RecordBatch,
        row: usize,
        origin_col: &'static str,
    ) -> Result<Trade, LoadError> {
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
        let origin = MarketId::new(string(origin_col)?.to_string());

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

            return Some(
                Self::parse_row(&batch, row, self.origin_col).and_then(|trade| {
                    if trade.timestamp < self.last_timestamp {
                        return Err(LoadError::OutOfOrder {
                            previous: self.last_timestamp,
                            got: trade.timestamp,
                        });
                    }
                    self.last_timestamp = trade.timestamp;
                    Ok(trade)
                }),
            );
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
    fn side_typed_list_null_reads_as_empty_ladder() {
        // When a side is empty in EVERY row, Arrow can't infer the inner struct
        // and types the whole column `list<null>` (real settlement-day files do
        // this — the book has collapsed to one side). That side must read as an
        // empty ladder, not fail the struct cast. The other side still decodes.
        use arrow_array::{ArrayRef, ListArray, NullArray};
        use arrow_buffer::OffsetBuffer;

        let bids = ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0i32, 0].into()), // one row, empty
            Arc::new(NullArray::new(0)) as ArrayRef,
            None,
        );
        let asks = ladder_array(&[&[("0.60", "5")]]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("asset_id", DataType::Utf8, false),
            Field::new(
                "bids",
                DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
                true,
            ),
            Field::new(
                "asks",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(level_fields()),
                    true,
                ))),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_000])),
                Arc::new(StringArray::from(vec!["token-0"])),
                Arc::new(bids),
                Arc::new(asks),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut src = TelonexBookSource::from_chunk_reader(Bytes::from(buf)).unwrap();
        let state = src.next().unwrap().unwrap();
        assert!(state.bid().is_none()); // the list<null> side
        assert_eq!(
            state.ask().unwrap().price,
            Price::parse_decimal("0.60").unwrap()
        );
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
        let path = std::fs::read_dir(crate::test_support::data_path("data/telonex_example"))
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

    /// Build an in-memory `trades` file from explicit fields, so a test can
    /// choose exactly which of `asset_id` / `origin_asset_id` are present.
    fn trades_bytes(schema: Arc<Schema>, columns: Vec<arrow_array::ArrayRef>) -> Bytes {
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    #[test]
    fn trades_without_origin_column_fall_back_to_asset_id() {
        // Real Telonex omits `origin_asset_id` on ~1/3 of trades files; the
        // file's own `asset_id` is then the correct origin (one file = one token).
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("price", DataType::Utf8, false),
            Field::new("size", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("asset_id", DataType::Utf8, false),
        ]));
        let bytes = trades_bytes(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_000])),
                Arc::new(StringArray::from(vec!["0.42"])),
                Arc::new(StringArray::from(vec!["7"])),
                Arc::new(StringArray::from(vec!["buy"])),
                Arc::new(StringArray::from(vec!["token-99"])),
            ],
        );

        let mut src = TelonexTradesSource::from_chunk_reader(bytes).unwrap();
        let t = src.next().unwrap().unwrap();
        assert_eq!(t.price, Price::parse_decimal("0.42").unwrap());
        assert_eq!(t.origin_asset_id, MarketId::new("token-99".to_string()));
        assert!(src.next().is_none());
    }

    #[test]
    fn trades_prefer_origin_asset_id_over_asset_id() {
        // When both columns exist (the regression corpus), origin comes from
        // `origin_asset_id` — the complement-aware value — not `asset_id`, so a
        // file that carries the column decodes exactly as before this fallback.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("price", DataType::Utf8, false),
            Field::new("size", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("asset_id", DataType::Utf8, false),
            Field::new("origin_asset_id", DataType::Utf8, false),
        ]));
        let bytes = trades_bytes(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_000])),
                Arc::new(StringArray::from(vec!["0.5"])),
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(StringArray::from(vec!["sell"])),
                Arc::new(StringArray::from(vec!["file-token"])),
                Arc::new(StringArray::from(vec!["complement-token"])),
            ],
        );

        let mut src = TelonexTradesSource::from_chunk_reader(bytes).unwrap();
        let t = src.next().unwrap().unwrap();
        assert_eq!(
            t.origin_asset_id,
            MarketId::new("complement-token".to_string())
        );
    }
}
