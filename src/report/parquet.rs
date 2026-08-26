//! Parquet output — a finished run as tables you can load straight into pandas.
//!
//! The JSON contract ([`super::write_json`]) is exact but verbose: a 72k-snapshot
//! run is ~41 MB of pretty-printed text, and a busy day is closer to 350 MB.
//! That is fine as a wire format for the bundled HTML report and hopeless for
//! iterating in a notebook. The same data as columnar Parquet is roughly
//! 20–40x smaller, typed, and `pd.read_parquet` loads it instantly — and it
//! makes the pipeline pleasingly symmetric: Telonex Parquet in, pmbt Parquet out.
//!
//! A run writes a **directory**, because these are genuinely different tables:
//!
//! | file | one row per |
//! |---|---|
//! | `samples.parquet` | book snapshot — price, cash, position, equity, PnL |
//! | `fills.parquet` | execution — time, side, price, size, maker/taker |
//! | `blotter.parquet` | execution, with running position and realized P&L |
//! | `depth.parquet` | book *level* — long format, not nested |
//! | `summary.json` | the scalars: result, stats, trade stats |
//!
//! `depth` is deliberately **long** (`ts, side, level, price, size`) rather than
//! a nested list-of-structs: nested Parquet is awkward in pandas, while a long
//! table pivots in one line.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::engine::BacktestResult;

use super::record::TimeSeriesRecorder;
use super::{blotter, series, stats, Summary};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Write one table to `dir/name.parquet`, SNAPPY-compressed like the converter.
fn write_table(dir: &Path, name: &str, columns: Vec<(&str, ArrayRef)>) -> Result<()> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, array)| array).collect(),
    )?;

    let file = std::fs::File::create(dir.join(format!("{name}.parquet")))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Write a finished run into `dir` as the table set above, creating the
/// directory if needed.
pub fn write_run(dir: &Path, result: &BacktestResult, recorder: &TimeSeriesRecorder) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let samples = recorder.samples();
    let fills = recorder.fills();

    // --- samples: one row per snapshot -------------------------------------
    let f64s = |values: Vec<f64>| Arc::new(Float64Array::from(values)) as ArrayRef;
    let opts = |values: Vec<Option<f64>>| Arc::new(Float64Array::from(values)) as ArrayRef;
    write_table(
        dir,
        "samples",
        vec![
            (
                "ts",
                Arc::new(Int64Array::from(
                    samples.iter().map(|s| s.ts).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            ("bid", opts(samples.iter().map(|s| s.bid).collect())),
            ("ask", opts(samples.iter().map(|s| s.ask).collect())),
            ("mid", opts(samples.iter().map(|s| s.mid).collect())),
            ("cash", f64s(samples.iter().map(|s| s.cash).collect())),
            ("qty", f64s(samples.iter().map(|s| s.qty).collect())),
            (
                "avg_cost",
                f64s(samples.iter().map(|s| s.avg_cost).collect()),
            ),
            ("equity", f64s(samples.iter().map(|s| s.equity).collect())),
            ("pnl", f64s(samples.iter().map(|s| s.pnl).collect())),
            // Where OUR quotes sat — the envelope the report draws.
            (
                "quote_bid",
                opts(samples.iter().map(|s| s.quote_bid).collect()),
            ),
            (
                "quote_ask",
                opts(samples.iter().map(|s| s.quote_ask).collect()),
            ),
        ],
    )?;

    // --- fills: one row per execution --------------------------------------
    write_table(
        dir,
        "fills",
        vec![
            (
                "ts",
                Arc::new(Int64Array::from(
                    fills.iter().map(|f| f.ts).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "side",
                Arc::new(StringArray::from(
                    fills.iter().map(|f| f.side).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            ("price", f64s(fills.iter().map(|f| f.price).collect())),
            ("qty", f64s(fills.iter().map(|f| f.qty).collect())),
        ],
    )?;

    // --- blotter: executions folded through the engine's own accounting ----
    let (rows, trade_stats) = blotter::compute(fills);
    write_table(
        dir,
        "blotter",
        vec![
            (
                "ts",
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "side",
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.side).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            ("price", f64s(rows.iter().map(|r| r.price).collect())),
            ("qty", f64s(rows.iter().map(|r| r.qty).collect())),
            (
                "position_after",
                f64s(rows.iter().map(|r| r.position_after).collect()),
            ),
            ("realized", f64s(rows.iter().map(|r| r.realized).collect())),
        ],
    )?;

    // --- depth: long form, one row per level -------------------------------
    let (mut ts, mut side, mut level, mut price, mut size) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for frame in recorder.depth() {
        for (name, levels) in [("bid", &frame.bids), ("ask", &frame.asks)] {
            for (index, entry) in levels.iter().enumerate() {
                ts.push(frame.ts);
                side.push(name);
                level.push(index as i32);
                price.push(entry.price);
                size.push(entry.size);
            }
        }
    }
    write_table(
        dir,
        "depth",
        vec![
            ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
            ("side", Arc::new(StringArray::from(side)) as ArrayRef),
            ("level", Arc::new(Int32Array::from(level)) as ArrayRef),
            ("price", f64s(price)),
            ("size", f64s(size)),
        ],
    )?;

    // --- orders: the lifecycle, one row per event --------------------------
    let events = recorder.orders();
    write_table(
        dir,
        "orders",
        vec![
            (
                "ts",
                Arc::new(Int64Array::from(
                    events.iter().map(|e| e.ts).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int64Array::from(
                    events.iter().map(|e| e.id as i64).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "kind",
                Arc::new(StringArray::from(
                    events
                        .iter()
                        .map(|e| match e.kind {
                            crate::report::record::OrderEventKind::Placed => "placed",
                            crate::report::record::OrderEventKind::Rested => "rested",
                            crate::report::record::OrderEventKind::Cancelled => "cancelled",
                            crate::report::record::OrderEventKind::Expired => "expired",
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "side",
                Arc::new(StringArray::from(
                    events.iter().map(|e| e.side).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            ("price", opts(events.iter().map(|e| e.price).collect())),
            (
                "quantity",
                f64s(events.iter().map(|e| e.quantity).collect()),
            ),
        ],
    )?;

    // --- summary: the scalars, small enough to stay readable ---------------
    let grid = series::choose_grid(samples);
    let document = serde_json::json!({
        "summary": Summary::from(result),
        "stats": stats::compute(samples, fills, result, grid),
        "trade_stats": trade_stats,
        "order_stats": super::orders::compute(events, result.fills),
    });
    serde_json::to_writer_pretty(
        std::io::BufWriter::new(std::fs::File::create(dir.join("summary.json"))?),
        &document,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::marketstate::{Level, MarketState};
    use crate::domain::types::{MarketId, Price};
    use crate::engine::{Engine, Simulation};
    use crate::fill::sweep::SweepFillModel;
    use crate::io;
    use crate::report::record::RecorderConfig;

    fn snapshot(ts: i64, bid: f64, ask: f64) -> MarketState {
        let level = |p: f64| {
            Box::from([Level {
                price: Price::from_probability(p).unwrap(),
                size: 1_000.0,
            }])
        };
        MarketState::new(
            MarketId::new("token".to_string()),
            ts,
            level(bid),
            level(ask),
        )
        .unwrap()
    }

    #[test]
    fn writes_every_table() {
        let mut recorder = TimeSeriesRecorder::new(RecorderConfig::default());
        let feed = io::fixture(vec![
            snapshot(1_000_000, 0.30, 0.31),
            snapshot(2_000_000, 0.40, 0.41),
        ]);
        let result = Engine::new(10_000.0, Box::new(SweepFillModel), Simulation::zero())
            .run_recorded(
                feed,
                &mut crate::buyandhold::BuyAndHold::new(100.0),
                Some(Price::from_probability(1.0).unwrap()),
                &mut recorder,
            )
            .unwrap();

        let dir = std::env::temp_dir().join(format!("pmbt_parquet_{}", std::process::id()));
        write_run(&dir, &result, &recorder).unwrap();

        for name in ["samples", "fills", "blotter", "depth"] {
            let path = dir.join(format!("{name}.parquet"));
            assert!(path.exists(), "{name}.parquet should be written");
            // Readable back, with the rows we expect.
            let source = std::fs::File::open(&path).unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(source)
                    .unwrap()
                    .build()
                    .unwrap();
            let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
            assert!(rows > 0, "{name}.parquet should have rows");
        }
        assert!(dir.join("summary.json").exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
