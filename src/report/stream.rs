//! Bounded-history Parquet recording. Table rows are flushed in batches;
//! summaries retain scalar state plus currently resting order IDs. Return
//! metrics reread only timestamp/equity columns at the end on a bounded grid.
use super::{
    blotter, orders,
    parquet::emit_tables,
    record::{OrderEvent, Quotes, Recorder, RecorderConfig, TimeSeriesRecorder},
    stats, Summary,
};
use crate::{
    domain::{fill::Fill, marketstate::MarketState, order::OrderSide},
    engine::{BacktestResult, Settlement},
    portfolio::Portfolio,
};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{Field, Schema};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const BATCH_ROWS: usize = 4096;

pub struct ParquetRecorder {
    dir: PathBuf,
    buffer: TimeSeriesRecorder,
    writers: HashMap<String, ArrowWriter<Box<dyn Write + Send>>>,
    error: Option<String>,
    limit: usize,
    samples: u64,
    notional: f64,
    blotter: blotter::Accumulator,
    orders: orders::Accumulator,
    path: stats::PathAccumulator,
}
impl ParquetRecorder {
    /// Require an absent or empty directory so a failed run cannot leave a
    /// stale success summary from an earlier run.
    pub fn new(dir: &Path, config: RecorderConfig) -> Result<Self> {
        if dir.exists() {
            if dir.read_dir()?.next().is_some() {
                return Err("record directory must be empty".into());
            }
        } else {
            std::fs::create_dir_all(dir)?;
        }
        let mut this = Self {
            dir: dir.to_path_buf(),
            buffer: TimeSeriesRecorder::new(config),
            writers: HashMap::new(),
            error: None,
            limit: BATCH_ROWS,
            samples: 0,
            notional: 0.,
            blotter: blotter::Accumulator::default(),
            orders: orders::Accumulator::default(),
            path: stats::PathAccumulator::default(),
        };
        // Initialize schemas even if the run never emits a row.
        this.flush()?;
        Ok(this)
    }
    fn flush(&mut self) -> Result<()> {
        let rows: Vec<_> = self
            .buffer
            .fills()
            .iter()
            .map(|f| self.blotter.push(f))
            .collect();
        let dir = &self.dir;
        let writers = &mut self.writers;
        emit_tables(
            &self.buffer,
            &rows,
            |name, columns: Vec<(&str, ArrayRef)>| {
                let schema = Arc::new(Schema::new(
                    columns
                        .iter()
                        .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
                        .collect::<Vec<_>>(),
                ));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    columns.into_iter().map(|(_, a)| a).collect(),
                )?;
                if !writers.contains_key(name) {
                    let props = WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .set_max_row_group_row_count(Some(BATCH_ROWS))
                        .build();
                    writers.insert(
                        name.into(),
                        ArrowWriter::try_new(
                            Box::new(File::create(dir.join(format!("{name}.parquet")))?)
                                as Box<dyn Write + Send>,
                            schema,
                            Some(props),
                        )?,
                    );
                }
                let writer = writers.get_mut(name).expect("initialized above");
                if batch.num_rows() > 0 {
                    writer.write(&batch)?;
                    writer.flush()?;
                    writer.sync()?;
                }
                Ok(())
            },
        )?;
        self.buffer.clear_buffers();
        Ok(())
    }
    fn maybe_flush(&mut self) {
        if self.error.is_none() && self.buffer.buffered_rows() >= self.limit {
            if let Err(err) = self.flush() {
                self.error = Some(err.to_string());
            }
        }
    }
    fn sample(
        &mut self,
        state: &MarketState,
        portfolio: &Portfolio,
        quotes: Quotes,
        is_book: bool,
    ) {
        if self.error.is_some() {
            return;
        }
        if is_book {
            self.buffer.on_snapshot(state, portfolio, quotes);
        } else {
            self.buffer.on_event(state, portfolio, quotes);
        }
        if let Some(s) = self.buffer.samples().last() {
            self.path.observe(s.ts, s.qty, s.equity);
        }
        self.samples += 1;
        self.maybe_flush();
    }
    /// Close all tables before publishing the success summary. On failure the
    /// directory remains incomplete and no summary is published.
    pub fn finish(mut self, result: &BacktestResult) -> Result<u64> {
        self.check()?;
        self.flush()?;
        for (_, writer) in self.writers.drain() {
            writer.close()?;
        }
        let grid_us = self.path.grid_us();
        let grid = read_grid(&self.dir.join("samples.parquet"), result, grid_us)?;
        if !matches!(result.settlement, Settlement::Unresolved) {
            if let Some(ts) = self.path.last_ts {
                self.path.observe(ts, 0., result.final_cash);
            }
        }
        let turnover = if result.initial_capital > 0. {
            self.notional / result.initial_capital
        } else {
            0.
        };
        let summary = serde_json::json!({
            "summary":Summary::from(result),
            "stats":stats::from_grid(&grid,result,grid_us,self.path.drawdown,self.path.exposure(),turnover),
            "trade_stats":self.blotter.stats(),
            "order_stats":self.orders.stats(result.fills),
        });
        let temporary = self.dir.join("summary.json.tmp");
        let mut file = std::io::BufWriter::new(File::create(&temporary)?);
        serde_json::to_writer_pretty(&mut file, &summary)?;
        file.flush()?;
        drop(file);
        std::fs::rename(temporary, self.dir.join("summary.json"))?;
        Ok(self.samples)
    }
}
impl Recorder for ParquetRecorder {
    fn on_snapshot(&mut self, s: &MarketState, p: &Portfolio, q: Quotes) {
        self.sample(s, p, q, true);
    }
    fn on_event(&mut self, s: &MarketState, p: &Portfolio, q: Quotes) {
        self.sample(s, p, q, false);
    }
    fn on_fill(&mut self, ts: i64, side: OrderSide, fill: &Fill) {
        if self.error.is_some() {
            return;
        }
        self.notional += fill.price.as_f64() * fill.quantity;
        self.buffer.on_fill(ts, side, fill);
        self.maybe_flush();
    }
    fn on_order(&mut self, event: &OrderEvent) {
        if self.error.is_some() {
            return;
        }
        self.orders.push(event);
        self.buffer.on_order(event);
        self.maybe_flush();
    }
    fn on_order_filled(&mut self, id: u64) {
        self.orders.filled(id);
    }
    fn check(&self) -> std::io::Result<()> {
        match &self.error {
            Some(err) => Err(std::io::Error::other(err.clone())),
            None => Ok(()),
        }
    }
}

/// LOCF resampling in one pass. Only ~480 grid values survive, regardless of
/// run length. Duplicate timestamps use the last event (including settlement).
fn read_grid(path: &Path, result: &BacktestResult, grid_us: i64) -> Result<Vec<f64>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let indices = [
        builder.schema().index_of("ts")?,
        builder.schema().index_of("equity")?,
    ];
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indices);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_ROWS)
        .build()?;
    let mut previous: Option<(i64, f64)> = None;
    let mut first: Option<(i64, f64)> = None;
    let mut next = 0;
    let mut grid = Vec::new();
    for batch in reader {
        let batch = batch?;
        let timestamps = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column_by_name("equity")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let ts = timestamps.value(i);
            let value = values.value(i);
            if let Some((_, prev)) = previous {
                while next < ts {
                    grid.push(prev);
                    next += grid_us;
                }
            } else {
                next = ts;
                first = Some((ts, value));
            }
            previous = Some((ts, value));
        }
    }
    if let Some((ts, value)) = previous {
        // Preserve the existing zero-span metric convention.
        if first.unwrap().0 == ts {
            return Ok(vec![first.unwrap().1]);
        }
        let value = if matches!(result.settlement, Settlement::Unresolved) {
            value
        } else {
            result.final_cash
        };
        if next <= ts {
            grid.push(value);
        }
    }
    Ok(grid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            marketstate::Level,
            types::{MarketId, Price},
        },
        engine::{Engine, RunError, Simulation},
        fill::sweep::SweepFillModel,
        io,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "pmbt_stream_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn snapshot(ts: i64) -> MarketState {
        MarketState::new(
            MarketId::new("token".into()),
            ts,
            Box::from([Level {
                price: Price::new(400_000).unwrap(),
                size: 100.,
            }]),
            Box::from([Level {
                price: Price::new(500_000).unwrap(),
                size: 100.,
            }]),
        )
        .unwrap()
    }
    fn run(recorder: &mut dyn Recorder, n: usize, resolved: bool) -> BacktestResult {
        let feed = io::fixture((0..n).map(|i| snapshot(i as i64 * 1_000_003)).collect());
        let mut sim = Simulation::zero();
        sim.fee_rate = 0.05;
        Engine::new(10000., Box::new(SweepFillModel), sim)
            .run_recorded(
                feed,
                &mut crate::test_support::AlternateSides::new(1.),
                resolved.then(|| Price::new(1_000_000).unwrap()),
                recorder,
            )
            .unwrap()
    }
    fn json(path: &Path) -> serde_json::Value {
        serde_json::from_reader(File::open(path).unwrap()).unwrap()
    }
    fn table_rows(path: &Path) -> Vec<String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .with_batch_size(1)
            .build()
            .unwrap();
        reader
            .flat_map(|b| {
                let b = b.unwrap();
                b.columns()
                    .iter()
                    .map(|c| format!("{c:?}"))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    #[test]
    fn streaming_matches_buffered_tables_and_summaries_across_batches() {
        for (n, resolved) in [(0, false), (1, true), (37, false), (1001, true)] {
            let streamed = dir();
            let buffered = dir();
            let mut stream = ParquetRecorder::new(&streamed, RecorderConfig::default()).unwrap();
            stream.limit = 8;
            let result = run(&mut stream, n, resolved);
            assert!(stream.buffer.buffered_rows() < 8);
            assert_eq!(stream.finish(&result).unwrap(), n as u64);
            let mut rec = TimeSeriesRecorder::new(RecorderConfig::default());
            let other = run(&mut rec, n, resolved);
            assert_eq!(result, other);
            super::super::parquet::write_run(&buffered, &other, &rec).unwrap();
            assert_eq!(
                json(&streamed.join("summary.json")),
                json(&buffered.join("summary.json")),
                "{n} samples"
            );
            for name in ["samples", "fills", "blotter", "depth", "orders"] {
                assert_eq!(
                    table_rows(&streamed.join(format!("{name}.parquet"))),
                    table_rows(&buffered.join(format!("{name}.parquet"))),
                    "{name}"
                );
            }
            std::fs::remove_dir_all(streamed).unwrap();
            std::fs::remove_dir_all(buffered).unwrap();
        }
    }
    #[test]
    fn refuses_a_previous_run_directory() {
        let path = dir();
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("summary.json"), b"previous").unwrap();
        assert!(ParquetRecorder::new(&path, RecorderConfig::default()).is_err());
        assert_eq!(
            std::fs::read(path.join("summary.json")).unwrap(),
            b"previous"
        );
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn sink_failure_aborts_engine_and_does_not_publish_summary() {
        let path = dir();
        let mut stream = ParquetRecorder::new(&path, RecorderConfig::default()).unwrap();
        stream.limit = 1;
        struct FullDisk {
            remaining: usize,
        }
        impl Write for FullDisk {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.remaining == 0 {
                    return Err(std::io::Error::other("simulated full disk"));
                }
                let n = bytes.len().min(self.remaining);
                self.remaining -= n;
                Ok(n)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut schema = None;
        emit_tables(&stream.buffer, &[], |name, columns| {
            if name == "samples" {
                schema = Some(Arc::new(Schema::new(
                    columns
                        .iter()
                        .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
                        .collect::<Vec<_>>(),
                )));
            }
            Ok(())
        })
        .unwrap();
        // Permit the Parquet header, then fail during the first row group.
        let sink = Box::new(FullDisk { remaining: 4 }) as Box<dyn Write + Send>;
        stream.writers.insert(
            "samples".into(),
            ArrowWriter::try_new(sink, schema.unwrap(), None).unwrap(),
        );
        let result = Engine::new(100., Box::new(SweepFillModel), Simulation::zero()).run_recorded(
            io::fixture(vec![snapshot(0), snapshot(10)]),
            &mut crate::test_support::BuyOnce::new(1.),
            None,
            &mut stream,
        );
        assert!(matches!(result, Err(RunError::Recorder(_))));
        assert!(!path.join("summary.json").exists());
        drop(stream);
        std::fs::remove_dir_all(path).unwrap();
    }
}
