//! Synthetic Telonex fixtures, so public API tests need no downloaded data.
use arrow_array::{
    builder::{ListBuilder, StringBuilder, StructBuilder},
    Array, ArrayRef, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub struct TestDir(PathBuf);
impl TestDir {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pmbt-backtest-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write(path: &Path, columns: Vec<(&str, ArrayRef)>) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, a)| a).collect(),
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

pub fn book(path: &Path, token: &str, rows: &[(i64, &str, &str)]) {
    let ladder = |ask: bool| -> ArrayRef {
        let fields = vec![
            Field::new("price", DataType::Utf8, false),
            Field::new("size", DataType::Utf8, false),
        ];
        let mut builder = ListBuilder::new(StructBuilder::new(
            fields,
            vec![
                Box::new(StringBuilder::new()),
                Box::new(StringBuilder::new()),
            ],
        ));
        for row in rows {
            let level = builder.values();
            level
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(if ask { row.2 } else { row.1 });
            level
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value("1000");
            level.append(true);
            builder.append(true);
        }
        Arc::new(builder.finish())
    };
    write(
        path,
        vec![
            (
                "timestamp_us",
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
            ),
            (
                "asset_id",
                Arc::new(StringArray::from(vec![token; rows.len()])),
            ),
            ("bids", ladder(false)),
            ("asks", ladder(true)),
        ],
    );
}

pub fn trades(path: &Path) {
    write(
        path,
        vec![
            ("timestamp_us", Arc::new(Int64Array::from(vec![1_500_000]))),
            ("price", Arc::new(StringArray::from(vec!["0.5"]))),
            ("size", Arc::new(StringArray::from(vec!["5"]))),
            ("side", Arc::new(StringArray::from(vec!["buy"]))),
            (
                "origin_asset_id",
                Arc::new(StringArray::from(vec!["token"])),
            ),
        ],
    );
}

pub fn json(path: &Path) -> serde_json::Value {
    serde_json::from_reader(File::open(path).unwrap()).unwrap()
}
pub fn html_data(path: &Path) -> serde_json::Value {
    let html = fs::read_to_string(path).unwrap();
    let data = html
        .split("<script id=\"report-data\" type=\"application/json\">")
        .nth(1)
        .unwrap()
        .split("</script>")
        .next()
        .unwrap();
    serde_json::from_str(data).unwrap()
}

#[allow(dead_code)]
pub mod strategy {
    use pmbt::{
        domain::{
            marketstate::MarketState,
            order::{OrderRequest, OrderSide, TimeInForce},
        },
        portfolio::Portfolio,
        strategy::{Action, OpenOrder, Strategy},
    };
    pub struct BuyOnce {
        size: f64,
        submitted: bool,
    }
    impl BuyOnce {
        pub fn new(size: f64) -> Self {
            Self {
                size,
                submitted: false,
            }
        }
    }
    impl Strategy for BuyOnce {
        fn on_market_state(
            &mut self,
            state: &MarketState,
            _: &Portfolio,
            _: &[OpenOrder],
        ) -> Vec<Action> {
            if self.submitted {
                return vec![];
            }
            self.submitted = true;
            vec![Action::Place(OrderRequest::market(
                state.market_id().clone(),
                OrderSide::Buy,
                self.size,
                TimeInForce::Fok,
            ))]
        }
    }
}
