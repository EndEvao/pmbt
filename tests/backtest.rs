mod common;

use common::{book, html_data, json, trades, TestDir};
use pmbt::{
    config::{resolve, SimulationConfig},
    domain::{
        marketstate::MarketState,
        order::{OrderRequest, OrderSide, TimeInForce},
        types::{MarketId, Price},
    },
    engine::Engine,
    feed::FileFeed,
    portfolio::Portfolio,
    strategy::{Action, OpenOrder, Strategy},
    universe::Category,
    ArtifactKind, Backtest, BacktestConfig, BacktestError, OutputConfig, RecordingMode,
    RunDiagnostic, StrategyMetadata,
};
use std::{fs, path::Path};

// Written against the public trait, with no registry or changes inside pmbt.
#[derive(Default)]
struct PrivateStrategy {
    calls: usize,
}
impl Strategy for PrivateStrategy {
    fn on_market_state(&mut self, _: &MarketState, _: &Portfolio, _: &[OpenOrder]) -> Vec<Action> {
        self.calls += 1;
        if self.calls != 1 {
            return vec![];
        }
        vec![Action::Place(OrderRequest::market(
            MarketId::new("token".into()),
            OrderSide::Buy,
            20.0,
            TimeInForce::Fok,
        ))]
    }
}
fn fixture(dir: &TestDir) -> BacktestConfig {
    let path = dir.path("token_book_snapshot_full.parquet");
    book(
        &path,
        "token",
        &[(1_000_000, "0.4", "0.5"), (2_000_000, "0.5", "0.6")],
    );
    let mut config = BacktestConfig::new(path);
    config.initial_capital = 100.0;
    config.simulation.fill = Some("sweep".into());
    config.simulation.fees = Some(false);
    config.simulation.taker_delay = Some(false);
    config.resolution = Some(Price::parse_decimal("1").unwrap());
    config
}
fn outputs(dir: &TestDir, mask: u8) -> OutputConfig {
    OutputConfig {
        html: (mask & 1 != 0).then(|| dir.path("report.html")),
        json: (mask & 2 != 0).then(|| dir.path("run.json")),
        parquet: (mask & 4 != 0).then(|| dir.path("tables")),
    }
}

#[test]
fn custom_strategy_matches_direct_engine_without_recording() {
    let dir = TestDir::new();
    let config = fixture(&dir);
    let (feed, _) = FileFeed::from_book_paths(&config.book_paths).unwrap();
    let settings = resolve(Some(&config.simulation), &SimulationConfig::default()).unwrap();
    let expected = Engine::new(
        config.initial_capital,
        settings.fill(),
        settings.simulation(None),
    )
    .run(feed, &mut PrivateStrategy::default(), config.resolution)
    .unwrap();
    let mut strategy = PrivateStrategy::default();
    let outcome = Backtest::new(config.clone()).run(&mut strategy).unwrap();
    assert_eq!(outcome.result, expected);
    assert_eq!(outcome.result.final_cash, 110.0);
    assert_eq!(outcome.result.fills, 1);
    assert_eq!(strategy.calls, 2);
    assert_eq!(outcome.recording, RecordingMode::None);
    assert_eq!(outcome.recorded_samples, None);
    assert!(outcome.artifacts.is_empty());
    assert_eq!(
        outcome.diagnostics,
        vec![RunDiagnostic::MissingTradeTape {
            book_path: config.book_paths[0].clone()
        }]
    );
}

#[test]
fn every_output_combination_preserves_results_and_reports_actual_metadata() {
    for mask in 0..8 {
        let dir = TestDir::new();
        let mut config = fixture(&dir);
        config.category = Some(Category::Crypto);
        config.simulation.category = Some(Category::Sports);
        config.market_name = Some("Private market </script>".into());
        config.outcome = Some("Yes".into());
        let mut metadata = StrategyMetadata::new("Private strategy <v1>");
        metadata.parameters.push(("Size".into(), "20".into()));
        let requested = outputs(&dir, mask);
        let outcome = Backtest::new(config)
            .outputs(requested.clone())
            .metadata(metadata)
            .run(&mut PrivateStrategy::default())
            .unwrap();
        assert_eq!(outcome.result.final_cash, 110.0, "mask {mask}");
        assert_eq!(outcome.simulation.category, Some(Category::Crypto));
        assert_eq!(
            outcome.recording,
            match mask {
                0 => RecordingMode::None,
                4 => RecordingMode::StreamingParquet,
                _ => RecordingMode::Buffered,
            }
        );
        assert_eq!(outcome.artifacts.len(), mask.count_ones() as usize);
        for artifact in &outcome.artifacts {
            assert!(artifact.path.exists());
        }
        if let Some(path) = requested.json {
            let data = json(&path);
            assert_eq!(data["summary"]["final_cash"], 110.0);
            assert_eq!(
                data["samples"].as_array().unwrap().len() as u64,
                outcome.recorded_samples.unwrap()
            );
        }
        if let Some(path) = requested.parquet {
            assert_eq!(
                json(&path.join("summary.json"))["summary"]["final_cash"],
                110.0
            );
            assert!(path.join("samples.parquet").is_file());
        }
        if let Some(path) = requested.html {
            let data = html_data(&path);
            assert_eq!(data["context"]["market_name"], "Private market </script>");
            assert_eq!(data["context"]["outcome"], "Yes");
            assert_eq!(data["context"]["tape_present"], false);
            let settings = data["context"]["settings"].as_array().unwrap();
            assert!(settings.contains(&serde_json::json!(["Fill model", "sweep"])));
            assert!(settings.contains(&serde_json::json!(["Strategy / Size", "20"])));
            assert!(settings.contains(&serde_json::json!(["Category", "crypto"])));
            assert!(settings.contains(&serde_json::json!(["Trade-tape coverage", "0 / 1 files"])));
            let html = fs::read_to_string(path).unwrap();
            assert!(html.contains("Private strategy &lt;v1&gt;"));
        }
    }
}

#[test]
fn streaming_and_buffered_parquet_match_across_flush_boundaries() {
    let dir = TestDir::new();
    let mut config = fixture(&dir);
    let rows: Vec<_> = (1..=5000).map(|i| (i * 1_000_000, "0.4", "0.5")).collect();
    book(&config.book_paths[0], "token", &rows);
    config.resolution = None;
    let streamed = Backtest::new(config.clone())
        .outputs(OutputConfig::parquet(dir.path("stream")))
        .run(&mut PrivateStrategy::default())
        .unwrap();
    let buffered = Backtest::new(config)
        .outputs(OutputConfig {
            json: Some(dir.path("buffer.json")),
            parquet: Some(dir.path("buffer")),
            ..Default::default()
        })
        .run(&mut PrivateStrategy::default())
        .unwrap();
    assert_eq!(streamed.result, buffered.result);
    assert!(streamed.result.open_position.is_some());
    assert_eq!(streamed.recorded_samples, buffered.recorded_samples);
    assert!(streamed.recorded_samples.unwrap() > 4096);
    assert_eq!(
        json(&dir.path("stream/summary.json")),
        json(&dir.path("buffer/summary.json"))
    );
    // Verify the data tables, not only the summaries.
    fn table(path: &Path) -> Vec<arrow_array::RecordBatch> {
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(path).unwrap(),
        )
        .unwrap()
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }
    for name in ["samples", "fills", "depth", "orders"] {
        assert_eq!(
            table(&dir.path(&format!("stream/{name}.parquet"))),
            table(&dir.path(&format!("buffer/{name}.parquet"))),
            "{name}"
        );
    }
}

#[test]
fn missing_tapes_are_per_file_diagnostics_and_mixed_coverage_is_reported() {
    let dir = TestDir::new();
    let mut config = fixture(&dir);
    let second = dir.path("day2_book_snapshot_full.parquet");
    book(&second, "token", &[(3_000_000, "0.4", "0.5")]);
    config.book_paths.push(second.clone());
    trades(&dir.path("token_trades.parquet"));
    let result = Backtest::new(config.clone())
        .outputs(OutputConfig::html(dir.path("mixed.html")))
        .run(&mut PrivateStrategy::default())
        .unwrap();
    assert_eq!(
        result.diagnostics,
        vec![RunDiagnostic::MissingTradeTape { book_path: second }]
    );
    let data = html_data(&dir.path("mixed.html"));
    assert!(data["context"]["tape_present"].is_null());
    assert!(data["context"]["settings"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(["Trade-tape coverage", "1 / 2 files"])));
    config.book_paths.pop();
    let result = Backtest::new(config)
        .outputs(OutputConfig::html(dir.path("full.html")))
        .run(&mut PrivateStrategy::default())
        .unwrap();
    assert!(result.diagnostics.is_empty());
    assert_eq!(
        html_data(&dir.path("full.html"))["context"]["tape_present"],
        true
    );
}

#[test]
fn invalid_configuration_fails_before_running_strategy_or_creating_outputs() {
    let dir = TestDir::new();
    let config = fixture(&dir);
    for capital in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut invalid = config.clone();
        invalid.initial_capital = capital;
        let mut strategy = PrivateStrategy::default();
        assert!(matches!(
            Backtest::new(invalid)
                .outputs(OutputConfig::parquet(dir.path("invalid")))
                .run(&mut strategy),
            Err(BacktestError::InvalidCapital(_))
        ));
        assert_eq!(strategy.calls, 0);
        assert!(!dir.path("invalid").exists());
    }
    let mut empty = config.clone();
    empty.book_paths.clear();
    assert!(matches!(
        Backtest::new(empty).run(&mut PrivateStrategy::default()),
        Err(BacktestError::NoInputs)
    ));
    let mut invalid = config.clone();
    invalid.simulation.fill = Some("typo".into());
    assert!(matches!(
        Backtest::new(invalid).run(&mut PrivateStrategy::default()),
        Err(BacktestError::Configuration(_))
    ));
    let invalid = BacktestConfig::new(dir.path("wrong.parquet"));
    assert!(matches!(
        Backtest::new(invalid).run(&mut PrivateStrategy::default()),
        Err(BacktestError::InvalidBookPath(_))
    ));
}

#[test]
fn input_errors_are_distinct_from_missing_optional_tapes() {
    let dir = TestDir::new();
    let config = fixture(&dir);
    fs::write(dir.path("token_trades.parquet"), "invalid parquet").unwrap();
    let mut strategy = PrivateStrategy::default();
    assert!(matches!(
        Backtest::new(config.clone()).run(&mut strategy),
        Err(BacktestError::Input { .. })
    ));
    assert_eq!(strategy.calls, 0);
    fs::remove_file(&config.book_paths[0]).unwrap();
    assert!(matches!(
        Backtest::new(config.clone()).run(&mut strategy),
        Err(BacktestError::Input { .. })
    ));
    fs::write(&config.book_paths[0], "invalid book").unwrap();
    assert!(matches!(
        Backtest::new(config).run(&mut strategy),
        Err(BacktestError::Input { .. })
    ));
}

#[test]
fn feed_errors_do_not_publish_success_artifacts() {
    for mask in [0, 2, 4] {
        let dir = TestDir::new();
        let config = fixture(&dir);
        book(
            &config.book_paths[0],
            "token",
            &[(2_000_000, "0.4", "0.5"), (1_000_000, "0.4", "0.5")],
        );
        let error = Backtest::new(config)
            .outputs(outputs(&dir, mask))
            .run(&mut PrivateStrategy::default())
            .unwrap_err();
        assert!(matches!(error, BacktestError::Simulation(_)), "{error}");
        assert!(!dir.path("run.json").exists());
        assert!(!dir.path("tables/summary.json").exists());
    }
}

#[test]
fn multiple_tokens_are_rejected_in_one_run() {
    let dir = TestDir::new();
    let mut config = fixture(&dir);
    let other = dir.path("other_book_snapshot_full.parquet");
    book(&other, "different-token", &[(3_000_000, "0.4", "0.5")]);
    config.book_paths.push(other);
    let error = Backtest::new(config)
        .outputs(OutputConfig::json(dir.path("run.json")))
        .run(&mut PrivateStrategy::default())
        .unwrap_err();
    assert!(
        matches!(error, BacktestError::MixedMarkets { expected, found } if expected == "token" && found == "different-token")
    );
    assert!(!dir.path("run.json").exists());
}

#[test]
fn output_conflicts_fail_before_strategy_and_preserve_existing_data() {
    let dir = TestDir::new();
    let config = fixture(&dir);
    fs::write(dir.path("existing.json"), "keep me").unwrap();
    fs::create_dir(dir.path("existing-tables")).unwrap();
    fs::write(dir.path("existing-tables/summary.json"), "keep me too").unwrap();
    fs::create_dir(dir.path("nested")).unwrap();
    for output in [
        OutputConfig::json(dir.path("existing.json")),
        OutputConfig::parquet(dir.path("existing-tables")),
        OutputConfig::html(config.book_paths[0].clone()),
        OutputConfig {
            html: Some(dir.path("same")),
            json: Some(dir.path("same")),
            ..Default::default()
        },
        OutputConfig {
            json: Some(dir.path("nested/run.json")),
            parquet: Some(dir.path("nested")),
            ..Default::default()
        },
    ] {
        let mut strategy = PrivateStrategy::default();
        assert!(matches!(
            Backtest::new(config.clone())
                .outputs(output)
                .run(&mut strategy),
            Err(BacktestError::InvalidOutput { .. })
        ));
        assert_eq!(strategy.calls, 0);
    }
    assert_eq!(
        fs::read_to_string(dir.path("existing.json")).unwrap(),
        "keep me"
    );
    assert_eq!(
        fs::read_to_string(dir.path("existing-tables/summary.json")).unwrap(),
        "keep me too"
    );
    let error = Backtest::new(config)
        .outputs(OutputConfig::html(dir.path("missing/report.html")))
        .run(&mut PrivateStrategy::default())
        .unwrap_err();
    assert!(matches!(
        error,
        BacktestError::Output {
            kind: ArtifactKind::Html,
            ..
        }
    ));
}

#[test]
fn empty_feeds_work_in_every_recording_mode() {
    for mask in [0, 2, 4] {
        let dir = TestDir::new();
        let config = fixture(&dir);
        book(&config.book_paths[0], "token", &[]);
        if mask == 4 {
            fs::create_dir(dir.path("tables")).unwrap();
        }
        let mut strategy = PrivateStrategy::default();
        let result = Backtest::new(config)
            .outputs(outputs(&dir, mask))
            .run(&mut strategy)
            .unwrap();
        assert_eq!(strategy.calls, 0);
        assert_eq!(result.result.final_cash, 100.0);
        assert_eq!(
            result.recorded_samples,
            if mask == 0 { None } else { Some(0) }
        );
    }
}

#[test]
fn writer_failure_after_simulation_returns_output_error() {
    // The destination is valid at preflight, then disappears during the run.
    // Exercise the writer error path rather than only validation errors.
    struct RemoveOutputParent(std::path::PathBuf);
    impl Strategy for RemoveOutputParent {
        fn on_market_state(
            &mut self,
            _: &MarketState,
            _: &Portfolio,
            _: &[OpenOrder],
        ) -> Vec<Action> {
            if self.0.exists() {
                fs::remove_dir(&self.0).unwrap();
            }
            vec![]
        }
    }
    for kind in [ArtifactKind::Html, ArtifactKind::Json] {
        let dir = TestDir::new();
        let config = fixture(&dir);
        let parent = dir.path("output");
        fs::create_dir(&parent).unwrap();
        let path = parent.join("report");
        let output = match kind {
            ArtifactKind::Html => OutputConfig::html(&path),
            _ => OutputConfig::json(&path),
        };
        let error = Backtest::new(config)
            .outputs(output)
            .run(&mut RemoveOutputParent(parent))
            .unwrap_err();
        assert!(
            matches!(error, BacktestError::Output { kind: actual_kind, path: actual_path, .. } if actual_kind == kind && actual_path == path)
        );
        assert!(!path.exists());
    }
}

#[test]
fn supplied_provenance_reaches_outcome_and_report_without_being_inferred() {
    use pmbt::{RunProvenance, SourceRevision};
    for dirty in [None, Some(false), Some(true)] {
        let dir = TestDir::new();
        let provenance = RunProvenance {
            engine: SourceRevision {
                revision: Some("engine-revision".into()),
                dirty,
            },
            research: SourceRevision {
                revision: Some("private </script> revision".into()),
                dirty: None,
            },
        };
        let outcome = Backtest::new(fixture(&dir))
            .provenance(provenance.clone())
            .outputs(OutputConfig::html(dir.path("report.html")))
            .run(&mut PrivateStrategy::default())
            .unwrap();
        assert_eq!(outcome.provenance, Some(provenance));
        let data = html_data(&dir.path("report.html"));
        let settings = data["context"]["settings"].as_array().unwrap();
        assert!(settings.contains(&serde_json::json!(["Engine revision", "engine-revision"])));
        assert!(settings.contains(&serde_json::json!([
            "Engine dirty",
            match dirty {
                None => "unknown",
                Some(false) => "false",
                Some(true) => "true",
            }
        ])));
        assert!(settings.contains(&serde_json::json!([
            "Research revision",
            "private </script> revision"
        ])));
        assert!(settings.contains(&serde_json::json!(["Research dirty", "unknown"])));
        assert_eq!(
            outcome.simulation,
            pmbt::config::resolve(
                Some(&outcome.config.simulation),
                &SimulationConfig::default()
            )
            .unwrap()
        );
    }
    let dir = TestDir::new();
    let plain = Backtest::new(fixture(&dir))
        .run(&mut PrivateStrategy::default())
        .unwrap();
    assert_eq!(plain.provenance, None);
    let unknown = Backtest::new(fixture(&dir))
        .provenance(RunProvenance::default())
        .outputs(OutputConfig::html(dir.path("unknown.html")))
        .run(&mut PrivateStrategy::default())
        .unwrap();
    assert_eq!(unknown.provenance, Some(RunProvenance::default()));
    assert!(html_data(&dir.path("unknown.html"))["context"]["settings"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(["Engine revision", "unknown"])));
}
