#[allow(dead_code)]
mod common;
use common::strategy::BuyOnce;
use common::{book, TestDir};
use pmbt::{
    config::{resolve, SimulationConfig},
    domain::types::Price,
    experiments::{
        comparison_matrix, run_batch, run_comparison, BatchConfig, ComparisonConfig,
        ExperimentError, ExperimentOutputs,
    },
    universe::{Category, Universe},
    Backtest, BacktestConfig, BacktestError,
};
use std::fs;

fn universe(dir: &TestDir) -> Universe {
    for token in ["one", "two"] {
        book(
            &dir.path(&format!("{token}_book_snapshot_full.parquet")),
            token,
            &[(1_000_000, "0.4", "0.5"), (5_000_000, "0.5", "0.6")],
        );
    }
    let text = format!("name='test'\ndata_dir={:?}\n[simulation]\ncategory='sports'\ntaker_delay=false\nfees=false\n[[markets]]\nmarket_id='one'\ncategory='crypto'\nresolution=1\n[[markets]]\nmarket_id='two'\ncategory='geopolitics'\nresolution=0\n", dir.path("").display().to_string());
    fs::write(dir.path("universe.toml"), text).unwrap();
    Universe::load(dir.path("universe.toml")).unwrap()
}
fn comparison(dir: &TestDir, axis: &str) -> ComparisonConfig {
    let path = dir.path("token_book_snapshot_full.parquet");
    book(
        &path,
        "token",
        &[(1_000_000, "0.4", "0.5"), (5_000_000, "0.5", "0.6")],
    );
    let mut input = BacktestConfig::new(path);
    input.resolution = Some(Price::parse_decimal("1").unwrap());
    let base = resolve(
        None,
        &SimulationConfig {
            category: Some(Category::Crypto),
            ..Default::default()
        },
    )
    .unwrap();
    ComparisonConfig {
        input,
        variants: comparison_matrix(axis, &base, &SimulationConfig::default()).unwrap(),
        strategy_name: "buyandhold".into(),
        ladder: axis == "realism",
        outputs: ExperimentOutputs::default(),
    }
}

#[test]
fn batch_factory_is_fresh_and_total_capital_is_split_equally() {
    let dir = TestDir::new();
    let mut config = BatchConfig::new(universe(&dir));
    config.outputs = ExperimentOutputs {
        json: Some(dir.path("lib.json")),
        html: Some(dir.path("lib.html")),
    };
    let mut called = Vec::new();
    let result = run_batch(config, |entry| {
        called.push(entry.market_id.clone());
        Box::new(BuyOnce::new(100.0))
    })
    .unwrap();
    assert_eq!(called, ["one", "two"]);
    assert_eq!(result.aggregate.total_pnl, 0.0);
    assert_eq!(result.aggregate.fills, 2);
    assert_eq!(result.aggregate.initial_capital, 10_000.0);
    assert_eq!(result.simulations[0].category, Some(Category::Crypto));
    assert_eq!(result.simulations[1].category, Some(Category::Geopolitics));
    for run in &result.runs {
        assert_eq!(run.result.initial_capital, 5_000.0);
        assert!(!run.curve.is_empty());
    }
    assert_eq!(result.diagnostics.len(), 2);
    assert_eq!(result.artifacts.len(), 2);
    let plain = run_batch(BatchConfig::new(universe(&dir)), |_| {
        Box::new(BuyOnce::new(100.0))
    })
    .unwrap();
    assert_eq!(plain.aggregate, result.aggregate);
    assert!(plain.runs.iter().all(|r| r.curve.is_empty()));
}

#[test]
fn weighted_budgets_constrain_fills_and_never_borrow_from_other_markets() {
    let dir = TestDir::new();
    let mut u = universe(&dir);
    u.portfolio.initial_capital = 100.0;
    u.markets[0].allocation_weight = Some(1.0);
    u.markets[1].allocation_weight = Some(3.0);
    let mut config = BatchConfig::new(u);
    config.outputs.json = Some(dir.path("weighted.json"));
    let result = run_batch(config, |_| Box::new(BuyOnce::new(100.0))).unwrap();
    assert_eq!(result.aggregate.initial_capital, 100.0);
    let first = &result.runs[0].result;
    let second = &result.runs[1].result;
    assert_eq!(
        (first.initial_capital, second.initial_capital),
        (25.0, 75.0)
    );
    // Each FOK buy costs $50: the first cannot borrow from the second;
    // its untouched $25 is not recycled into the second allocation.
    assert_eq!(
        (first.fills, first.rejections, first.final_cash),
        (0, 1, 25.0)
    );
    assert_eq!((second.fills, second.final_cash), (1, 25.0));
    assert_eq!(result.aggregate.total_pnl, -50.0);
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path("weighted.json")).unwrap()).unwrap();
    assert_eq!(report["agg"]["initial_capital"], 100.0);
    let markets = report["markets"].as_array().unwrap();
    assert_eq!(
        markets.iter().find(|m| m["id"] == "one").unwrap()["initial_capital"],
        25.0
    );
    assert_eq!(
        markets.iter().find(|m| m["id"] == "two").unwrap()["initial_capital"],
        75.0
    );
}

#[test]
fn library_override_is_a_total_budget_and_invalid_allocations_precede_factory() {
    let dir = TestDir::new();
    let mut u = universe(&dir);
    u.portfolio.initial_capital = 100.0;
    let mut config = BatchConfig::new(u);
    config.initial_capital = 200.0;
    let result = run_batch(config, |_| Box::new(BuyOnce::new(100.0))).unwrap();
    assert_eq!(result.aggregate.initial_capital, 200.0);
    assert!(result
        .runs
        .iter()
        .all(|r| r.result.initial_capital == 100.0));
    for capital in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut config = BatchConfig::new(universe(&dir));
        config.initial_capital = capital;
        assert!(matches!(
            run_batch(config, |_| panic!("must validate before strategy")),
            Err(ExperimentError::Universe(_))
        ));
    }
    let mut config = BatchConfig::new(universe(&dir));
    config.universe.markets[1].allocation_weight = Some(3.0);
    assert!(matches!(
        run_batch(config, |_| panic!("must validate all allocations first")),
        Err(ExperimentError::Universe(_))
    ));
}

#[test]
fn comparison_axes_match_independent_single_runs() {
    for axis in ["realism", "latency", "fees", "perception", "taker-delay"] {
        let dir = TestDir::new();
        let mut config = comparison(&dir, axis);
        config.outputs = ExperimentOutputs {
            json: Some(dir.path("lib.json")),
            html: Some(dir.path("lib.html")),
        };
        let mut calls = 0;
        let outcome = run_comparison(config, || {
            calls += 1;
            Box::new(BuyOnce::new(100.0))
        })
        .unwrap();
        assert_eq!(calls, outcome.runs.len());
        assert_eq!(outcome.diagnostics.len(), 1);
        assert_eq!(outcome.artifacts.len(), 2);
        for (run, (_, _, options)) in outcome.runs.iter().zip(&outcome.variants) {
            assert_eq!(run.result.fills, 1, "{axis}: {}", run.label);
            let mut input = comparison(&dir, axis).input;
            input.simulation = SimulationConfig {
                fill: Some(options.fill_model.name().into()),
                latency: Some(if axis == "latency" {
                    run.label.clone()
                } else if options.preset == pmbt::latency::LatencyProfile::zero() {
                    "zero".into()
                } else {
                    "dublin".into()
                }),
                fees: Some(options.fees),
                taker_delay: Some(options.taker_delay),
                perception: Some(options.perception),
                category: options.category,
                ..Default::default()
            };
            let single = Backtest::new(input).run(&mut BuyOnce::new(100.0)).unwrap();
            assert_eq!(run.result, single.result);
        }
    }
}

#[test]
fn later_market_failure_aborts_without_aggregate_files() {
    let dir = TestDir::new();
    let mut config = BatchConfig::new(universe(&dir));
    fs::write(dir.path("two_book_snapshot_full.parquet"), "corrupt").unwrap();
    config.outputs.json = Some(dir.path("failed.json"));
    let mut calls = 0;
    let result = run_batch(config, |_| {
        calls += 1;
        Box::new(BuyOnce::new(10.0))
    });
    assert!(
        matches!(result, Err(ExperimentError::Run {label, source:BacktestError::Input {..}}) if label == "two")
    );
    assert_eq!(calls, 1);
    assert!(!dir.path("failed.json").exists());
}

#[test]
fn malformed_feed_and_empty_comparison_fail_without_success_outputs() {
    let dir = TestDir::new();
    let mut config = comparison(&dir, "fees");
    config.outputs.json = Some(dir.path("failed.json"));
    book(
        &config.input.book_paths[0],
        "token",
        &[(5_000_000, "0.4", "0.5"), (1_000_000, "0.4", "0.5")],
    );
    let result = run_comparison(config, || Box::new(BuyOnce::new(10.0)));
    assert!(matches!(
        result,
        Err(ExperimentError::Run {
            source: BacktestError::Simulation(_),
            ..
        })
    ));
    assert!(!dir.path("failed.json").exists());
    let mut config = comparison(&dir, "fees");
    config.variants.clear();
    assert!(matches!(
        run_comparison(config, || panic!("must not run")),
        Err(ExperimentError::EmptyComparison)
    ));
}

#[test]
fn invalid_outputs_and_universes_fail_before_factory() {
    let dir = TestDir::new();
    let mut config = BatchConfig::new(universe(&dir));
    config.outputs.json = Some(dir.path("universe.toml"));
    assert!(matches!(
        run_batch(config, |_| panic!("must not run")),
        Err(ExperimentError::Backtest(
            BacktestError::InvalidOutput { .. }
        ))
    ));
    let mut config = BatchConfig::new(universe(&dir));
    config.universe.markets.clear();
    assert!(matches!(
        run_batch(config, |_| panic!("must not run")),
        Err(ExperimentError::Universe(_))
    ));
    let mut config = comparison(&dir, "fees");
    config.input.initial_capital = f64::NAN;
    assert!(matches!(
        run_comparison(config, || panic!("must not run")),
        Err(ExperimentError::Run {
            source: BacktestError::InvalidCapital(_),
            ..
        })
    ));
}
