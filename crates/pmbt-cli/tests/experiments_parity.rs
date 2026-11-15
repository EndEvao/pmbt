#[allow(dead_code)]
mod common;
use common::{book, json, strategy::BuyOnce, TestDir};
use pmbt::{
    config::{resolve, SimulationConfig},
    domain::types::Price,
    experiments::{
        comparison_matrix, run_batch, run_comparison, BatchConfig, ComparisonConfig,
        ExperimentOutputs,
    },
    universe::{Category, Universe},
    BacktestConfig,
};
use std::{fs, process::Command};

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
fn batch_cli_matches_library_report() {
    let dir = TestDir::new();
    let mut config = BatchConfig::new(universe(&dir));
    config.outputs.json = Some(dir.path("lib.json"));
    run_batch(config, |_| Box::new(BuyOnce::new(100.0))).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_pmbt"))
        .args(["batch", "buyandhold"])
        .arg(dir.path("universe.toml"))
        .arg("--record")
        .arg(dir.path("cli.json"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(json(&dir.path("cli.json")), json(&dir.path("lib.json")));
}
#[test]
fn batch_cli_uses_portfolio_budget_and_weights_from_universe() {
    let dir = TestDir::new();
    universe(&dir);
    let path = dir.path("universe.toml");
    let text = fs::read_to_string(&path)
        .unwrap()
        .replace("market_id='one'", "market_id='one'\nallocation_weight=1.0")
        .replace("market_id='two'", "market_id='two'\nallocation_weight=3.0");
    fs::write(
        &path,
        format!("{text}\n[portfolio]\ninitial_capital=100.0\n"),
    )
    .unwrap();
    let mut config = BatchConfig::new(Universe::load(&path).unwrap());
    config.outputs.json = Some(dir.path("lib.json"));
    run_batch(config, |_| Box::new(BuyOnce::new(100.0))).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_pmbt"))
        .args(["batch", "buyandhold"])
        .arg(path)
        .arg("--record")
        .arg(dir.path("cli.json"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(json(&dir.path("cli.json")), json(&dir.path("lib.json")));
    assert_eq!(json(&dir.path("cli.json"))["agg"]["initial_capital"], 100.0);
    assert!(String::from_utf8_lossy(&out.stdout).contains("portfolio capital $100.00"));
}

#[test]
fn comparison_cli_matches_library_for_every_axis() {
    for axis in ["realism", "latency", "fees", "perception", "taker-delay"] {
        let dir = TestDir::new();
        let mut config = comparison(&dir, axis);
        config.outputs.json = Some(dir.path("lib.json"));
        run_comparison(config, || Box::new(BuyOnce::new(100.0))).unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_pmbt"))
            .args(["compare", "buyandhold"])
            .arg(dir.path("token_book_snapshot_full.parquet"))
            .args([
                "--axis",
                axis,
                "--category",
                "crypto",
                "--resolution",
                "1",
                "--record",
            ])
            .arg(dir.path("cli.json"))
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(json(&dir.path("cli.json")), json(&dir.path("lib.json")));
    }
}
