mod common;
use common::{book, html_data, json, trades, TestDir};
use pmbt::{
    config::SimulationConfig, domain::types::Price, Backtest, BacktestConfig, OutputConfig,
};
use std::{fs, process::Command};

#[test]
fn cli_and_library_match_under_profile_overrides_for_all_outputs() {
    for mask in 0u8..8 {
        let dir = TestDir::new();
        let book_path = dir.path("token_book_snapshot_full.parquet");
        book(
            &book_path,
            "token",
            &[(1_000_000, "0.4", "0.5"), (3_000_000, "0.5", "0.6")],
        );
        if mask % 2 == 0 {
            trades(&dir.path("token_trades.parquet"));
        }
        let profile = dir.path("profile.toml");
        fs::write(
            &profile,
            "[simulation]\nfill='naive'\nfees=true\ncategory='crypto'\nlatency='dublin'\n",
        )
        .unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pmbt"));
        cmd.arg("run")
            .arg("buyandhold")
            .arg(&book_path)
            .arg("--config")
            .arg(&profile)
            .args([
                "--fill",
                "sweep",
                "--fees",
                "off",
                "--resolution",
                "1",
                "--market-name",
                "Example",
                "--outcome",
                "Yes",
            ]);
        for (bit, flag, name) in [
            (1, "--report", "cli.html"),
            (2, "--record", "cli.json"),
            (4, "--record-dir", "cli-tables"),
        ] {
            if mask & bit != 0 {
                cmd.arg(flag).arg(dir.path(name));
            }
        }
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut config = BacktestConfig::new(book_path);
        config.simulation = pmbt::config::SimulationProfile::load(profile)
            .unwrap()
            .simulation
            .unwrap();
        config.resolution = Some(Price::parse_decimal("1").unwrap());
        let result = Backtest::new(config)
            .overrides(SimulationConfig {
                fill: Some("sweep".into()),
                fees: Some(false),
                ..Default::default()
            })
            .outputs(OutputConfig::json(dir.path("lib.json")))
            .run(&mut pmbt::buyandhold::BuyAndHold::new(100.0))
            .unwrap();
        let expected = json(&dir.path("lib.json"))["summary"].clone();
        if mask & 2 != 0 {
            assert_eq!(json(&dir.path("cli.json"))["summary"], expected);
        }
        if mask & 4 != 0 {
            assert_eq!(
                json(&dir.path("cli-tables/summary.json"))["summary"],
                expected
            );
        }
        if mask & 1 != 0 {
            let data = html_data(&dir.path("cli.html"));
            assert_eq!(data["summary"], expected);
            assert_eq!(data["context"]["market_name"], "Example");
            let settings = data["context"]["settings"].as_array().unwrap();
            assert!(settings.contains(&serde_json::json!(["Strategy / Order size", "100 shares"])));
            assert!(!settings.iter().any(|s| s[0] == "Strategy / Inventory cap"));
        }
        assert_eq!(result.result.fills, 1);
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).contains("no trades file"),
            mask % 2 != 0
        );
    }
}

#[test]
fn cli_rejects_bad_arguments_before_reading_inputs() {
    for args in [
        vec!["--resolution", "0.5"],
        vec!["--fill", "unknown"],
        vec!["--queue", "typo"],
        vec!["--report"],
        vec!["--unknown"],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_pmbt"))
            .args(["run", "buyandhold", "missing_book_snapshot_full.parquet"])
            .args(args)
            .output()
            .unwrap();
        assert!(!out.status.success());
        let error = String::from_utf8_lossy(&out.stderr);
        assert!(!error.contains("could not open input"), "{error}");
    }
}
