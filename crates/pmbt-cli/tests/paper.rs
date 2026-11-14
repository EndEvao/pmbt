use serde_json::{json, Value};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

fn temp() -> PathBuf {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "pmbt-cli-paper-{}-{}",
        std::process::id(),
        ID.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir(&path).unwrap();
    path
}
fn book(asset: &str) -> Value {
    json!({"event_type":"book","market":"condition","asset_id":asset,"timestamp":"1",
    "bids":[{"price":"0.4","size":"20"}],"asks":[{"price":"0.5","size":"100"}]})
}
fn trade(side: &str, price: &str, size: &str) -> Value {
    json!({"event_type":"last_trade_price","market":"condition","asset_id":"yes","timestamp":"1","side":side,"price":price,"size":size})
}
fn fixture(dir: &std::path::Path, strategy: &str) {
    fs::create_dir(dir).unwrap();
    fs::write(dir.join("manifest.json"),json!({"format_version":1,"execution_mode":"paper_receive_time","config":{
        "yes_asset":"yes","no_asset":"no","strategy":strategy,"capital":100.,"size":10.,"max_inventory":10.,"fee_rate":0.,"transit_ms":0,"taker_delay_ms":0}}).to_string()).unwrap();
    let frames = vec![
        json!([book("yes"), book("no")]),
        trade("SELL", "0.4", "30"),
        book("yes"),
        trade("BUY", "0.5", "110"),
    ];
    let mut input = vec![json!({"type":"connected","at_us":1})];
    for (i, frame) in frames.into_iter().enumerate() {
        input.push(json!({"type":"frame","at_us":i+2,"received_us":i+2,"raw":frame.to_string()}));
    }
    input.push(json!({"type":"stop","at_us":6}));
    let lines = input
        .into_iter()
        .enumerate()
        .map(|(i, v)| json!({"kind":"input","input_sequence":i+1,"input":v}).to_string() + "\n")
        .collect::<String>();
    fs::write(dir.join("events-000000.jsonl"), lines).unwrap();
}

#[test]
fn buyandhold_then_quoter_replay_through_the_actual_cli() {
    let root = temp();
    for strategy in ["buyandhold", "quoter"] {
        let source = root.join(strategy);
        fixture(&source, strategy);
        let output = root.join(format!("{strategy}-out"));
        let result = Command::new(env!("CARGO_BIN_EXE_pmbt"))
            .args(["paper", "--replay"])
            .arg(&source)
            .arg("--record-dir")
            .arg(&output)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let status: Value = serde_json::from_slice(&result.stdout).unwrap();
        if strategy == "buyandhold" {
            assert_eq!(status["cash"], 90.);
            assert_eq!(status["assets"][0]["quantity"], 10.);
            assert_eq!(status["assets"][1]["quantity"], 10.);
        } else {
            assert_eq!(status["cash"], 101.);
            assert_eq!(status["assets"][0]["quantity"], 0.);
            assert_eq!(status["assets"][0]["fills"], 2);
            assert_eq!(status["reserved_cash"], 4.);
        }
        let repeat = root.join(format!("{strategy}-repeat"));
        let result = Command::new(env!("CARGO_BIN_EXE_pmbt"))
            .args(["paper", "--replay"])
            .arg(&output)
            .arg("--record-dir")
            .arg(&repeat)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            fs::read(output.join("events-000000.jsonl")).unwrap(),
            fs::read(repeat.join("events-000000.jsonl")).unwrap()
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_config_is_rejected_before_creating_session_output() {
    let root = temp();
    let path = root.join("bad.toml");
    fs::write(&path, "yes_asset='yes'\nno_asset='yes'\n").unwrap();
    let out = root.join("output");
    let result = Command::new(env!("CARGO_BIN_EXE_pmbt"))
        .args(["paper", "--config"])
        .arg(path)
        .arg("--record-dir")
        .arg(&out)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!out.exists());
    fs::remove_dir_all(root).unwrap();
}
