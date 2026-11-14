//! Rotated, streaming JSONL captures. These are replay artifacts, not checkpoints.
use crate::{Config, Driver, Input, Result};
use pmbt::{
    domain::{fill::Fill, order::OrderSide, types::MarketId},
    engine::paper::{PaperRecorder, PaperStatus},
    report::record::{OrderEvent, Recorder},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub config: Config,
    pub execution_mode: String,
}

pub struct Capture {
    dir: PathBuf,
    file: File,
    segment: u64,
    bytes: u64,
    opened: Instant,
    sequence: u64,
    asset: String,
    error: Option<String>,
}

impl Capture {
    pub fn create(dir: &Path, config: &Config) -> Result<Self> {
        config.validate()?;
        fs::create_dir_all(dir)?;
        if fs::read_dir(dir)?.next().is_some() {
            return Err("record directory must be empty; use a new session directory".into());
        }
        // Atomic ownership claim, retained as provenance after normal completion.
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.join("session.lock"))?;
        let manifest = Manifest {
            format_version: 1,
            config: config.clone(),
            execution_mode: "paper_receive_time".into(),
        };
        fs::write(
            dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.join("events-000000.jsonl.part"))?;
        Ok(Self {
            dir: dir.into(),
            file,
            segment: 0,
            bytes: 0,
            opened: Instant::now(),
            sequence: 0,
            asset: String::new(),
            error: None,
        })
    }

    fn path(&self, suffix: &str) -> PathBuf {
        self.dir
            .join(format!("events-{:06}.jsonl{suffix}", self.segment))
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.file.sync_all()?;
        fs::rename(self.path(".part"), self.path(""))?;
        self.segment += 1;
        self.file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.path(".part"))?;
        self.bytes = 0;
        self.opened = Instant::now();
        Ok(())
    }

    pub fn write(&mut self, mut value: Value) -> std::io::Result<()> {
        self.check()?;
        value["input_sequence"] = self.sequence.into();
        let result = (|| {
            let mut bytes = serde_json::to_vec(&value)?;
            bytes.push(b'\n');
            self.file.write_all(&bytes)?;
            self.bytes += bytes.len() as u64;
            if self.bytes >= 64 * 1024 * 1024 || self.opened.elapsed() >= Duration::from_secs(3600)
            {
                self.rotate()?;
            }
            Ok(())
        })();
        if let Err(ref e) = result {
            self.error = Some(format!("capture failed: {e}"));
        }
        result
    }

    pub fn input(&mut self, input: &Input) -> std::io::Result<()> {
        self.sequence += 1;
        self.write(json!({"kind":"input", "input": input}))
    }

    pub fn status(&mut self, status: &PaperStatus, state: &str) -> Result<()> {
        self.check()?;
        let value = json!({"state":state, "last_input_sequence":self.sequence, "paper":status});
        fs::write(
            self.dir.join("status.json.tmp"),
            serde_json::to_vec_pretty(&value)?,
        )?;
        fs::rename(
            self.dir.join("status.json.tmp"),
            self.dir.join("status.json"),
        )?;
        Ok(())
    }

    pub fn finish(mut self, status: &PaperStatus, state: &str) -> Result<()> {
        self.write(json!({"kind":"summary", "state":state, "paper":status}))?;
        self.file.sync_all()?;
        fs::rename(self.path(".part"), self.path(""))?;
        self.status(status, state)
    }
}

impl Recorder for Capture {
    fn check(&self) -> std::io::Result<()> {
        match &self.error {
            Some(e) => Err(std::io::Error::other(e.clone())),
            None => Ok(()),
        }
    }
    fn on_fill(&mut self, ts: i64, side: OrderSide, fill: &Fill) {
        let _ = self.write(json!({"kind":"fill", "asset_id":self.asset, "timestamp_us":ts,
            "side":format!("{side:?}"), "price":fill.price().as_f64(), "quantity":fill.quantity(), "liquidity":format!("{:?}", fill.liquidity())}));
    }
    fn on_order(&mut self, event: &OrderEvent) {
        let _ = self.write(json!({"kind":"order", "asset_id":self.asset, "event":event}));
    }
}
impl PaperRecorder for Capture {
    fn on_asset(&mut self, asset: &MarketId) {
        self.asset = asset.as_str().into();
    }
}

pub fn read_manifest(dir: &Path) -> Result<Manifest> {
    let m: Manifest = serde_json::from_reader(File::open(dir.join("manifest.json"))?)?;
    if m.format_version != 1 || m.execution_mode != "paper_receive_time" {
        return Err("unsupported capture format".into());
    }
    m.config.validate()?;
    Ok(m)
}

/// Replays a normally closed capture; never silently skips a partial tail.
pub fn replay(dir: &Path, driver: &mut Driver, out: &mut Capture) -> Result<PaperStatus> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if name.starts_with("events-") && name.ends_with(".part") {
            return Err(
                "capture is incomplete; automatic crash recovery is not implemented".into(),
            );
        }
        if name.starts_with("events-") && name.ends_with(".jsonl") {
            paths.push(path);
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err("capture has no completed segments".into());
    }
    let mut sequence = 0;
    let mut ended = false;
    for path in paths {
        for line in BufReader::new(File::open(path)?).lines() {
            let record: Value = serde_json::from_str(&line?)?;
            if record["kind"] == "input" {
                if ended {
                    return Err("capture has inputs after stop".into());
                }
                sequence += 1;
                if record["input_sequence"].as_u64() != Some(sequence) {
                    return Err("capture input sequence is incomplete".into());
                }
                let input: Input = serde_json::from_value(record["input"].clone())?;
                out.input(&input)?;
                if let Err(error) = driver.apply(&input, out) {
                    if error.downcast_ref::<crate::ResyncRequired>().is_none() {
                        return Err(error);
                    }
                }
                ended = matches!(input, Input::Stop { .. });
            }
        }
    }
    if !ended {
        return Err("capture has no clean stop; crash recovery is not implemented".into());
    }
    Ok(driver.status())
}
