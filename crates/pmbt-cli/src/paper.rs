use crate::strategies::{buyandhold::BuyAndHold, quoter::Quoter};
use pmbt::strategy::Strategy;
use pmbt_live::{
    record::{self, Capture},
    transport, Config, Driver, Result,
};
use std::{fs, path::PathBuf, time::Duration};

fn strategy(config: &Config) -> Box<dyn Strategy> {
    match config.strategy.as_str() {
        "buyandhold" => Box::new(BuyAndHold::new(config.size)),
        "quoter" => Box::new(Quoter::new(config.size, 0, config.max_inventory)),
        _ => unreachable!("validated strategy"),
    }
}

pub fn command(mut args: impl Iterator<Item = String>) -> Result<()> {
    let mut config_path: Option<PathBuf> = None;
    let mut replay_path: Option<PathBuf> = None;
    let mut record_path: Option<PathBuf> = None;
    let mut duration = 300u64;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--config" if config_path.is_none() => config_path = Some(value.into()),
            "--replay" if replay_path.is_none() => replay_path = Some(value.into()),
            "--record-dir" if record_path.is_none() => record_path = Some(value.into()),
            "--duration-secs" => duration = value.parse()?,
            _ => return Err(format!("unknown or repeated paper option: {flag}").into()),
        }
    }
    let config: Config = match (&config_path, &replay_path) {
        (Some(path), None) => toml::from_str(&fs::read_to_string(path)?)?,
        (None, Some(path)) => record::read_manifest(path)?.config,
        _ => {
            return Err(
                "supply exactly one of --config <paper.toml> or --replay <session-dir>".into(),
            )
        }
    };
    config.validate()?;
    let dir = record_path.ok_or("--record-dir <new-dir> is required")?;
    let mut driver = Driver::new(config.clone(), [strategy(&config), strategy(&config)])?;
    let mut capture = Capture::create(&dir, &config)?;
    eprintln!(
        "paper: {} on YES={} and NO={}, shared cash ${:.2}; simulated execution only",
        config.strategy, config.yes_asset, config.no_asset, config.capital
    );
    let result = match replay_path {
        Some(path) => record::replay(&path, &mut driver, &mut capture),
        None => transport::run(
            &mut driver,
            &mut capture,
            [config.yes_asset, config.no_asset],
            (duration != 0).then(|| Duration::from_secs(duration)),
        ),
    };
    match result {
        Ok(status) => {
            capture.finish(&status, "stopped")?;
            println!("{}", serde_json::to_string_pretty(&status)?);
            Ok(())
        }
        Err(error) => {
            let _ =
                capture.write(serde_json::json!({"kind":"failure","message":error.to_string()}));
            let _ = capture.finish(&driver.status(), "failed");
            Err(error)
        }
    }
}
