//! Exercise configuration through the public API, as an external consumer would.
use std::path::Path;
use std::time::Duration;

use pmbt::config::{
    latency_preset, resolve, ConfigError, FillModelKind, SimulationConfig, SimulationProfile,
};
use pmbt::engine::Simulation;
use pmbt::latency::LatencyProfile;
use pmbt::resting::QueueModel;
use pmbt::universe::{Category, Universe, UniverseError};

fn profile(text: &str) -> SimulationProfile {
    toml::from_str(text).unwrap()
}

#[test]
fn defaults_match_a_bare_cli_run() {
    let settings = resolve(None, &SimulationConfig::default()).unwrap();
    assert_eq!(settings.fill_model, FillModelKind::Naive);
    assert_eq!(settings.preset, LatencyProfile::zero());
    assert_eq!(settings.queue, QueueModel::Proportional);
    assert!(!settings.perception);
    assert!(settings.fees);
    assert!(settings.taker_delay);
    assert_eq!(settings.category, None);
    assert_eq!(settings.simulation(None), Simulation::zero());
}

#[test]
fn loading_keeps_unspecified_values_optional_and_resolution_applies_defaults() {
    let profile = profile("[simulation]\nfill = 'sweep'");
    let input = profile.simulation.as_ref().unwrap();
    assert_eq!(input.perception, None);
    assert_eq!(input.fees, None);
    assert_eq!(input.latency, None);
    let settings = resolve(Some(input), &SimulationConfig::default()).unwrap();
    assert_eq!(settings.fill_model, FillModelKind::Sweep);
    assert_eq!(settings.simulation(None), Simulation::zero());
    // Resolution must not mutate the source profile.
    assert_eq!(input.fees, None);
}

#[test]
fn every_explicit_setting_overrides_the_profile_including_false() {
    let profile = profile(
        r#"
        [simulation]
        fill = "sweep"
        latency = "dublin"
        queue = "front"
        perception = true
        fees = true
        taker_delay = true
        category = "sports"
    "#,
    );
    let overrides = SimulationConfig {
        fill: Some("naive".into()),
        latency: Some("retail-eu".into()),
        queue: Some("trades-only".into()),
        perception: Some(false),
        fees: Some(false),
        taker_delay: Some(false),
        category: Some(Category::Crypto),
    };
    let settings = resolve(profile.simulation.as_ref(), &overrides).unwrap();
    assert_eq!(settings.fill_model, FillModelKind::Naive);
    assert_eq!(settings.preset, LatencyProfile::retail_eu());
    assert_eq!(settings.queue, QueueModel::TradesOnly);
    assert_eq!(settings.category, Some(Category::Crypto));
    assert_eq!(
        settings.simulation(None),
        Simulation {
            latency: LatencyProfile::retail_eu(),
            perception: false,
            fee_rate: 0.0,
            queue: QueueModel::TradesOnly,
        }
    );
}

#[test]
fn unspecified_overrides_preserve_profile_false_values() {
    let profile = profile("[simulation]\nfees = false\ntaker_delay = false\ncategory = 'sports'");
    let settings = resolve(profile.simulation.as_ref(), &SimulationConfig::default()).unwrap();
    assert!(!settings.fees);
    assert!(!settings.taker_delay);
    assert_eq!(settings.simulation(None).fee_rate, 0.0);
    assert_eq!(
        settings.simulation(None).latency.taker_delay,
        Duration::ZERO
    );
}

#[test]
fn effective_names_fail_during_resolution_and_shadowed_names_do_not() {
    for (field, expected) in [
        ("fill", ConfigError::UnknownFill("typo".into())),
        ("latency", ConfigError::UnknownLatency("typo".into())),
        ("queue", ConfigError::UnknownQueue("typo".into())),
    ] {
        let profile = profile(&format!("[simulation]\n{field} = 'typo'"));
        assert_eq!(
            resolve(profile.simulation.as_ref(), &SimulationConfig::default()).unwrap_err(),
            expected
        );
        let overrides = SimulationConfig {
            fill: Some("sweep".into()),
            latency: Some("zero".into()),
            queue: Some("front".into()),
            ..Default::default()
        };
        assert!(resolve(profile.simulation.as_ref(), &overrides).is_ok());
        // The same validation applies to explicit overrides, without a file.
        assert_eq!(
            resolve(None, profile.simulation.as_ref().unwrap()).unwrap_err(),
            expected
        );
    }
}

#[test]
fn all_latency_and_queue_names_retain_their_existing_meanings() {
    for (name, expected) in [
        ("zero", LatencyProfile::zero()),
        ("dublin", LatencyProfile::dublin()),
        ("retail-eu", LatencyProfile::retail_eu()),
        ("us-east", LatencyProfile::us_east()),
    ] {
        assert_eq!(latency_preset(name).unwrap(), expected);
    }
    for (name, expected) in [
        ("front", QueueModel::Front),
        ("proportional", QueueModel::Proportional),
        ("trades-only", QueueModel::TradesOnly),
    ] {
        let overrides = SimulationConfig {
            queue: Some(name.into()),
            ..Default::default()
        };
        assert_eq!(resolve(None, &overrides).unwrap().queue, expected);
    }
}

#[test]
fn category_defaults_and_per_market_overrides_control_fees_and_holds() {
    let profile = profile("[simulation]\nlatency = 'dublin'\ncategory = 'sports'");
    let settings = resolve(profile.simulation.as_ref(), &SimulationConfig::default()).unwrap();
    let sports = settings.simulation(None);
    assert_eq!(sports.fee_rate, pmbt::fees::fee_rate(Category::Sports));
    assert_eq!(
        sports.latency,
        LatencyProfile::dublin().with_taker_delay(Duration::from_secs(3))
    );
    let crypto = settings.simulation(Some(Category::Crypto));
    assert_eq!(crypto.fee_rate, pmbt::fees::fee_rate(Category::Crypto));
    assert_eq!(crypto.latency.taker_delay, Duration::from_millis(250));
    let fee_free = settings.simulation(Some(Category::Geopolitics));
    assert_eq!(fee_free.fee_rate, 0.0);
    assert_eq!(fee_free.latency.taker_delay, Duration::ZERO);
}

#[test]
fn fee_and_hold_switches_are_independent() {
    for (fees, taker_delay) in [(false, true), (true, false), (false, false), (true, true)] {
        let overrides = SimulationConfig {
            fees: Some(fees),
            taker_delay: Some(taker_delay),
            category: Some(Category::Sports),
            ..Default::default()
        };
        let sim = resolve(None, &overrides).unwrap().simulation(None);
        assert_eq!(
            sim.fee_rate,
            if fees {
                pmbt::fees::fee_rate(Category::Sports)
            } else {
                0.0
            }
        );
        assert_eq!(
            sim.latency.taker_delay,
            if taker_delay {
                Duration::from_secs(3)
            } else {
                Duration::ZERO
            }
        );
    }
}

#[test]
fn existing_profile_import_and_load_error_type_still_compile() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/universe.toml");
    let loaded: Result<pmbt::universe::SimulationProfile, UniverseError> =
        pmbt::universe::SimulationProfile::load(&path);
    let loaded: SimulationProfile = loaded.unwrap();
    let universe = Universe::load(&path).unwrap();
    assert_eq!(loaded.market_count(), universe.markets.len());
    let old_config: pmbt::universe::SimulationConfig = SimulationConfig::default();
    assert!(resolve(loaded.simulation.as_ref(), &old_config).is_ok());
    // A directory is not a readable TOML file on supported hosts.
    assert!(matches!(
        SimulationProfile::load(Path::new(env!("CARGO_MANIFEST_DIR"))),
        Err(UniverseError::Io(_))
    ));
}

#[test]
fn universe_documents_and_missing_simulation_sections_remain_valid_profiles() {
    let text = r#"
        name = "example"
        data_dir = "data"
        [simulation]
        fill = "sweep"
        [[markets]]
        market_id = "token"
        category = "sports"
        resolution = 1
    "#;
    let profile = profile(text);
    let universe: Universe = toml::from_str(text).unwrap();
    assert_eq!(profile.market_count(), 1);
    assert_eq!(
        resolve(profile.simulation.as_ref(), &SimulationConfig::default()).unwrap(),
        resolve(universe.simulation.as_ref(), &SimulationConfig::default()).unwrap()
    );
    let empty: SimulationProfile = toml::from_str("name = 'no settings'").unwrap();
    assert!(empty.simulation.is_none());
    assert_eq!(empty.market_count(), 0);
    assert_eq!(
        resolve(empty.simulation.as_ref(), &SimulationConfig::default())
            .unwrap()
            .simulation(None),
        Simulation::zero()
    );
}

#[test]
fn misspelled_fields_categories_and_non_boolean_toggles_are_rejected() {
    for body in ["percepton = true", "fees = 'off'", "category = 'sport'"] {
        assert!(toml::from_str::<SimulationProfile>(&format!("[simulation]\n{body}")).is_err());
    }
}
