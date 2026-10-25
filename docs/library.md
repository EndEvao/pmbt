# Using pmbt from your own project

`pmbt` is a Rust library and a CLI in the same package. Put research code in a
separate repository beside it. Only the research application depends on pmbt:

```text
Desktop/
  pmbt/            public engine, tests, CLI and report templates
  pmbt-research/   your strategies, experiment settings and Cargo.lock
```

In the research application's `Cargo.toml`:

```toml
[package]
name = "pmbt-research"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
pmbt = { path = "../pmbt" }
```

`publish = false` prevents Cargo registry publication; it does not configure Git
hosting privacy. Keep separate Git histories and choose a private remote for the
research repository if you later host it. Private code never needs an entry in
the public CLI registry or `src/lib.rs`.

## Single runs

The runnable [library example](../examples/library_usage.rs) defines its own
strategy against public APIs, including token ID, timestamp, and level accessors.
From the pmbt checkout:

```sh
cargo run --example library_usage -- /path/token_book_snapshot_full.parquet report.html
```

The report path must be fresh. Omitting it runs without retaining history.
A strategy implements `Strategy::on_market_state`, optionally `on_time`, and
returns ordered `Action`s. Snapshots and portfolio state are borrowed read-only.
An order request may be rejected; submitting one is not evidence of a fill.
Use a fresh strategy for each independent run.

```rust,no_run
use pmbt::{Backtest, BacktestConfig, OutputConfig, StrategyMetadata};
use pmbt::config::{SimulationConfig, SimulationProfile};

let profile = SimulationProfile::load("profiles/realistic.toml")?;
let mut config = BacktestConfig::new("data/token_book_snapshot_full.parquet");
config.simulation = profile.simulation.unwrap_or_default();
config.initial_capital = 5_000.0;
// Add more chronological files for the SAME token via config.book_paths.
// config.resolution = None leaves inventory unresolved.
let overrides = SimulationConfig { fees: Some(false), ..Default::default() };
let mut strategy = pmbt::buyandhold::BuyAndHold::new(25.0);
let outcome = Backtest::new(config)
    .overrides(overrides)
    .metadata(StrategyMetadata::new("Baseline"))
    .outputs(OutputConfig::html("runs/baseline.html"))
    .run(&mut strategy)?;
println!("{} fills", outcome.result.fills);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The existing `[simulation]` TOML profile remains unchanged. Resolution order is
explicit overrides, profile values, then defaults. `Some(false)` is an explicit
setting. `BacktestConfig.category`, when supplied, has final precedence as the
per-market category. `RunOutcome.simulation` contains the resolved settings;
`RunOutcome.config` retains the supplied inputs. Strategy parameters are your
application's types, not fields added to the public simulation configuration.
Pass their display values through `StrategyMetadata.parameters`.

Paths in the public API are relative to the process working directory. Input
names must end in `_book_snapshot_full.parquet`; sibling `_trades.parquet`
files are discovered automatically. All input files must describe one token
and have chronological events. Missing tapes yield `RunDiagnostic` values;
malformed tapes and books abort the run.

## Outputs and failures

| Requested outputs | Recording |
|---|---|
| None | No retained history |
| Parquet only | Incremental Parquet tables with bounded row buffers |
| HTML or JSON, including combinations with Parquet | Full history buffered |

Set `OutputConfig.html`, `.json`, and `.parquet` to request combinations. Parquet
paths are directories; HTML/JSON paths are files. Parents must exist, file
outputs must be absent, Parquet directories absent or empty, and destinations
must not overlap. This also applies to the migrated CLI. Outputs are not an
atomic group: failures can leave partial files. Retry with fresh destinations.
Only successful runs return `RunOutcome` and its artifact list. A strategy may
have advanced its internal state before an error; recreate it for a retry.

`BacktestError` distinguishes configuration errors, invalid capital/input lists,
input-open errors, feed errors, mixed tokens, output validation, and writer
errors. Library code returns errors and diagnostics; it does not print or exit.
The lower-level `Engine`, `EventSource`, `FillModel`, and recorder APIs remain
available for custom feeds, fill models, or recording policies.

## Batches

```rust,no_run
use pmbt::experiments::{BatchConfig, run_batch};
use pmbt::universe::Universe;
let mut config = BatchConfig::new(Universe::load("universes/example.toml")?);
config.outputs.html = Some("runs/batch.html".into());
let outcome = run_batch(config, |_entry| {
    Box::new(pmbt::buyandhold::BuyAndHold::new(25.0))
})?;
println!("{} markets, P&L {}", outcome.aggregate.markets, outcome.aggregate.total_pnl);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The factory receives each `MarketEntry`, allowing private parameters to vary by
market. Every market starts with the full configured capital; results are not a
shared capital allocation simulation. Entry categories override profile
categories. No requested output means no retained history; aggregate reports
reduce one market's history to a curve before starting the next. A failure
aborts the batch without publishing aggregate outputs. Outcome simulations and
runs have matching order.

## Comparisons

```rust,no_run
use pmbt::{BacktestConfig, config::{resolve, SimulationConfig}};
use pmbt::experiments::{comparison_matrix, run_comparison, ComparisonConfig, ExperimentOutputs};
let flags = SimulationConfig::default();
let base = resolve(None, &flags)?;
let config = ComparisonConfig {
    input: BacktestConfig::new("data/token_book_snapshot_full.parquet"),
    variants: comparison_matrix("realism", &base, &flags)?,
    strategy_name: "Baseline".into(),
    ladder: true,
    outputs: ExperimentOutputs { html: Some("runs/compare.html".into()), json: None },
};
let outcome = run_comparison(config, || Box::new(pmbt::buyandhold::BuyAndHold::new(25.0)))?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Supported axes are `realism`, `latency`, `fees`, `perception`, and `taker-delay`.
They preserve the CLI presets: realism/perception use the explicit latency flag
or Dublin, even if the base profile names another preset. Other axes reset their
specified frictions; they are not generic overlays of every base toggle.

For a custom experiment, supply `(label, detail, ResolvedSimulation)` tuples.
Variants supply the simulation options; `input.simulation` is unused, while
`input.category` overrides each variant's category. The factory takes no
arguments and creates the same fresh strategy each time. The input feed is
reopened per variant, with one full history reduced and dropped at a time.
Comparison outcomes always include curves and return effective variants.

## Developing and pinning the engine

With a local path dependency, edits in the sibling engine checkout are picked
up on the next Cargo build. Commit generic improvements in pmbt; commit private
strategies and configuration in the research repository. Run the public tests
and private experiments before updating the research dependency.

After a reviewed engine commit has been pushed, replace the path dependency
with an exact Git revision and update/commit the research application's lockfile:

```toml
pmbt = { git = "https://github.com/EndEvao/pmbt.git", rev = "FULL_REVIEWED_COMMIT_SHA" }
```

Keep the dependency local until that revision exists remotely. A path dependency
lockfile does not pin the sibling checkout's Git revision. Save revision and
working-tree state with experiment results; a clean revision alone also does
not identify external data or runtime settings. Keep datasets, secrets, and
generated reports out of both source histories.

## Run provenance

Attach `RunProvenance { engine: SourceRevision { revision, dirty }, research: ... }`
with `Backtest::provenance`. Both fields inside `SourceRevision` are optional;
missing values are rendered as `unknown`, not inferred as clean. The library
never runs Git. Supplied values are returned in `RunOutcome.provenance` and
included in HTML report settings with the engine package version. JSON and
Parquet retain their numerical-report schemas; save a separate manifest if you
need provenance alongside those exports alone.

Specify whether your application collects build-time or run-time checkout
state. A run-time Git revision does not prove that an already-built binary came
from that exact tree. For strict reproducibility, build from clean pinned
sources and archive the configuration and data identifiers as well.
