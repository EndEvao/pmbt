# pmbt

A Rust library for event-driven prediction-market backtesting. It provides
simulation configuration, single-run and experiment runners, market-data
adapters, and HTML/JSON/Parquet reports.

Implement `pmbt::strategy::Strategy` in your application and pass it to
`Backtest::run`. The engine does not import the CLI or bundled strategies.
See [the custom strategy example](examples/library_usage.rs), or run it from
the source workspace:

```sh
cargo run -p pmbt --example library_usage -- /path/token_book_snapshot_full.parquet report.html
```

Output files must be new and their parent directories must exist. Parquet
output directories must be absent or empty. Use the lower-level `Engine`
API for custom feeds, fill models, or recorders.

- [Library guide](https://github.com/EndEvao/pmbt/blob/main/docs/library.md)
- [Workspace and packaging](https://github.com/EndEvao/pmbt/blob/main/docs/rust-packaging.md)

The `pmbt` executable and demonstration strategies live in the separate
`pmbt-cli` package. The library includes its report templates and synthetic
test fixtures; it does not require the repository's datasets or CLI to build.

MIT license. The library is experimental; review changes before updating a
pinned dependency.
