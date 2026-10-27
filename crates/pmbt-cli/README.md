# pmbt-cli

Command-line tools for the `pmbt` Rust backtesting library. The package is named
`pmbt-cli`; the installed executable is named `pmbt`.

From the source workspace:

```sh
cargo run -p pmbt-cli -- run buyandhold /path/token_book_snapshot_full.parquet
cargo install --path crates/pmbt-cli --locked
```

Commands: `run`, `batch`, `compare`, and `convert`. Running without arguments
prints the full usage description. Input paths are relative to the invoking
process's working directory, including when running from the workspace root.

Bundled strategies (`buyandhold`, `buyandsell`, `quoter`, `quoter-improve`, and
`accumulator`) are demonstrations compiled into this binary, not library
exports. [Their source and documentation](src/strategies/README.md) show how
to use public engine APIs. For private strategies, build your own application
against `pmbt` instead of modifying this registry.

The CLI depends on the sibling library through a versioned path dependency.
Publish the matching library version before publishing this CLI package.
See the [packaging guide](https://github.com/EndEvao/pmbt/blob/main/docs/rust-packaging.md).

MIT license.
