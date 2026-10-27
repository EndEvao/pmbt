# Rust workspace, packaging, and migration

The repository root is a virtual Cargo workspace. It is not itself a package.

```text
Cargo.toml                    workspace membership
Cargo.lock                    shared workspace dependency lock
crates/pmbt/                  Rust library and custom-strategy example
  src/                        engine, configuration, runners, reporting
  assets/                     embedded report templates
  tests/                      self-contained synthetic fixtures and API tests
crates/pmbt-cli/              pmbt-cli package; pmbt binary
  src/strategies/             CLI demonstrations using public engine APIs
  tests/                      subprocess CLI/library parity checks
```

Library tests have no dependency on a CLI executable or demonstration strategy.
They use deterministic test-only strategy fixtures. CLI tests own subprocess
checks. Local capture tests remain ignored by default and locate the
repository's `data/` directory explicitly; datasets are never packaged.

## Commands

From the workspace root:

```sh
cargo run -- run buyandhold /path/token_book_snapshot_full.parquet
cargo run -p pmbt --example library_usage -- /path/token_book_snapshot_full.parquet report.html
cargo test --workspace
cargo test --workspace --release
cargo test -p pmbt
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
node scripts/check_report.mjs
```

The executable name and CLI arguments are unchanged. Invoking Cargo from the
root still uses the caller's working directory for data/output paths. The
`pmbt-cli` binary does not emit API documentation, avoiding a documentation
path collision with the `pmbt` library.

## Updating consumers

Change local dependencies from the old root path to the library package:

```toml
pmbt = { path = "../pmbt/crates/pmbt" }
```

Existing Git dependencies can keep targeting the repository URL and a reviewed
revision: Cargo locates the named `pmbt` package within the workspace. The
requested revision must exist remotely before other machines can fetch it.
Do not pin a local-only commit in a shared Git dependency yet.

The four bundled strategy modules are no longer exported by `pmbt`. Implement
the public `Strategy` trait in your application; the named CLI strategies still
work. Other library modules and runner entry points retain their import paths.
Use `Level::new` for checked external book-level construction.

Source provenance tools must account for the package being nested inside a Git
repository. Find the enclosing repository for the declared dependency package;
do not assume its manifest directory is the repository root.

## Packaging without publishing

The library and CLI each include a license and package README. Explicit package
file lists keep data, generated reports, research code, and unrelated tools out
of release archives. Report templates are within the library package, so
`include_str!` remains valid when building an extracted archive.

```sh
cargo package -p pmbt --locked --list
cargo package -p pmbt --locked
```

The second command builds and verifies the packaged library locally; it does
not upload anything. For a local uncommitted packaging edit, add `--allow-dirty`.
The package contains its tests and examples so they can also be run from an
extracted archive. Use `--offline` when the dependencies are already cached.

Install the CLI locally with `cargo install --path crates/pmbt-cli --locked`.
For registry releases, publish the library first, then the CLI with the matching
library dependency version. Registry name availability and release permissions
must be checked before a first publication; package verification alone does not
establish either. No publishing or Python bindings are part of this cleanup.
