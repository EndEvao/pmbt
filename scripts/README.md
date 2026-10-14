# scripts/

Data-acquisition tooling. Everything here produces inputs for `pmbt`; nothing
here is part of the engine.

## capture_ws.py — raw Polymarket websocket capture

Records CLOB websocket frames in the exact envelope `pmbt convert` consumes
(`{recv_t, recv_iso, raw}` JSONL, same as `data/england_croatia/`). Improvements
over the recordings that produced our known gap problems: auto-reconnect with
backoff (the book-on-subscribe snapshot doubles as the resync BookBuilder
expects), keepalive PINGs, per-line flush, loud disconnect logging.

```bash
pip install websockets
python3 scripts/capture_ws.py --assets-file tokens.txt --out data/captures/wc_final.jsonl
# then: cargo run -- convert data/captures/wc_final.jsonl data/converted/
```

Token ids (decimal, one per outcome) come from the Gamma API:
`https://gamma-api.polymarket.com/markets?slug=<slug>` → `clobTokenIds`.

## telonex_pull.py — spend the free tier (5 downloads) on one market

Free tier = 5 *downloads* (one download = one channel × one day), so the
budget buys one well-chosen market. Chosen: **NBA Finals Game 5, Knicks vs.
Spurs, 2026-06-13** (`nba-nyk-sas-2026-06-13`) — a championship clincher:
single-day intensity, pre-game/in-game/near-resolution regimes, known
settlement (Knicks won → `--resolution 1` for the Knicks token). Allocation:
game-day `book_snapshot_full` + `trades`, plus the pre-game day's
`book_snapshot_full` + `trades` for the calm-regime contrast — four downloads;
the fifth is held in reserve (quotes vs. a thin-market book day, undecided).
The script pre-checks availability (free, no key) before spending anything.

```bash
# one-time setup (from repo root; .venv is gitignored):
python3 -m venv .venv && .venv/bin/pip install telonex

export TELONEX_API_KEY=...
.venv/bin/python scripts/telonex_pull.py
```
