#!/usr/bin/env python3
"""Pull the chosen development market from Telonex (free tier: 5 downloads).

The market: **2026 NBA Finals Game 5, Knicks vs. Spurs, 2026-06-13** — the
championship clincher (Knicks won; catalogue `result_id = 0` = outcome_0 =
Knicks, so the Knicks token redeemed $1 → backtest with `--resolution 1`).
One intense single-day market maximizes value per daily-file download:
pre-game drift, in-game swings, near-resolution one-sided books, and a
known settlement to verify against.

    slug       nba-nyk-sas-2026-06-13
    market_id  0xd90bf5f45f24c5de7881707cb5374f3d991388a2d220fa7e0fe2be912cd7642a
    token      Knicks (asset_id_0), coverage 2026-06-07 .. 2026-06-15

Telonex dates its files in **UTC**, and each daily file covers 00:00–24:00 UTC
(= 20:00 ET the previous evening → 20:00 ET). Game 5 tips ~20:30 ET on 06-13 —
i.e. just *after* the 06-13 UTC file ends — so the in-game swings and the
resolution to $1 fall entirely in the **06-14** UTC file. Verified: the 06-13
file ends at 19:59 ET sitting flat at a 0.36 mid with no resolution move; it is
pre-game drift only. That correction (the original pull assumed "06-13" = game
day) is why 06-14 is now pulled — it holds the actual game action and settlement.

The download allocation (one download = one channel × one day):

    1. book_snapshot_full  2026-06-14   game action + resolution — the main course
    2. trades              2026-06-14   in-game tape: limit-order/queue model
    3. book_snapshot_full  2026-06-13   pre-game calm regime, for contrast
    4. trades              2026-06-13   quiet-period tape
    5. book_snapshot_full  2026-06-12   deeper pre-game history
    6. trades              2026-06-12   quiet-period tape

Note: this is **6** channel-days, one past the free tier's 5. 06-12/06-13 are
already on disk, and the script skips any file it already finds there, so a
re-run spends quota only on the two new 06-14 files. Pulling both 06-14 channels
needs 2 downloads — confirm your remaining Telonex quota covers them first.

Requires: pip install telonex ; export TELONEX_API_KEY=...
"""

import datetime as dt
import os
import sys

from telonex import download, get_availability

EXCHANGE = "polymarket"
SLUG = "nba-nyk-sas-2026-06-13"
KNICKS_TOKEN = (
    "110136933550893624733134445460153301975615510734202337526927943993346922198810"
)
DOWNLOAD_DIR = "data/telonex"

# (channel, date) — see the allocation table in the module docs. 06-14 (the
# game + resolution) is the new pull; 06-12/06-13 are already on disk and get
# skipped, so a re-run only spends quota on the two 06-14 files.
PULLS = [
    # ("book_snapshot_full", "2026-06-14"),
    ("trades", "2026-06-14"),
    # ("book_snapshot_full", "2026-06-13"),
    # ("trades", "2026-06-13"),
    # ("book_snapshot_full", "2026-06-12"),
    # ("trades", "2026-06-12"),
]


def next_day(date: str) -> str:
    """The SDK's `to_date` is EXCLUSIVE — one day's file needs [d, d+1)."""
    return (dt.date.fromisoformat(date) + dt.timedelta(days=1)).isoformat()


def sample_style_name(exchange: str, channel: str, date, identifier: str) -> str:
    """Match the existing corpus naming ({asset}_{date}_{channel}.parquet),
    not the SDK default ({exchange}_{channel}_{date}_{identifier})."""
    return f"{identifier}_{date:%Y-%m-%d}_{channel}.parquet"


def local_path(channel: str, date: str) -> str:
    """Where a given channel-day file lands on disk (used to skip re-downloads
    so we never re-spend a scarce free-tier download on data we already have)."""
    return os.path.join(DOWNLOAD_DIR, f"{KNICKS_TOKEN}_{date}_{channel}.parquet")


def main() -> None:
    api_key = os.environ.get("TELONEX_API_KEY")
    if not api_key:
        sys.exit("set TELONEX_API_KEY first (https://telonex.io account page)")

    # Only the files not already on disk cost a download; skip the rest so a
    # re-run never re-spends quota on data we already pulled.
    todo = [(c, d) for c, d in PULLS if not os.path.exists(local_path(c, d))]
    have = len(PULLS) - len(todo)
    if not todo:
        print("all files already on disk — nothing to download.")
        return
    print(f"{have} file(s) already on disk; {len(todo)} to download.")

    # Free pre-flight: confirm every file we actually intend to fetch is covered
    # before spending a single download on a mistake.
    availability = get_availability(exchange=EXCHANGE, asset_id=KNICKS_TOKEN)
    channels = availability["channels"]
    for channel, date in todo:
        dates = channels.get(channel)
        if not dates or not (dates["from_date"] <= date <= dates["to_date"]):
            sys.exit(
                f"availability check failed: {channel} on {date} not covered ({dates})"
            )
    print(f"availability OK for all {len(todo)} pulls — downloading to {DOWNLOAD_DIR}/")

    for channel, date in todo:
        print(f"  {channel} {date} …", flush=True)
        paths = download(
            api_key=api_key,
            exchange=EXCHANGE,
            channel=channel,
            asset_id=KNICKS_TOKEN,
            from_date=date,
            to_date=next_day(date),  # to_date is exclusive
            download_dir=DOWNLOAD_DIR,
            get_filename=sample_style_name,
        )
        for path in paths:
            print(f"    -> {path}")
        if not paths:
            sys.exit(
                f"no file returned for {channel} {date} — stopping before spending more quota"
            )
    print("done. backtest the game + resolution with:")
    print(
        f"  cargo run -- run buyandhold {DOWNLOAD_DIR}/{KNICKS_TOKEN}_2026-06-14_book_snapshot_full.parquet \\"
    )
    print("    --fill sweep --resolution 1   # Knicks won → token redeemed $1")


if __name__ == "__main__":
    main()
