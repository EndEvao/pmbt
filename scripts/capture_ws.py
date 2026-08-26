#!/usr/bin/env python3
"""Capture Polymarket CLOB websocket frames to pmbt-convertible JSONL.

Writes exactly the envelope `pmbt convert` consumes — one line per frame:

    {"recv_t": 1781727341.561, "recv_iso": "2026-06-17T20:15:41Z", "raw": "<frame text>"}

Design notes (learned from the england_croatia captures, which are missing
timeframes and cost us ~10% suppressed frames in conversion):

- **Auto-reconnect with backoff.** A dropped connection is logged loudly and
  re-subscribed; the exchange's book-on-subscribe snapshot doubles as the
  resync, which is exactly what BookBuilder's gap machinery expects.
- **Keepalive.** The CLOB endpoint drops quiet connections; we send PING on an
  interval and drop the PONG replies (they are not CLOB events — writing them
  would make the converter fail loud on an unknown frame).
- **Flush per line.** A crash loses at most the current frame, not a buffer.

Usage:
    python3 scripts/capture_ws.py --assets <token_id> [<token_id> ...] --out data/captures/<name>.jsonl
    python3 scripts/capture_ws.py --assets-file tokens.txt --out data/captures/<name>.jsonl

Token ids are the decimal CLOB asset ids (one per outcome side). Find them via
the Gamma API (https://gamma-api.polymarket.com/markets?slug=<slug>) in the
`clobTokenIds` field.

Requires: pip install websockets
"""

import argparse
import asyncio
import datetime as dt
import json
import signal
import sys
import time

import websockets

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_S = 10.0
RECONNECT_BACKOFF_S = [1, 2, 5, 10, 30]  # then stays at the last value


def iso_utc(now: float) -> str:
    return dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


async def keepalive(ws) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL_S)
        await ws.send("PING")


async def capture(assets: list[str], out_path: str) -> None:
    attempt = 0
    frames = 0
    with open(out_path, "a", encoding="utf-8") as out:
        while True:
            try:
                async with websockets.connect(WS_URL, max_size=None) as ws:
                    await ws.send(json.dumps({"type": "market", "assets_ids": assets}))
                    attempt = 0  # healthy connection resets the backoff
                    print(
                        f"[{iso_utc(time.time())}] connected, {len(assets)} assets",
                        flush=True,
                    )
                    ping = asyncio.create_task(keepalive(ws))
                    try:
                        async for frame in ws:
                            if frame == "PONG":
                                continue
                            now = time.time()
                            out.write(
                                json.dumps(
                                    {
                                        "recv_t": round(now, 3),
                                        "recv_iso": iso_utc(now),
                                        "raw": frame,
                                    },
                                    separators=(",", ":"),
                                )
                                + "\n"
                            )
                            out.flush()
                            frames += 1
                    finally:
                        ping.cancel()
            except (websockets.WebSocketException, OSError) as err:
                backoff = RECONNECT_BACKOFF_S[
                    min(attempt, len(RECONNECT_BACKOFF_S) - 1)
                ]
                attempt += 1
                # Loud: every disconnect is a future gap episode in conversion.
                print(
                    f"[{iso_utc(time.time())}] DISCONNECT after {frames} frames: {err!r} — "
                    f"reconnecting in {backoff}s",
                    file=sys.stderr,
                    flush=True,
                )
                await asyncio.sleep(backoff)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--assets", nargs="*", default=[], help="decimal CLOB token ids"
    )
    parser.add_argument(
        "--assets-file", help="file with one token id per line (# comments ok)"
    )
    parser.add_argument("--out", required=True, help="output JSONL path (appends)")
    args = parser.parse_args()

    assets = list(args.assets)
    if args.assets_file:
        with open(args.assets_file, encoding="utf-8") as fh:
            assets += [
                line.strip()
                for line in fh
                if line.strip() and not line.lstrip().startswith("#")
            ]
    if not assets:
        parser.error("no token ids given (--assets or --assets-file)")

    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    asyncio.run(capture(assets, args.out))


if __name__ == "__main__":
    main()
