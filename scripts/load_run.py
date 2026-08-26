#!/usr/bin/env python3
"""Load a `pmbt run --record-dir` directory into pandas.

    cargo run --release -- run quoter <book.parquet> \
        --resolution 1 --fill sweep --category sports --record-dir runs/quoter

    from scripts.load_run import load_run
    run = load_run("runs/quoter")
    run.samples.plot(x="time", y="pnl")

Every table is returned as a DataFrame with a `time` column (pandas datetime,
UTC) alongside the raw microsecond `ts`, so plots have a real time axis without
per-notebook boilerplate.
"""

from dataclasses import dataclass
from pathlib import Path
import json

import pandas as pd


@dataclass
class Run:
    """One backtest, as tables plus its scalar summary."""

    #: One row per book snapshot: bid/ask/mid, cash, qty, avg_cost, equity, pnl.
    samples: pd.DataFrame
    #: One row per execution: side, price, qty.
    fills: pd.DataFrame
    #: Executions with running position and realized P&L (engine accounting).
    blotter: pd.DataFrame
    #: Long-form book depth: one row per level (ts, side, level, price, size).
    depth: pd.DataFrame
    #: Scalars: result summary, return statistics, round-trip stats.
    summary: dict

    @property
    def stats(self) -> dict:
        return self.summary["stats"]

    def depth_wide(self, side: str = "bid") -> pd.DataFrame:
        """Depth pivoted to one column per level — handy for a ladder plot."""
        one = self.depth[self.depth["side"] == side]
        return one.pivot(index="time", columns="level", values="size")


def _with_time(frame: pd.DataFrame) -> pd.DataFrame:
    if "ts" in frame.columns:
        frame.insert(0, "time", pd.to_datetime(frame["ts"], unit="us", utc=True))
    return frame


def load_run(directory: str | Path) -> Run:
    """Read every table a run wrote. Missing tables come back empty rather than
    raising: a strategy that never traded writes no fills, and that is a result,
    not an error."""
    directory = Path(directory)
    if not directory.is_dir():
        raise FileNotFoundError(f"{directory} is not a run directory")

    def table(name: str) -> pd.DataFrame:
        path = directory / f"{name}.parquet"
        return _with_time(pd.read_parquet(path)) if path.exists() else pd.DataFrame()

    with open(directory / "summary.json") as handle:
        summary = json.load(handle)

    return Run(
        samples=table("samples"),
        fills=table("fills"),
        blotter=table("blotter"),
        depth=table("depth"),
        summary=summary,
    )


if __name__ == "__main__":
    import sys

    run = load_run(sys.argv[1] if len(sys.argv) > 1 else "runs/latest")
    s = run.summary["summary"]
    print(f"total P&L   ${s['total_pnl']:,.2f}  on ${s['initial_capital']:,.0f}")
    print(f"fills       {s['fills']}   fees ${s['fees']:.2f}")
    print(f"samples     {len(run.samples):,}   fills {len(run.fills):,}")
    print(f"sharpe      {run.stats['sharpe']:.2f} ann / {run.stats['sharpe_raw']:.4f} raw")
