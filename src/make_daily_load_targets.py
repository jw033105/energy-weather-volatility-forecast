from __future__ import annotations

import argparse
import ast
from pathlib import Path

import pandas as pd


def parse_interval_start(x: str) -> pd.Timestamp:
    """
    timeInterval arrives as a string like:
    "{'resolution': 'hourly', 'start': '2025-11-28T00:00:00', 'end': '2025-11-28T01:00:00', 'value': '1'}"
    We parse it and return the 'start' timestamp.
    """
    if pd.isna(x):
        return pd.NaT
    try:
        d = ast.literal_eval(x) if isinstance(x, str) else x
        start = d.get("start")
        return pd.to_datetime(start, errors="coerce")
    except Exception:
        return pd.NaT


def main() -> int:
    p = argparse.ArgumentParser(description="Build daily total load targets (avg/peak) from load_hourly.csv.")
    p.add_argument("--infile", default="data/processed/training/load_hourly.csv")
    p.add_argument("--outfile", default="data/processed/training/load_daily_targets.csv")
    p.add_argument("--region", default="ALL", help="Use ALL (sum) or a region name like Central/North/South")
    args = p.parse_args()

    df = pd.read_csv(args.infile)

    # Basic cleaning
    df["load"] = pd.to_numeric(df["load"], errors="coerce")
    df["interval_start"] = df["timeInterval"].apply(parse_interval_start)

    df = df.dropna(subset=["interval_start", "load"]).copy()
    df["date"] = df["interval_start"].dt.date.astype(str)

    if args.region.upper() != "ALL":
        df = df[df["region"].astype(str).str.lower() == args.region.lower()].copy()
        if df.empty:
            raise RuntimeError(f"No rows found for region={args.region}. Available: {sorted(df['region'].unique())}")

        # Daily stats for that region
        daily = (
            df.groupby("date")["load"]
            .agg(load_avg_mw="mean", load_peak_mw="max")
            .reset_index()
            .sort_values("date")
        )
        daily["region"] = args.region
    else:
        # Sum across regions per hour, then compute daily avg/peak of the summed series
        hourly_total = (
            df.groupby(["date", "interval_start"], as_index=False)["load"]
            .sum()
            .rename(columns={"load": "total_load_mw"})
        )

        daily = (
            hourly_total.groupby("date")["total_load_mw"]
            .agg(load_avg_mw="mean", load_peak_mw="max")
            .reset_index()
            .sort_values("date")
        )
        daily["region"] = "ALL"

    out = Path(args.outfile)
    out.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out, index=False)

    print(f"[ok] wrote: {out} ({len(daily)} days) region={daily['region'].iloc[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
