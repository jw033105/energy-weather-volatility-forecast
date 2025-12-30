from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd


def daterange(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch many days of Actual Load and combine to one file.")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--processed-dir", default="data/processed/miso_exchange", help="Where daily CSVs are written")
    p.add_argument("--out", default="data/processed/training/load_hourly.csv", help="Combined output CSV")
    args = p.parse_args()

    start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_d = datetime.strptime(args.end, "%Y-%m-%d").date()

    processed_dir = Path(args.processed_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = []
    missing: list[str] = []

    for d in daterange(start_d, end_d):
        ds = d.strftime("%Y-%m-%d")
        daily = processed_dir / f"actual_load_{ds}.csv"
        if daily.exists() and daily.stat().st_size > 10:
            df = pd.read_csv(daily)
            df["date"] = ds
            frames.append(df)
        else:
            missing.append(ds)

    if not frames:
        raise RuntimeError("No daily files found in the requested range. Fetch them first.")

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(out_path, index=False)

    # Write a small log of missing days
    log_path = out_path.with_suffix(".missing_days.txt")
    log_path.write_text("\n".join(missing), encoding="utf-8")

    print(f"[ok] wrote combined hourly load: {out_path} ({len(combined)} rows)")
    print(f"[info] missing days list: {log_path} ({len(missing)} days)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

