from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def run_day(date_str: str, geo: str, timeres: str) -> int:
    """
    Calls: python -m src.fetch_actual_load_day --date YYYY-MM-DD ...
    Uses the current Python interpreter (sys.executable) so it runs in the same conda env.
    """
    cmd = [
        sys.executable,
        "-m",
        "src.fetch_actual_load_day",
        "--date",
        date_str,
        "--geo",
        geo,
        "--timeres",
        timeres,
    ]
    p = subprocess.run(cmd)
    return p.returncode


def main() -> int:
    p = argparse.ArgumentParser(description="Download Actual Load daily files over a date range.")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--geo", default="region")
    p.add_argument("--timeres", default="hourly")
    args = p.parse_args()

    start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_d = datetime.strptime(args.end, "%Y-%m-%d").date()

    ok = 0
    skipped = 0

    for d in daterange(start_d, end_d):
        ds = d.strftime("%Y-%m-%d")
        rc = run_day(ds, args.geo, args.timeres)
        if rc == 0:
            ok += 1
        else:
            skipped += 1

    print(f"[done] days attempted={ok+skipped}, ok={ok}, skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
