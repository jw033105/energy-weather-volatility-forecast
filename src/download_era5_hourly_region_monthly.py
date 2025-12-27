# src/download_era5_hourly_region_monthly.py
from __future__ import annotations

import argparse
from pathlib import Path
import cdsapi


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start_year", type=int, default=1994)
    p.add_argument("--end_year", type=int, default=2020)
    p.add_argument("--south", type=float, default=25.0)
    p.add_argument("--north", type=float, default=37.0)
    p.add_argument("--west", type=float, default=-107.0)
    p.add_argument("--east", type=float, default=-93.0)
    p.add_argument("--times", nargs="+", default=["00:00", "06:00", "12:00", "18:00"])
    p.add_argument("--out_dir", type=str, default="data/raw/era5_hourly_monthly")
    p.add_argument("--test_one", action="store_true",
                   help="Download just one month (1994-01) to test setup quickly.")
    return p.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    c = cdsapi.Client()

    area = [args.north, args.west, args.south, args.east]
    days = [f"{d:02d}" for d in range(1, 32)]

    years = [args.start_year]
    months = [1]
    if not args.test_one:
        years = list(range(args.start_year, args.end_year + 1))
        months = list(range(1, 13))

    for year in years:
        for month in months:
            m = f"{month:02d}"
            target = out_dir / f"era5_{year}_{m}.nc"
            if target.exists() and target.stat().st_size > 0:
                print(f"Skip existing: {target} ({target.stat().st_size/1e6:.1f} MB)")
                continue

            req = {
                "product_type": "reanalysis",
                "format": "netcdf",
                "variable": [
                    "2m_temperature",
                    "10m_u_component_of_wind",
                    "10m_v_component_of_wind",
                ],
                "year": str(year),
                "month": m,
                "day": days,
                "time": args.times,
                "area": area,
            }

            print(f"\nRequesting ERA5 {year}-{m}")
            print(f"Area N/W/S/E: {area}")
            print(f"Times: {args.times}")
            print(f"Saving to: {target}")

            result = c.retrieve("reanalysis-era5-single-levels", req, str(target))
            # result may be a dict-like object; print something helpful
            print("CDS response:", result)

            if not target.exists() or target.stat().st_size == 0:
                raise RuntimeError(f"Download did not create a valid file: {target}")

            print(f"Downloaded OK: {target} ({target.stat().st_size/1e6:.1f} MB)")

    print("\nDone.")


if __name__ == "__main__":
    main()
