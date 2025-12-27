from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("\n▶ " + " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(f"Command failed with exit code {result.returncode}: {' '.join(cmd)}")


def parse_args():
    p = argparse.ArgumentParser(description="End-to-end forecast run: GFS → anomalies → map → features → prediction.")
    p.add_argument("--fxx", type=int, default=24, help="Forecast lead hours (24 = tomorrow).")
    p.add_argument("--init", type=str, default=None, help="Optional init time UTC like '2025-12-25 18:00'.")
    p.add_argument("--map_out", type=str, default="reports/figures/anom_t2m.png")
    return p.parse_args()


def main():
    args = parse_args()

    # Ensure output dirs exist
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("reports/figures").mkdir(parents=True, exist_ok=True)
    Path("outputs").mkdir(parents=True, exist_ok=True)

    # 1) Download / subset forecast
    cmd = [sys.executable, "src/get_gfs_forecast.py", "--fxx", str(args.fxx), "--out", "data/processed/gfs_subset.nc"]
    if args.init:
        cmd += ["--init", args.init]
    run(cmd)

    # 2) Compute anomalies (uses climatology_doy.nc)
    run([sys.executable, "src/compute_forecast_anomalies.py",
         "--forecast_nc", "data/processed/gfs_subset.nc",
         "--clim_nc", "data/processed/climatology_doy.nc",
         "--out", "data/processed/forecast_anoms.nc"])

    # 3) Make anomaly map
    run([sys.executable, "src/make_anomaly_map.py",
         "--anoms_nc", "data/processed/forecast_anoms.nc",
         "--var", "t2m_anom_c",
         "--out", args.map_out])

    # 4) Extract forecast features
    run([sys.executable, "src/extract_forecast_features.py",
         "--anoms_nc", "data/processed/forecast_anoms.nc",
         "--out", "data/processed/forecast_features.csv"])

    # 5) Predict volatility + regime label
    run([sys.executable, "src/predict.py"])

    print("\n✅ Done.")
    print("Artifacts:")
    print(" - reports/figures/anom_t2m.png")
    print(" - data/processed/forecast_features.csv")
    print(" - outputs/volatility_forecast.csv")


if __name__ == "__main__":
    main()
