# src/make_anomaly_map.py
from __future__ import annotations
import argparse
import xarray as xr
import matplotlib.pyplot as plt

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--anoms_nc", type=str, default="data/processed/forecast_anoms.nc")
    p.add_argument("--var", type=str, default="t2m_anom_c")
    p.add_argument("--out", type=str, default="reports/figures/anomaly_map.png")
    return p.parse_args()

def main():
    args = parse_args()
    ds = xr.open_dataset(args.anoms_nc)
    da = ds[args.var]

    # If time dimension exists, take first
    for dim in ["time", "valid_time"]:
        if dim in da.dims:
            da = da.isel({dim: 0})

    plt.figure()
    plt.imshow(
        da.values,
        origin="lower",
        aspect="auto",
        extent=[
            float(ds.longitude.min()), float(ds.longitude.max()),
            float(ds.latitude.min()), float(ds.latitude.max()),
        ],
    )
    plt.colorbar(label=args.var)
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Forecast anomaly")
    plt.savefig(args.out, dpi=150, bbox_inches="tight")
    print(f"Saved map: {args.out}")

if __name__ == "__main__":
    main()
