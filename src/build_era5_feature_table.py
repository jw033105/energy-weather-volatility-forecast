from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import xarray as xr
import numpy as np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--hourly_dir", type=str, default="data/raw/era5_hourly_monthly")
    p.add_argument("--clim_nc", type=str, default="data/processed/climatology_doy.nc")
    p.add_argument("--out", type=str, default="data/processed/era5_features.csv")
    # thresholds (C) for “extreme area” feature
    p.add_argument("--hot_thresh", type=float, default=8.0)
    p.add_argument("--cold_thresh", type=float, default=-8.0)
    return p.parse_args()

def open_hourly(hourly_dir: Path) -> xr.Dataset:
    files = sorted(hourly_dir.glob("*.nc"))
    if not files:
        raise FileNotFoundError(f"No .nc files found in {hourly_dir}")

    ds = xr.open_mfdataset(files, combine="by_coords")

    # Normalize time coordinate name
    if "time" not in ds.dims and "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})

    return ds

def main():
    args = parse_args()
    hourly_dir = Path(args.hourly_dir)
    clim = xr.open_dataset(args.clim_nc)

    ds_hr = open_hourly(hourly_dir)

    # Ensure Kelvin -> C if needed
    if float(ds_hr["t2m"].max()) > 200:
        ds_hr["t2m"] = ds_hr["t2m"] - 273.15

    # Hourly -> daily mean
    ds_day = ds_hr[["t2m", "u10", "v10"]].resample(time="1D").mean()

    # Compute anomalies by day-of-year
    doy = ds_day["time"].dt.dayofyear
    Tc = clim["t2m"].sel(doy=doy)
    Uc = clim["u10"].sel(doy=doy)
    Vc = clim["v10"].sel(doy=doy)

    t_anom = ds_day["t2m"] - Tc
    u_anom = ds_day["u10"] - Uc
    v_anom = ds_day["v10"] - Vc
    wind_anom_mag = np.sqrt(u_anom**2 + v_anom**2)

    # Degree days (base 18C)
    base = 18.0
    t_daily = ds_day["t2m"]
    t_clim = Tc

    cdd = (t_daily - base).clip(min=0.0)
    hdd = (base - t_daily).clip(min=0.0)

    cdd_clim = (t_clim - base).clip(min=0.0)
    hdd_clim = (base - t_clim).clip(min=0.0)

    cdd_anom = cdd - cdd_clim
    hdd_anom = hdd - hdd_clim

    cdd_mean = cdd.mean(dim=("latitude", "longitude"))
    hdd_mean = hdd.mean(dim=("latitude", "longitude"))
    cdd_anom_mean = cdd_anom.mean(dim=("latitude", "longitude"))
    hdd_anom_mean = hdd_anom.mean(dim=("latitude", "longitude"))


    # Feature extraction over region (mean/max + “extreme area fraction”)
    t_mean = t_anom.mean(dim=("latitude", "longitude"))
    t_max = t_anom.max(dim=("latitude", "longitude"))
    t_min = t_anom.min(dim=("latitude", "longitude"))
    hot_frac = (t_anom > args.hot_thresh).mean(dim=("latitude", "longitude"))
    cold_frac = (t_anom < args.cold_thresh).mean(dim=("latitude", "longitude"))
    wind_mean = wind_anom_mag.mean(dim=("latitude", "longitude"))

    out = pd.DataFrame({
        "date": pd.to_datetime(ds_day["time"].values).date,
        "t2m_anom_mean_c": t_mean.values,
        "t2m_anom_max_c": t_max.values,
        "t2m_anom_min_c": t_min.values,
        "hot_area_frac": hot_frac.values,
        "cold_area_frac": cold_frac.values,
        "wind_anom_mag_mean": wind_mean.values,
        "cdd_mean": cdd_mean.values,
        "hdd_mean": hdd_mean.values,
        "cdd_anom_mean": cdd_anom_mean.values,
        "hdd_anom_mean": hdd_anom_mean.values,

    })

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Saved ERA5 features to {out_path.resolve()}")

if __name__ == "__main__":
    main()
