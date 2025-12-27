# src/build_climatology_era5.py
from __future__ import annotations

import argparse
from pathlib import Path
import xarray as xr


def parse_args():
    p = argparse.ArgumentParser(description="Build day-of-year climatology from ERA5.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--era5_daily_nc", type=str, help="Single ERA5 DAILY NetCDF with t2m/u10/v10.")
    g.add_argument("--daily_dir", type=str, help="Directory of ERA5 DAILY NetCDFs.")
    g.add_argument("--hourly_dir", type=str, help="Directory of ERA5 HOURLY NetCDFs (will be aggregated to daily).")
    p.add_argument("--out", type=str, default="data/processed/climatology_doy.nc")
    return p.parse_args()


def normalize_varnames(ds: xr.Dataset) -> xr.Dataset:
    # Try common ERA5 variable naming; adjust here if yours differs.
    rename = {}
    for v in list(ds.data_vars):
        lv = v.lower()
        if lv in ["t2m", "2t"] and "t2m" not in ds.data_vars:
            rename[v] = "t2m"
        if lv in ["u10", "10u"] and "u10" not in ds.data_vars:
            rename[v] = "u10"
        if lv in ["v10", "10v"] and "v10" not in ds.data_vars:
            rename[v] = "v10"
    if rename:
        ds = ds.rename(rename)

    # Kelvin -> Celsius for t2m
    if "t2m" in ds and float(ds["t2m"].max()) > 200:
        ds["t2m"] = ds["t2m"] - 273.15

    return ds


def open_from_dir(d: Path) -> xr.Dataset:
    files = sorted(d.glob("*.nc"))
    if not files:
        raise FileNotFoundError(f"No .nc files found in {d}")
    return xr.open_mfdataset(files, combine="by_coords")


def main():
    args = parse_args()

    if args.era5_daily_nc:
        ds = xr.open_dataset(args.era5_daily_nc)
        ds = normalize_varnames(ds)

    elif args.daily_dir:
        ds = open_from_dir(Path(args.daily_dir))
        ds = normalize_varnames(ds)

    else:  # hourly_dir
        ds_hr = open_from_dir(Path(args.hourly_dir))
        ds_hr = normalize_varnames(ds_hr)

        # Some datasets use valid_time instead of time
        if "time" not in ds_hr.dims:
            if "valid_time" in ds_hr.dims:
                ds_hr = ds_hr.rename({"valid_time": "time"})
            elif "valid_time" in ds_hr.coords:
                ds_hr = ds_hr.set_coords("valid_time").rename({"valid_time": "time"})

        required = ["t2m", "u10", "v10"]
        missing = [v for v in required if v not in ds_hr.data_vars]
        if missing:
            raise SystemExit(f"Missing vars in hourly ERA5: {missing}. Found: {list(ds_hr.data_vars)}")

        # Aggregate HOURLY -> DAILY mean
        ds = ds_hr[required].resample(time="1D").mean()

    required = ["t2m", "u10", "v10"]
    missing = [v for v in required if v not in ds.data_vars]
    if missing:
        raise SystemExit(f"Missing required vars: {missing}. Found: {list(ds.data_vars)}")

    # Day-of-year climatology
    clim = ds[required].groupby(ds["time"].dt.dayofyear).mean("time")
    if "dayofyear" in clim.dims:
        clim = clim.rename({"dayofyear": "doy"})

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    clim.to_netcdf(out_path)
    print(f"Saved climatology to {out_path.resolve()}")


if __name__ == "__main__":
    main()
