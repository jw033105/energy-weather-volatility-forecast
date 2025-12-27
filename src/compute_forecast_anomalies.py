# src/compute_forecast_anomalies.py
from __future__ import annotations
import argparse
import xarray as xr


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--forecast_nc", type=str, default="data/processed/gfs_subset.nc")
    p.add_argument("--clim_nc", type=str, default="data/processed/climatology_doy.nc")
    p.add_argument("--out", type=str, default="data/processed/forecast_anoms.nc")
    return p.parse_args()


def pick_var(ds: xr.Dataset, contains: str) -> str:
    for v in ds.data_vars:
        if contains.lower() in v.lower():
            return v
    raise KeyError(f"Could not find var containing '{contains}'. Vars: {list(ds.data_vars)}")


def main():
    args = parse_args()
    fc = xr.open_dataset(args.forecast_nc)
    clim = xr.open_dataset(args.clim_nc)

    # Forecast valid time
    valid_time = fc["valid_time"] if "valid_time" in fc.coords else fc["time"]
    doy = int(valid_time.dt.dayofyear.values)

    # Forecast vars (should be t2m/u10/v10 if you used the fixed get_gfs_forecast.py)
    tvar = "t2m" if "t2m" in fc.data_vars else pick_var(fc, "t2m")
    uvar = "u10" if "u10" in fc.data_vars else pick_var(fc, "u10")
    vvar = "v10" if "v10" in fc.data_vars else pick_var(fc, "v10")

    T = fc[tvar]
    if float(T.max()) > 200:
        T = T - 273.15

    # Select the climatology for this day-of-year
    Tc = clim["t2m"].sel(doy=doy)
    Uc = clim["u10"].sel(doy=doy)
    Vc = clim["v10"].sel(doy=doy)

    # Regrid climatology to forecast grid (lat/lon)
    Tc_i = Tc.interp(latitude=fc["latitude"], longitude=fc["longitude"])
    Uc_i = Uc.interp(latitude=fc["latitude"], longitude=fc["longitude"])
    Vc_i = Vc.interp(latitude=fc["latitude"], longitude=fc["longitude"])

    out = xr.Dataset(
        {
            "t2m_anom_c": (T - Tc_i),
            "u10_anom": (fc[uvar] - Uc_i),
            "v10_anom": (fc[vvar] - Vc_i),
        }
    )

    out.to_netcdf(args.out)
    print(f"Saved anomalies to {args.out}")


if __name__ == "__main__":
    main()
