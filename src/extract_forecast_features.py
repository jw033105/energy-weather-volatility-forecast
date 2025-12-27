import argparse
from pathlib import Path
import pandas as pd
import xarray as xr
import numpy as np


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--anoms_nc", type=str, default="data/processed/forecast_anoms.nc")
    p.add_argument("--clim_nc", type=str, default="data/processed/climatology_doy.nc")
    p.add_argument("--out", type=str, default="data/processed/forecast_features.csv")
    p.add_argument("--hot_thresh", type=float, default=8.0)
    p.add_argument("--cold_thresh", type=float, default=-8.0)
    p.add_argument("--base_c", type=float, default=18.0, help="Base temp for degree days (°C).")
    return p.parse_args()


def _squeeze_time(da: xr.DataArray) -> xr.DataArray:
    # Forecast files sometimes have time/valid_time dims of length 1
    for dim in ["time", "valid_time", "step"]:
        if dim in da.dims:
            da = da.isel({dim: 0})
    return da


def main():
    args = parse_args()
    ds = xr.open_dataset(args.anoms_nc)
    clim = xr.open_dataset(args.clim_nc)

    # --- anomalies (2m temp, 10m winds) ---
    da_t = _squeeze_time(ds["t2m_anom_c"])
    da_u = _squeeze_time(ds["u10_anom"])
    da_v = _squeeze_time(ds["v10_anom"])
    wind_mag = np.sqrt(da_u**2 + da_v**2)

    # --- valid date & day-of-year ---
    valid_date = None
    valid_dt = None
    for name in ["valid_time", "time"]:
        if name in ds.coords:
            try:
                valid_dt = pd.to_datetime(ds[name].values)
                # if array-like, take first
                if hasattr(valid_dt, "__len__") and not isinstance(valid_dt, pd.Timestamp):
                    valid_dt = pd.to_datetime(valid_dt.ravel()[0])
                valid_date = valid_dt.date()
                break
            except Exception:
                pass
    if valid_dt is None:
        # fallback: just use "today" not ideal, but prevents crash
        valid_dt = pd.Timestamp.utcnow()
        valid_date = valid_dt.date()

    doy = int(valid_dt.dayofyear)

    # --- reconstruct absolute forecast temperature from climatology + anomaly ---
    # Select climatology for doy, then interpolate to forecast grid
    Tc = clim["t2m"].sel(doy=doy)

    # Make sure climatology lon/lat names match forecast
    # (your files use latitude/longitude)
    Tc_i = Tc.interp(latitude=ds["latitude"], longitude=ds["longitude"])

    T_forecast = Tc_i + da_t  # °C

    base = float(args.base_c)

    # Degree days (absolute)
    cdd = (T_forecast - base).clip(min=0.0)
    hdd = (base - T_forecast).clip(min=0.0)

    # Degree days climatology for that doy
    cdd_clim = (Tc_i - base).clip(min=0.0)
    hdd_clim = (base - Tc_i).clip(min=0.0)

    cdd_anom = cdd - cdd_clim
    hdd_anom = hdd - hdd_clim

    # --- features ---
    features = {
        "t2m_anom_mean_c": float(da_t.mean().values),
        "t2m_anom_max_c": float(da_t.max().values),
        "t2m_anom_min_c": float(da_t.min().values),
        "hot_area_frac": float((da_t > args.hot_thresh).mean().values),
        "cold_area_frac": float((da_t < args.cold_thresh).mean().values),
        "wind_anom_mag_mean": float(wind_mag.mean().values),

        # Degree day features
        "cdd_mean": float(cdd.mean().values),
        "hdd_mean": float(hdd.mean().values),
        "cdd_anom_mean": float(cdd_anom.mean().values),
        "hdd_anom_mean": float(hdd_anom.mean().values),

        "valid_date": valid_date,
        "doy": doy,
    }

    out_df = pd.DataFrame([features])

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print(f"Saved forecast features to {out_path.resolve()}")


if __name__ == "__main__":
    main()
