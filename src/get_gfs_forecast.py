# src/get_gfs_forecast.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from datetime import timedelta

import xarray as xr
from herbie import Herbie

def candidate_inits_utc(n_cycles: int = 6) -> list[str]:
    """
    Return a list of recent GFS cycle init times (UTC) as strings,
    newest first. Each cycle is 6 hours apart (00/06/12/18).
    """
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # snap down to last 6-hour cycle
    cycle = (now.hour // 6) * 6
    base = now.replace(hour=cycle)

    out = []
    t = base
    for _ in range(n_cycles):
        out.append(t.strftime("%Y-%m-%d %H:%M"))
        t = t.replace(hour=t.hour) - timedelta(hours=6)
    return out


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--init", type=str, default=None,
                   help="Init time UTC like '2025-12-25 18:00'. Default: latest 00/06/12/18 cycle today (UTC).")
    p.add_argument("--fxx", type=int, default=24, help="Forecast lead hours (e.g., 24 for tomorrow).")
    p.add_argument("--product", type=str, default="pgrb2.0p25")

    # Default box ~Texas/OK region; change later if you want.
    p.add_argument("--south", type=float, default=25.0)
    p.add_argument("--north", type=float, default=37.0)
    p.add_argument("--west", type=float, default=-107.0)
    p.add_argument("--east", type=float, default=-93.0)

    p.add_argument("--out", type=str, default="data/processed/gfs_subset.nc")
    return p.parse_args()


def guess_latest_init_utc() -> str:
    now = datetime.now(timezone.utc)
    cycle = (now.hour // 6) * 6
    init = now.replace(hour=cycle, minute=0, second=0, microsecond=0)
    return init.strftime("%Y-%m-%d %H:%M")


def _pick_surface_cube(ds_or_list, exact_var: str) -> xr.Dataset:
    """
    Herbie/cfgrib may return a list of Datasets (multiple hypercubes).
    Pick the Dataset that contains the exact surface var name we want (t2m/u10/v10).
    """
    if isinstance(ds_or_list, list):
        for d in ds_or_list:
            if exact_var in d.data_vars:
                return d
        # Fallback: first dataset if exact not found
        return ds_or_list[0]
    return ds_or_list


def _normalize_lon(ds: xr.Dataset) -> xr.Dataset:
    lon = ds.longitude
    if lon.max() > 180:
        ds = ds.assign_coords(longitude=((lon + 180) % 360) - 180).sortby("longitude")
    return ds


def main():
    args = parse_args()
    # If user provides --init, use it. Otherwise try a few recent cycles.
    init_candidates = [args.init] if args.init else candidate_inits_utc(n_cycles=8)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    H = None
    last_err = None
    for init in init_candidates:
        try:
            H = Herbie(init, model="gfs", product=args.product, fxx=args.fxx)
            # Force an inventory check early so we know it exists
            _ = H.inventory("TMP:2 m")
            print(f"Using init {init} UTC (found inventory)")
            break
        except Exception as e:
            last_err = e
            H = None

    if H is None:
        raise RuntimeError(f"Could not find an available GFS cycle for fxx={args.fxx}. Last error:\n{last_err}")
    
    # These may return multiple "hypercube" datasets; pick the surface cube.
    ds_t = _pick_surface_cube(H.xarray("TMP:2 m"), "t2m")
    ds_u = _pick_surface_cube(H.xarray("UGRD:10 m"), "u10")
    ds_v = _pick_surface_cube(H.xarray("VGRD:10 m"), "v10")

    # Keep only surface vars
    ds_t = ds_t[["t2m"]]
    ds_u = ds_u[["u10"]]
    ds_v = ds_v[["v10"]]

    ds = xr.merge([ds_t, ds_u, ds_v], compat="override")

    ds = _normalize_lon(ds)

    # Subset region (latitude in GRIB is often descending)
    ds = ds.sel(latitude=slice(args.north, args.south),
                longitude=slice(args.west, args.east))

    ds.to_netcdf(out_path)
    print(f"Saved forecast subset to {out_path.resolve()}")


if __name__ == "__main__":
    main()
