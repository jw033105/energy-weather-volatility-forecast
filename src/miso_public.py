from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .cache_http import fetch_json_cached


# These endpoints are the ones commonly used behind MISO’s public web displays.
# (They’re referenced in the gridstatus MISO implementation.) :contentReference[oaicite:1]{index=1}
# src/miso_public.py

MISO_PUBLIC_API_BASE = "https://public-api.misoenergy.org/api"

URL_TOTAL_LOAD = f"{MISO_PUBLIC_API_BASE}/RealTimeTotalLoad"
URL_FUEL_MIX   = f"{MISO_PUBLIC_API_BASE}/FuelMix"



def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def raw_dir() -> Path:
    return repo_root() / "data" / "raw" / "miso_rt"


def processed_dir() -> Path:
    return repo_root() / "data" / "processed" / "miso_rt"


def fetch_total_load(ttl_seconds: int = 60) -> dict[str, Any]:
    data, meta = fetch_json_cached(
        URL_TOTAL_LOAD,
        cache_path=raw_dir() / "total_load.latest.json",
        ttl_seconds=ttl_seconds,
    )
    return data


def fetch_fuel_mix(ttl_seconds: int = 60) -> dict[str, Any]:
    data, meta = fetch_json_cached(
        URL_FUEL_MIX,
        cache_path=raw_dir() / "fuel_mix.latest.json",
        ttl_seconds=ttl_seconds,
    )
    return data


def total_load_to_df(payload: dict[str, Any]) -> pd.DataFrame:
    li = payload.get("LoadInfo", {}) if isinstance(payload, dict) else {}
    ref = li.get("RefId", "")

    rows = li.get("FiveMinTotalLoad", [])
    # rows look like: [{"Load":{"Time":"00:00","Value":"71422"}}, ...]
    flat = []
    for r in rows:
        load = (r or {}).get("Load", {})
        flat.append({"Time": load.get("Time"), "Value": load.get("Value")})

    df = pd.DataFrame(flat)
    if not df.empty and "Value" in df.columns:
        df["LoadMW"] = pd.to_numeric(df["Value"], errors="coerce")
        df = df.drop(columns=["Value"])
    df["RefId"] = ref
    return df


def fuel_mix_to_df(payload: dict[str, Any]) -> pd.DataFrame:
    # payload looks like: {"RefId": "...", "TotalMW":"...", "Fuel":{"Type":[...]}}
    if not isinstance(payload, dict):
        return pd.DataFrame()

    ref = payload.get("RefId", "")
    total_mw = payload.get("TotalMW", "")

    types = ((payload.get("Fuel", {}) or {}).get("Type", [])) or []
    df = pd.DataFrame(types)

    if df.empty:
        return pd.DataFrame([{"error": "empty_fuelmix", "RefId": ref, "TotalMW": total_mw}])

    df["RefId"] = ref
    df["TotalMW"] = total_mw

    # Keep these for your Streamlit chart (CATEGORY/ACT/INTERVALEST)
    for col in ["ACT", "TotalMW"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def write_processed(df: pd.DataFrame, name: str) -> Path:
    processed_dir().mkdir(parents=True, exist_ok=True)
    out = processed_dir() / f"{name}.csv"
    df.to_csv(out, index=False)
    return out
