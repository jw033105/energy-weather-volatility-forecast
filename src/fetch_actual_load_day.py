from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config_exchange import (
    MISO_DEX_ACTUAL_LOAD_URL_TEMPLATE,
    MISO_DEX_KEY_ENV,
    MISO_DEX_SUBSCRIPTION_HEADER,
)
from .miso_data_exchange import fetch_json, require_env, write_json


def _payload_to_df(payload: Any) -> pd.DataFrame:
    """
    Best-effort conversion of API payload into a tabular DataFrame.

    Common patterns:
    - dict with 'data' or 'items' or 'results' -> list[dict]
    - top-level list[dict]
    """
    if payload is None:
        return pd.DataFrame()

    if isinstance(payload, dict):
        for k in ("data", "items", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                return pd.DataFrame(v)
        # Fallback: flatten dict
        return pd.json_normalize(payload)

    if isinstance(payload, list):
        return pd.DataFrame(payload)

    return pd.DataFrame()


def _reason(payload: Any, fallback_text: str = "") -> str:
    if isinstance(payload, dict) and "reason" in payload:
        try:
            return str(payload.get("reason", ""))
        except Exception:
            return ""
    return fallback_text or ""


def main() -> int:
    p = argparse.ArgumentParser(
        description="Fetch MISO Data Exchange Actual Load for one date (paged) and write raw JSON + parsed CSV."
    )
    p.add_argument("--date", required=True, help="YYYY-MM-DD (e.g., 2025-12-28)")
    p.add_argument("--geo", default="region", help="geoResolution (Try it showed 'region')")
    p.add_argument("--timeres", default="hourly", help="timeResolution (Try it showed 'hourly')")
    p.add_argument("--start-page", type=int, default=1, help="Starting pageNumber (default 1)")
    p.add_argument("--max-pages", type=int, default=50, help="Safety cap on number of pages")
    p.add_argument("--outdir", default="data/raw/miso_exchange/actual_load", help="Raw JSON output directory")
    args = p.parse_args()

    key = require_env(MISO_DEX_KEY_ENV)

    url = MISO_DEX_ACTUAL_LOAD_URL_TEMPLATE.format(date=args.date)

    outdir = Path(args.outdir) / args.date
    outdir.mkdir(parents=True, exist_ok=True)

    all_pages: list[pd.DataFrame] = []

    page = args.start_page
    while page <= args.max_pages:
        params = {
            "geoResolution": args.geo,
            "timeResolution": args.timeres,
            "pageNumber": page,
        }

        resp = fetch_json(url, key, header_name=MISO_DEX_SUBSCRIPTION_HEADER, params=params)

        # Handle "no data / no more pages" behavior
        if resp.status_code == 404:
            msg = _reason(resp.payload, resp.text)
            # Common message you saw: {"status":404,"reason":"Empty data returned for date"}
            if "Empty data" in msg:
                if page == args.start_page:
                    # No data at all for this date
                    print(f"[warn] {args.date}: {msg}")
                    # Save the response for debugging
                    write_json(outdir / f"page_{page}_404.json", resp.payload)
                    return 0
                else:
                    # End of pagination
                    print(f"[info] reached end after page {page-1}: {msg}")
                    break

            # Other 404s are unexpected
            raise RuntimeError(f"HTTP 404 for {resp.url}\n{resp.text}")

        # Any other error should stop and show body
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code} for {resp.url}\n{resp.text}")

        # Save raw JSON for this page
        write_json(outdir / f"page_{page}.json", resp.payload)

        # Parse into rows
        df = _payload_to_df(resp.payload)
        if df.empty:
            # If the API returns success but no rows, treat as end
            print(f"[info] page {page} returned no rows; stopping.")
            break

        df["date"] = args.date
        df["pageNumber"] = page
        df["geoResolution"] = args.geo
        df["timeResolution"] = args.timeres
        all_pages.append(df)

        page += 1

    if not all_pages:
        print(f"[warn] no rows parsed for {args.date}. Check raw JSON in {outdir}")
        return 0

    combined = pd.concat(all_pages, ignore_index=True)

    processed_dir = Path("data/processed/miso_exchange")
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_csv = processed_dir / f"actual_load_{args.date}.csv"
    combined.to_csv(out_csv, index=False)

    # Optional: write a small manifest
    manifest = {
        "date": args.date,
        "geoResolution": args.geo,
        "timeResolution": args.timeres,
        "pages_fetched": [int(pn) for pn in combined["pageNumber"].unique()],
        "rows": int(len(combined)),
        "source_url_template": MISO_DEX_ACTUAL_LOAD_URL_TEMPLATE,
    }
    (processed_dir / f"actual_load_{args.date}.manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print(f"[ok] wrote raw pages to: {outdir}")
    print(f"[ok] wrote parsed CSV to: {out_csv} ({len(combined)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
