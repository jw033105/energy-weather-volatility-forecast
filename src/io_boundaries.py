from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests


LAYER_URL = "https://services3.arcgis.com/fwwoCWVtaahwlvxO/arcgis/rest/services/MISO_Market/FeatureServer/41"


def repo_root() -> Path:
    # src/io_boundaries.py -> repo root is two parents up
    return Path(__file__).resolve().parents[1]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _get_count(session: requests.Session) -> int:
    url = f"{LAYER_URL}/query"
    params = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    }
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "count" not in data:
        raise RuntimeError(f"Unexpected count response: {data}")
    return int(data["count"])


def _fetch_chunk_geojson(
    session: requests.Session,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    url = f"{LAYER_URL}/query"
    params = {
        "where": "1=1",
        "outFields": "Hub,Zone",
        "returnGeometry": "true",
        "outSR": "4326",            # WGS84 lat/lon
        "resultOffset": str(offset),
        "resultRecordCount": str(limit),
        "f": "geojson",
    }
    r = session.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def download_miso_market_geojson(
    out_path: Path,
    force: bool = False,
    chunk_size: int = 2000,
) -> Path:
    """
    Downloads the ArcGIS layer as a single GeoJSON FeatureCollection.

    - Uses pagination via resultOffset/resultRecordCount
    - Writes to out_path
    """
    ensure_dir(out_path.parent)

    if out_path.exists() and not force:
        print(f"[skip] already exists: {out_path}")
        return out_path

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "energy-weather-volatility-forecast (boundary fetcher)",
            "Accept": "application/json",
        }
    )

    total = _get_count(session)
    if total <= 0:
        raise RuntimeError("Count returned 0; nothing to download?")

    print(f"[info] total features: {total}")

    all_features: list[dict[str, Any]] = []
    offset = 0

    while offset < total:
        print(f"[info] fetching offset={offset} limit={chunk_size} ...")
        chunk = _fetch_chunk_geojson(session, offset=offset, limit=chunk_size)

        features = chunk.get("features", [])
        if not isinstance(features, list):
            raise RuntimeError(f"Unexpected chunk features type: {type(features)}")

        all_features.extend(features)

        # Some ArcGIS servers can return fewer than requested even before the end.
        got = len(features)
        if got == 0:
            break

        offset += got

    # Construct final FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "name": "MISO Market (Layer 41)",
        "features": all_features,
        # Keep CRS implicit: GeoJSON defaults to WGS84
    }

    # Write pretty but not huge
    out_path.write_text(json.dumps(feature_collection), encoding="utf-8")

    print(f"[ok] wrote {len(all_features)} features to: {out_path}")
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download MISO market zone polygons as GeoJSON.")
    parser.add_argument(
        "--out",
        type=str,
        default=str(repo_root() / "data" / "raw" / "boundaries" / "miso_market.geojson"),
        help="Output GeoJSON path.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite if file exists.")
    parser.add_argument("--chunk-size", type=int, default=2000, help="Pagination chunk size.")
    args = parser.parse_args(argv)

    out_path = Path(args.out)

    try:
        download_miso_market_geojson(out_path=out_path, force=args.force, chunk_size=args.chunk_size)
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
