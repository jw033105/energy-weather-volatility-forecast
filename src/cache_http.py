from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class CacheResult:
    path: Path
    from_cache: bool


def _is_fresh(path: Path, ttl_seconds: int) -> bool:
    if not path.exists():
        return False
    age = time.time() - path.stat().st_mtime
    return age <= ttl_seconds


def fetch_json_cached(
    url: str,
    cache_path: Path,
    ttl_seconds: int = 60,
    timeout_seconds: int = 30,
    headers: Optional[dict[str, str]] = None,
) -> tuple[dict[str, Any], CacheResult]:
    """
    Fetch JSON from URL with a simple file cache.

    - If cache file is newer than ttl_seconds, returns cached JSON
    - Else fetches fresh, writes JSON to cache_path, returns it
    """
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if _is_fresh(cache_path, ttl_seconds):
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        return data, CacheResult(path=cache_path, from_cache=True)

    h = {"Accept": "application/json", "User-Agent": "energy-weather-volatility-forecast"}
    if headers:
        h.update(headers)

    r = requests.get(url, headers=h, timeout=timeout_seconds)
    r.raise_for_status()

    data = r.json()

    # atomic write
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    tmp.replace(cache_path)

    return data, CacheResult(path=cache_path, from_cache=False)
