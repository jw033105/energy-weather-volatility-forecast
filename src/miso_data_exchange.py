from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests


@dataclass
class DexResponse:
    url: str
    status_code: int
    payload: Any
    text: str


def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing env var {name}. Set it and re-run.")
    return val


def fetch_json(
    url: str,
    subscription_key: str,
    header_name: str,
    params: Optional[dict[str, Any]] = None,
    timeout: int = 60,
    retries: int = 3,
    backoff: float = 1.5,
) -> DexResponse:
    headers = {
        "Accept": "application/json",
        "User-Agent": "energy-weather-volatility-forecast",
        "Cache-Control": "no-cache",
        header_name: subscription_key,
    }

    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Auth errors should fail loudly
            if r.status_code in (401, 403):
                raise RuntimeError(f"Auth error {r.status_code}. Check subscription key + header name.\n{r.text}")

            # Try to parse JSON even for 4xx, because MISO returns useful messages
            try:
                payload = r.json()
            except Exception:
                payload = None

            return DexResponse(url=r.url, status_code=r.status_code, payload=payload, text=r.text)

        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
            else:
                raise last_err

    raise last_err  # type: ignore[misc]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
