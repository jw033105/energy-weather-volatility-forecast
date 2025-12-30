from __future__ import annotations

import argparse

from .miso_public import (
    fetch_fuel_mix,
    fetch_total_load,
    fuel_mix_to_df,
    total_load_to_df,
    write_processed,
)


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch latest MISO public RT data (cached) and write processed CSVs.")
    p.add_argument("--ttl", type=int, default=60, help="Cache TTL seconds (default 60).")
    args = p.parse_args()

    load_payload = fetch_total_load(ttl_seconds=args.ttl)
    fuel_payload = fetch_fuel_mix(ttl_seconds=args.ttl)

    load_df = total_load_to_df(load_payload)
    fuel_df = fuel_mix_to_df(fuel_payload)

    load_out = write_processed(load_df, "total_load_latest")
    fuel_out = write_processed(fuel_df, "fuel_mix_latest")

    print(f"[ok] wrote: {load_out}")
    print(f"[ok] wrote: {fuel_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
