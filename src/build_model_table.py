from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--features", type=str, default="data/processed/era5_features.csv")
    p.add_argument("--prices", type=str, default="data/processed/prices.csv")
    p.add_argument("--out", type=str, default="data/processed/model_table.csv")
    return p.parse_args()

def main():
    args = parse_args()
    feat = pd.read_csv(args.features, parse_dates=["date"])
    px = pd.read_csv(args.prices, parse_dates=["date"])

    df = feat.merge(px[["date", "target_next_absret"]], on="date", how="inner")
    df = df.dropna(subset=["target_next_absret"])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Saved model table to {out.resolve()} (rows={len(df)})")

if __name__ == "__main__":
    main()
