from __future__ import annotations
import argparse
import pandas as pd
import yfinance as yf
from pathlib import Path

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", type=str, default="XLE")
    p.add_argument("--start", type=str, default="2005-01-01")
    p.add_argument("--out", type=str, default="data/processed/prices.csv")
    return p.parse_args()

def main():
    args = parse_args()
    df = yf.download(args.ticker, start=args.start, auto_adjust=True, progress=False)

    if df.empty:
        raise SystemExit("No price data downloaded. Check ticker or internet connection.")

    df = df.reset_index()[["Date", "Close"]].rename(columns={"Date": "date", "Close": "close"})
    df["ret"] = df["close"].pct_change()

    # target: next-day absolute return (a simple volatility proxy)
    df["target_next_absret"] = df["ret"].shift(-1).abs()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Saved prices to {out.resolve()}")

if __name__ == "__main__":
    main()
