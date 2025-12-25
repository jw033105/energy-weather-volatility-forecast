# PLACE HODLER DATASET, TO BE REPLACED WITH ACTUAL DATASET CODE LATER

import numpy as np
import pandas as pd
from config import PROCESSED_DIR

def main():
    rng = np.random.default_rng(42)

    dates = pd.date_range("2020-01-01", "2022-12-31", freq="D")
    n = len(dates)

    # Example "weather" features (placeholder)
    temp = 50 + 20 * np.sin(np.linspace(0, 8 * np.pi, n)) + rng.normal(0, 5, n)
    wind = 10 + rng.normal(0, 3, n)

    # Example "economic outcome" (placeholder)
    # pretend "demand" increases when temp is far from 65F + some wind effect
    demand = 1000 + 8 * np.abs(temp - 65) + 3 * wind + rng.normal(0, 20, n)

    df = pd.DataFrame({
        "date": dates,
        "temp_f": temp,
        "wind_mph": wind,
        "demand_mw": demand
    })

    out_path = PROCESSED_DIR / "modeling_table.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path} ({len(df)} rows)")

if __name__ == "__main__":
    main()
