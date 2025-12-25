# (generate predictions file)

import joblib
import pandas as pd
from config import PROCESSED_DIR, MODELS_DIR, OUTPUTS_DIR

def main():
    df = pd.read_parquet(PROCESSED_DIR / "modeling_table.parquet")
    model = joblib.load(MODELS_DIR / "model.joblib")

    X = df[["temp_f", "wind_mph"]]
    df_out = df[["date"]].copy()
    df_out["prediction"] = model.predict(X)

    out_path = OUTPUTS_DIR / "predictions.csv"
    df_out.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()
