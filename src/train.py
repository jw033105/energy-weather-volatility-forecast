import joblib
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from config import PROCESSED_DIR, MODELS_DIR

def main():
    df = pd.read_parquet(PROCESSED_DIR / "modeling_table.parquet")

    X = df[["temp_f", "wind_mph"]]
    y = df["demand_mw"]

    # Simple model pipeline
    model = Pipeline([
        ("scaler", StandardScaler()),
        ("reg", Ridge(alpha=1.0))
    ])

    # Quick time-series validation (last fold as “validation”)
    tscv = TimeSeriesSplit(n_splits=5)
    maes = []
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        model.fit(X_train, y_train)
        preds = model.predict(X_val)
        maes.append(mean_absolute_error(y_val, preds))

    print(f"CV MAE (mean): {sum(maes)/len(maes):.2f}")

    # Fit on all data and save
    model.fit(X, y)
    out_path = MODELS_DIR / "model.joblib"
    joblib.dump(model, out_path)
    print(f"Saved model: {out_path}")

if __name__ == "__main__":
    main()
