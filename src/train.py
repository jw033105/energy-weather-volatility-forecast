import joblib
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from config import PROCESSED_DIR, MODELS_DIR


FEATURE_COLS = [
    "t2m_anom_mean_c",
    "t2m_anom_max_c",
    "t2m_anom_min_c",
    "hot_area_frac",
    "cold_area_frac",
    "wind_anom_mag_mean",
    "cdd_mean",
    "hdd_mean",
    "cdd_anom_mean",
    "hdd_anom_mean"

]

TARGET_COL = "target_next_absret"


def main():
    # Load model table
    df = pd.read_csv(PROCESSED_DIR / "model_table.csv", parse_dates=["date"])

    # Drop rows with missing target/features
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL]).sort_values("date")

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("reg", Ridge(alpha=1.0))
    ])

    # Time-series CV
    tscv = TimeSeriesSplit(n_splits=5)
    maes = []
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model.fit(X_train, y_train)
        preds = model.predict(X_val)
        maes.append(mean_absolute_error(y_val, preds))

    print(f"CV MAE (mean): {sum(maes)/len(maes):.6f}")

    # Fit on all data and save
    model.fit(X, y)
    out_path = MODELS_DIR / "model.joblib"
    joblib.dump(
        {
            "model": model,
            "feature_cols": FEATURE_COLS,
            "target_col": TARGET_COL,
        },
        out_path
    )
    print(f"Saved model bundle: {out_path}")


if __name__ == "__main__":
    main()
