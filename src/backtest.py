import joblib
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error
from config import PROCESSED_DIR, MODELS_DIR

def main():
    bundle = joblib.load(MODELS_DIR / "model.joblib")
    model = bundle["model"]
    feature_cols = bundle["feature_cols"]
    target_col = bundle["target_col"]

    df = pd.read_csv(PROCESSED_DIR / "model_table.csv", parse_dates=["date"]).sort_values("date")
    df = df.dropna(subset=feature_cols + [target_col])

    X = df[feature_cols]
    y = df[target_col].values
    dates = df["date"].values

    tscv = TimeSeriesSplit(n_splits=5)
    preds_all = [None] * len(df)

    for train_idx, test_idx in tscv.split(X):
        model.fit(X.iloc[train_idx], y[train_idx])
        preds = model.predict(X.iloc[test_idx])
        for i, p in zip(test_idx, preds):
            preds_all[i] = float(p)

    out = df[["date"]].copy()
    out["y_true"] = y
    out["y_pred"] = preds_all
    out = out.dropna()

    mae = mean_absolute_error(out["y_true"], out["y_pred"])
    corr = out["y_true"].corr(out["y_pred"])

    print(f"Backtest rows: {len(out)}")
    print(f"Backtest MAE:  {mae:.6f}")
    print(f"Correlation:   {corr:.3f}")

    # Plot 1: time series (pred vs actual)
    plt.figure()
    plt.plot(out["date"], out["y_true"], label="Actual")
    plt.plot(out["date"], out["y_pred"], label="Predicted")
    plt.legend()
    plt.xlabel("Date")
    plt.ylabel("Next-day abs return")
    plt.title("Backtest: Predicted vs Actual Next-day Abs Return")
    plt.tight_layout()
    plt.savefig("reports/figures/backtest_pred_vs_actual.png", dpi=150)
    plt.close()

    # Plot 2: scatter
    plt.figure()
    plt.scatter(out["y_true"], out["y_pred"], s=10)
    plt.xlabel("Actual next-day abs return")
    plt.ylabel("Predicted next-day abs return")
    plt.title("Backtest Scatter: Predicted vs Actual")
    plt.tight_layout()
    plt.savefig("reports/figures/backtest_scatter.png", dpi=150)
    plt.close()

        # --- Calibration by bins (quintiles) ---
    out2 = out.copy()
    out2["pred_bin"] = pd.qcut(out2["y_pred"], q=5, duplicates="drop")

    calib = out2.groupby("pred_bin").agg(
        pred_mean=("y_pred", "mean"),
        actual_mean=("y_true", "mean"),
        count=("y_true", "size"),
    ).reset_index()

    print("\nCalibration (quintiles of predicted risk):")
    print(calib.to_string(index=False))

    # Bar plot: actual_mean by bin
    plt.figure()
    plt.bar(range(len(calib)), calib["actual_mean"].values)
    plt.xticks(range(len(calib)), [str(b) for b in calib["pred_bin"]], rotation=30, ha="right")
    plt.ylabel("Mean actual next-day abs return")
    plt.title("Calibration: Realized Volatility by Predicted Risk Quintile")
    plt.tight_layout()
    plt.savefig("reports/figures/backtest_calibration_bins.png", dpi=150)
    plt.close()

    print(" - reports/figures/backtest_calibration_bins.png")


    print("Saved:")
    print(" - reports/figures/backtest_pred_vs_actual.png")
    print(" - reports/figures/backtest_scatter.png")



if __name__ == "__main__":
    main()
