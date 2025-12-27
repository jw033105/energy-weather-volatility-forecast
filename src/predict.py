import joblib
import pandas as pd
from config import PROCESSED_DIR, MODELS_DIR, OUTPUTS_DIR

def main():
    bundle = joblib.load(MODELS_DIR / "model.joblib")
    model = bundle["model"]
    feature_cols = bundle["feature_cols"]

    feat = pd.read_csv(PROCESSED_DIR / "forecast_features.csv")
    X = feat[feature_cols]
    pred = float(model.predict(X)[0])

    # Load historical target distribution for context
    hist = pd.read_csv(PROCESSED_DIR / "model_table.csv")
    y = hist["target_next_absret"].dropna()

    p50 = float(y.quantile(0.50))
    p75 = float(y.quantile(0.75))
    p90 = float(y.quantile(0.90))
    p95 = float(y.quantile(0.95))

    # Regime label
    if pred >= p95:
        label = "EXTREME (>= P95)"
    elif pred >= p90:
        label = "HIGH (P90–P95)"
    elif pred >= p75:
        label = "ELEVATED (P75–P90)"
    elif pred >= p50:
        label = "TYPICAL (P50–P75)"
    else:
        label = "LOW (< P50)"

    out = feat.copy()
    out["pred_next_absret"] = pred
    out["pred_next_absret_pct"] = pred * 100.0
    out["vol_regime"] = label

    out["hist_p50"] = p50
    out["hist_p75"] = p75
    out["hist_p90"] = p90
    out["hist_p95"] = p95

    out_path = OUTPUTS_DIR / "volatility_forecast.csv"
    out.to_csv(out_path, index=False)
    print(f"Saved forecast to {out_path}")

        # --- Executive summary ---
    valid_date = out.loc[0, "valid_date"] if "valid_date" in out.columns else "N/A"
    pred_pct = float(out.loc[0, "pred_next_absret_pct"])
    regime = out.loc[0, "vol_regime"]

    # Pull a couple “drivers” from forecast features if present
    tmean = float(out.loc[0, "t2m_anom_mean_c"]) if "t2m_anom_mean_c" in out.columns else None
    hotfrac = float(out.loc[0, "hot_area_frac"]) if "hot_area_frac" in out.columns else None
    windm = float(out.loc[0, "wind_anom_mag_mean"]) if "wind_anom_mag_mean" in out.columns else None

    lines = []
    lines.append(f"Forecast date: {valid_date}")
    lines.append(f"Predicted next-day abs move: {pred_pct:.2f}%")
    lines.append(f"Volatility regime: {regime}")
    lines.append("")
    lines.append("Weather anomaly drivers (region):")
    if tmean is not None:
        lines.append(f" - Mean 2m temp anomaly: {tmean:.2f} °C")
    if hotfrac is not None:
        lines.append(f" - Hot area fraction (> +8°C): {hotfrac:.3f}")
    if windm is not None:
        lines.append(f" - Mean wind anomaly magnitude: {windm:.2f}")
    lines.append("")
    lines.append("Notes:")
    lines.append(" - This is a statistical forecast based on historical relationships between weather anomalies and next-day volatility.")
    lines.append(" - It predicts magnitude (abs move), not direction.")

    summary_text = "\n".join(lines)

    summary_path = OUTPUTS_DIR / "summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary_text)

    print("\n" + summary_text)
    print(f"\nSaved summary to {summary_path}")


    # Also print a friendly one-liner
    if "valid_date" in out.columns:
        vd = out.loc[0, "valid_date"]
        print(f"Forecast for {vd}: {pred*100:.2f}% abs move → {label}")
    else:
        print(f"Forecast: {pred*100:.2f}% abs move → {label}")

if __name__ == "__main__":
    main()
