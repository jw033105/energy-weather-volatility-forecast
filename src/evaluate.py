# (make one plot + metric)

import joblib
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from config import PROCESSED_DIR, MODELS_DIR, REPORTS_DIR

def main():
    df = pd.read_parquet(PROCESSED_DIR / "modeling_table.parquet")
    model = joblib.load(MODELS_DIR / "model.joblib")

    X = df[["temp_f", "wind_mph"]]
    y = df["demand_mw"]
    preds = model.predict(X)

    mae = mean_absolute_error(y, preds)
    rmse = mean_squared_error(y, preds) ** 0.5
    print(f"MAE: {mae:.2f} | RMSE: {rmse:.2f}")

    # Plot actual vs predicted (simple)
    plt.figure()
    plt.plot(df["date"], y, label="Actual")
    plt.plot(df["date"], preds, label="Predicted")
    plt.legend()
    plt.title("Actual vs Predicted")
    fig_path = REPORTS_DIR / "figures" / "actual_vs_pred.png"
    plt.savefig(fig_path, dpi=150, bbox_inches="tight")
    print(f"Saved figure: {fig_path}")

if __name__ == "__main__":
    main()
