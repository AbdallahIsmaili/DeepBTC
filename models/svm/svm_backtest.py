# models/svm/svm_backtest.py

import os
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from svm_utils import load_features, split_features_target


# ======================================================
# PATHS ROBUSTES (basés sur la racine du projet)
# ======================================================

def get_project_root():
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )

PROJECT_ROOT = get_project_root()

ARTIFACTS_DIR = os.path.join(
    PROJECT_ROOT, "models", "svm", "artifacts"
)

MODEL_PATH = os.path.join(ARTIFACTS_DIR, "svm_model.pkl")
SCALER_PATH = os.path.join(ARTIFACTS_DIR, "scaler.pkl")

TARGET_COL = "target_direction_1h"
RETURN_COL = "future_return_1h"   # rendement réel à t+1h


class SVMBacktester:
    def __init__(self):
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(f"Model not found: {MODEL_PATH}")
        if not os.path.exists(SCALER_PATH):
            raise FileNotFoundError(f"Scaler not found: {SCALER_PATH}")

        self.model = joblib.load(MODEL_PATH)
        self.scaler = joblib.load(SCALER_PATH)

    def load_data(self):
        df = load_features()

        # garder uniquement les lignes complètes
        df = df.dropna().reset_index(drop=True)

        return df

    def generate_signals(self, df):
        X, _ = split_features_target(df, TARGET_COL)

        X_scaled = self.scaler.transform(X)

        df["signal"] = self.model.predict(X_scaled)

        return df

    def run_backtest(self, df):
        # stratégie long-only
        df["strategy_return"] = df["signal"] * df[RETURN_COL]

        df["equity_curve"] = (1 + df["strategy_return"]).cumprod()

        return df

    def compute_metrics(self, df):
        returns = df["strategy_return"]

        total_return = df["equity_curve"].iloc[-1] - 1

        sharpe = (
            np.sqrt(24 * 365) * returns.mean() / returns.std()
            if returns.std() != 0 else 0
        )

        max_drawdown = (
            df["equity_curve"] / df["equity_curve"].cummax() - 1
        ).min()

        print("\n📈 BACKTEST RESULTS")
        print(f"Total Return     : {total_return:.2%}")
        print(f"Sharpe Ratio     : {sharpe:.2f}")
        print(f"Max Drawdown     : {max_drawdown:.2%}")

    def plot_equity(self, df):
        plt.figure(figsize=(12, 6))
        plt.plot(df["equity_curve"], label="SVM Strategy")
        plt.title("SVM Trading Strategy Equity Curve")
        plt.xlabel("Time")
        plt.ylabel("Equity")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def run(self):
        df = self.load_data()
        df = self.generate_signals(df)
        df = self.run_backtest(df)
        self.compute_metrics(df)
        self.plot_equity(df)


if __name__ == "__main__":
    backtester = SVMBacktester()
    backtester.run()
