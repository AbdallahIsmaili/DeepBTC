# models/svm/svm_utils.py

import pandas as pd
import os


def get_project_root():
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )


def load_features(filename="btc_features_complete.csv"):
    project_root = get_project_root()
    features_path = os.path.join(project_root, "data", "features", filename)

    print(f"📂 Loading features from: {features_path}")

    if not os.path.exists(features_path):
        raise FileNotFoundError(f"Features file not found: {features_path}")

    return pd.read_csv(features_path)


def split_features_target(df, target_col):
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found")

    # 🎯 Target
    y = df[target_col]

    # ❌ Colonnes à exclure explicitement
    drop_cols = [
        target_col,
        "Datetime",
        "fear_greed_classification"
    ]

    # 🔢 Garder UNIQUEMENT les colonnes numériques
    X = (
        df
        .drop(columns=[c for c in drop_cols if c in df.columns])
        .select_dtypes(include=["int64", "float64"])
    )

    if X.empty:
        raise ValueError("No numeric features left after filtering")

    return X, y
