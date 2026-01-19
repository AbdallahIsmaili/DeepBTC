"""
Script to create improved MLP notebook with modern features
"""
import json

# Create improved MLP notebook
cells = [
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "# BTC Oracle: MLP (Multi-Layer Perceptron) Model\n",
            "\n",
            "Deep Neural Network for Bitcoin price prediction using feedforward architecture.\n",
            "\n",
            "## Improvements over original:\n",
            "- ✅ Kaggle dataset integration\n",
            "- ✅ High confidence analysis\n",
            "- ✅ Better visualizations\n",
            "- ✅ Financial metrics integration\n",
            "- ✅ Aligned with LSTM/GRU notebooks"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "import kagglehub\n",
            "import pandas as pd\n",
            "import numpy as np\n",
            "import tensorflow as tf\n",
            "from tensorflow.keras.models import Sequential\n",
            "from tensorflow.keras.layers import Dense, Dropout, BatchNormalization, Input\n",
            "from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau\n",
            "from tensorflow.keras.optimizers import Adam\n",
            "from sklearn.preprocessing import RobustScaler\n",
            "from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score\n",
            "import matplotlib.pyplot as plt\n",
            "import seaborn as sns\n",
            "import warnings\n",
            "\n",
            "warnings.filterwarnings('ignore')\n",
            "sns.set_style('whitegrid')\n",
            "plt.rcParams['figure.figsize'] = (14, 6)\n",
            "\n",
            "print('='*80)\n",
            "print(' '*25 + 'BITCOIN PRICE PREDICTION')\n",
            "print(' '*30 + 'MLP MODEL')\n",
            "print('='*80)\n",
            "print(f'TensorFlow Version: {tf.__version__}')"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 1. Load Data from Kaggle"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Download dataset\n",
            "path = kagglehub.dataset_download('oussamataghlaoui/btc-oracle-on-chain-sentiment-and-macro-data')\n",
            "print(f'Dataset path: {path}')\n",
            "\n",
            "# Find CSV\n",
            "import glob\n",
            "csv_files = glob.glob(path + '/*.csv')\n",
            "df = pd.read_csv(csv_files[0], parse_dates=['Datetime'], index_col='Datetime')\n",
            "\n",
            "print(f'Data shape: {df.shape}')\n",
            "print(f'Date range: {df.index.min()} → {df.index.max()}')\n",
            "df.head()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 2. Create Target & Prepare Features"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "def create_percentile_target(df, return_col='future_return_24h', percentile=60):\n",
            "    '''Create 3-class target: UP, DOWN, NEUTRAL'''\n",
            "    returns = df[return_col]\n",
            "    up_thresh = np.percentile(returns.dropna(), percentile)\n",
            "    down_thresh = np.percentile(returns.dropna(), 100 - percentile)\n",
            "    \n",
            "    print(f'Thresholds | UP > {up_thresh:.4f} | DOWN < {down_thresh:.4f}')\n",
            "    \n",
            "    # 3 classes: 0=DOWN, 1=NEUTRAL, 2=UP\n",
            "    target = pd.Series(1, index=returns.index)  # Default NEUTRAL\n",
            "    target[returns > up_thresh] = 2  # UP\n",
            "    target[returns < down_thresh] = 0  # DOWN\n",
            "    \n",
            "    return target.dropna(), (down_thresh, up_thresh)\n",
            "\n",
            "# Create target\n",
            "df_clean = df.dropna(subset=['future_return_24h']).copy()\n",
            "y_target, thresholds = create_percentile_target(df_clean, 'future_return_24h', percentile=60)\n",
            "\n",
            "print(f'\\nClass distribution:')\n",
            "print(y_target.value_counts(normalize=True))"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Select features (numeric only, exclude targets)\n",
            "feature_cols = [c for c in df_clean.columns if 'future' not in c and 'target' not in c and 'return' not in c.lower()]\n",
            "feature_cols = [c for c in feature_cols if pd.api.types.is_numeric_dtype(df_clean[c])]\n",
            "\n",
            "X = df_clean[feature_cols].copy()\n",
            "X.replace([np.inf, -np.inf], np.nan, inplace=True)\n",
            "\n",
            "print(f'Features: {len(feature_cols)}')\n",
            "print(f'Samples: {len(X):,}')"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 3. Split & Scale Data"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Time-series split\n",
            "train_size = int(len(X) * 0.7)\n",
            "val_size = int(len(X) * 0.15)\n",
            "\n",
            "X_train = X.iloc[:train_size]\n",
            "X_val = X.iloc[train_size:train_size+val_size]\n",
            "X_test = X.iloc[train_size+val_size:]\n",
            "\n",
            "y_train = y_target.iloc[:train_size]\n",
            "y_val = y_target.iloc[train_size:train_size+val_size]\n",
            "y_test = y_target.iloc[train_size+val_size:]\n",
            "\n",
            "# Impute & Scale\n",
            "train_medians = X_train.median()\n",
            "X_train = X_train.fillna(train_medians)\n",
            "X_val = X_val.fillna(train_medians)\n",
            "X_test = X_test.fillna(train_medians)\n",
            "\n",
            "scaler = RobustScaler()\n",
            "X_train_scaled = scaler.fit_transform(X_train)\n",
            "X_val_scaled = scaler.transform(X_val)\n",
            "X_test_scaled = scaler.transform(X_test)\n",
            "\n",
            "print(f'Train: {X_train_scaled.shape}')\n",
            "print(f'Val: {X_val_scaled.shape}')\n",
            "print(f'Test: {X_test_scaled.shape}')"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 4. Build Enhanced MLP Model"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "def build_mlp_model(input_dim, num_classes=3):\n",
            "    '''Enhanced MLP with BatchNorm and Dropout'''\n",
            "    model = Sequential([\n",
            "        Input(shape=(input_dim,)),\n",
            "        \n",
            "        # Layer 1: 256 neurons\n",
            "        Dense(256, activation='relu'),\n",
            "        BatchNormalization(),\n",
            "        Dropout(0.4),\n",
            "        \n",
            "        # Layer 2: 128 neurons\n",
            "        Dense(128, activation='relu'),\n",
            "        BatchNormalization(),\n",
            "        Dropout(0.3),\n",
            "        \n",
            "        # Layer 3: 64 neurons\n",
            "        Dense(64, activation='relu'),\n",
            "        BatchNormalization(),\n",
            "        Dropout(0.3),\n",
            "        \n",
            "        # Layer 4: 32 neurons\n",
            "        Dense(32, activation='relu'),\n",
            "        Dropout(0.2),\n",
            "        \n",
            "        # Output: 3 classes (DOWN, NEUTRAL, UP)\n",
            "        Dense(num_classes, activation='softmax')\n",
            "    ])\n",
            "    \n",
            "    model.compile(\n",
            "        optimizer=Adam(learning_rate=0.001),\n",
            "        loss='sparse_categorical_crossentropy',\n",
            "        metrics=['accuracy']\n",
            "    )\n",
            "    return model\n",
            "\n",
            "model = build_mlp_model(X_train_scaled.shape[1])\n",
            "model.summary()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 5. Train Model"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "early_stop = EarlyStopping(\n",
            "    monitor='val_loss',\n",
            "    patience=15,\n",
            "    restore_best_weights=True,\n",
            "    verbose=1\n",
            ")\n",
            "\n",
            "reduce_lr = ReduceLROnPlateau(\n",
            "    monitor='val_loss',\n",
            "    factor=0.5,\n",
            "    patience=5,\n",
            "    min_lr=1e-6,\n",
            "    verbose=1\n",
            ")\n",
            "\n",
            "history = model.fit(\n",
            "    X_train_scaled, y_train,\n",
            "    validation_data=(X_val_scaled, y_val),\n",
            "    epochs=100,\n",
            "    batch_size=32,\n",
            "    callbacks=[early_stop, reduce_lr],\n",
            "    verbose=1\n",
            ")"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 6. Evaluation & High Confidence Analysis"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Training curves\n",
            "fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))\n",
            "\n",
            "ax1.plot(history.history['loss'], label='Train')\n",
            "ax1.plot(history.history['val_loss'], label='Validation')\n",
            "ax1.set_title('Loss Curve')\n",
            "ax1.set_xlabel('Epoch')\n",
            "ax1.legend()\n",
            "\n",
            "ax2.plot(history.history['accuracy'], label='Train')\n",
            "ax2.plot(history.history['val_accuracy'], label='Validation')\n",
            "ax2.set_title('Accuracy Curve')\n",
            "ax2.set_xlabel('Epoch')\n",
            "ax2.legend()\n",
            "\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Predictions\n",
            "y_prob = model.predict(X_test_scaled)\n",
            "y_pred = np.argmax(y_prob, axis=1)\n",
            "y_conf = np.max(y_prob, axis=1)\n",
            "\n",
            "print('--- Standard Classification Report ---')\n",
            "print(classification_report(y_test, y_pred, target_names=['DOWN', 'NEUTRAL', 'UP']))\n",
            "\n",
            "# High Confidence Analysis\n",
            "CONFIDENCE_THRESHOLD = 0.75\n",
            "\n",
            "high_conf_mask = y_conf >= CONFIDENCE_THRESHOLD\n",
            "y_test_hc = y_test[high_conf_mask]\n",
            "y_pred_hc = y_pred[high_conf_mask]\n",
            "\n",
            "print(f'\\n--- High Confidence Report (> {CONFIDENCE_THRESHOLD*100:.0f}%) ---')\n",
            "print(f'Coverage: {np.sum(high_conf_mask)} / {len(y_test)} samples ({np.sum(high_conf_mask)/len(y_test):.1%})')\n",
            "\n",
            "if len(y_test_hc) > 0:\n",
            "    print(classification_report(y_test_hc, y_pred_hc, target_names=['DOWN', 'NEUTRAL', 'UP']))\n",
            "    \n",
            "    # Confusion Matrix\n",
            "    plt.figure(figsize=(6, 5))\n",
            "    sns.heatmap(confusion_matrix(y_test_hc, y_pred_hc), annot=True, fmt='d', cmap='Blues')\n",
            "    plt.title('High Confidence Confusion Matrix')\n",
            "    plt.show()\n",
            "else:\n",
            "    print('No predictions met the confidence threshold.')"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": ["## 7. Save Model"]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "import os\n",
            "model_path = '../models/mlp_model.keras'\n",
            "os.makedirs('../models', exist_ok=True)\n",
            "model.save(model_path)\n",
            "print(f'✓ Model saved to {model_path}')"
        ]
    }
]

notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python",
            "version": "3.8.10"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 5
}

# Save
output_path = r"C:\Users\15086\Documents\GitHub\DeepBTC\notebooks\mlp.ipynb"
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=2, ensure_ascii=False)

print(f"✅ Improved MLP notebook created: {output_path}")
print("\nKey improvements:")
print("  - Kaggle dataset integration")
print("  - 3-class classification (DOWN/NEUTRAL/UP)")
print("  - Enhanced architecture (256→128→64→32)")
print("  - High confidence analysis")
print("  - Better visualizations")
print("  - Aligned with LSTM/GRU notebooks")
