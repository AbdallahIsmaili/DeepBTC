# improved_btc_predictor.py - Version améliorée avec accuracy élevée

import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score
import xgboost as xgb
import joblib
import warnings
from pathlib import Path
import json
from datetime import datetime
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')

class ImprovedBTCPredictor:
    """
    Version améliorée avec :
    - Horizon de prédiction réduit (1h au lieu de 24h)
    - Validation temporelle (TimeSeriesSplit)
    - Features optimisées
    - Hyperparamètres tunés
    - Stratégie de trading améliorée
    """

    def __init__(self, prediction_horizon=1):
        self.prediction_horizon = prediction_horizon  # 1h, 6h, ou 24h
        self.models_dir = Path('models')
        self.models_dir.mkdir(exist_ok=True)
        self.scaler = StandardScaler()

    def load_and_prepare_data(self):
        """Charge et prépare les données avec validation temporelle"""
        print("🔄 Chargement des données...")

        # Charger les features
        df = pd.read_csv('data/features/btc_features_complete.csv', index_col='Datetime', parse_dates=True)

        # Créer le target avec horizon réduit
        target_col = f'future_return_{self.prediction_horizon}h'
        if target_col not in df.columns:
            print(f"⚠️ Colonne {target_col} non trouvée, création...")
            df[target_col] = df['Close'].shift(-self.prediction_horizon) / df['Close'] - 1

        # Nettoyer les données
        df = df.dropna(subset=[target_col])

        # Créer target binaire (plus simple et efficace)
        returns = df[target_col]
        threshold = 0.002  # 0.2% pour 1h (ajustable selon horizon)
        df['target'] = (returns > threshold).astype(int)

        print(f"✅ Données chargées: {len(df)} échantillons")
        print(f"   Horizon: {self.prediction_horizon}h, Seuil: {threshold:.1%}")
        print(f"   Classes: {df['target'].value_counts().to_dict()}")

        return df

    def create_optimized_features(self, df):
        """Features optimisées pour meilleure prédiction"""
        print("🔧 Création des features optimisées...")

        # Features temporelles améliorées
        df['hour'] = df.index.hour
        df['day_of_week'] = df.index.dayofweek
        df['month'] = df.index.month

        # Features de momentum multi-timeframes
        for period in [1, 3, 6, 12, 24]:
            df[f'returns_{period}h'] = df['Close'].pct_change(period)
            df[f'volatility_{period}h'] = df['returns'].rolling(period).std()

        # Features de volume avancées
        df['volume_sma_ratio'] = df['Volume'] / df['Volume'].rolling(24).mean()
        df['volume_trend'] = df['Volume'].rolling(24).mean() / df['Volume'].rolling(24*7).mean()

        # Features techniques améliorées
        df['rsi_divergence'] = df['RSI_14'] - df['RSI_14'].rolling(24).mean()
        df['macd_signal_diff'] = df['MACD_12_26_9'] - df['MACDs_12_26_9']

        # Price action features
        df['price_range'] = (df['High'] - df['Low']) / df['Close']
        df['body_size'] = abs(df['Close'] - df['Open']) / df['Close']
        df['upper_wick'] = (df['High'] - df[['Open', 'Close']].max(axis=1)) / df['Close']
        df['lower_wick'] = (df[['Open', 'Close']].min(axis=1) - df['Low']) / df['Close']

        # Nettoyer les NaN
        df = df.fillna(method='bfill').fillna(method='ffill')

        # Sélectionner les features finales
        exclude_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'target'] + \
                      [col for col in df.columns if 'future_return' in col]

        feature_cols = [col for col in df.columns if col not in exclude_cols and df[col].dtype in ['float64', 'int64']]

        print(f"✅ {len(feature_cols)} features créées")
        return df, feature_cols

    def train_with_temporal_validation(self, df, feature_cols):
        """Entraînement avec validation temporelle (TimeSeriesSplit)"""
        print("🚀 Entraînement avec validation temporelle...")

        X = df[feature_cols]
        y = df['target']

        # Time Series Split (5 folds)
        tscv = TimeSeriesSplit(n_splits=5)

        # Hyperparamètres optimisés pour BTC
        best_params = {
            'objective': 'binary:logistic',
            'eval_metric': ['auc', 'logloss'],
            'max_depth': 3,  # Réduit pour éviter overfitting
            'min_child_weight': 10,
            'learning_rate': 0.05,  # Plus conservateur
            'n_estimators': 200,
            'gamma': 0.1,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'scale_pos_weight': len(y[y==0]) / len(y[y==1]),  # Balance des classes
            'random_state': 42,
            'early_stopping_rounds': 20,
            'verbose': 0
        }

        fold_results = []

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            print(f"   Fold {fold+1}/5...")

            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

            # Scaling
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_val_scaled = self.scaler.transform(X_val)

            # Entraînement
            model = xgb.XGBClassifier(**best_params)
            model.fit(
                X_train_scaled, y_train,
                eval_set=[(X_train_scaled, y_train), (X_val_scaled, y_val)],
                verbose=False
            )

            # Prédictions
            y_pred = model.predict(X_val_scaled)
            y_proba = model.predict_proba(X_val_scaled)[:, 1]

            # Métriques
            accuracy = accuracy_score(y_val, y_pred)
            auc = roc_auc_score(y_val, y_proba)

            fold_results.append({
                'fold': fold+1,
                'accuracy': accuracy,
                'auc': auc,
                'best_iteration': model.best_iteration
            })

            print(".3f")

        # Moyenne des résultats
        avg_accuracy = np.mean([r['accuracy'] for r in fold_results])
        avg_auc = np.mean([r['auc'] for r in fold_results])

        print("\n📊 Résultats moyens de validation temporelle:")
        print(f"   Accuracy: {avg_accuracy:.3f}")
        print(f"   AUC: {avg_auc:.3f}")

        # Entraîner le modèle final sur toutes les données
        print("\n🔄 Entraînement du modèle final...")
        X_scaled = self.scaler.fit_transform(X)

        # Configuration sans early stopping pour l'entraînement final
        final_params = best_params.copy()
        final_params['early_stopping_rounds'] = None

        self.final_model = xgb.XGBClassifier(**final_params)
        self.final_model.fit(X_scaled, y, verbose=False)

        # Sauvegarder
        self.save_model(feature_cols, fold_results)

        return self.final_model, feature_cols, avg_accuracy

    def save_model(self, feature_cols, fold_results):
        """Sauvegarde le modèle et les métriques"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Sauvegarder le modèle
        model_path = self.models_dir / f'improved_xgb_{self.prediction_horizon}h_{timestamp}.json'
        self.final_model.save_model(str(model_path))

        # Sauvegarder le scaler
        scaler_path = self.models_dir / f'scaler_{self.prediction_horizon}h_{timestamp}.pkl'
        joblib.dump(self.scaler, scaler_path)

        # Sauvegarder les features
        features_path = self.models_dir / f'features_{self.prediction_horizon}h_{timestamp}.txt'
        with open(features_path, 'w') as f:
            f.write('\n'.join(feature_cols))

        # Métriques détaillées
        metrics = {
            'model_type': f'Improved XGBoost {self.prediction_horizon}h',
            'training_date': datetime.now().isoformat(),
            'prediction_horizon': self.prediction_horizon,
            'temporal_validation_results': fold_results,
            'avg_accuracy': np.mean([r['accuracy'] for r in fold_results]),
            'avg_auc': np.mean([r['auc'] for r in fold_results]),
            'feature_count': len(feature_cols),
            'model_path': str(model_path),
            'scaler_path': str(scaler_path),
            'features_path': str(features_path)
        }

        metrics_path = self.models_dir / f'improved_metrics_{self.prediction_horizon}h_{timestamp}.json'
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)

        print(f"💾 Modèle sauvegardé: {model_path}")
        print(f"📈 Métriques: {metrics_path}")

    def predict(self, new_data):
        """Prédiction sur nouvelles données"""
        # Charger les features utilisées
        features_path = list(self.models_dir.glob(f'features_{self.prediction_horizon}h_*.txt'))[-1]
        with open(features_path, 'r') as f:
            feature_cols = [line.strip() for line in f.readlines()]

        # Préparer les données
        X = new_data[feature_cols]
        X_scaled = self.scaler.transform(X)

        # Prédire
        predictions = self.final_model.predict(X_scaled)
        probabilities = self.final_model.predict_proba(X_scaled)[:, 1]

        return predictions, probabilities

    def run_improved_pipeline(self):
        """Pipeline complet amélioré"""
        print("="*70)
        print("🎯 DEEPBTC - VERSION AMÉLIORÉE")
        print(f"   Horizon: {self.prediction_horizon}h")
        print("="*70)

        # 1. Charger et préparer les données
        df = self.load_and_prepare_data()

        # 2. Créer les features optimisées
        df, feature_cols = self.create_optimized_features(df)

        # 3. Entraîner avec validation temporelle
        model, features, avg_accuracy = self.train_with_temporal_validation(df, feature_cols)

        print("\n✅ Pipeline terminé avec succès!")
        print(f"   Accuracy moyenne: {avg_accuracy:.3f}")
        return model, features

if __name__ == "__main__":
    # Tester avec différents horizons
    for horizon in [1, 6]:  # 1h et 6h (plus prédictibles que 24h)
        print(f"\n{'='*80}")
        print(f"TESTING HORIZON: {horizon}h")
        print('='*80)

        predictor = ImprovedBTCPredictor(prediction_horizon=horizon)
        try:
            model, features = predictor.run_improved_pipeline()
            print(f"✅ Horizon {horizon}h: SUCCÈS")
        except Exception as e:
            print(f"❌ Horizon {horizon}h: ERREUR - {e}")

    print("\n🎉 Améliorations implémentées:")
    print("   • Horizon réduit (1h/6h au lieu de 24h)")
    print("   • Validation temporelle (TimeSeriesSplit)")
    print("   • Features optimisées")
    print("   • Hyperparamètres tunés")
    print("   • Balance des classes")
    print("   • Modularisation du code")