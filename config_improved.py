# config_improved.py - Configuration centralisée

"""
Configuration centralisée pour DeepBTC - Version Améliorée
Toutes les constantes et paramètres importants sont définis ici.
"""

from pathlib import Path

# === CHEMINS DE FICHIERS ===
PROJECT_ROOT = Path(__file__).parent.resolve()
DATA_DIR = PROJECT_ROOT / 'data'
FEATURES_DIR = DATA_DIR / 'features'
RAW_DATA_DIR = DATA_DIR / 'raw'
MODELS_DIR = PROJECT_ROOT / 'models'
LOGS_DIR = PROJECT_ROOT / 'logs'
REPORTS_DIR = PROJECT_ROOT / 'reports'

# Créer les répertoires automatiquement
for dir_path in [DATA_DIR, FEATURES_DIR, RAW_DATA_DIR, MODELS_DIR, LOGS_DIR, REPORTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Fichiers de données
FEATURES_FILE = FEATURES_DIR / 'btc_features_complete.csv'
RAW_OHLCV_FILE = RAW_DATA_DIR / 'binance_BTCUSDT_1h.csv'
RAW_BLOCKCHAIN_FILE = RAW_DATA_DIR / 'blockchain_metrics_daily.csv'
RAW_SENTIMENT_FILE = RAW_DATA_DIR / 'sentiment_metrics.csv'
RAW_MACRO_FILE = RAW_DATA_DIR / 'macro_indicators.csv'

# === PARAMÈTRES DE MODÈLE ===
MODEL_CONFIGS = {
    'xgboost_optimized': {
        'objective': 'binary:logistic',
        'eval_metric': ['auc', 'logloss'],
        'max_depth': 3,              # Réduit pour éviter overfitting
        'min_child_weight': 10,      # Plus conservateur
        'learning_rate': 0.05,       # Learning rate réduit
        'n_estimators': 200,         # Nombre d'arbres
        'gamma': 0.1,               # Régularisation
        'reg_alpha': 0.1,           # L1 régularisation
        'reg_lambda': 1.0,          # L2 régularisation
        'subsample': 0.8,           # Sous-échantillonnage
        'colsample_bytree': 0.8,    # Feature sampling
        'scale_pos_weight': 'auto',  # Balance des classes
        'random_state': 42,
        'early_stopping_rounds': 20,
        'verbose': 0
    }
}

# === PARAMÈTRES DE VALIDATION ===
VALIDATION_CONFIG = {
    'time_series_splits': 5,        # Nombre de folds temporels
    'test_size': 0.15,             # Taille du test set
    'val_size': 0.15,              # Taille du validation set
    'random_state': 42
}

# === PARAMÈTRES DE FEATURES ===
FEATURE_CONFIG = {
    # Horizons temporels pour features
    'momentum_periods': [1, 3, 6, 12, 24],
    'volatility_periods': [1, 3, 6, 12, 24],

    # Seuils pour target binaire
    'target_thresholds': {
        1: 0.002,   # 0.2% pour 1h
        6: 0.008,   # 0.8% pour 6h
        24: 0.015   # 1.5% pour 24h (référence)
    },

    # Colonnes à exclure des features
    'exclude_columns': [
        'Open', 'High', 'Low', 'Close', 'Volume',
        'target', 'Datetime'
    ] + [f'future_return_{h}h' for h in [1, 6, 24]]
}

# === PARAMÈTRES DE TRADING ===
TRADING_CONFIG = {
    'initial_capital': 10000,
    'transaction_fee': 0.001,      # 0.1% frais de trading

    # Configurations de stratégie
    'strategy_configs': [
        {
            'name': 'Conservative',
            'min_confidence': 0.60,
            'cooldown_hours': 6,
            'stop_loss_pct': 0.02,      # 2%
            'take_profit_pct': 0.04,    # 4%
            'max_drawdown_limit': 0.15  # 15%
        },
        {
            'name': 'Balanced',
            'min_confidence': 0.55,
            'cooldown_hours': 3,
            'stop_loss_pct': 0.03,      # 3%
            'take_profit_pct': 0.05,    # 5%
            'max_drawdown_limit': 0.15
        },
        {
            'name': 'Aggressive',
            'min_confidence': 0.50,
            'cooldown_hours': 1,
            'stop_loss_pct': 0.04,      # 4%
            'take_profit_pct': 0.06,    # 6%
            'max_drawdown_limit': 0.15
        }
    ],

    # Risk management
    'risk_per_trade': 0.02,        # 2% du capital par trade
    'max_position_size_pct': 0.10, # Max 10% du capital en position
    'volatility_adjustment': True   # Ajustement selon volatilité
}

# === PARAMÈTRES DE PERFORMANCE ===
PERFORMANCE_CONFIG = {
    'metrics_to_track': [
        'accuracy', 'precision', 'recall', 'f1_score', 'roc_auc',
        'total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate',
        'profit_factor', 'avg_win', 'avg_loss', 'num_trades'
    ],

    'benchmark_periods': [1, 6, 12, 24],  # Périodes pour benchmark

    'reporting': {
        'save_models': True,
        'save_metrics': True,
        'save_backtests': True,
        'generate_reports': True,
        'plot_results': True
    }
}

# === PARAMÈTRES DE DONNÉES ===
DATA_CONFIG = {
    'min_samples_required': 1000,   # Minimum d'échantillons
    'max_nan_percentage': 0.05,     # Max 5% de NaN autorisés
    'interpolation_method': 'linear', # Méthode d'interpolation
    'scaling_method': 'standard',     # StandardScaler

    # Filtres de qualité
    'price_filters': {
        'min_price': 1000,           # Prix minimum (éviter données anciennes)
        'max_price': 200000,         # Prix maximum raisonnable
        'min_volume': 1000           # Volume minimum
    }
}

# === PARAMÈTRES DE LOGGING ===
LOGGING_CONFIG = {
    'log_level': 'INFO',
    'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_file': LOGS_DIR / 'deepbtc_improved.log',
    'max_log_size': 10*1024*1024,   # 10MB
    'backup_count': 5
}

# === CONSTANTES UTILITAIRES ===
CONSTANTS = {
    'hours_per_day': 24,
    'trading_days_per_year': 365,
    'benchmark_return_calculation': 'cumulative',

    # Seuils de performance
    'good_accuracy_threshold': 0.70,
    'good_sharpe_threshold': 1.5,
    'good_win_rate_threshold': 0.60,

    # Codes d'erreur
    'ERROR_DATA_NOT_FOUND': 1001,
    'ERROR_MODEL_TRAINING': 1002,
    'ERROR_BACKTEST_FAILED': 1003,
    'ERROR_INVALID_CONFIG': 1004
}

# === FONCTIONS UTILITAIRES ===
def get_model_config(model_name='xgboost_optimized'):
    """Récupère la configuration d'un modèle"""
    return MODEL_CONFIGS.get(model_name, MODEL_CONFIGS['xgboost_optimized'])

def get_trading_config(strategy_name='Balanced'):
    """Récupère la configuration de trading"""
    for config in TRADING_CONFIG['strategy_configs']:
        if config['name'] == strategy_name:
            return config
    return TRADING_CONFIG['strategy_configs'][1]  # Balanced par défaut

def get_target_threshold(horizon=1):
    """Récupère le seuil de target pour un horizon"""
    return FEATURE_CONFIG['target_thresholds'].get(horizon, 0.002)

def validate_config():
    """Valide la configuration"""
    errors = []

    # Vérifier les chemins
    required_paths = [FEATURES_FILE, RAW_OHLCV_FILE]
    for path in required_paths:
        if not path.exists():
            errors.append(f"Fichier requis manquant: {path}")

    # Vérifier les paramètres critiques
    if TRADING_CONFIG['initial_capital'] <= 0:
        errors.append("Capital initial doit être positif")

    if not 0 < TRADING_CONFIG['transaction_fee'] < 1:
        errors.append("Frais de transaction invalides")

    if errors:
        print("❌ Erreurs de configuration:")
        for error in errors:
            print(f"   - {error}")
        return False

    print("✅ Configuration validée")
    return True

# Validation au chargement
if __name__ == "__main__":
    print("🔧 Validation de la configuration...")
    validate_config()