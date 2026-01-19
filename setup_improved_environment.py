# setup_improved_environment.py - Installation automatique

import subprocess
import sys
import os
from pathlib import Path

def install_dependencies():
    """Installe les dépendances nécessaires"""
    print("🔧 Installation des dépendances améliorées...")

    dependencies = [
        'pandas>=2.0.0',
        'numpy>=1.24.0',
        'xgboost>=1.7.0',
        'scikit-learn>=1.3.0',
        'matplotlib>=3.7.0',
        'seaborn>=0.12.0',
        'joblib>=1.3.0'
    ]

    for dep in dependencies:
        try:
            print(f"Installation de {dep}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', dep])
            print(f"✅ {dep} installé")
        except subprocess.CalledProcessError as e:
            print(f"❌ Erreur installation {dep}: {e}")
            return False

    print("✅ Toutes les dépendances installées!")
    return True

def verify_installation():
    """Vérifie que tout est correctement installé"""
    print("\n🔍 Vérification de l'installation...")

    try:
        import pandas as pd
        import numpy as np
        import xgboost as xgb
        import sklearn
        import matplotlib
        import seaborn
        import joblib

        print("✅ pandas:", pd.__version__)
        print("✅ numpy:", np.__version__)
        print("✅ xgboost:", xgb.__version__)
        print("✅ scikit-learn:", sklearn.__version__)
        print("✅ matplotlib:", matplotlib.__version__)
        print("✅ seaborn:", seaborn.__version__)
        print("✅ joblib:", joblib.__version__)

        return True

    except ImportError as e:
        print(f"❌ Module manquant: {e}")
        return False

def create_directories():
    """Crée les répertoires nécessaires"""
    dirs = ['models', 'data/features', 'data/raw', 'logs', 'reports']

    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Répertoire créé: {dir_path}")

def main():
    print("="*60)
    print("🚀 SETUP - DEEPBTC VERSION AMÉLIORÉE")
    print("="*60)

    # Créer les répertoires
    print("\n[1/3] Création des répertoires...")
    create_directories()

    # Installer les dépendances
    print("\n[2/3] Installation des dépendances...")
    if not install_dependencies():
        print("❌ Échec de l'installation des dépendances")
        return False

    # Vérifier l'installation
    print("\n[3/3] Vérification de l'installation...")
    if not verify_installation():
        print("❌ Vérification échouée")
        return False

    print("\n" + "="*60)
    print("✅ SETUP TERMINÉ AVEC SUCCÈS!")
    print("="*60)
    print("\nProchaines étapes:")
    print("1. Vérifiez que vos données existent:")
    print("   data/features/btc_features_complete.csv")
    print("   data/raw/binance_BTCUSDT_1h.csv")
    print("")
    print("2. Exécutez le pipeline amélioré:")
    print("   python main_improved.py --horizon 1")
    print("")
    print("3. Comparez avec l'ancienne version:")
    print("   python main_improved.py --compare")
    print("")
    print("Améliorations implémentées:")
    print("• Horizon de prédiction réduit (1h/6h)")
    print("• Validation temporelle (évite overfitting)")
    print("• Features optimisées")
    print("• Risk management avancé")
    print("• Code modulaire et maintenable")

    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)