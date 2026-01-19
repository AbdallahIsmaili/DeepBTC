# test_improved_system.py - Test rapide du système amélioré

import sys
import os
from pathlib import Path

def test_data_loading():
    """Test le chargement des données"""
    print("🔍 Test chargement données...")

    try:
        import pandas as pd
        df = pd.read_csv('data/features/btc_features_complete.csv', nrows=100)
        print(f"✅ Données chargées: {df.shape}")
        return True
    except Exception as e:
        print(f"❌ Erreur données: {e}")
        return False

def test_model_training():
    """Test rapide de l'entraînement"""
    print("🔍 Test entraînement modèle...")

    try:
        from improved_btc_predictor import ImprovedBTCPredictor

        # Test rapide avec petit échantillon
        predictor = ImprovedBTCPredictor(prediction_horizon=1)

        # Charger données
        df = predictor.load_and_prepare_data()

        # Features limitées pour test rapide
        df, feature_cols = predictor.create_optimized_features(df)

        # Petit entraînement (1 fold seulement pour test)
        from sklearn.model_selection import train_test_split
        X = df[feature_cols][:1000]  # Petit échantillon
        y = df['target'][:1000]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Entraînement simple
        import xgboost as xgb
        model = xgb.XGBClassifier(max_depth=3, n_estimators=50, random_state=42)
        model.fit(X_train, y_train)

        # Prédiction
        pred = model.predict(X_test)
        accuracy = (pred == y_test).mean()

        print(f"✅ Accuracy test: {accuracy:.1%}")
        return True

    except Exception as e:
        print(f"❌ Erreur entraînement: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_trading_strategy():
    """Test rapide de la stratégie de trading"""
    print("🔍 Test stratégie de trading...")

    try:
        from improved_trading_strategy import ImprovedTradingStrategy
        import numpy as np

        # Données de test simulées
        np.random.seed(42)
        n_samples = 100
        prices = np.random.normal(50000, 1000, n_samples)
        predictions = np.random.choice([0, 1], n_samples, p=[0.6, 0.4])
        probabilities = np.random.uniform(0.4, 0.8, n_samples)

        # Créer DataFrame simulé
        import pandas as pd
        dates = pd.date_range('2024-01-01', periods=n_samples, freq='H')
        df = pd.DataFrame({
            'Close': prices,
            'returns': np.random.normal(0, 0.02, n_samples)
        }, index=dates)

        strategy = ImprovedTradingStrategy(initial_capital=10000)
        results = strategy.advanced_backtest(df, predictions, probabilities, horizon=1)

        print(f"   Return: {results['total_return']:.2f}%")
        print(f"   Sharpe: {results['sharpe_ratio']:.2f}")
        return True

    except Exception as e:
        print(f"❌ Erreur stratégie: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("="*60)
    print("🧪 TEST DU SYSTÈME AMÉLIORÉ")
    print("="*60)

    tests = [
        ("Chargement des données", test_data_loading),
        ("Entraînement du modèle", test_model_training),
        ("Stratégie de trading", test_trading_strategy)
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n[Test] {test_name}")
        success = test_func()
        results.append((test_name, success))

        if success:
            print(f"✅ {test_name}: RÉUSSI")
        else:
            print(f"❌ {test_name}: ÉCHEC")

    # Résumé
    print("\n" + "="*60)
    print("📊 RÉSULTATS DES TESTS")
    print("="*60)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:<25} {status}")

    print("-"*60)
    print(f"Résultat global: {passed}/{total} tests réussis")

    if passed == total:
        print("\n🎉 TOUS LES TESTS RÉUSSIS!")
        print("Votre système amélioré est prêt à être utilisé.")
        print("\nCommandes suivantes:")
        print("  python main_improved.py --horizon 1    # Pipeline complet")
        print("  python main_improved.py --compare      # Comparaison")
    else:
        print(f"\n⚠️ {total - passed} test(s) échoué(s).")
        print("Vérifiez les erreurs ci-dessus et corrigez-les.")

    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)