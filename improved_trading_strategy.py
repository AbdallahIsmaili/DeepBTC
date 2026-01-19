# improved_trading_strategy.py - Stratégie de trading optimisée

import pandas as pd
import numpy as np
from datetime import datetime
import json
from pathlib import Path
from improved_btc_predictor import ImprovedBTCPredictor

class ImprovedTradingStrategy:
    """
    Stratégie de trading améliorée avec :
    - Seuils de confiance dynamiques
    - Risk management avancé
    - Position sizing adaptatif
    - Stop-loss et take-profit optimisés
    """

    def __init__(self, initial_capital=10000, transaction_fee=0.001):
        self.initial_capital = initial_capital
        self.transaction_fee = transaction_fee
        self.models_dir = Path('models')

    def load_model_and_scaler(self, horizon=1):
        """Charge le modèle et scaler entraînés"""
        try:
            # Trouver les fichiers les plus récents
            model_files = list(self.models_dir.glob(f'improved_xgb_{horizon}h_*.json'))
            scaler_files = list(self.models_dir.glob(f'scaler_{horizon}h_*.pkl'))
            features_files = list(self.models_dir.glob(f'features_{horizon}h_*.txt'))

            if not model_files or not scaler_files or not features_files:
                raise FileNotFoundError(f"Modèle pour horizon {horizon}h non trouvé")

            # Charger le modèle
            import xgboost as xgb
            import joblib

            model_path = sorted(model_files)[-1]  # Plus récent
            scaler_path = sorted(scaler_files)[-1]
            features_path = sorted(features_files)[-1]

            model = xgb.XGBClassifier()
            model.load_model(str(model_path))
            scaler = joblib.load(scaler_path)

            with open(features_path, 'r') as f:
                features = [line.strip() for line in f.readlines()]

            print(f"✅ Modèle chargé: {model_path}")
            return model, scaler, features

        except Exception as e:
            print(f"❌ Erreur chargement modèle: {e}")
            return None, None, None

    def dynamic_confidence_thresholds(self, probabilities, market_volatility):
        """
        Seuils de confiance dynamiques basés sur la volatilité
        Plus le marché est volatil, plus le seuil est élevé
        """
        base_threshold = 0.55  # Seuil de base

        # Ajustement selon volatilité (concept simplifié)
        if market_volatility > 0.03:  # Volatilité élevée
            threshold_buy = base_threshold + 0.05
            threshold_sell = base_threshold + 0.05
        elif market_volatility > 0.02:  # Volatilité moyenne
            threshold_buy = base_threshold + 0.02
            threshold_sell = base_threshold + 0.02
        else:  # Volatilité faible
            threshold_buy = base_threshold - 0.02
            threshold_sell = base_threshold - 0.02

        return threshold_buy, threshold_sell

    def calculate_position_size(self, capital, risk_per_trade=0.02, volatility_adjustment=True):
        """
        Calcul adaptatif de la taille de position
        - Risque fixe par trade (2% du capital)
        - Ajustement selon volatilité
        """
        base_risk = capital * risk_per_trade

        if volatility_adjustment:
            # Réduction de la taille en cas de forte volatilité
            current_volatility = self.get_current_volatility()
            if current_volatility > 0.03:
                base_risk *= 0.5  # Réduire de 50%
            elif current_volatility > 0.02:
                base_risk *= 0.75  # Réduire de 25%

        return base_risk

    def get_current_volatility(self):
        """Estimation simplifiée de la volatilité actuelle"""
        # Dans un vrai système, ceci viendrait des données en temps réel
        return 0.025  # Valeur par défaut réaliste

    def advanced_backtest(self, df, predictions, probabilities, horizon=1,
                         min_confidence=0.55, max_drawdown_limit=0.15,
                         stop_loss_pct=0.03, take_profit_pct=0.05):
        """
        Backtest avancé avec risk management
        """
        capital = self.initial_capital
        btc_held = 0
        trades = []
        portfolio_values = [capital]
        peak_capital = capital
        max_drawdown = 0

        entry_price = None
        position_size = 0

        for i in range(len(predictions)):
            current_price = df['Close'].iloc[i]
            pred = predictions[i]
            prob = probabilities[i]

            # Calcul de la volatilité (rolling 24h)
            volatility = df['returns'].rolling(24).std().iloc[i] if i >= 24 else 0.02

            # Seuils dynamiques
            threshold_buy, threshold_sell = self.dynamic_confidence_thresholds(prob, volatility)

            # Signal d'achat
            if pred == 1 and prob > threshold_buy and btc_held == 0:
                # Calculer la taille de position
                risk_amount = self.calculate_position_size(capital)
                position_size = risk_amount / current_price

                # Frais de transaction
                transaction_cost = position_size * current_price * self.transaction_fee
                position_size -= (transaction_cost / current_price)

                if position_size > 0:
                    btc_held = position_size
                    entry_price = current_price
                    capital -= (position_size * current_price + transaction_cost)

                    trades.append({
                        'type': 'BUY',
                        'price': current_price,
                        'size': position_size,
                        'capital_before': capital + (position_size * current_price),
                        'timestamp': df.index[i],
                        'confidence': prob
                    })

            # Signal de vente ou stop-loss/take-profit
            elif btc_held > 0:
                # Calcul des P&L
                current_pnl_pct = (current_price - entry_price) / entry_price

                # Conditions de sortie
                sell_signal = False
                exit_reason = ""

                if pred == 0 and prob > threshold_sell:
                    sell_signal = True
                    exit_reason = "PREDICTION_SELL"
                elif current_pnl_pct <= -stop_loss_pct:
                    sell_signal = True
                    exit_reason = "STOP_LOSS"
                elif current_pnl_pct >= take_profit_pct:
                    sell_signal = True
                    exit_reason = "TAKE_PROFIT"

                if sell_signal:
                    # Calculer la valeur de vente
                    sell_value = btc_held * current_price
                    transaction_cost = sell_value * self.transaction_fee
                    net_sell_value = sell_value - transaction_cost

                    capital += net_sell_value
                    btc_held = 0

                    trades.append({
                        'type': 'SELL',
                        'price': current_price,
                        'size': position_size,
                        'capital_after': capital,
                        'pnl': net_sell_value - (position_size * entry_price),
                        'pnl_pct': current_pnl_pct,
                        'timestamp': df.index[i],
                        'confidence': prob,
                        'exit_reason': exit_reason
                    })

                    entry_price = None
                    position_size = 0

            # Mise à jour de la valeur du portfolio
            portfolio_value = capital + (btc_held * current_price if btc_held > 0 else 0)
            portfolio_values.append(portfolio_value)

            # Max drawdown
            if portfolio_value > peak_capital:
                peak_capital = portfolio_value
            drawdown = (peak_capital - portfolio_value) / peak_capital
            max_drawdown = max(max_drawdown, drawdown)

            # Stop si max drawdown dépassé
            if max_drawdown > max_drawdown_limit:
                print(f"⚠️ Max drawdown limit ({max_drawdown_limit:.1%}) atteint, arrêt du backtest")
                break

        # Résultats finaux
        final_value = capital + (btc_held * current_price if btc_held > 0 else 0)
        total_return = (final_value - self.initial_capital) / self.initial_capital * 100

        # Buy & Hold benchmark
        initial_price = df['Close'].iloc[0]
        final_price = df['Close'].iloc[-1]
        buy_hold_return = (final_price - initial_price) / initial_price * 100

        # Métriques de performance
        returns_series = pd.Series(portfolio_values).pct_change().dropna()
        sharpe_ratio = (returns_series.mean() / returns_series.std()) * np.sqrt(365*24) if returns_series.std() > 0 else 0

        # Statistiques des trades
        winning_trades = [t for t in trades if t.get('pnl', 0) > 0]
        losing_trades = [t for t in trades if t.get('pnl', 0) < 0]

        win_rate = len(winning_trades) / len(trades) if trades else 0
        avg_win = np.mean([t['pnl'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t['pnl'] for t in losing_trades]) if losing_trades else 0
        profit_factor = abs(sum([t['pnl'] for t in winning_trades]) / sum([t['pnl'] for t in losing_trades])) if losing_trades else float('inf')

        results = {
            'initial_capital': self.initial_capital,
            'final_value': final_value,
            'total_return': total_return,
            'buy_hold_return': buy_hold_return,
            'outperformance': total_return - buy_hold_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,
            'num_trades': len(trades),
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'portfolio_values': portfolio_values,
            'trades': trades,
            'horizon': horizon,
            'min_confidence': min_confidence,
            'stop_loss_pct': stop_loss_pct,
            'take_profit_pct': take_profit_pct
        }

        return results

    def run_improved_backtest(self, horizon=1):
        """Exécuter le backtest amélioré"""
        print(f"📊 Backtest amélioré pour horizon {horizon}h")

        # Charger le modèle
        model, scaler, features = self.load_model_and_scaler(horizon)
        if model is None:
            return None

        # Charger les données
        df = pd.read_csv('data/features/btc_features_complete.csv', index_col='Datetime', parse_dates=True)

        # Recréer les features comme dans l'entraînement
        predictor = ImprovedBTCPredictor(prediction_horizon=horizon)
        df, features = predictor.create_optimized_features(df)

        # Préparer les features
        X = df[features]
        X_scaled = scaler.transform(X.fillna(0))  # Gestion simple des NaN

        # Prédictions
        predictions = model.predict(X_scaled)
        probabilities = model.predict_proba(X_scaled)[:, 1]

        # Backtest avec différentes configurations
        configs = [
            {'name': 'Conservative', 'min_confidence': 0.60, 'stop_loss': 0.02, 'take_profit': 0.04},
            {'name': 'Balanced', 'min_confidence': 0.55, 'stop_loss': 0.03, 'take_profit': 0.05},
            {'name': 'Aggressive', 'min_confidence': 0.50, 'stop_loss': 0.04, 'take_profit': 0.06}
        ]

        results = []

        for config in configs:
            print(f"   Test configuration: {config['name']}")

            bt_result = self.advanced_backtest(
                df, predictions, probabilities, horizon,
                min_confidence=config['min_confidence'],
                stop_loss_pct=config['stop_loss'],
                take_profit_pct=config['take_profit']
            )

            results.append((config, bt_result))

        # Sauvegarder les résultats
        self.save_backtest_results(results, horizon)

        # Afficher les résultats
        self.display_results(results)

        return results

    def save_backtest_results(self, results, horizon):
        """Sauvegarder les résultats du backtest"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        results_dict = {
            'timestamp': timestamp,
            'horizon': horizon,
            'configurations': []
        }

        for config, result in results:
            results_dict['configurations'].append({
                'config': config,
                'results': result
            })

        results_path = self.models_dir / f'improved_backtest_{horizon}h_{timestamp}.json'
        with open(results_path, 'w') as f:
            json.dump(results_dict, f, indent=2, default=str)

        print(f"💾 Résultats sauvegardés: {results_path}")

    def display_results(self, results):
        """Afficher les résultats formatés"""
        print("\n" + "="*80)
        print("📊 RÉSULTATS DU BACKTEST AMÉLIORÉ")
        print("="*80)
        print(f"{'Configuration':<15} {'Return':>10} {'Trades':>8} {'Win Rate':>10} {'Sharpe':>8} {'vs B&H':>10} {'Max DD':>10}")
        print("-"*80)

        for config, result in results:
            print(f"{config['name']:<15} {result['total_return']:>9.2f}% {result['num_trades']:>8} {result['win_rate']:>9.1%} {result['sharpe_ratio']:>8.2f} {result['outperformance']:>9.2f}% {result['max_drawdown']:>9.2f}%")

        print("-"*80)

        # Buy & Hold de référence
        buy_hold = results[0][1]['buy_hold_return']
        print(f"{'Buy & Hold':<15} {buy_hold:>9.2f}%")

        print("="*80)

        # Recommandation
        best_config = max(results, key=lambda x: x[1]['sharpe_ratio'])
        print(f"🏆 MEILLEURE CONFIGURATION: {best_config[0]['name']}")
        print(f"   Return: {best_config[1]['total_return']:.2f}%")
        print(f"   Win Rate: {best_config[1]['win_rate']:.1f}%")
        print(f"   Sharpe: {best_config[1]['sharpe_ratio']:.2f}")
if __name__ == "__main__":
    strategy = ImprovedTradingStrategy()

    for horizon in [1, 6]:
        print(f"\n{'='*90}")
        print(f"BACKTEST HORIZON: {horizon}h")
        print('='*90)

        try:
            results = strategy.run_improved_backtest(horizon)
            if results:
                print(f"✅ Backtest {horizon}h: TERMINÉ")
            else:
                print(f"❌ Backtest {horizon}h: MODÈLE NON TROUVÉ")
        except Exception as e:
            print(f"❌ Erreur backtest {horizon}h: {e}")