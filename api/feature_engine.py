# api/feature_engine.py

import pandas as pd
import pandas_ta as ta
import os
import numpy as np
from datetime import datetime

# --- Configuration ---
RAW_DATA_PATHS = {
    'ohlcv': 'data/raw/binance_btcusdt_1h.csv',
    'blockchain': 'data/raw/blockchain_metrics_daily.csv',
    'sentiment': 'data/raw/sentiment_metrics.csv',
    'macro': 'data/raw/macro_indicators.csv'
}

FEATURE_DATA_PATH = 'data/features/btc_features_complete.csv'
OUTPUT_DIR = 'data/features'

class EnhancedFeatureEngine:
    """
    Advanced feature engineering combining:
    1. OHLCV Technical Indicators
    2. Blockchain/On-chain Metrics
    3. Sentiment Indicators
    4. Macroeconomic Factors
    """
    
    def __init__(self):
        self.data = {}
    
    def load_all_data(self):
        """Loads all available data sources."""
        
        print("Loading all data sources...")
        print("="*50)
        
        # Load OHLCV (required)
        if not os.path.exists(RAW_DATA_PATHS['ohlcv']):
            print(f"❌ ERROR: OHLCV data not found at {RAW_DATA_PATHS['ohlcv']}")
            print("Please run: python api/fetch_historical.py")
            return False
        
        self.data['ohlcv'] = pd.read_csv(
            RAW_DATA_PATHS['ohlcv'], 
            index_col='Datetime', 
            parse_dates=True
        )
        print(f"✅ OHLCV loaded: {self.data['ohlcv'].shape}")
        
        # Load Blockchain metrics (optional)
        if os.path.exists(RAW_DATA_PATHS['blockchain']):
            self.data['blockchain'] = pd.read_csv(
                RAW_DATA_PATHS['blockchain'], 
                index_col='date', 
                parse_dates=True
            )
            print(f"✅ Blockchain metrics loaded: {self.data['blockchain'].shape}")
        else:
            print(f"⚠️  Blockchain data not found. Run: python api/fetch_blockchain_metrics.py")
        
        # Load Sentiment data (optional)
        if os.path.exists(RAW_DATA_PATHS['sentiment']):
            self.data['sentiment'] = pd.read_csv(
                RAW_DATA_PATHS['sentiment'], 
                index_col='date', 
                parse_dates=True
            )
            print(f"✅ Sentiment data loaded: {self.data['sentiment'].shape}")
        else:
            print(f"⚠️  Sentiment data not found. Run: python api/fetch_sentiment.py")
        
        # Load Macro indicators (optional)
        if os.path.exists(RAW_DATA_PATHS['macro']):
            self.data['macro'] = pd.read_csv(
                RAW_DATA_PATHS['macro'], 
                index_col=0, 
                parse_dates=True
            )
            print(f"✅ Macro data loaded: {self.data['macro'].shape}")
        else:
            print(f"⚠️  Macro data not found. Run: python api/fetch_macro_data.py")
        
        print("="*50)
        return True
    
    def generate_price_features(self, df):
        """Generates technical indicators from OHLCV data."""
        
        print("\n[1/4] Generating price-based features...")
        
        # Returns
        df['returns'] = df['Close'].pct_change()
        df['log_returns'] = np.log(df['Close'] / df['Close'].shift(1))
        df['future_return_1h'] = df['Close'].shift(-1) / df['Close'] - 1
        df['future_return_6h'] = df['Close'].shift(-6) / df['Close'] - 1
        df['future_return_24h'] = df['Close'].shift(-24) / df['Close'] - 1
        
        # Price momentum
        df['price_momentum_24h'] = df['Close'] / df['Close'].shift(24) - 1
        df['price_momentum_7d'] = df['Close'] / df['Close'].shift(24*7) - 1
        
        # Volatility
        df['volatility_24h'] = df['returns'].rolling(24).std()
        df['volatility_7d'] = df['returns'].rolling(24*7).std()
        
        # Volume features
        df['volume_ma_24h'] = df['Volume'].rolling(24).mean()
        df['volume_ratio'] = df['Volume'] / df['volume_ma_24h']
        df['volume_momentum'] = df['Volume'] / df['Volume'].shift(24) - 1
        
        # Technical Indicators
        df.ta.vwap(append=True)
        df.ta.sma(length=20, append=True)
        df.ta.sma(length=50, append=True)
        df.ta.sma(length=200, append=True)
        df.ta.ema(length=12, append=True)
        df.ta.ema(length=26, append=True)
        df.ta.macd(append=True)
        df.ta.rsi(length=14, append=True)
        df.ta.rsi(length=21, append=True)
        df.ta.stoch(append=True)
        df.ta.atr(length=14, append=True)
        df.ta.bbands(length=20, append=True)
        df.ta.adx(length=14, append=True)
        df.ta.cci(length=20, append=True)
        df.ta.willr(length=14, append=True)
        df.ta.mfi(length=14, append=True)
        df.ta.obv(append=True)
        
        # Price position relative to MAs
        df['price_to_sma20'] = df['Close'] / df['SMA_20'] - 1
        df['price_to_sma50'] = df['Close'] / df['SMA_50'] - 1
        df['price_to_sma200'] = df['Close'] / df['SMA_200'] - 1
        
        # Golden/Death Cross proximity
        df['sma50_to_sma200'] = df['SMA_50'] / df['SMA_200'] - 1
        
        print(f"   Generated {df.shape[1] - self.data['ohlcv'].shape[1]} price features")
        return df
    
    def merge_blockchain_features(self, df):
        """Merges blockchain metrics with hourly OHLCV data."""
        
        if 'blockchain' not in self.data:
            print("\n[2/4] Skipping blockchain features (data not available)")
            return df
        
        print("\n[2/4] Merging blockchain features...")
        
        # Resample blockchain data to hourly (forward fill)
        blockchain_hourly = self.data['blockchain'].resample('1H').ffill()
        
        # Merge with OHLCV
        df = df.join(blockchain_hourly, how='left')
        
        # Calculate blockchain-derived features
        if 'hash_rate_th_s' in df.columns:
            df['hash_rate_change_7d'] = df['hash_rate_th_s'] / df['hash_rate_th_s'].shift(24*7) - 1
        
        if 'difficulty' in df.columns:
            df['difficulty_change_7d'] = df['difficulty'] / df['difficulty'].shift(24*7) - 1
        
        if 'tx_count_daily' in df.columns:
            df['tx_count_change_7d'] = df['tx_count_daily'] / df['tx_count_daily'].shift(24*7) - 1
        
        # Network value to transactions ratio (approximation)
        if 'market_price_usd' in df.columns and 'tx_count_daily' in df.columns:
            df['nvt_ratio'] = (df['market_price_usd'] * df['total_btc_supply']) / (df['tx_count_daily'] * 1e6)
        
        # Miner revenue
        if 'tx_fees_btc' in df.columns and 'market_price_usd' in df.columns:
            df['miner_revenue_usd'] = df['tx_fees_btc'] * df['market_price_usd']
        
        print(f"   Merged blockchain features")
        return df
    
    def merge_sentiment_features(self, df):
        """Merges sentiment indicators with hourly OHLCV data."""
        
        if 'sentiment' not in self.data:
            print("\n[3/4] Skipping sentiment features (data not available)")
            return df
        
        print("\n[3/4] Merging sentiment features...")
        
        # Resample sentiment data to hourly (forward fill)
        sentiment_hourly = self.data['sentiment'].resample('1H').ffill()
        
        # Merge with OHLCV
        df = df.join(sentiment_hourly, how='left')
        
        # Calculate sentiment-derived features
        if 'fear_greed_value' in df.columns:
            df['fear_greed_change_7d'] = df['fear_greed_value'] - df['fear_greed_value'].shift(24*7)
            df['fear_greed_ma_7d'] = df['fear_greed_value'].rolling(24*7).mean()
            
            # Extreme sentiment flags
            df['extreme_fear'] = (df['fear_greed_value'] < 25).astype(int)
            df['extreme_greed'] = (df['fear_greed_value'] > 75).astype(int)
        
        print(f"   Merged sentiment features")
        return df
    
    def merge_macro_features(self, df):
        """Merges macroeconomic indicators with hourly OHLCV data."""
        
        if 'macro' not in self.data:
            print("\n[4/4] Skipping macro features (data not available)")
            return df
        
        print("\n[4/4] Merging macro features...")
        
        # Resample macro data to hourly (forward fill)
        macro_hourly = self.data['macro'].resample('1H').ffill()
        
        # Merge with OHLCV
        df = df.join(macro_hourly, how='left')
        
        # Calculate macro-derived features
        if 'SP500' in df.columns:
            df['sp500_returns'] = df['SP500'].pct_change()
            df['btc_sp500_correlation'] = df['returns'].rolling(24*30).corr(df['sp500_returns'])
        
        if 'VIX' in df.columns:
            df['vix_change'] = df['VIX'].pct_change()
        
        if 'DXY' in df.columns:
            df['dxy_change'] = df['DXY'].pct_change()
            df['btc_dxy_correlation'] = df['returns'].rolling(24*30).corr(df['dxy_change'])
        
        if 'GOLD' in df.columns:
            df['gold_returns'] = df['GOLD'].pct_change()
            df['btc_gold_correlation'] = df['returns'].rolling(24*30).corr(df['gold_returns'])
        
        print(f"   Merged macro features")
        return df
    
    def create_target_labels(self, df):
        """Creates multiple target labels for different trading strategies."""
        
        print("\nCreating target labels...")
        
        # Binary classification: Up or Down in next hour
        df['target_direction_1h'] = (df['future_return_1h'] > 0).astype(int)
        
        # Multi-class classification: Strong down, down, neutral, up, strong up
        df['target_multiclass_1h'] = pd.cut(
            df['future_return_1h'],
            bins=[-np.inf, -0.01, -0.002, 0.002, 0.01, np.inf],
            labels=[0, 1, 2, 3, 4]
        )
        
        # Regression target: Actual return
        df['target_return_1h'] = df['future_return_1h']
        
        print("   Created classification and regression targets")
        return df
    
    def clean_and_finalize(self, df):
        """Final data cleaning and preparation."""
        
        print("\nFinalizing dataset...")
        
        # Remove infinite values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Drop rows with NaN in critical columns
        initial_rows = len(df)
        df.dropna(subset=['Close', 'Volume'], inplace=True)
        
        # Forward fill remaining NaNs (for macro/blockchain data)
        df.fillna(method='ffill', inplace=True)
        
        # Drop remaining NaNs
        df.dropna(inplace=True)
        
        final_rows = len(df)
        print(f"   Dropped {initial_rows - final_rows} rows with missing data")
        print(f"   Final dataset shape: {df.shape}")
        
        return df
    
    def generate_all_features(self):
        """Main pipeline to generate all features."""
        
        if not self.load_all_data():
            return None
        
        print("\n" + "="*50)
        print("FEATURE ENGINEERING PIPELINE")
        print("="*50)
        
        # Start with OHLCV
        df = self.data['ohlcv'].copy()
        
        # Generate features from each source
        df = self.generate_price_features(df)
        df = self.merge_blockchain_features(df)
        df = self.merge_sentiment_features(df)
        df = self.merge_macro_features(df)
        df = self.create_target_labels(df)
        df = self.clean_and_finalize(df)
        
        return df
    
    def save_features(self, df):
        """Saves the complete feature set."""
        
        if df is None:
            return
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df.to_csv(FEATURE_DATA_PATH)
        
        print("\n" + "="*50)
        print("✅ FEATURE ENGINEERING COMPLETE")
        print("="*50)
        print(f"Saved to: {FEATURE_DATA_PATH}")
        print(f"Total features: {df.shape[1]}")
        print(f"Total samples: {df.shape[0]}")
        print(f"Date range: {df.index.min()} to {df.index.max()}")
        print(f"\nFeature categories:")
        print(f"  - Technical indicators: ~40 features")
        print(f"  - Blockchain metrics: ~15 features")
        print(f"  - Sentiment indicators: ~5 features")
        print(f"  - Macro indicators: ~10 features")
        print(f"  - Target variables: 3 labels")

def main():
    """Main execution."""
    engine = EnhancedFeatureEngine()
    features_df = engine.generate_all_features()
    
    if features_df is not None:
        engine.save_features(features_df)
        
        # Display sample
        print("\nSample of features (last 5 rows):")
        print(features_df.tail())
        
        print("\nFeature names:")
        print(features_df.columns.tolist())

if __name__ == '__main__':
    main()