# api/fetch_blockchain_metrics.py - ENHANCED VERSION

import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# --- Configuration ---
OUTPUT_DIR = 'data/raw'
OUTPUT_FILE = 'blockchain_metrics_daily.csv'

class BlockchainMetricsFetcher:
    """Fetches free on-chain Bitcoin metrics from multiple sources."""
    
    def __init__(self):
        self.blockchain_info_base = "https://api.blockchain.info"
        self.mempool_space_base = "https://mempool.space/api"
        
    def fetch_blockchain_info_stats(self, days=1825):  # 5 years to match OHLCV
        """
        Fetches Bitcoin network statistics from Blockchain.info
        Completely free, no API key required.
        Extended to 5 years (1825 days) to match OHLCV data.
        """
        print(f"Fetching blockchain.info statistics ({days} days)...")
        
        try:
            # Market price (USD)
            print("  → Fetching market price...")
            market_price_url = f"{self.blockchain_info_base}/charts/market-price?timespan={days}days&format=json"
            response = requests.get(market_price_url, timeout=15)
            market_price_data = response.json()
            time.sleep(1)  # Rate limiting
            
            # Hash rate
            print("  → Fetching hash rate...")
            hash_rate_url = f"{self.blockchain_info_base}/charts/hash-rate?timespan={days}days&format=json"
            response = requests.get(hash_rate_url, timeout=15)
            hash_rate_data = response.json()
            time.sleep(1)
            
            # Difficulty
            print("  → Fetching difficulty...")
            difficulty_url = f"{self.blockchain_info_base}/charts/difficulty?timespan={days}days&format=json"
            response = requests.get(difficulty_url, timeout=15)
            difficulty_data = response.json()
            time.sleep(1)
            
            # Transaction count
            print("  → Fetching transaction count...")
            tx_count_url = f"{self.blockchain_info_base}/charts/n-transactions?timespan={days}days&format=json"
            response = requests.get(tx_count_url, timeout=15)
            tx_count_data = response.json()
            time.sleep(1)
            
            # Total transaction fees (BTC)
            print("  → Fetching transaction fees...")
            tx_fees_url = f"{self.blockchain_info_base}/charts/transaction-fees?timespan={days}days&format=json"
            response = requests.get(tx_fees_url, timeout=15)
            tx_fees_data = response.json()
            time.sleep(1)
            
            # Average block size
            print("  → Fetching block size...")
            block_size_url = f"{self.blockchain_info_base}/charts/avg-block-size?timespan={days}days&format=json"
            response = requests.get(block_size_url, timeout=15)
            block_size_data = response.json()
            time.sleep(1)
            
            # Mempool size
            print("  → Fetching mempool size...")
            mempool_size_url = f"{self.blockchain_info_base}/charts/mempool-size?timespan={days}days&format=json"
            response = requests.get(mempool_size_url, timeout=15)
            mempool_size_data = response.json()
            time.sleep(1)
            
            # Total bitcoins in circulation
            print("  → Fetching total BTC supply...")
            total_btc_url = f"{self.blockchain_info_base}/charts/total-bitcoins?timespan={days}days&format=json"
            response = requests.get(total_btc_url, timeout=15)
            total_btc_data = response.json()
            
            print(f"✅ Successfully fetched blockchain.info metrics")
            
            return {
                'market_price': market_price_data['values'],
                'hash_rate': hash_rate_data['values'],
                'difficulty': difficulty_data['values'],
                'tx_count': tx_count_data['values'],
                'tx_fees': tx_fees_data['values'],
                'block_size': block_size_data['values'],
                'mempool_size': mempool_size_data['values'],
                'total_btc': total_btc_data['values']
            }
            
        except Exception as e:
            print(f"❌ Error fetching blockchain.info data: {e}")
            return None
    
    def fetch_mempool_space_stats(self):
        """
        Fetches current mempool statistics from mempool.space
        Free API, no authentication required.
        """
        print("Fetching mempool.space real-time statistics...")
        
        try:
            # Current mempool stats
            mempool_url = f"{self.mempool_space_base}/mempool"
            response = requests.get(mempool_url, timeout=10)
            mempool_data = response.json()
            
            # Fee estimates
            fees_url = f"{self.mempool_space_base}/v1/fees/recommended"
            response = requests.get(fees_url, timeout=10)
            fees_data = response.json()
            
            # Difficulty adjustment
            difficulty_url = f"{self.mempool_space_base}/v1/difficulty-adjustment"
            response = requests.get(difficulty_url, timeout=10)
            difficulty_data = response.json()
            
            print(f"✅ Successfully fetched mempool.space metrics")
            
            return {
                'mempool_count': mempool_data.get('count'),
                'mempool_vsize': mempool_data.get('vsize'),
                'mempool_total_fee': mempool_data.get('total_fee'),
                'fee_fastest': fees_data.get('fastestFee'),
                'fee_halfhour': fees_data.get('halfHourFee'),
                'fee_hour': fees_data.get('hourFee'),
                'difficulty_change': difficulty_data.get('difficultyChange'),
                'estimated_retarget': difficulty_data.get('estimatedRetargetDate')
            }
            
        except Exception as e:
            print(f"❌ Error fetching mempool.space data: {e}")
            return None
    
    def combine_to_dataframe(self, blockchain_data):
        """Converts the fetched data into a unified pandas DataFrame."""
        
        if not blockchain_data:
            return None
        
        # Start with market price as base
        df = pd.DataFrame(blockchain_data['market_price'])
        df.columns = ['timestamp', 'market_price_usd']
        df['date'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('date', inplace=True)
        df.drop('timestamp', axis=1, inplace=True)
        
        # Add other metrics
        metrics = {
            'hash_rate_th_s': 'hash_rate',
            'difficulty': 'difficulty',
            'tx_count_daily': 'tx_count',
            'tx_fees_btc': 'tx_fees',
            'avg_block_size_mb': 'block_size',
            'mempool_size_bytes': 'mempool_size',
            'total_btc_supply': 'total_btc'
        }
        
        for new_col, data_key in metrics.items():
            temp_df = pd.DataFrame(blockchain_data[data_key])
            temp_df.columns = ['timestamp', new_col]
            temp_df['date'] = pd.to_datetime(temp_df['timestamp'], unit='s')
            temp_df.set_index('date', inplace=True)
            temp_df.drop('timestamp', axis=1, inplace=True)
            
            # Merge on date index
            df = df.join(temp_df, how='outer')
        
        # Sort by date and forward fill missing values
        df.sort_index(inplace=True)
        df.ffill(inplace=True)  # FIXED: Use ffill() instead of fillna(method='ffill')
        
        return df
    
    def save_data(self, df):
        """Saves the DataFrame to CSV."""
        if df is None:
            return
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        full_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        df.to_csv(full_path)
        print(f"\n✅ Blockchain metrics saved to {full_path}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df.index.min()} to {df.index.max()}")
        print(f"Duration: {(df.index.max() - df.index.min()).days} days")
        print(f"\nColumns: {list(df.columns)}")

def main():
    """Main execution function."""
    fetcher = BlockchainMetricsFetcher()
    
    # Fetch blockchain metrics (last 5 years to match OHLCV)
    blockchain_data = fetcher.fetch_blockchain_info_stats(days=1825)
    
    if blockchain_data:
        df = fetcher.combine_to_dataframe(blockchain_data)
        fetcher.save_data(df)
        
        # Also fetch and display current mempool stats
        print("\n" + "="*50)
        print("CURRENT MEMPOOL STATISTICS")
        print("="*50)
        current_stats = fetcher.fetch_mempool_space_stats()
        if current_stats:
            for key, value in current_stats.items():
                print(f"{key}: {value}")
    
    print("\n✅ Blockchain metrics fetch complete!")

if __name__ == '__main__':
    main()