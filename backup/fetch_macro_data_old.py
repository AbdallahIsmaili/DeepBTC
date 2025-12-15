# api/fetch_macro_data.py - FIXED API KEY HANDLING

import requests
import pandas as pd
import os
from datetime import datetime

# --- Configuration ---
OUTPUT_DIR = 'data/raw'
OUTPUT_FILE = 'macro_indicators.csv'

# FRED API - OPTIONAL (get free key at https://fred.stlouisfed.org/docs/api/api_key.html)
FRED_API_KEY = os.environ.get('FRED_API_KEY', None)  # Read from environment variable
FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations'

class MacroDataFetcher:
    """
    Fetches macroeconomic indicators from multiple FREE sources.
    
    Priority:
    1. Yahoo Finance (NO API KEY REQUIRED) - Primary source
    2. FRED (OPTIONAL - Better data, requires free API key)
    3. Alternative forex data (NO API KEY REQUIRED)
    """
    
    def __init__(self, api_key=None):
        self.api_key = api_key or FRED_API_KEY
        self.use_fred = bool(self.api_key and self.api_key != '99b1e3b0816299e5fe8ddac476442843')
        
        if not self.use_fred:
            print("\n" + "="*60)
            print("ℹ️  FRED API KEY NOT SET (Using Yahoo Finance only)")
            print("="*60)
            print("Yahoo Finance provides 5 essential indicators:")
            print("  ✅ S&P 500, NASDAQ, DXY, GOLD, VIX")
            print("\nTo get MORE indicators (optional):")
            print("  1. Get free key: https://fred.stlouisfed.org/docs/api/api_key.html")
            print("  2. Set environment variable:")
            print("     Linux/Mac: export FRED_API_KEY='99b1e3b0816299e5fe8ddac476442843'")
            print("     Windows: set FRED_API_KEY=99b1e3b0816299e5fe8ddac476442843")
            print("  3. Or edit this file and set FRED_API_KEY at top")
            print("="*60 + "\n")
    
    def fetch_fred_series(self, series_id, start_date='2020-01-01'):
        """Fetches data from FRED (only if API key is set)."""
        
        if not self.use_fred:
            return None
        
        try:
            params = {
                'series_id': series_id,
                'api_key': self.api_key,
                'file_type': 'json',
                'observation_start': start_date
            }
            
            response = requests.get(FRED_BASE_URL, params=params, timeout=10)
            data = response.json()
            
            if 'observations' not in data:
                print(f"⚠️  FRED Error for {series_id}: {data.get('error_message', 'Unknown error')}")
                return None
            
            df = pd.DataFrame(data['observations'])
            df = df[['date', 'value']]
            df.columns = ['date', series_id]
            df['date'] = pd.to_datetime(df['date'])
            df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
            df.set_index('date', inplace=True)
            
            print(f"✅ Fetched FRED {series_id}: {len(df)} observations")
            return df
            
        except Exception as e:
            print(f"⚠️  Error fetching FRED {series_id}: {e}")
            return None
    
    def fetch_yahoo_finance_indices(self):
        """
        Fetches major market indices from Yahoo Finance.
        FREE - NO API KEY REQUIRED!
        """
        try:
            import yfinance as yf
            
            print("="*60)
            print("Fetching stock market indices...")
            print("="*60)
            
            indices = {
                'SP500': '^GSPC',
                'NASDAQ': '^IXIC',
                'DXY': 'DX-Y.NYB',
                'GOLD': 'GC=F',
                'VIX': '^VIX'
            }
            
            dfs = []
            for name, ticker in indices.items():
                try:
                    data = yf.download(ticker, start='2020-01-01', progress=False)
                    if not data.empty:
                        df = data[['Close']].copy()
                        df.columns = [name]
                        dfs.append(df)
                        print(f"✅ Fetched {name}")
                    else:
                        print(f"⚠️  No data for {name}")
                except Exception as e:
                    print(f"⚠️  Could not fetch {name}: {e}")
            
            if dfs:
                combined = pd.concat(dfs, axis=1)
                print(f"\n✅ Yahoo Finance: {combined.shape[1]} indicators, {len(combined)} days")
                return combined
            
            return None
            
        except ImportError:
            print("\n❌ ERROR: yfinance not installed")
            print("Install it with: pip install yfinance")
            return None
        except Exception as e:
            print(f"❌ Error fetching Yahoo Finance data: {e}")
            return None
    
    def fetch_alternative_forex(self):
        """Fetches current forex rates (FREE, no API key)."""
        
        try:
            url = "https://api.exchangerate-api.com/v4/latest/USD"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if 'rates' in data:
                return {
                    'usd_to_eur': data['rates'].get('EUR', 0),
                    'usd_to_jpy': data['rates'].get('JPY', 0),
                    'usd_to_gbp': data['rates'].get('GBP', 0),
                    'usd_to_cny': data['rates'].get('CNY', 0),
                    'timestamp': datetime.now()
                }
            return None
            
        except Exception as e:
            print(f"⚠️  Error fetching forex: {e}")
            return None
    
    def fetch_all_macro_data(self):
        """Fetches all available macroeconomic data."""
        
        all_data = []
        
        # 1. FRED data (if API key available)
        if self.use_fred:
            print("\n" + "="*60)
            print("Fetching FRED economic data...")
            print("="*60)
            
            fred_series = {
                'fed_funds_rate': 'DFF',
                'treasury_10y': 'DGS10',
                'treasury_2y': 'DGS2',
                'oil_wti': 'DCOILWTICO',
                'gold_price': 'GOLDAMGBD228NLBM',
                'usd_eur': 'DEXUSEU'
            }
            
            for name, series_id in fred_series.items():
                df = self.fetch_fred_series(series_id)
                if df is not None:
                    df.columns = [name]
                    all_data.append(df)
        
        # 2. Yahoo Finance (ALWAYS fetch - no API key needed)
        yahoo_data = self.fetch_yahoo_finance_indices()
        if yahoo_data is not None:
            all_data.append(yahoo_data)
        
        if not all_data:
            print("\n❌ No macro data fetched")
            return None
        
        # Combine all sources
        combined_df = pd.concat(all_data, axis=1)
        combined_df.sort_index(inplace=True)
        
        # Forward fill missing values
        combined_df = combined_df.ffill()
        
        print(f"\n✅ Combined macro data: {combined_df.shape}")
        return combined_df
    
    def save_data(self, df):
        """Saves macro data to CSV."""
        
        if df is None:
            return
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        full_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        df.to_csv(full_path)
        print(f"\n✅ Macro indicators saved to {full_path}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df.index.min()} to {df.index.max()}")
        print(f"Columns: {list(df.columns)}")

def main():
    """Main execution."""
    
    fetcher = MacroDataFetcher()
    
    print("\nFetching macroeconomic indicators...")
    print("="*60)
    
    macro_df = fetcher.fetch_all_macro_data()
    
    if macro_df is not None:
        fetcher.save_data(macro_df)
        
        # Display latest values
        print("\n" + "="*60)
        print("LATEST MACRO INDICATORS")
        print("="*60)
        latest = macro_df.tail(1).T
        latest.columns = ['Value']
        print(latest.to_string())
    
    # Fetch current forex rates
    print("\n" + "="*60)
    print("CURRENT FOREX RATES")
    print("="*60)
    forex = fetcher.fetch_alternative_forex()
    if forex:
        for key, value in forex.items():
            print(f"{key}: {value}")
    
    print("\n✅ Macro data fetch complete!")

if __name__ == '__main__':
    main()