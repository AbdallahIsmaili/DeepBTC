# api/fetch_macro_data.py

import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# --- Configuration ---
OUTPUT_DIR = 'data/raw'
OUTPUT_FILE = 'macro_indicators.csv'

# FRED API - Free but requires registration for API key
# Get your free API key at: https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = '99b1e3b0816299e5fe8ddac476442843'  # Replace with your key
FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations'

class MacroDataFetcher:
    """
    Fetches macroeconomic indicators that influence Bitcoin prices.
    Uses FRED (Federal Reserve Economic Data) - Free API with registration.
    """
    
    def __init__(self, api_key=None):
        self.api_key = api_key or FRED_API_KEY
        
        if self.api_key == '99b1e3b0816299e5fe8ddac476442843':
            print("⚠️  WARNING: Please set your FRED API key!")
            print("Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
            self.use_alternative = True
        else:
            self.use_alternative = False
    
    def fetch_fred_series(self, series_id, start_date='2020-01-01'):
        """
        Fetches a single economic time series from FRED.
        
        Popular series for Bitcoin analysis:
        - DFF: Federal Funds Rate
        - T10Y2Y: 10-Year Treasury Constant Maturity Minus 2-Year
        - DEXUSEU: U.S. Dollar to Euro Exchange Rate
        - DGS10: 10-Year Treasury Constant Maturity Rate
        - VIXCLS: CBOE Volatility Index (VIX)
        - DCOILWTICO: Crude Oil Prices (WTI)
        - GOLDAMGBD228NLBM: Gold Fixing Price
        """
        
        if self.use_alternative:
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
                print(f"Error fetching {series_id}: {data}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(data['observations'])
            df = df[['date', 'value']]
            df.columns = ['date', series_id]
            df['date'] = pd.to_datetime(df['date'])
            df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
            df.set_index('date', inplace=True)
            
            print(f"✅ Fetched {series_id}: {len(df)} observations")
            return df
            
        except Exception as e:
            print(f"Error fetching FRED series {series_id}: {e}")
            return None
    
    def fetch_alternative_forex(self):
        """
        Fetches forex data from a free source (exchangerate-api.com).
        No API key required for basic usage.
        """
        print("Fetching alternative forex data...")
        
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
            print(f"Error fetching forex data: {e}")
            return None
    
    def fetch_yahoo_finance_indices(self):
        """
        Alternative method using yfinance library for stock indices.
        Free and no API key required.
        Note: Requires 'pip install yfinance'
        """
        try:
            import yfinance as yf
            
            print("Fetching stock market indices...")
            
            # Major indices
            indices = {
                'SP500': '^GSPC',      # S&P 500
                'NASDAQ': '^IXIC',     # NASDAQ Composite
                'DXY': 'DX-Y.NYB',     # US Dollar Index
                'GOLD': 'GC=F',        # Gold Futures
                'VIX': '^VIX'          # Volatility Index
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
                except Exception as e:
                    print(f"Could not fetch {name}: {e}")
            
            if dfs:
                combined = pd.concat(dfs, axis=1)
                return combined
            return None
            
        except ImportError:
            print("yfinance not installed. Run: pip install yfinance")
            return None
        except Exception as e:
            print(f"Error fetching Yahoo Finance data: {e}")
            return None
    
    def fetch_all_macro_data(self):
        """Fetches all macroeconomic indicators."""
        
        all_data = []
        
        if not self.use_alternative:
            # FRED Series to fetch
            fred_series = {
                'fed_funds_rate': 'DFF',
                'treasury_10y': 'DGS10',
                'treasury_2y': 'DGS2',
                'vix': 'VIXCLS',
                'oil_wti': 'DCOILWTICO',
                'gold_price': 'GOLDAMGBD228NLBM',
                'usd_eur': 'DEXUSEU'
            }
            
            for name, series_id in fred_series.items():
                df = self.fetch_fred_series(series_id)
                if df is not None:
                    df.columns = [name]
                    all_data.append(df)
        
        # Fetch Yahoo Finance data (works without FRED API)
        yahoo_data = self.fetch_yahoo_finance_indices()
        if yahoo_data is not None:
            all_data.append(yahoo_data)
        
        if not all_data:
            print("No macro data fetched successfully")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(all_data, axis=1)
        combined_df.sort_index(inplace=True)
        
        # Forward fill missing values (for different trading days)
        combined_df.fillna(method='ffill', inplace=True)
        
        return combined_df
    
    def save_data(self, df):
        """Saves the macro data to CSV."""
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
    """Main execution function."""
    
    # Try to read API key from environment variable
    api_key = os.environ.get('FRED_API_KEY', FRED_API_KEY)
    
    fetcher = MacroDataFetcher(api_key=api_key)
    
    print("Fetching macroeconomic indicators...")
    print("="*50)
    
    macro_df = fetcher.fetch_all_macro_data()
    
    if macro_df is not None:
        fetcher.save_data(macro_df)
        
        # Display current values
        print("\n" + "="*50)
        print("LATEST MACRO INDICATORS")
        print("="*50)
        print(macro_df.tail(1).T)
    
    # Fetch current forex rates
    print("\n" + "="*50)
    print("CURRENT FOREX RATES")
    print("="*50)
    forex = fetcher.fetch_alternative_forex()
    if forex:
        for key, value in forex.items():
            print(f"{key}: {value}")
    
    print("\n✅ Macro data fetch complete!")

if __name__ == '__main__':
    main()