# maining/fetch_historical.py - CORRECTED

import ccxt
import pandas as pd
from datetime import datetime
import time
import os

# --- Configuration ---
EXCHANGE_ID = 'binance'
SYMBOL = 'BTC/USDT'
TIMEFRAME = '1h'  # 1 hour data points
SINCE_DATE = '2020-01-01T00:00:00Z' # Start date for historical data
LIMIT = 1000 # Max candles per request (Binance limit)
OUTPUT_DIR = 'data/raw'
OUTPUT_FILE = f'{EXCHANGE_ID}_{SYMBOL.replace("/", "")}_{TIMEFRAME}.csv'
MAX_RETRIES = 3

def fetch_all_ohlcv(exchange, symbol, timeframe, since, limit):
    """Fetches all OHLCV data from a starting timestamp using pagination."""
    
    # Convert 'since' date string to milliseconds timestamp
    since_ms = exchange.parse8601(since)
    
    # Initialize list to store all candles
    all_ohlcv = []
    
    print(f"Starting historical fetch for {symbol} ({timeframe}) from {since}...")

    while True:
        try:
            # Fetch a batch of candles
            ohlcv = exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=since_ms,
                limit=limit
            )

            if not ohlcv:
                break # No more data to fetch

            # Append new data and update the 'since' timestamp for the next batch
            all_ohlcv.extend(ohlcv)
            
            # The next batch starts from the timestamp of the last candle fetched
            since_ms = ohlcv[-1][0] + exchange.parse_timeframe(timeframe)
            
            # Print status update
            print(f"-> Fetched {len(all_ohlcv)} total candles. Last date: {exchange.iso8601(ohlcv[-1][0])}")
            
            # Rate limit handling
            time.sleep(exchange.rateLimit / 1000)

        except ccxt.NetworkError as e:
            print(f"[ERROR] Network error: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[ERROR] General error during fetch: {e}")
            break

    return all_ohlcv

def save_data(ohlcv_data, filename):
    """Converts OHLCV data to a DataFrame and saves it to CSV."""
    
    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    full_path = os.path.join(OUTPUT_DIR, filename)
    
    # Convert list of lists to DataFrame
    df = pd.DataFrame(ohlcv_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    
    # Convert Timestamp from milliseconds to datetime object
    df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='ms')
    df.set_index('Datetime', inplace=True)
    df.drop('Timestamp', axis=1, inplace=True)

    # Save to CSV
    df.to_csv(full_path)
    print(f"\n✅ Data saved successfully to {full_path}")
    print(f"Total rows: {len(df)}")


def main(): # <-- Correctly added main function
    """Initializes exchange, fetches OHLCV data, and saves it."""
    # Initialize the exchange
    try:
        exchange = getattr(ccxt, EXCHANGE_ID)({'enableRateLimit': True})
    except AttributeError:
        print(f"Exchange {EXCHANGE_ID} not found in CCXT.")
        return # Exit gracefully if exchange is not found

    # Fetch and save
    ohlcv_data = fetch_all_ohlcv(exchange, SYMBOL, TIMEFRAME, SINCE_DATE, LIMIT)
    
    if ohlcv_data:
        save_data(ohlcv_data, OUTPUT_FILE)


if __name__ == '__main__':
    main()