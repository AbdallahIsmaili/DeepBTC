# api/fetch_sentiment.py

import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import time

# --- Configuration ---
OUTPUT_DIR = 'data/raw'
OUTPUT_FILE = 'sentiment_metrics.csv'

class SentimentFetcher:
    """
    Fetches free sentiment data for Bitcoin from multiple sources.
    No API keys required.
    """
    
    def __init__(self):
        self.fear_greed_base = "https://api.alternative.me/fng"
        
    def fetch_fear_greed_index(self, limit=365):
        """
        Fetches the Crypto Fear & Greed Index (very popular sentiment indicator).
        Completely free, no API key required.
        
        Returns historical data with values from 0 (Extreme Fear) to 100 (Extreme Greed).
        """
        print(f"Fetching Fear & Greed Index (last {limit} days)...")
        
        try:
            url = f"{self.fear_greed_base}/?limit={limit}&format=json"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('metadata', {}).get('error'):
                print(f"API Error: {data['metadata']['error']}")
                return None
            
            # Parse the data
            records = []
            for entry in data['data']:
                records.append({
                    'timestamp': int(entry['timestamp']),
                    'date': datetime.fromtimestamp(int(entry['timestamp'])),
                    'fear_greed_value': int(entry['value']),
                    'fear_greed_classification': entry['value_classification']
                })
            
            df = pd.DataFrame(records)
            df.set_index('date', inplace=True)
            df.drop('timestamp', axis=1, inplace=True)
            df.sort_index(inplace=True)
            
            print(f"✅ Successfully fetched {len(df)} days of Fear & Greed data")
            return df
            
        except Exception as e:
            print(f"Error fetching Fear & Greed Index: {e}")
            return None
    
    def fetch_coingecko_trends(self):
        """
        Fetches trending coins and Bitcoin market data from CoinGecko.
        Free tier: No API key required.
        """
        print("Fetching CoinGecko trending data...")
        
        try:
            # Trending coins
            trending_url = "https://api.coingecko.com/api/v3/search/trending"
            response = requests.get(trending_url, timeout=10)
            trending_data = response.json()
            
            # Check if BTC is in trending
            btc_trending_rank = None
            if 'coins' in trending_data:
                for idx, coin in enumerate(trending_data['coins']):
                    if coin['item']['symbol'].lower() == 'btc':
                        btc_trending_rank = idx + 1
                        break
            
            # Bitcoin market data
            btc_url = "https://api.coingecko.com/api/v3/coins/bitcoin"
            response = requests.get(btc_url, timeout=10)
            btc_data = response.json()
            
            sentiment = {
                'btc_trending_rank': btc_trending_rank if btc_trending_rank else 999,
                'sentiment_votes_up': btc_data.get('sentiment_votes_up_percentage', 0),
                'sentiment_votes_down': btc_data.get('sentiment_votes_down_percentage', 0),
                'market_cap_rank': btc_data.get('market_cap_rank', 1),
                'coingecko_score': btc_data.get('coingecko_score', 0),
                'developer_score': btc_data.get('developer_score', 0),
                'community_score': btc_data.get('community_score', 0),
                'liquidity_score': btc_data.get('liquidity_score', 0),
                'public_interest_score': btc_data.get('public_interest_score', 0)
            }
            
            print(f"✅ Successfully fetched CoinGecko sentiment metrics")
            return sentiment
            
        except Exception as e:
            print(f"Error fetching CoinGecko data: {e}")
            return None
    
    def fetch_cryptopanic_news(self):
        """
        Fetches crypto news sentiment from CryptoPanic.
        Free tier available without API key (limited).
        """
        print("Fetching CryptoPanic news sentiment...")
        
        try:
            # Public feed (no auth required, but limited)
            url = "https://cryptopanic.com/api/v1/posts/?auth_token=free&currencies=BTC&public=true"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if 'results' not in data:
                print("No news data available")
                return None
            
            # Analyze sentiment from news
            positive_count = 0
            negative_count = 0
            neutral_count = 0
            
            for post in data['results']:
                votes = post.get('votes', {})
                if votes.get('positive', 0) > votes.get('negative', 0):
                    positive_count += 1
                elif votes.get('negative', 0) > votes.get('positive', 0):
                    negative_count += 1
                else:
                    neutral_count += 1
            
            total = len(data['results'])
            sentiment_score = (positive_count - negative_count) / total if total > 0 else 0
            
            print(f"✅ Analyzed {total} news articles")
            
            return {
                'news_positive_pct': (positive_count / total * 100) if total > 0 else 0,
                'news_negative_pct': (negative_count / total * 100) if total > 0 else 0,
                'news_neutral_pct': (neutral_count / total * 100) if total > 0 else 0,
                'news_sentiment_score': sentiment_score,
                'total_news_count': total
            }
            
        except Exception as e:
            print(f"Error fetching news sentiment: {e}")
            return None
    
    def save_data(self, fear_greed_df):
        """Saves the sentiment data to CSV."""
        if fear_greed_df is None:
            return
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        full_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        fear_greed_df.to_csv(full_path)
        print(f"\n✅ Sentiment data saved to {full_path}")
        print(f"Total rows: {len(fear_greed_df)}")
        print(f"Date range: {fear_greed_df.index.min()} to {fear_greed_df.index.max()}")

def main():
    """Main execution function."""
    fetcher = SentimentFetcher()
    
    # Fetch Fear & Greed Index (historical)
    fear_greed_df = fetcher.fetch_fear_greed_index(limit=730)
    
    if fear_greed_df is not None:
        fetcher.save_data(fear_greed_df)
    
    # Fetch current sentiment indicators
    print("\n" + "="*50)
    print("CURRENT SENTIMENT INDICATORS")
    print("="*50)
    
    coingecko_sentiment = fetcher.fetch_coingecko_trends()
    if coingecko_sentiment:
        print("\nCoinGecko Metrics:")
        for key, value in coingecko_sentiment.items():
            print(f"  {key}: {value}")
    
    time.sleep(1)  # Rate limiting
    
    news_sentiment = fetcher.fetch_cryptopanic_news()
    if news_sentiment:
        print("\nNews Sentiment:")
        for key, value in news_sentiment.items():
            print(f"  {key}: {value}")
    
    print("\n✅ Sentiment data fetch complete!")

if __name__ == '__main__':
    main()