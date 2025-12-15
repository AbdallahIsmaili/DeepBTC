# 🤖 DeepBTC: Deep Learning Strategies for Bitcoin Investment

## 📝 Overview
This project implements **Machine Learning (ML)** and **Deep Learning (DL)** models to develop adaptive investment strategies for the Bitcoin (BTC) market. The system leverages multiple free data sources including:

- **High-frequency market data** (OHLCV) from Binance
- **Blockchain/On-chain metrics** from Blockchain.info and Mempool.space
- **Sentiment indicators** from Fear & Greed Index and CoinGecko
- **Macroeconomic factors** from FRED and Yahoo Finance
- **Real-time trade streams** via WebSocket

## 🎯 Project Goals
1. ✅ Acquire comprehensive, multi-source Bitcoin data (100% FREE)
2. ✅ Engineer rich feature sets combining technical, on-chain, and macro indicators
3. 🚧 Train LSTM/Transformer models for price prediction
4. 🚧 Implement adaptive trading strategies
5. 🚧 Backtest and optimize strategy performance
6. 🚧 Deploy real-time trading system

## 📁 Enhanced Project Structure

```
.
├── api/                                    # All data acquisition & processing modules
│   ├── fetch_historical.py                # OHLCV data from Binance (✅ Implemented)
│   ├── fetch_blockchain_metrics.py        # On-chain data (✅ NEW)
│   ├── fetch_sentiment.py                 # Fear & Greed Index (✅ NEW)
│   ├── fetch_macro_data.py                # Economic indicators (✅ NEW)
│   ├── realtime_listener.py               # Live WebSocket stream (✅ Implemented)
│   ├── feature_engine.py         # Complete feature set (✅ NEW)
│   └── __init__.py
│
├── data/
│   ├── raw/                               # Raw data from all sources
│   │   ├── binance_btcusdt_1h.csv        # Hourly OHLCV
│   │   ├── blockchain_metrics_daily.csv   # On-chain metrics
│   │   ├── sentiment_metrics.csv          # Sentiment indicators
│   │   └── macro_indicators.csv           # Economic data
│   │
│   └── features/                          # Processed features ready for ML
│       ├── btc_features_hourly.csv       # Basic features
│       └── btc_features_complete.csv      # All features combined
│
├── models/                                # Trained models (to be created)
│   ├── lstm_predictor.h5
│   ├── transformer_model.h5
│   └── ensemble_model.pkl
│
├── notebooks/                             # Jupyter notebooks for analysis
│   ├── exploratory_analysis.ipynb
│   ├── feature_importance.ipynb
│   └── model_evaluation.ipynb
│
├── main.py                                # Enhanced main runner
├── requirements.txt                       # Complete dependencies
└── README.md                              # This file
```

## 🚀 Installation & Setup

### 1. Clone and Install Dependencies

```bash
# Clone the repository
git clone <your-repo-url>
cd DeepBTC

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install all dependencies
pip install -r requirements.txt
```

### 2. Optional: Get Free API Keys

Most data sources require **NO API keys**, but for macroeconomic data you can optionally register for:

| Service | Purpose | Registration | Cost |
|---------|---------|--------------|------|
| **FRED** | US Economic Data | [Get Free Key](https://fred.stlouisfed.org/docs/api/api_key.html) | FREE |
| **None** | Everything else works without any registration! | N/A | FREE |

**Note:** Even FRED is optional - the system uses Yahoo Finance as a fallback!

## 📊 Complete Data Acquisition Workflow

### Quick Start: Fetch ALL Data at Once

```bash
# Fetch all data sources in one command
python main.py fetch-all
```

This will sequentially fetch:
1. ✅ Historical OHLCV (2020-present)
2. ✅ Blockchain metrics (hash rate, difficulty, tx count, fees, etc.)
3. ✅ Sentiment indicators (Fear & Greed Index)
4. ✅ Macroeconomic data (S&P 500, VIX, Gold, DXY, etc.)

### Or Fetch Data Sources Individually

```bash
# 1. Fetch historical OHLCV data (hourly, 2020-present)
python main.py fetch

# 2. Fetch blockchain/on-chain metrics
python main.py fetch-blockchain

# 3. Fetch sentiment indicators
python main.py fetch-sentiment

# 4. Fetch macroeconomic indicators
python main.py fetch-macro
```

### Real-Time Data Stream

```bash
# Start live WebSocket stream for real-time trading
python main.py live
```

## 🔧 Feature Engineering

### Basic Features (Technical Indicators Only)

```bash
python main.py features
```

Generates ~40 technical indicators:
- **Trend**: SMA, EMA, MACD
- **Momentum**: RSI, Stochastic, MFI
- **Volatility**: ATR, Bollinger Bands
- **Volume**: OBV, VWAP, Volume ratios

### Complete Features (Recommended for ML)

```bash
python main.py features-complete
```

Generates **70+ features** combining:
- ✅ All technical indicators (~40 features)
- ✅ Blockchain metrics (~15 features)
  - Hash rate, difficulty, transaction count
  - Network value to transactions (NVT) ratio
  - Miner revenue, mempool size
- ✅ Sentiment indicators (~5 features)
  - Fear & Greed Index and derivatives
  - Extreme sentiment flags
- ✅ Macroeconomic factors (~10 features)
  - S&P 500 correlation
  - VIX (market volatility)
  - Gold correlation
  - Dollar Index (DXY)

**Output:** `data/features/btc_features_complete.csv` - Ready for ML training!

## 🤖 Model Training (Next Steps)

```bash
# Train ML model (to be implemented)
python main.py train
```

### Recommended Model Architectures

1. **LSTM (Long Short-Term Memory)**
   - Best for capturing temporal dependencies
   - Input: Sequence of 24-168 hours
   - Output: Next 1-24 hour price movement

2. **Transformer with Attention**
   - Captures long-range dependencies
   - Better than LSTM for complex patterns
   - Requires more data and compute

3. **Ensemble Approach** (Recommended)
   - XGBoost for feature importance
   - LSTM for temporal patterns
   - Combine predictions with weighted voting

### Training Pipeline (To Implement)

```python
# Pseudocode for api/train_model.py
1. Load features from data/features/btc_features_complete.csv
2. Split data: 70% train, 15% validation, 15% test
3. Normalize features (StandardScaler)
4. Create sequences (e.g., 168 hours lookback)
5. Build LSTM model:
   - Input: (batch_size, 168, num_features)
   - LSTM layers: 128, 64, 32 units
   - Dropout: 0.2-0.3
   - Dense output: 1 (regression) or 2 (classification)
6. Compile with Adam optimizer
7. Train with early stopping
8. Evaluate on test set
9. Save model to models/
```

## 📈 Data Sources Summary

| Data Type | Source | Frequency | API Key Required | Status |
|-----------|--------|-----------|------------------|---------|
| **OHLCV Market Data** | Binance (CCXT) | Hourly | ❌ No | ✅ Implemented |
| **Real-Time Trades** | Binance WebSocket | Live | ❌ No | ✅ Implemented |
| **Blockchain Metrics** | Blockchain.info | Daily | ❌ No | ✅ Implemented |
| **Mempool Stats** | Mempool.space | Real-time | ❌ No | ✅ Implemented |
| **Fear & Greed Index** | Alternative.me | Daily | ❌ No | ✅ Implemented |
| **Crypto Sentiment** | CoinGecko | Current | ❌ No | ✅ Implemented |
| **Economic Data** | Yahoo Finance | Daily | ❌ No | ✅ Implemented |
| **Economic Data (Alt)** | FRED | Daily | ⚠️ Optional | ✅ Implemented |

**Total Free Data Sources: 8** (7 require NO registration!)

## 🎓 Feature Categories Explained

### 1. Technical Indicators (Price-Based)
- **Purpose**: Capture price momentum, trends, and volatility
- **Examples**: RSI shows overbought/oversold, MACD shows trend changes
- **ML Insight**: Most predictive for short-term movements

### 2. Blockchain Metrics (On-Chain)
- **Purpose**: Network health and activity indicators
- **Examples**: Rising hash rate = miner confidence, high fees = network congestion
- **ML Insight**: Leading indicators for medium-term trends

### 3. Sentiment Indicators
- **Purpose**: Market psychology and crowd behavior
- **Examples**: Extreme fear often precedes bounces, extreme greed precedes corrections
- **ML Insight**: Contrarian signals, useful for risk management

### 4. Macroeconomic Factors
- **Purpose**: Broader market context and risk appetite
- **Examples**: VIX spike = risk-off (BTC may drop), weak DXY = BTC rally
- **ML Insight**: Essential for understanding regime changes

## 🔍 Data Quality Checks

Before training, verify your data:

```bash
# Check data availability
ls -lh data/raw/
ls -lh data/features/

# Analyze data coverage
python -c "
import pandas as pd
ohlcv = pd.read_csv('data/raw/binance_btcusdt_1h.csv')
print(f'OHLCV: {len(ohlcv)} hours, {ohlcv.Datetime.min()} to {ohlcv.Datetime.max()}')

features = pd.read_csv('data/features/btc_features_complete.csv')
print(f'Features: {features.shape[0]} samples, {features.shape[1]} columns')
print(f'Missing values: {features.isnull().sum().sum()}')
"
```

## ⚡ Performance Tips

1. **Data Fetching**
   - Run `fetch-all` during off-hours (takes 5-10 minutes)
   - Blockchain.info has rate limits - script includes delays

2. **Feature Engineering**
   - Complete features take 2-5 minutes to generate
   - Most time spent on rolling calculations (correlations, MAs)

3. **Real-Time Stream**
   - WebSocket is lightweight, minimal CPU/memory
   - Perfect for paper trading and live predictions

## 🚧 Roadmap

- [x] OHLCV data acquisition
- [x] Real-time WebSocket stream
- [x] Blockchain metrics integration
- [x] Sentiment indicators integration
- [x] Macroeconomic data integration
- [x] Enhanced feature engineering
- [ ] LSTM model implementation
- [ ] Transformer model implementation
- [ ] Ensemble model
- [ ] Backtesting framework
- [ ] Paper trading system
- [ ] Live trading (with risk management)
- [ ] Web dashboard for monitoring

## 📚 Resources & References

- [CCXT Documentation](https://docs.ccxt.com/)
- [Pandas-TA Library](https://github.com/twopirllc/pandas-ta)
- [Blockchain.info API](https://www.blockchain.com/api)
- [Fear & Greed Index](https://alternative.me/crypto/fear-and-greed-index/)
- [FRED API](https://fred.stlouisfed.org/docs/api/)
- [TensorFlow Time Series](https://www.tensorflow.org/tutorials/structured_data/time_series)

## 🤝 Contributing

Contributions welcome! Areas needing help:
- Additional data sources
- Feature engineering ideas
- Model architectures
- Backtesting strategies

## ⚠️ Disclaimer

This project is for educational and research purposes only. Cryptocurrency trading involves substantial risk of loss. Past performance does not guarantee future results. Always do your own research and never invest more than you can afford to lose.

## 📄 License

MIT License - Feel free to use and modify for your own projects!

---

**Next Step**: Run `python main.py fetch-all` to start collecting data! 🚀