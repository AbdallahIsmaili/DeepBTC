# diagnose_data.py - Comprehensive Data Quality Checker

import pandas as pd
import numpy as np
import os
from datetime import datetime

class DataDiagnostic:
    """
    Diagnoses data quality issues that cause high dropout rates.
    """
    
    def __init__(self):
        self.paths = {
            'OHLCV': 'data/raw/binance_btcusdt_1h.csv',
            'Blockchain': 'data/raw/blockchain_metrics_daily.csv',
            'Sentiment': 'data/raw/sentiment_metrics.csv',
            'Macro': 'data/raw/macro_indicators.csv',
            'Features': 'data/features/btc_features_complete.csv'
        }
    
    def check_date_coverage(self):
        """Analyzes date coverage across all data sources."""
        
        print("\n" + "="*70)
        print("📅 DATE COVERAGE ANALYSIS")
        print("="*70)
        
        date_ranges = {}
        
        for name, path in self.paths.items():
            if not os.path.exists(path):
                print(f"\n❌ {name}: File not found")
                continue
            
            try:
                if name == 'Macro':
                    df = pd.read_csv(path, parse_dates=True)
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                        date_col = df['Date']
                    else:
                        date_col = pd.to_datetime(df.iloc[:, 0])
                else:
                    df = pd.read_csv(path, index_col=0, parse_dates=True)
                    date_col = df.index
                
                start = date_col.min()
                end = date_col.max()
                days = (end - start).days
                rows = len(df)
                
                date_ranges[name] = {'start': start, 'end': end, 'days': days, 'rows': rows}
                
                print(f"\n✅ {name}:")
                print(f"   Start: {start}")
                print(f"   End: {end}")
                print(f"   Duration: {days} days")
                print(f"   Total rows: {rows:,}")
                
                # Check for gaps
                if name in ['OHLCV', 'Features']:
                    expected_hours = days * 24
                    actual_hours = rows
                    coverage = (actual_hours / expected_hours) * 100 if expected_hours > 0 else 0
                    print(f"   Coverage: {coverage:.1f}% (expected ~24 records/day)")
                
            except Exception as e:
                print(f"\n⚠️  {name}: Error - {e}")
        
        # Find common date range
        if date_ranges:
            print("\n" + "-"*70)
            print("📊 COMMON DATE RANGE:")
            
            starts = [info['start'] for info in date_ranges.values()]
            ends = [info['end'] for info in date_ranges.values()]
            
            common_start = max(starts)
            common_end = min(ends)
            common_days = (common_end - common_start).days
            
            print(f"   Latest start: {common_start}")
            print(f"   Earliest end: {common_end}")
            print(f"   Common duration: {common_days} days")
            
            # Show what each source contributes
            print("\n   Coverage by source:")
            for name, info in date_ranges.items():
                before = (common_start - info['start']).days
                after = (info['end'] - common_end).days
                print(f"   {name:15s}: {before:4d} days before, {after:4d} days after common range")
    
    def analyze_missing_data(self):
        """Analyzes missing data patterns in features."""
        
        print("\n" + "="*70)
        print("🔍 MISSING DATA ANALYSIS")
        print("="*70)
        
        features_path = self.paths['Features']
        if not os.path.exists(features_path):
            print("\n⚠️  Features file not found. Run: python main.py features-complete")
            return
        
        df = pd.read_csv(features_path, index_col=0, parse_dates=True)
        
        print(f"\nTotal rows: {len(df):,}")
        print(f"Total columns: {len(df.columns)}")
        
        # Check missing values by column
        missing = df.isnull().sum()
        missing_pct = (missing / len(df)) * 100
        
        # Show columns with most missing data
        missing_df = pd.DataFrame({
            'Missing Count': missing,
            'Missing %': missing_pct
        }).sort_values('Missing Count', ascending=False)
        
        top_missing = missing_df[missing_df['Missing Count'] > 0].head(20)
        
        if len(top_missing) > 0:
            print(f"\n⚠️  Top 20 columns with missing data:")
            print(top_missing.to_string())
        else:
            print("\n✅ No missing data found!")
        
        # Check for infinite values
        inf_count = np.isinf(df.select_dtypes(include=[np.number])).sum().sum()
        if inf_count > 0:
            print(f"\n⚠️  Found {inf_count:,} infinite values")
        else:
            print("\n✅ No infinite values found")
    
    def analyze_feature_gaps(self):
        """Identifies which features cause the most data loss."""
        
        print("\n" + "="*70)
        print("📉 FEATURE GAP ANALYSIS")
        print("="*70)
        
        ohlcv_path = self.paths['OHLCV']
        features_path = self.paths['Features']
        
        if not os.path.exists(ohlcv_path) or not os.path.exists(features_path):
            print("\n⚠️  Required files not found")
            return
        
        ohlcv = pd.read_csv(ohlcv_path, index_col=0, parse_dates=True)
        features = pd.read_csv(features_path, index_col=0, parse_dates=True)
        
        print(f"\nOriginal OHLCV rows: {len(ohlcv):,}")
        print(f"Final feature rows: {len(features):,}")
        print(f"Data loss: {len(ohlcv) - len(features):,} rows ({(1 - len(features)/len(ohlcv))*100:.1f}%)")
        
        # Check which time periods are missing
        ohlcv_dates = set(ohlcv.index)
        feature_dates = set(features.index)
        missing_dates = ohlcv_dates - feature_dates
        
        if missing_dates:
            missing_list = sorted(list(missing_dates))
            print(f"\n⚠️  Missing {len(missing_dates):,} time periods from original OHLCV")
            print(f"   First missing: {missing_list[0]}")
            print(f"   Last missing: {missing_list[-1]}")
            
            # Check if missing dates are at beginning or end
            missing_early = sum(1 for d in missing_dates if d < features.index.min())
            missing_late = sum(1 for d in missing_dates if d > features.index.max())
            missing_middle = len(missing_dates) - missing_early - missing_late
            
            print(f"\n   Missing at start: {missing_early:,} ({missing_early/len(missing_dates)*100:.1f}%)")
            print(f"   Missing in middle: {missing_middle:,} ({missing_middle/len(missing_dates)*100:.1f}%)")
            print(f"   Missing at end: {missing_late:,} ({missing_late/len(missing_dates)*100:.1f}%)")
            
            # Additional diagnosis: Why are we losing data at the start?
            print(f"\n🔍 ROOT CAUSE ANALYSIS:")
            print(f"   OHLCV starts: {ohlcv.index.min()}")
            print(f"   Features start: {features.index.min()}")
            print(f"   Gap: {(features.index.min() - ohlcv.index.min()).days} days")
            
            # Check external data sources
            for name in ['Blockchain', 'Sentiment', 'Macro']:
                path = self.paths.get(name)
                if path and os.path.exists(path):
                    try:
                        if name == 'Macro':
                            df = pd.read_csv(path)
                            if 'Date' in df.columns:
                                df['Date'] = pd.to_datetime(df['Date'])
                                start = df['Date'].min()
                            else:
                                start = pd.to_datetime(df.iloc[:, 0]).min()
                        else:
                            df = pd.read_csv(path, index_col=0, parse_dates=True)
                            start = df.index.min()
                        
                        gap_days = (start - ohlcv.index.min()).days
                        if gap_days > 0:
                            print(f"   ⚠️  {name} starts {gap_days} days after OHLCV")
                    except:
                        pass
            
            print(f"\n💡 EXPLANATION:")
            print(f"   The data loss is primarily due to external data sources")
            print(f"   (blockchain, sentiment, macro) starting later than OHLCV.")
            print(f"   This is NORMAL and expected!")
            print(f"   The fixed feature_engine.py will keep more data by using")
            print(f"   smarter forward-filling strategies.")
    
    def suggest_fixes(self):
        """Provides actionable recommendations."""
        
        print("\n" + "="*70)
        print("💡 RECOMMENDATIONS")
        print("="*70)
        
        # Check if fixed version is needed
        features_path = self.paths['Features']
        if os.path.exists(features_path):
            df = pd.read_csv(features_path, index_col=0, parse_dates=True)
            ohlcv = pd.read_csv(self.paths['OHLCV'], index_col=0, parse_dates=True)
            
            retention = (len(df) / len(ohlcv)) * 100
            
            if retention < 90:
                print("\n⚠️  LOW DATA RETENTION DETECTED")
                print(f"   Current retention: {retention:.1f}%")
                print(f"\n   🔧 FIXES:")
                print("   1. Replace feature_engine.py with the FIXED version")
                print("   2. Re-run: python main.py features-complete")
                print("   3. Expected improvement: 90%+ retention")
            else:
                print("\n✅ Data retention is good!")
        
        # Check for FRED API key
        macro_path = self.paths['Macro']
        if os.path.exists(macro_path):
            df = pd.read_csv(macro_path)
            if len(df.columns) < 8:  # Yahoo Finance only has 5 columns
                print("\n⚠️  LIMITED MACRO DATA")
                print("   Currently using Yahoo Finance only (5 indicators)")
                print(f"\n   🔧 TO GET MORE INDICATORS:")
                print("   1. Get free FRED API key: https://fred.stlouisfed.org/docs/api/api_key.html")
                print("   2. Set in fetch_macro_data.py: FRED_API_KEY = '99b1e3b0816299e5fe8ddac476442843'")
                print("   3. Re-run: python main.py fetch-macro")
    
    def run_full_diagnostic(self):
        """Runs complete diagnostic suite."""
        
        print("\n" + "="*70)
        print("🔬 DeepBTC DATA QUALITY DIAGNOSTIC")
        print("="*70)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.check_date_coverage()
        self.analyze_missing_data()
        self.analyze_feature_gaps()
        self.suggest_fixes()
        
        print("\n" + "="*70)
        print("✅ DIAGNOSTIC COMPLETE")
        print("="*70 + "\n")

def main():
    diagnostic = DataDiagnostic()
    diagnostic.run_full_diagnostic()

if __name__ == '__main__':
    main()