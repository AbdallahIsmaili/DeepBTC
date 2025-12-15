# api/validate_data.py

import pandas as pd
import os
from datetime import datetime
import numpy as np

class DataValidator:
    """
    Validates and provides summary statistics for all data sources.
    Run this to verify data quality before training models.
    """
    
    def __init__(self):
        self.data_paths = {
            'OHLCV': 'data/raw/binance_btcusdt_1h.csv',
            'Blockchain': 'data/raw/blockchain_metrics_daily.csv',
            'Sentiment': 'data/raw/sentiment_metrics.csv',
            'Macro': 'data/raw/macro_indicators.csv',
            'Features Basic': 'data/features/btc_features_hourly.csv',
            'Features Complete': 'data/features/btc_features_complete.csv'
        }
        self.results = {}
    
    def check_file_exists(self, name, path):
        """Checks if file exists and returns basic info."""
        if not os.path.exists(path):
            return None
        
        try:
            df = pd.read_csv(path, nrows=5)
            file_size = os.path.getsize(path) / (1024 * 1024)  # MB
            
            return {
                'exists': True,
                'size_mb': round(file_size, 2),
                'columns': len(df.columns),
                'sample_columns': list(df.columns[:5])
            }
        except Exception as e:
            return {'exists': True, 'error': str(e)}
    
    def analyze_dataframe(self, name, path):
        """Performs detailed analysis of a dataset."""
        if not os.path.exists(path):
            return None
        
        try:
            # Load with date parsing
            if 'csv' in path:
                df = pd.read_csv(path, parse_dates=[0], index_col=0)
            else:
                return None
            
            analysis = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'date_range_start': str(df.index.min()),
                'date_range_end': str(df.index.max()),
                'missing_values': df.isnull().sum().sum(),
                'missing_pct': round(df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100, 2),
                'duplicates': df.index.duplicated().sum(),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / (1024**2), 2)
            }
            
            # Numeric columns statistics
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                analysis['numeric_columns'] = len(numeric_cols)
                analysis['infinite_values'] = np.isinf(df[numeric_cols]).sum().sum()
                
                # Sample statistics for first numeric column
                first_col = numeric_cols[0]
                analysis['sample_stats'] = {
                    'column': first_col,
                    'mean': round(df[first_col].mean(), 4),
                    'std': round(df[first_col].std(), 4),
                    'min': round(df[first_col].min(), 4),
                    'max': round(df[first_col].max(), 4)
                }
            
            # Data frequency (for time series)
            if len(df) > 1:
                time_diff = (df.index[-1] - df.index[0]).total_seconds() / 3600  # hours
                analysis['avg_frequency_hours'] = round(time_diff / len(df), 2)
            
            return analysis
            
        except Exception as e:
            return {'error': str(e)}
    
    def validate_data_alignment(self):
        """Checks if different data sources align properly."""
        print("\n" + "="*70)
        print("DATA ALIGNMENT CHECK")
        print("="*70)
        
        # Load all available data
        dfs = {}
        for name, path in self.data_paths.items():
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path, parse_dates=[0], index_col=0)
                    dfs[name] = df
                except:
                    pass
        
        if not dfs:
            print("❌ No data files found to validate")
            return
        
        # Find common date range
        start_dates = {name: df.index.min() for name, df in dfs.items()}
        end_dates = {name: df.index.max() for name, df in dfs.items()}
        
        common_start = max(start_dates.values())
        common_end = min(end_dates.values())
        
        print(f"\n📅 Common Date Range Across All Sources:")
        print(f"   Start: {common_start}")
        print(f"   End: {common_end}")
        print(f"   Duration: {(common_end - common_start).days} days")
        
        # Check for gaps
        print(f"\n🔍 Date Range Coverage by Source:")
        for name, df in dfs.items():
            days = (df.index.max() - df.index.min()).days
            coverage = round((df.index.max() - df.index.min()) / (common_end - common_start) * 100, 1)
            print(f"   {name:20s}: {df.index.min()} to {df.index.max()} ({days} days, {coverage}% of common range)")
    
    def generate_report(self):
        """Generates a comprehensive validation report."""
        print("\n" + "="*70)
        print("DATA VALIDATION REPORT")
        print("="*70)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        for name, path in self.data_paths.items():
            print(f"\n📊 {name}")
            print("-" * 70)
            
            # Check file existence
            file_info = self.check_file_exists(name, path)
            
            if file_info is None:
                print("   ❌ File not found")
                print(f"   Expected location: {path}")
                continue
            
            if 'error' in file_info:
                print(f"   ⚠️  Error reading file: {file_info['error']}")
                continue
            
            print(f"   ✅ File exists: {file_info['size_mb']} MB")
            
            # Detailed analysis
            analysis = self.analyze_dataframe(name, path)
            
            if analysis is None:
                print("   ⚠️  Could not analyze file")
                continue
            
            if 'error' in analysis:
                print(f"   ⚠️  Analysis error: {analysis['error']}")
                continue
            
            # Print analysis
            print(f"   📈 Rows: {analysis['total_rows']:,}")
            print(f"   📊 Columns: {analysis['total_columns']}")
            print(f"   📅 Date Range: {analysis['date_range_start']} to {analysis['date_range_end']}")
            
            if analysis.get('avg_frequency_hours'):
                print(f"   ⏱️  Average Frequency: {analysis['avg_frequency_hours']} hours")
            
            # Data quality
            if analysis['missing_values'] > 0:
                print(f"   ⚠️  Missing Values: {analysis['missing_values']:,} ({analysis['missing_pct']}%)")
            else:
                print(f"   ✅ Missing Values: 0")
            
            if analysis.get('infinite_values', 0) > 0:
                print(f"   ⚠️  Infinite Values: {analysis['infinite_values']}")
            
            if analysis['duplicates'] > 0:
                print(f"   ⚠️  Duplicate Rows: {analysis['duplicates']}")
            
            # Sample statistics
            if 'sample_stats' in analysis:
                stats = analysis['sample_stats']
                print(f"   📊 Sample Stats ({stats['column']}):")
                print(f"      Mean: {stats['mean']}, Std: {stats['std']}")
                print(f"      Range: [{stats['min']}, {stats['max']}]")
            
            print(f"   💾 Memory Usage: {analysis['memory_usage_mb']} MB")
        
        # Data alignment check
        self.validate_data_alignment()
        
        # Final recommendations
        print("\n" + "="*70)
        print("RECOMMENDATIONS")
        print("="*70)
        
        missing_sources = []
        for name, path in self.data_paths.items():
            if not os.path.exists(path):
                missing_sources.append(name)
        
        if missing_sources:
            print("\n⚠️  Missing Data Sources:")
            for source in missing_sources:
                if 'OHLCV' in source:
                    print(f"   • {source}: Run 'python main.py fetch'")
                elif 'Blockchain' in source:
                    print(f"   • {source}: Run 'python main.py fetch-blockchain'")
                elif 'Sentiment' in source:
                    print(f"   • {source}: Run 'python main.py fetch-sentiment'")
                elif 'Macro' in source:
                    print(f"   • {source}: Run 'python main.py fetch-macro'")
                elif 'Features Complete' in source:
                    print(f"   • {source}: Run 'python main.py features-complete'")
        else:
            print("\n✅ All data sources present!")
        
        # Check if ready for training
        features_complete = os.path.exists(self.data_paths['Features Complete'])
        ohlcv_exists = os.path.exists(self.data_paths['OHLCV'])
        
        print("\n🎯 Training Readiness:")
        if features_complete:
            print("   ✅ Ready to train ML models!")
            print("   Next step: python main.py train")
        elif ohlcv_exists:
            print("   ⚠️  Basic data available, but missing complete features")
            print("   Recommendation: python main.py fetch-all && python main.py features-complete")
        else:
            print("   ❌ Not ready for training")
            print("   Required: python main.py fetch-all && python main.py features-complete")
        
        print("\n" + "="*70)
        print("END OF REPORT")
        print("="*70 + "\n")

def main():
    """Main execution."""
    validator = DataValidator()
    validator.generate_report()

if __name__ == '__main__':
    main()