# main.py - Enhanced Version

import argparse
import sys
import os
import importlib.util

# --- Helper function to load modules dynamically ---
def load_module_from_path(module_name, path):
    """Loads a module from a specific file path."""
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None:
        raise FileNotFoundError(f"Could not find module file: {path}")
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

# --- Load the modules from the 'api' directory ---
def load_modules():
    """Loads all API modules."""
    modules = {}
    API_DIR = os.path.join(os.path.dirname(__file__), 'api')
    
    module_files = {
        'fetch_historical': 'fetch_historical.py',
        'realtime_listener': 'realtime_listener.py',
        'feature_engine': 'feature_engine.py',
        'fetch_blockchain_metrics': 'fetch_blockchain_metrics.py',
        'fetch_sentiment': 'fetch_sentiment.py',
        'fetch_macro_data': 'fetch_macro_data.py',
        'feature_engine': 'feature_engine.py'
    }
    
    for module_name, filename in module_files.items():
        try:
            modules[module_name] = load_module_from_path(
                module_name, 
                os.path.join(API_DIR, filename)
            )
        except FileNotFoundError:
            print(f"⚠️  Module not found: {filename}")
            modules[module_name] = None
    
    return modules

def print_header(text):
    """Prints a formatted header."""
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60 + "\n")

def main():
    parser = argparse.ArgumentParser(
        description="DeepBTC: ML/DL Bitcoin Investment Strategies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py fetch-all          # Fetch ALL data sources
  python main.py fetch              # Fetch only OHLCV data
  python main.py fetch-blockchain   # Fetch blockchain metrics
  python main.py fetch-sentiment    # Fetch sentiment data
  python main.py fetch-macro        # Fetch macroeconomic data
  python main.py features           # Generate basic features
  python main.py features-complete  # Generate ALL features (recommended)
  python main.py live               # Start real-time stream
  python main.py train              # Train ML model
        """
    )
    
    parser.add_argument(
        'action', 
        type=str, 
        choices=[
            'fetch', 'fetch-all', 'fetch-blockchain', 'fetch-sentiment', 
            'fetch-macro', 'features', 'features-complete', 'live', 'train'
        ],
        help="Action to perform (see examples below)"
    )
    
    args = parser.parse_args()
    
    # Load modules
    print_header("LOADING MODULES")
    modules = load_modules()
    
    # Execute action
    print_header(f"EXECUTING ACTION: {args.action.upper()}")
    
    if args.action == 'fetch':
        # Fetch only OHLCV data
        if modules['fetch_historical']:
            modules['fetch_historical'].main()
        else:
            print("❌ fetch_historical.py not found")
    
    elif args.action == 'fetch-all':
        # Fetch ALL data sources
        print("📊 Fetching all data sources...\n")
        
        # 1. OHLCV
        print_header("1/4: FETCHING OHLCV DATA")
        if modules['fetch_historical']:
            modules['fetch_historical'].main()
        
        # 2. Blockchain metrics
        print_header("2/4: FETCHING BLOCKCHAIN METRICS")
        if modules['fetch_blockchain_metrics']:
            modules['fetch_blockchain_metrics'].main()
        else:
            print("⚠️  fetch_blockchain_metrics.py not found. Please create it.")
        
        # 3. Sentiment data
        print_header("3/4: FETCHING SENTIMENT DATA")
        if modules['fetch_sentiment']:
            modules['fetch_sentiment'].main()
        else:
            print("⚠️  fetch_sentiment.py not found. Please create it.")
        
        # 4. Macro data
        print_header("4/4: FETCHING MACROECONOMIC DATA")
        if modules['fetch_macro_data']:
            modules['fetch_macro_data'].main()
        else:
            print("⚠️  fetch_macro_data.py not found. Please create it.")
        
        print_header("✅ ALL DATA SOURCES FETCHED")
        print("Next step: python main.py features-complete")
    
    elif args.action == 'fetch-blockchain':
        # Fetch only blockchain metrics
        if modules['fetch_blockchain_metrics']:
            modules['fetch_blockchain_metrics'].main()
        else:
            print("❌ fetch_blockchain_metrics.py not found")
    
    elif args.action == 'fetch-sentiment':
        # Fetch only sentiment data
        if modules['fetch_sentiment']:
            modules['fetch_sentiment'].main()
        else:
            print("❌ fetch_sentiment.py not found")
    
    elif args.action == 'fetch-macro':
        # Fetch only macroeconomic data
        if modules['fetch_macro_data']:
            modules['fetch_macro_data'].main()
        else:
            print("❌ fetch_macro_data.py not found")
    
    elif args.action == 'features':
        # Generate basic features (original feature_engine.py)
        if modules['feature_engine']:
            data_df = modules['feature_engine'].load_data()
            if data_df is not None:
                features_df = modules['feature_engine'].generate_features(data_df)
                modules['feature_engine'].save_features(features_df)
        else:
            print("❌ feature_engine.py not found")
    
    elif args.action == 'features-complete':
        # Generate ALL features using enhanced engine
        if modules['feature_engine']:
            modules['feature_engine'].main()
        else:
            print("❌ feature_engine.py not found")
            print("Falling back to basic feature engine...")
            if modules['feature_engine']:
                data_df = modules['feature_engine'].load_data()
                if data_df is not None:
                    features_df = modules['feature_engine'].generate_features(data_df)
                    modules['feature_engine'].save_features(features_df)
    
    elif args.action == 'live':
        # Start real-time WebSocket listener
        if modules['realtime_listener']:
            print("Starting real-time listener... (Press Ctrl+C to stop)")
            modules['realtime_listener'].start_websocket_stream()
        else:
            print("❌ realtime_listener.py not found")
    
    elif args.action == 'train':
        # Placeholder for model training
        print("🤖 Model training script not yet implemented.")
        print("\nNext steps to implement:")
        print("  1. Create api/train_model.py")
        print("  2. Load features from data/features/btc_features_complete.csv")
        print("  3. Split into train/validation/test sets")
        print("  4. Build LSTM/Transformer model with TensorFlow/Keras")
        print("  5. Train with appropriate loss function")
        print("  6. Evaluate and save the model")
        print("\nRecommended model architectures:")
        print("  - LSTM for time series prediction")
        print("  - Transformer with attention mechanism")
        print("  - Ensemble: XGBoost + LSTM")
    
    else:
        parser.print_help()

if __name__ == '__main__':
    main()