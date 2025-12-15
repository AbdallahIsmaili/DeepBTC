# fix_macro_data.py - Quick fix for macro data CSV format

import pandas as pd
import os

def fix_macro_csv():
    """Fixes the macro_indicators.csv format to ensure proper date index."""
    
    macro_path = 'data/raw/macro_indicators.csv'
    
    if not os.path.exists(macro_path):
        print(f"❌ File not found: {macro_path}")
        return False
    
    print("🔧 Fixing macro data CSV format...")
    
    try:
        # Load the CSV
        df = pd.read_csv(macro_path)
        
        print(f"   Original shape: {df.shape}")
        print(f"   Original columns: {list(df.columns)}")
        
        # Find the date column
        date_col = None
        for col in ['Date', 'date', 'Datetime', 'datetime', 'Unnamed: 0']:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            # Use first column as date
            date_col = df.columns[0]
            print(f"   Using first column as date: {date_col}")
        
        # Convert to datetime
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Set as index
        df.set_index(date_col, inplace=True)
        
        # Rename index to 'Date' for consistency
        df.index.name = 'Date'
        
        # Sort by date
        df.sort_index(inplace=True)
        
        # Save back
        backup_path = macro_path.replace('.csv', '_backup.csv')
        
        # Create backup
        print(f"   Creating backup: {backup_path}")
        import shutil
        shutil.copy(macro_path, backup_path)
        
        # Save fixed version
        df.to_csv(macro_path)
        
        print(f"✅ Fixed macro data!")
        print(f"   New shape: {df.shape}")
        print(f"   Date range: {df.index.min()} to {df.index.max()}")
        print(f"   Columns: {list(df.columns)}")
        print(f"\n   Backup saved to: {backup_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == '__main__':
    print("="*60)
    print("MACRO DATA CSV FORMAT FIXER")
    print("="*60)
    
    success = fix_macro_csv()
    
    if success:
        print("\n✅ Success! Now run: python main.py features-complete")
    else:
        print("\n❌ Failed to fix macro data")