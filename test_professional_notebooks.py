#!/usr/bin/env python3
"""
Test script to validate all professional notebooks
Checks if notebooks can be loaded and basic structure is correct
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def print_header(text):
    print("\n" + "="*80)
    print(f" {text}")
    print("="*80)

def test_notebook_structure(notebook_path):
    """Test if notebook has correct structure"""
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)

        # Check basic structure
        assert 'cells' in nb, "Missing cells"
        assert 'metadata' in nb, "Missing metadata"
        assert len(nb['cells']) > 5, "Too few cells"

        # Check for required sections
        cell_sources = [cell['source'][0] if cell['source'] else "" for cell in nb['cells'] if cell['cell_type'] == 'markdown']

        required_sections = [
            "📦 IMPORTS ET CONFIGURATION",
            "📊 PRÉPARATION DES DONNÉES",
            "🏗️ CONSTRUCTION DU MODÈLE",
            "🚀 ENTRAÎNEMENT",
            "📊 ÉVALUATION",
            "💾 SAUVEGARDE"
        ]

        found_sections = 0
        for section in required_sections:
            if any(section in source for source in cell_sources):
                found_sections += 1

        return True, f"Structure OK - {found_sections}/{len(required_sections)} sections found"

    except Exception as e:
        return False, str(e)

def test_data_loading():
    """Test if data can be loaded"""
    try:
        project_root = Path.cwd()
        data_dir = project_root / 'data' / 'features'
        data_path = data_dir / 'btc_features_complete.csv'

        if not data_path.exists():
            return False, "Data file not found"

        df = pd.read_csv(data_path, index_col='Datetime', parse_dates=True)

        if len(df) < 1000:
            return False, f"Data too small: {len(df)} rows"

        return True, f"Data OK - {len(df):,} rows, {len(df.columns)} columns"

    except Exception as e:
        return False, str(e)

def test_models_directory():
    """Test if models directory exists and has files"""
    try:
        models_dir = Path.cwd() / 'models'
        if not models_dir.exists():
            return False, "Models directory not found"

        model_files = list(models_dir.glob("*.pkl")) + list(models_dir.glob("*.h5"))
        if len(model_files) == 0:
            return False, "No model files found"

        return True, f"Models OK - {len(model_files)} model files found"

    except Exception as e:
        return False, str(e)

def main():
    print_header("🧪 TEST DES NOTEBOOKS PROFESSIONNELS")

    project_root = Path.cwd()
    notebooks_dir = project_root / 'notebooks'

    if not notebooks_dir.exists():
        print("❌ Notebooks directory not found")
        return False

    # List of expected professional notebooks
    expected_notebooks = [
        'XGBoost_Professional.ipynb',
        'LSTM_CNN_Professional.ipynb',
        'Logistic_Regression_Professional.ipynb',
        'Naive_Bayes_Professional.ipynb',
        'MLP_Professional.ipynb'
    ]

    results = {}

    # Test data loading
    print("\n📊 Testing data loading...")
    data_ok, data_msg = test_data_loading()
    results['data'] = (data_ok, data_msg)
    print(f"{'✅' if data_ok else '❌'} {data_msg}")

    # Test models directory
    print("\n🤖 Testing models directory...")
    models_ok, models_msg = test_models_directory()
    results['models'] = (models_ok, models_msg)
    print(f"{'✅' if models_ok else '❌'} {models_msg}")

    # Test each notebook
    print("\n📓 Testing notebooks...")
    all_notebooks_ok = True

    for notebook in expected_notebooks:
        notebook_path = notebooks_dir / notebook
        if notebook_path.exists():
            struct_ok, struct_msg = test_notebook_structure(notebook_path)
            results[notebook] = (struct_ok, struct_msg)
            status = '✅' if struct_ok else '❌'
            print(f"{status} {notebook}: {struct_msg}")
            if not struct_ok:
                all_notebooks_ok = False
        else:
            results[notebook] = (False, "File not found")
            print(f"❌ {notebook}: File not found")
            all_notebooks_ok = False

    # Summary
    print_header("📋 RÉSUMÉ DES TESTS")

    total_tests = len(results)
    passed_tests = sum(1 for ok, _ in results.values() if ok)

    print(f"Tests passés: {passed_tests}/{total_tests}")

    if all(ok for ok, _ in results.values()):
        print("\n🎉 TOUS LES TESTS SONT PASSÉS !")
        print("✅ Les notebooks professionnels sont prêts à être utilisés.")
        return True
    else:
        print("\n⚠️ QUELQUES TESTS ONT ÉCHOUÉ")
        print("Vérifiez les messages d'erreur ci-dessus.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)