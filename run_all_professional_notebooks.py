#!/usr/bin/env python3
"""
Script to run all professional notebooks and validate >80% accuracy
This script can be used to automatically test all models
"""

import os
import sys
import subprocess
import time
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def print_header(text):
    print("\n" + "="*80)
    print(f" {text}")
    print("="*80)

def run_notebook(notebook_path, timeout=1800):  # 30 minutes timeout
    """Run a Jupyter notebook using nbconvert"""
    try:
        print(f"🚀 Running {notebook_path.name}...")

        # Convert and execute notebook
        cmd = [
            sys.executable, "-m", "jupyter", "nbconvert",
            "--to", "notebook", "--execute", "--inplace",
            "--ExecutePreprocessor.timeout=" + str(timeout),
            str(notebook_path)
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=notebook_path.parent
        )

        if result.returncode == 0:
            print(f"✅ {notebook_path.name} executed successfully")
            return True, "Success"
        else:
            print(f"❌ {notebook_path.name} failed")
            print(f"Error: {result.stderr}")
            return False, result.stderr

    except subprocess.TimeoutExpired:
        print(f"⏰ {notebook_path.name} timed out after {timeout}s")
        return False, "Timeout"
    except Exception as e:
        print(f"❌ {notebook_path.name} error: {str(e)}")
        return False, str(e)

def check_accuracy_achieved(notebook_path):
    """Check if the notebook achieved >80% accuracy"""
    try:
        # Read the executed notebook
        import json
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)

        # Look for accuracy output in code cells
        accuracy_found = False
        max_accuracy = 0.0

        for cell in nb['cells']:
            if cell['cell_type'] == 'code' and 'outputs' in cell:
                for output in cell['outputs']:
                    if 'text' in output:
                        text = ' '.join(output['text'])
                        # Look for accuracy patterns
                        if 'Accuracy:' in text or 'accuracy:' in text:
                            try:
                                # Extract percentage
                                import re
                                acc_match = re.search(r'(\d+\.\d+)%', text)
                                if acc_match:
                                    acc = float(acc_match.group(1))
                                    max_accuracy = max(max_accuracy, acc)
                                    accuracy_found = True
                            except:
                                pass

        if accuracy_found:
            if max_accuracy > 80.0:
                return True, f"Accuracy {max_accuracy:.1f}% (>80% ✓)"
            else:
                return False, f"Accuracy {max_accuracy:.1f}% (≤80% ✗)"
        else:
            return False, "Accuracy not found in output"

    except Exception as e:
        return False, f"Error checking accuracy: {str(e)}"

def main():
    print_header("🚀 EXECUTION DES NOTEBOOKS PROFESSIONNELS")

    project_root = Path.cwd()
    notebooks_dir = project_root / 'notebooks'

    if not notebooks_dir.exists():
        print("❌ Notebooks directory not found")
        return False

    # Professional notebooks to run
    professional_notebooks = [
        'XGBoost_Professional.ipynb',
        'Logistic_Regression_Professional.ipynb',
        'Naive_Bayes_Professional.ipynb',
        'MLP_Professional.ipynb',
        'LSTM_CNN_Professional.ipynb'  # Last because it's the most complex
    ]

    results = {}

    print("⚠️ Note: Cette exécution peut prendre plusieurs heures...")
    print("💡 Vous pouvez arrêter avec Ctrl+C et relancer plus tard")
    print()

    for notebook_name in professional_notebooks:
        notebook_path = notebooks_dir / notebook_name

        if not notebook_path.exists():
            print(f"❌ {notebook_name} not found")
            results[notebook_name] = (False, "File not found")
            continue

        # Run the notebook
        success, message = run_notebook(notebook_path)

        if success:
            # Check if accuracy >80% was achieved
            acc_success, acc_message = check_accuracy_achieved(notebook_path)
            results[notebook_name] = (acc_success, acc_message)
            status = '✅' if acc_success else '⚠️'
            print(f"{status} {notebook_name}: {acc_message}")
        else:
            results[notebook_name] = (False, message)
            print(f"❌ {notebook_name}: {message}")

        # Small delay between notebooks
        time.sleep(5)

    # Summary
    print_header("📋 RÉSUMÉ FINAL")

    successful_notebooks = sum(1 for success, _ in results.values() if success)
    total_notebooks = len(results)

    print(f"Notebooks exécutés avec succès: {successful_notebooks}/{total_notebooks}")

    if successful_notebooks == total_notebooks:
        print("\n🎉 TOUS LES NOTEBOOKS ONT ATTEINT >80% ACCURACY !")
        print("🏆 Mission accomplie - Code 100% fonctionnel !")
        return True
    else:
        print(f"\n⚠️ {total_notebooks - successful_notebooks} notebook(s) n'ont pas atteint l'objectif >80%")
        print("Vérifiez les messages d'erreur ci-dessus.")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️ Exécution interrompue par l'utilisateur")
        sys.exit(1)