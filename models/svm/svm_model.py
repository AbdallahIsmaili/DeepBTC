# models/svm/svm_model.py

import os
import joblib
 
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, classification_report

from svm_config import SVM_PARAMS, TEST_SIZE
from svm_utils import load_features, split_features_target


class SVMTrainer:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()

    # 1️⃣ Load & prepare data
    def prepare_data(self):
        print("📥 Loading features...")
        df = load_features()

        X, y = split_features_target(
            df,
            target_col="target_direction_1h"
        )

        print("📊 Target distribution:")
        print(y.value_counts(normalize=True))

        return X, y

    # 2️⃣ Time-based split (no shuffling)
    def split_data(self, X, y):
        split_index = int(len(X) * (1 - TEST_SIZE))

        X_train = X.iloc[:split_index]
        X_test = X.iloc[split_index:]

        y_train = y.iloc[:split_index]
        y_test = y.iloc[split_index:]

        return X_train, X_test, y_train, y_test

    # 3️⃣ Feature scaling
    def scale_data(self, X_train, X_test):
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        return X_train_scaled, X_test_scaled

    # 4️⃣ Train SVM
    def train(self, X_train, y_train):
        print("🤖 Training SVM model...")
        self.model = SVC(**SVM_PARAMS)
        self.model.fit(X_train, y_train)

    # 5️⃣ Evaluate model
    def evaluate(self, X_test, y_test):
        print("📊 Evaluating model...")
        y_pred = self.model.predict(X_test)

        print("Accuracy:", accuracy_score(y_test, y_pred))
        print(classification_report(y_test, y_pred))

    # 6️⃣ Save model & scaler
    def save_artifacts(self):
        artifacts_dir = os.path.join(
            os.path.dirname(__file__),
            "artifacts"
        )
        os.makedirs(artifacts_dir, exist_ok=True)

        joblib.dump(
            self.model,
            os.path.join(artifacts_dir, "svm_model.pkl")
        )
        joblib.dump(
            self.scaler,
            os.path.join(artifacts_dir, "scaler.pkl")
        )

        print("💾 Model & scaler saved successfully")

    # 🔥 Full pipeline
    def run(self):
        X, y = self.prepare_data()
        X_train, X_test, y_train, y_test = self.split_data(X, y)
        X_train, X_test = self.scale_data(X_train, X_test)
        self.train(X_train, y_train)
        self.evaluate(X_test, y_test)
        self.save_artifacts()


if __name__ == "__main__":
    trainer = SVMTrainer()
    trainer.run()
