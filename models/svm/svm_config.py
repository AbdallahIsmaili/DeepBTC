# models/svm/svm_config.py

SVM_PARAMS = {
    "kernel": "rbf",
    "C": 1.0,
    "gamma": "scale",
    "probability": True,
    "random_state": 42
}

TEST_SIZE = 0.2
TARGET_TYPE = "direction"  # direction / return / multiclass
