"""
train_model.py

PHASE 2: MODEL TRAINING

Trains a predictive model (Random Forest Regressor) to forecast
a player's fantasy points (total_fpts) in their next game.

1. Loads the feature-engineered dataset (data/ml_training_set.csv).
2. Trains the model.
3. Evaluates the model using Mean Absolute Error (MAE).
4. Saves the trained model object for later use.
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

# Define the path to the training data
TRAINING_DATA_PATH = "data/ml_training_set.csv"
MODEL_SAVE_PATH = "models/fpts_predictor.pkl"


def train_and_evaluate():
    print("=" * 70)
    print("PHASE 2: TRAINING MACHINE LEARNING MODEL")
    print("=" * 70)

    # --- 1. Load Data ---
    try:
        df = pd.read_csv(TRAINING_DATA_PATH)
        print(f"1. Successfully loaded {len(df)} rows from {TRAINING_DATA_PATH}")
    except FileNotFoundError:
        print(
            f"ERROR: Training data not found at {TRAINING_DATA_PATH}. Run build_training_set.py first."
        )
        return

    # --- 2. Prepare Features (X) and Target (y) ---

    # Target: the actual fantasy points scored
    y = df["target_fpts"]

    # Features (X): Drop the columns we don't want the model to see (identifiers, target)
    features_to_drop = [
        "player_id",
        "game_id",
        "game_date",
        "team_abbrev_player",
        "opponent_abbrev",
        "target_fpts",
    ]
    X = df.drop(columns=features_to_drop)

    # Ensure all features are numeric (they should be, due to feature engineering)
    X = X.apply(pd.to_numeric, errors="coerce").fillna(0)

    # --- 3. Split Data ---
    # Use 80% for training, 20% for testing (unseen data)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(
        f"2. Data split: Training set size={len(X_train)}, Test set size={len(X_test)}"
    )

    # --- 4. Choose and Train Model (Random Forest Regressor) ---
    print("3. Training Random Forest Regressor (100 estimators)...")

    # n_jobs=-1 uses all CPU cores for faster training
    model = RandomForestRegressor(
        n_estimators=100, random_state=42, max_depth=15, min_samples_leaf=5, n_jobs=-1
    )
    model.fit(X_train, y_train)
    print("   Training complete.")

    # --- 5. Evaluate Model ---
    predictions = model.predict(X_test)

    # Mean Absolute Error (MAE): How many points the model is off by on average
    mae = mean_absolute_error(y_test, predictions)
    # R-squared: How much variance the model explains (closer to 1.0 is better)
    r2 = r2_score(y_test, predictions)

    print("\n4. Model Evaluation on Test Data:")
    print(f"   Mean Absolute Error (MAE): {mae:.2f} FPts")
    print(f"   R-squared (R2 Score): {r2:.3f}")

    if mae < 2.0:
        print("   ✅ Prediction Accuracy is good (Avg error less than 2 FPts).")
    else:
        print(
            "   ⚠️ Prediction Accuracy is high (Error > 2 FPts). Consider more feature engineering."
        )

    # --- 6. Save Model ---
    print("\n5. Saving trained model...")
    joblib.dump(model, MODEL_SAVE_PATH)
    print(f"   ✅ Model saved successfully to {MODEL_SAVE_PATH}")

    # --- BONUS: Feature Importance ---
    # See which features the model relies on most (for debugging/improvement)
    feature_importance = pd.Series(model.feature_importances_, index=X.columns)
    top_10_features = feature_importance.nlargest(10)

    print("\nTop 10 Most Important Features:")
    print(top_10_features)


if __name__ == "__main__":
    # Ensure scikit-learn and joblib are installed: pip install scikit-learn joblib
    train_and_evaluate()
