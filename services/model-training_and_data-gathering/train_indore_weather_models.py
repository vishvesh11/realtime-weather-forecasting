import pandas as pd
import numpy as np
import json
import joblib  # For saving the model and scaler
from sklearn.preprocessing import StandardScaler
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, accuracy_score, precision_score, recall_score, \
    f1_score, roc_auc_score

# --- Config---
DATA_FILE_PATH = "/content/historical_weather_hourly_Indore_Palasia.jsonl"

PREDICTION_HORIZON_HOURS = 12


LAG_FEATURES = [
    1, 2, 3, 6, 12,  # Recent past
    24,  # Same hour yesterday
    24 * 2,  # Same hour two days ago
    24 * 7  # Same hour last week
]


API_FEATURES = [
    'temp', 'precip', 'rh', 'pres', 'wind_spd', 'clouds', 'vis', 'dewpt', 'app_temp', 'uv', 'solar_rad',
    'wind_dir', 'wind_gust_spd', 'slp', 'dhi', 'dni', 'ghi'
]


TEMP_MODEL_OUTPUT_PATH = "indore_hourly_temp_lgbm_model.joblib"
PRECIP_MODEL_OUTPUT_PATH = "indore_hourly_precip_lgbm_model.joblib"
SCALER_OUTPUT_PATH = "indore_hourly_weather_scaler.joblib"
FEATURE_NAMES_OUTPUT_PATH = "indore_weather_feature_names.joblib"


# --- Data Loading and Preprocessing ---

def load_and_preprocess_data(file_path, prediction_horizon_hours, lag_features, api_features):
    print(f"Loading data from {file_path}...")

    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                continue

    df = pd.DataFrame(data)

    print(f"Initial data shape: {df.shape}")
    print("Columns available:", df.columns.tolist())

    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d:%H')
    df = df.set_index('datetime').sort_index()

    # Drop duplicate indices
    df = df[~df.index.duplicated(keep='first')]

    if 'pod' in df.columns:
        df['is_night_time'] = (df['pod'] == 'n').astype(int)
        print("Created 'is_night_time' feature from 'pod'.")
    else:
        print("Warning: 'pod' column not found in data. 'is_night_time' feature will not be created.")

    actual_api_numeric_features = [f for f in api_features if f in df.columns and pd.api.types.is_numeric_dtype(df[f])]

    missing_api_features = [f for f in api_features if f not in df.columns]
    if missing_api_features:
        print(
            f"Warning: The following FEATURES not found skipping: {missing_api_features}")

    print(f"Shape before imputation of {len(actual_api_numeric_features)} actual numeric API features: {df.shape}")
    df[actual_api_numeric_features] = df[actual_api_numeric_features].ffill().bfill()
    print(f"Shape after handling duplicates and basic imputation: {df.shape}")

    initial_numeric_feature_count = len(actual_api_numeric_features)
    actual_api_numeric_features = [col for col in actual_api_numeric_features if df[col].isnull().sum() < len(df)]
    if len(actual_api_numeric_features) < initial_numeric_feature_count:
        removed_all_nan_features = set(api_features) - set(actual_api_numeric_features)
        print(f"Warning: Removed API features that were entirely NaN: {list(removed_all_nan_features)}")

    # --- Feature Engineering ---
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['day_of_year'] = df.index.dayofyear
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['is_weekend'] = (df.index.dayofweek >= 5).astype(int)

    features_for_lagging = list(set(['temp', 'precip'] + actual_api_numeric_features))  # Ensure existence and unique
    features_for_lagging = [f for f in features_for_lagging if f in df.columns and pd.api.types.is_numeric_dtype(df[f])]

    print(f"Generating lagged features for: {features_for_lagging}")
    for feature in features_for_lagging:
        for lag in lag_features:
            df[f'{feature}_lag_{lag}h'] = df[feature].shift(lag).copy()

    print(f"Features engineered. Current shape: {df.shape}")

    # Target Variables
    df['temp_target'] = df['temp'].shift(-prediction_horizon_hours).copy()
    df['precip_future'] = df['precip'].shift(-prediction_horizon_hours).copy()
    df['precip_chance_target'] = (df['precip_future'] > 0.0).astype(int).copy()

    #  NaNs dropping
    print("\n--- NaN counts before final dropna ---")
    nan_counts = df.isnull().sum()
    print(nan_counts[nan_counts > 0].sort_values(ascending=False))
    print(f"Total rows before dropna: {len(df)}")

    required_cols_for_training = [col for col in df.columns if
                                  '_lag_' in col or '_target' in col or 'is_night_time' in col]
    required_cols_for_training += ['hour', 'day_of_week', 'day_of_year', 'month', 'year',
                                   'is_weekend']
    required_cols_for_training += actual_api_numeric_features

    required_cols_for_training = [col for col in required_cols_for_training if col in df.columns]

    df.dropna(subset=required_cols_for_training, inplace=True)

    print(f"Shape after dropping NaNs (due to lags and targets, subset of critical columns): {df.shape}")

    if df.empty:
        raise ValueError(
            "DataFrame is empty after dropping NaNs. This means you don't have enough valid data after feature engineering or too many NaNs in critical columns.")

    columns_to_exclude = [
        'temp_target', 'precip_chance_target', 'precip_future',
        'datetime', 'ts', 'timestamp_local', 'timestamp_utc', 'revision_status',
        'location_name', 'lat', 'lon',
        'pod', 'weather',
        'h_angle'
    ]

    feature_cols = [col for col in df.columns if col not in columns_to_exclude]

    feature_cols = [col for col in feature_cols if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]

    X = df[feature_cols]

    feature_columns_ordered = X.columns.tolist()
    X = X[feature_columns_ordered]  # Reordering

    y_temp = df['temp_target']
    y_precip_chance = df['precip_chance_target']

    # Train Test Split (Chronological)
    split_point = int(len(df) * 0.8)

    X_train, X_test = X.iloc[:split_point], X.iloc[split_point:]
    y_temp_train, y_temp_test = y_temp.iloc[:split_point], y_temp.iloc[split_point:]
    y_precip_chance_train, y_precip_chance_test = y_precip_chance.iloc[:split_point], y_precip_chance.iloc[split_point:]

    print(f"\nTrain data shape: {X_train.shape}, Test data shape: {X_test.shape}")
    print(f"Train date range: {X_train.index.min()} to {X_train.index.max()}")
    print(f"Test date range: {X_test.index.min()} to {X_test.index.max()}")
    scaler = StandardScaler()

    if X_train.empty:
        raise ValueError("X_train is empty before scaling. Check data loading and preprocessing steps.")

    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    X_train_scaled_df = pd.DataFrame(X_train_scaled, columns=X_train.columns, index=X_train.index)
    X_test_scaled_df = pd.DataFrame(X_test_scaled, columns=X_test.columns, index=X_test.index)

    return X_train_scaled_df, X_test_scaled_df, \
        y_temp_train, y_temp_test, \
        y_precip_chance_train, y_precip_chance_test, \
        scaler, feature_columns_ordered


# --- Model Training Function ---
def train_lgbm_model(X_train, y_train, model_type='regression'):
    """
    Trains a LightGBM model based on the specified type.
    """
    print(f"\nTraining LightGBM {model_type} model...")
    if X_train.empty:
        raise ValueError(f"Cannot train {model_type} model: X_train is empty.")

    if model_type == 'regression':
        model = lgb.LGBMRegressor(
            objective='regression_l1',  # MAE objective for robustness
            metric='mae',
            n_estimators=1000,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=-1,
            min_child_samples=20,
            random_state=42,
            n_jobs=-1
        )
    elif model_type == 'classification':
        model = lgb.LGBMClassifier(
            objective='binary',
            metric='binary_logloss',  # Log loss for probabilities
            n_estimators=1000,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=-1,
            min_child_samples=20,
            random_state=42,
            n_jobs=-1,
            is_unbalance=True  # Useful if there are many more 'no precip' samples than 'precip'
        )
    else:
        raise ValueError("model_type must be 'regression' or 'classification'")

    model.fit(X_train, y_train)
    print(f"LightGBM {model_type} model training complete.")
    return model


# Evaluation Function
def evaluate_model(model, X_test, y_test, model_type='regression', target_name=''):
    """
    Evaluates the trained model on the test set.
    """
    print(f"\nEvaluating {target_name} model performance ({model_type})...")

    if X_test.empty or y_test.empty:
        print(f"  Skipping evaluation: Test data for {target_name} is empty.")
        return

    if model_type == 'regression':
        predictions = model.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        rmse = np.sqrt(mean_squared_error(y_test, predictions))

        print(f"  Mean Absolute Error (MAE): {mae:.2f}")
        print(f"  Root Mean Squared Error (RMSE): {rmse:.2f}")

        # Sample predictions
        sample_df = pd.DataFrame({'Actual': y_test, 'Predicted': predictions}).sample(min(10, len(y_test)),
                                                                                      random_state=42)
        print("\n  Sample Predictions vs Actuals:")
        print(sample_df)

    elif model_type == 'classification':
        # probabilities for 'precipitation chance'
        probabilities = model.predict_proba(X_test)[:, 1]  # Probability of class 1 (precipitation)
        binary_predictions = (probabilities > 0.5).astype(int)  # Convert probabilities to binary using 0.5 threshold

        accuracy = accuracy_score(y_test, binary_predictions)
        precision = precision_score(y_test, binary_predictions, zero_division=0)
        recall = recall_score(y_test, binary_predictions, zero_division=0)
        f1 = f1_score(y_test, binary_predictions, zero_division=0)

        try:
            auc_roc = roc_auc_score(y_test, probabilities)
            print(f"  ROC AUC Score: {auc_roc:.2f}")
        except ValueError:
            print("  ROC AUC cannot be calculated for a single class.")
            auc_roc = np.nan  # Assign NaN if not calculable

        print(f"  Accuracy: {accuracy:.2f}")
        print(f"  Precision: {precision:.2f}")
        print(f"  Recall: {recall:.2f}")
        print(f"  F1-Score: {f1:.2f}")

        # Sample probabilities
        sample_df = pd.DataFrame(
            {'Actual': y_test, 'Predicted_Prob': probabilities, 'Predicted_Binary': binary_predictions}).sample(
            min(10, len(y_test)), random_state=42)
        print("\n  Sample Predictions (Probabilities) vs Actuals:")
        print(sample_df)



if __name__ == "__main__":
    try:
        # Load, preprocess, and split data
        X_train, X_test, \
            y_temp_train, y_temp_test, \
            y_precip_chance_train, y_precip_chance_test, \
            scaler, feature_names = load_and_preprocess_data(
            DATA_FILE_PATH, PREDICTION_HORIZON_HOURS, LAG_FEATURES, API_FEATURES
        )

        # Check if X_train is empty
        if X_train.empty:
            print("Error: X_train is empty after preprocessing. Cannot proceed with training.")
        else:
            print(f"\nProceeding with training. X_train shape: {X_train.shape}")

            # Train Temp Model
            temp_model = train_lgbm_model(X_train, y_temp_train, model_type='regression')
            evaluate_model(temp_model, X_test, y_temp_test, model_type='regression', target_name='Temperature')
            joblib.dump(temp_model, TEMP_MODEL_OUTPUT_PATH)
            print(f"Temperature model saved to: {TEMP_MODEL_OUTPUT_PATH}")

            #Train Precipitation Model
            precip_model = train_lgbm_model(X_train, y_precip_chance_train, model_type='classification')
            evaluate_model(precip_model, X_test, y_precip_chance_test, model_type='classification',
                           target_name='Precipitation Chance')
            joblib.dump(precip_model, PRECIP_MODEL_OUTPUT_PATH)
            print(f"Precipitation Chance model saved to: {PRECIP_MODEL_OUTPUT_PATH}")

            joblib.dump(scaler, SCALER_OUTPUT_PATH)
            joblib.dump(feature_names, FEATURE_NAMES_OUTPUT_PATH)
            print(f"Scaler saved to: {SCALER_OUTPUT_PATH}")
            print(f"Feature names saved to: {FEATURE_NAMES_OUTPUT_PATH}")

    except FileNotFoundError:
        print(f"Error: Data file not found at {DATA_FILE_PATH}. Please ensure the path is correct and the file exists.")
    except Exception as e:
        print(f"An unexpected error occurred during training: {e}")
        import traceback

        traceback.print_exc()
