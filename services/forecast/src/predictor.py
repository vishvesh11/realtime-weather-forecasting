import json
import os
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import joblib
from datetime import datetime, timedelta
import pytz

# --- Configuration ---
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
PROCESSED_WEATHER_TOPIC = "processed-weather-data"
PREDICTIONS_TOPIC = "weather-predictions"


MODELS_DIR = "src"


TEMP_MODEL_PATH_HOURLY = "ml-models/indore_hourly_temp_lgbm_model.joblib"
PRECIP_MODEL_PATH_HOURLY = "ml-models/indore_hourly_precip_lgbm_model.joblib"
SCALER_PATH_HOURLY = "ml-models/indore_hourly_weather_scaler.joblib"
FEATURE_NAMES_PATH_HOURLY = "ml-models/indore_hourly_feature_names.joblib"
# Daily Model Assets
MIN_TEMP_MODEL_PATH_DAILY = "ml-models/indore_daily_min_temp_lgbm_model.joblib"
MAX_TEMP_MODEL_PATH_DAILY = "ml-models/indore_daily_max_temp_lgbm_model.joblib"
PRECIP_MODEL_PATH_DAILY = "ml-models/indore_daily_precip_lgbm_model.joblib"
SCALER_PATH_DAILY = "ml-models/indore_daily_weather_scaler.joblib"
FEATURE_NAMES_PATH_DAILY = "ml-models/indore_daily_feature_names.joblib"



PREDICTION_HORIZON_HOURS = 12
DAILY_PREDICTION_HORIZON_DAYS = 14

# Lag features (must match training script's constants)
LAG_FEATURES_HOURLY = [1, 2, 3, 6, 12, 24, 24 * 2, 24 * 7]  # 1, 2, 3, 6, 12, 24, 48, 168 hours
LAG_FEATURES_DAILY = [1, 2, 3, 7, 14, 28, 56]  # 1, 2, 3, 7, 14, 28, 56 days


HOURLY_HISTORY_MAX_SIZE = max(LAG_FEATURES_HOURLY) + 24
global hourly_history_df

hourly_history_df = pd.DataFrame(columns=['datetime', 'temp', 'precip', 'rh', 'pres', 'wind_spd',
                                          'clouds', 'vis', 'dewpt', 'app_temp', 'uv', 'solar_rad',
                                          'wind_dir', 'wind_gust_spd', 'slp', 'dhi', 'dni', 'ghi',
                                          'is_night_time']).set_index('datetime')

DAILY_HISTORY_MAX_SIZE = max(LAG_FEATURES_DAILY) + 30  # Need lags + 30 day buffer
daily_history_df = pd.DataFrame(
    columns=['datetime', 'temp_min', 'temp_max', 'temp_mean', 'precip_sum', 'rh_mean', 'pres_mean',
             'wind_spd_mean', 'clouds_mean', 'vis_mean', 'dewpt_mean', 'app_temp_mean',
             'uv_max', 'solar_rad_sum', 'wind_dir_mean', 'wind_gust_spd_max',
             'slp_mean', 'dhi_sum', 'dni_sum', 'ghi_sum']).set_index('datetime')


# --- Load Models and Scalers ---
def load_models_and_scalers():
    print("Loading models, scalers, and feature names...")
    models = {}
    scalers = {}
    feature_names = {}

    try:
        # Hourly
        models['hourly_temp'] = joblib.load(TEMP_MODEL_PATH_HOURLY)
        models['hourly_precip'] = joblib.load(PRECIP_MODEL_PATH_HOURLY)
        scalers['hourly'] = joblib.load(SCALER_PATH_HOURLY)
        feature_names['hourly'] = joblib.load(FEATURE_NAMES_PATH_HOURLY)

        # Daily
        models['daily_min_temp'] = joblib.load(MIN_TEMP_MODEL_PATH_DAILY)
        models['daily_max_temp'] = joblib.load(MAX_TEMP_MODEL_PATH_DAILY)
        models['daily_precip'] = joblib.load(PRECIP_MODEL_PATH_DAILY)
        scalers['daily'] = joblib.load(SCALER_PATH_DAILY)
        feature_names['daily'] = joblib.load(FEATURE_NAMES_PATH_DAILY)

        print("Models, scalers, and feature names loaded successfully.")
    except FileNotFoundError as e:
        print(f"Error loading model files. Make sure they are in the '{MODELS_DIR}' directory. {e}")
        exit(1)

    return models, scalers, feature_names


MODELS, SCALERS, FEATURE_NAMES = load_models_and_scalers()




def create_hourly_features_for_prediction(data_series, current_utc_dt):
    """
    Creates hourly features for a single data point, including lags.
    Requires `hourly_history_df` to be populated.
    data_series: a pandas Series of the current hour's  features.
    current_utc_dt: datetime object for the current observation.
    """

    # Create a temporary DataFrame
    df_single_obs = pd.DataFrame([data_series], index=[current_utc_dt])
    df_single_obs.index.name = 'datetime'

    # Time-based features
    df_single_obs['hour'] = df_single_obs.index.hour
    df_single_obs['day_of_week'] = df_single_obs.index.dayofweek
    df_single_obs['day_of_year'] = df_single_obs.index.dayofyear
    df_single_obs['month'] = df_single_obs.index.month
    df_single_obs['year'] = df_single_obs.index.year
    df_single_obs['is_weekend'] = (df_single_obs.index.dayofweek >= 5).astype(int)

    # Concatenate the current observation with the global history for lag calculation
    temp_history = pd.concat([hourly_history_df, df_single_obs]).drop_duplicates(
        subset=[df_single_obs.index.name]).sort_index()

    features_for_lagging = [
        'temp', 'precip', 'rh', 'pres', 'wind_spd', 'clouds', 'vis', 'dewpt',
        'app_temp', 'uv', 'solar_rad', 'wind_dir', 'wind_gust_spd', 'slp', 'dhi', 'dni', 'ghi'
    ]
    actual_features_to_lag = [f for f in features_for_lagging if f in temp_history.columns]

    for feature in actual_features_to_lag:
        for lag in LAG_FEATURES_HOURLY:

            df_single_obs[f'{feature}_lag_{lag}h'] = temp_history[feature].shift(lag).loc[df_single_obs.index]


    for col in df_single_obs.columns:
        if '_lag_' in col:
            df_single_obs[col] = df_single_obs[col].fillna(0)  # Common for numerical lags

    for col in FEATURE_NAMES['hourly']:
        if col not in df_single_obs.columns:
            df_single_obs[col] = 0

    # Reorder columns to match the training order
    X_features = df_single_obs[FEATURE_NAMES['hourly']]
    return X_features


def create_daily_features_for_prediction(data_series, current_daily_dt):

    # Create a temporary DataFrame for this single daily observation
    df_single_obs_daily = pd.DataFrame([data_series], index=[current_daily_dt])
    df_single_obs_daily.index.name = 'datetime'

    # Time-based features
    df_single_obs_daily['day_of_week'] = df_single_obs_daily.index.dayofweek
    df_single_obs_daily['day_of_year'] = df_single_obs_daily.index.dayofyear
    df_single_obs_daily['month'] = df_single_obs_daily.index.month
    df_single_obs_daily['year'] = df_single_obs_daily.index.year
    df_single_obs_daily['is_weekend'] = (df_single_obs_daily.index.dayofweek >= 5).astype(int)

    # Lag Features - similar to hourly, but using daily history
    temp_history_daily = pd.concat([daily_history_df, df_single_obs_daily]).drop_duplicates(
        subset=[df_single_obs_daily.index.name]).sort_index()

    features_for_lagging_daily = [col for col in FEATURE_NAMES['daily'] if
                                  '_lag_' not in col and 'day_of_week' not in col and 'day_of_year' not in col and 'month' not in col and 'year' not in col and 'is_weekend' not in col]
    actual_features_to_lag_daily = [f for f in features_for_lagging_daily if f in temp_history_daily.columns]

    for feature in actual_features_to_lag_daily:
        for lag in LAG_FEATURES_DAILY:
            df_single_obs_daily[f'{feature}_lag_{lag}d'] = temp_history_daily[feature].shift(lag).loc[
                df_single_obs_daily.index]

    # Fill any NaNs created by lags
    for col in df_single_obs_daily.columns:
        if '_lag_' in col:
            df_single_obs_daily[col] = df_single_obs_daily[col].fillna(0)

    for col in FEATURE_NAMES['daily']:
        if col not in df_single_obs_daily.columns:
            df_single_obs_daily[col] = 0

    X_features_daily = df_single_obs_daily[FEATURE_NAMES['daily']]
    return X_features_daily


def make_predictions(processed_record):
    predictions = {}
    current_time_utc = datetime.fromtimestamp(processed_record['current']['timestamp_utc'], tz=pytz.utc)

    current_hourly_data_flat = {
        'temp': processed_record['current'].get('temp'),
        'precip': processed_record['current'].get('rain_1h', 0) + processed_record['current'].get('snow_1h', 0),
        # Combine rain/snow for 'precip'
        'rh': processed_record['current'].get('humidity'),
        'pres': processed_record['current'].get('pressure'),
        'wind_spd': processed_record['current'].get('wind_speed'),
        'clouds': processed_record['current'].get('clouds'),
        'vis': processed_record['current'].get('visibility'),
        'dewpt': processed_record['current'].get('dew_point'),
        'app_temp': processed_record['current'].get('feels_like'),
        'uv': processed_record['current'].get('uvi'),
        'solar_rad': processed_record['current'].get('solar_rad', 0),  # Assuming 0 if not present in current
        'wind_dir': processed_record['current'].get('wind_deg', 0),
        'wind_gust_spd': processed_record['current'].get('wind_gust_spd', 0),
        'slp': processed_record['current'].get('pressure', 0),  # Assuming slp is pressure if not separate
        'dhi': processed_record['current'].get('dhi', 0),
        'dni': processed_record['current'].get('dni', 0),
        'ghi': processed_record['current'].get('ghi', 0),
        'is_night_time': (current_time_utc.hour < 6) or (current_time_utc.hour > 18)  # Simple proxy for night time
    }
    # Ensure all values are numeric
    for k, v in current_hourly_data_flat.items():
        if not isinstance(v, (int, float)):
            current_hourly_data_flat[k] = 0  # Default to 0 for non-numeric

    # Create a Pandas Series
    current_hourly_series = pd.Series(current_hourly_data_flat)

    # Create hourly features for prediction, including lags
    X_predict_hourly_df = create_hourly_features_for_prediction(current_hourly_series, current_time_utc)

    # Scale the features
    X_predict_hourly_scaled = SCALERS['hourly'].transform(X_predict_hourly_df)

    # Make hourly predictions
    predictions['hourly_temp_12h_ahead'] = MODELS['hourly_temp'].predict(X_predict_hourly_scaled)[0]
    predictions['hourly_precip_chance_12h_ahead'] = \
    MODELS['hourly_precip'].predict_proba(X_predict_hourly_scaled)[:, 1][0]

    if len(hourly_history_df) >= 24:
        today_midnight_utc = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        daily_agg_dict_for_pred = {
            'temp': ['min', 'max', 'mean'],
            'precip': 'sum',
            'rh': 'mean', 'pres': 'mean', 'wind_spd': 'mean', 'clouds': 'mean', 'vis': 'mean',
            'dewpt': 'mean', 'app_temp': 'mean', 'uv': 'max', 'solar_rad': 'sum', 'wind_dir': 'mean',
            'wind_gust_spd': 'max', 'slp': 'mean', 'dhi': 'sum', 'dni': 'sum', 'ghi': 'sum'
        }

        # Ensure 'precip' column exists from 'rain_1h' / 'snow_1h' in history for aggregation
        if 'rain_1h' in hourly_history_df.columns and 'precip' not in hourly_history_df.columns:
            hourly_history_df['precip'] = hourly_history_df['rain_1h'].fillna(0) + hourly_history_df['snow_1h'].fillna(
                0)

        current_date_data = hourly_history_df[hourly_history_df.index.date == current_time_utc.date()]

        if not current_date_data.empty:
            daily_agg_dict_filtered_for_pred = {
                col: methods for col, methods in daily_agg_dict_for_pred.items()
                if col in current_date_data.columns and pd.api.types.is_numeric_dtype(current_date_data[col])
            }
            today_daily_record_aggregated = current_date_data.agg(daily_agg_dict_filtered_for_pred)
            today_daily_record_aggregated.columns = ['_'.join(col).strip() for col in
                                                     today_daily_record_aggregated.columns.values]

            df_today_daily_for_features = pd.DataFrame([today_daily_record_aggregated.to_dict()],
                                                       index=[current_time_utc.date()])
            df_today_daily_for_features.index.name = 'datetime'

            X_predict_daily_df = create_daily_features_for_prediction(df_today_daily_for_features,
                                                                      current_time_utc.date())

            X_predict_daily_scaled = SCALERS['daily'].transform(X_predict_daily_df)

            # Make daily predictions
            predictions['daily_min_temp_14d_ahead'] = MODELS['daily_min_temp'].predict(X_predict_daily_scaled)[0]
            predictions['daily_max_temp_14d_ahead'] = MODELS['daily_max_temp'].predict(X_predict_daily_scaled)[0]
            predictions['daily_precip_chance_14d_ahead'] = \
            MODELS['daily_precip'].predict_proba(X_predict_daily_scaled)[:, 1][0]
        else:
            print(
                f"No hourly data for current date {current_time_utc.date()} in history yet. Cannot generate daily predictions.")
            predictions['daily_min_temp_14d_ahead'] = None
            predictions['daily_max_temp_14d_ahead'] = None
            predictions['daily_precip_chance_14d_ahead'] = None
    else:
        print("Not enough hourly history to aggregate for daily predictions.")
        predictions['daily_min_temp_14d_ahead'] = None
        predictions['daily_max_temp_14d_ahead'] = None
        predictions['daily_precip_chance_14d_ahead'] = None

    return predictions


# --- Main Prediction Loop ---
if __name__ == "__main__":
    consumer = KafkaConsumer(
        PROCESSED_WEATHER_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='weather-predictor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

    print(f"Listening for processed weather data on topic: {PROCESSED_WEATHER_TOPIC}")

    for message in consumer:
        processed_data = message.value
        try:
            current_timestamp_utc = processed_data['current']['timestamp_utc']
            current_dt_utc = datetime.fromtimestamp(current_timestamp_utc, tz=pytz.utc)
            location_name = processed_data.get('location_name', 'Unknown Location')

            print(f"Received processed data for {location_name} at {current_dt_utc}")

            current_hourly_obs_for_history = {
                'temp': processed_data['current'].get('temp'),
                'precip': processed_data['current'].get('rain_1h', 0) + processed_data['current'].get('snow_1h', 0),
                'rh': processed_data['current'].get('humidity'),
                'pres': processed_data['current'].get('pressure'),
                'wind_spd': processed_data['current'].get('wind_speed'),
                'clouds': processed_data['current'].get('clouds'),
                'vis': processed_data['current'].get('visibility'),
                'dewpt': processed_data['current'].get('dew_point'),
                'app_temp': processed_data['current'].get('feels_like'),
                'uv': processed_data['current'].get('uvi'),
                'solar_rad': processed_data['current'].get('solar_rad', 0),
                'wind_dir': processed_data['current'].get('wind_deg', 0),
                'wind_gust_spd': processed_data['current'].get('wind_gust_spd', 0),
                'slp': processed_data['current'].get('pressure', 0),
                'dhi': processed_data['current'].get('dhi', 0),
                'dni': processed_data['current'].get('dni', 0),
                'ghi': processed_data['current'].get('ghi', 0),
                'is_night_time': (current_dt_utc.hour < 6) or (current_dt_utc.hour > 18)
            }
            # Ensure all values are numeric for the history DF
            for k, v in current_hourly_obs_for_history.items():
                if not isinstance(v, (int, float)):
                    current_hourly_obs_for_history[k] = 0

            # Add to global hourly history

            new_hourly_row = pd.DataFrame([current_hourly_obs_for_history], index=[current_dt_utc])
            new_hourly_row.index.name = 'datetime'
            hourly_history_df = pd.concat([hourly_history_df, new_hourly_row]).drop_duplicates(
                subset=[new_hourly_row.index.name]).sort_index()
            hourly_history_df = hourly_history_df.tail(HOURLY_HISTORY_MAX_SIZE)  # Trim to keep memory usage in check

            # Make predictions
            predictions_result = make_predictions(processed_data)

            output_message = {
                "source_timestamp_utc": current_timestamp_utc,
                "location_name": location_name,
                "predictions": predictions_result
            }

            producer.send(PREDICTIONS_TOPIC, value=output_message)
            print(f"Published predictions for {location_name}: {predictions_result}")

        except KeyError as e:
            print(f"Error processing message (missing key): {e} in message: {processed_data}")
        except Exception as e:
            print(f"An unexpected error occurred: {e} in message: {processed_data}")