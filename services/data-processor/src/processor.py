import time
import json
import os
from kafka import KafkaConsumer, KafkaProducer

# Config
KAFKA_BROKER_URL = "realtime-weather-app-kafka:9092"
RAW_WEATHER_TOPIC = "raw-weather-data"
PROCESSED_WEATHER_TOPIC = "processed-weather-data"

# Consumer setup
consumer = KafkaConsumer(
    RAW_WEATHER_TOPIC,
    bootstrap_servers=[KAFKA_BROKER_URL],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='weather-data-processor-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'  # Ensure all replicas have received the message
)


def process_weather_data(raw_data):
    processed_data = {}

    # Add location metadata
    processed_data['location_name'] = raw_data.get('location_name')
    processed_data['lat'] = raw_data.get('lat')
    processed_data['lon'] = raw_data.get('lon')

    # --- Process Current Weather Data ---
    current = raw_data.get('current', {})
    processed_data['current'] = {
        'timestamp_utc': current.get('dt'),  # Unix timestamp, UTC
        'sunrise_utc': current.get('sunrise'),
        'sunset_utc': current.get('sunset'),
        'temp': current.get('temp'),
        'feels_like': current.get('feels_like'),
        'pressure': current.get('pressure'),
        'humidity': current.get('humidity'),
        'dew_point': current.get('dew_point'),
        'uvi': current.get('uvi'),
        'clouds': current.get('clouds'),  # Cloudiness %
        'visibility': current.get('visibility'),  # meters
        'wind_speed': current.get('wind_speed'),  # m/s
        'wind_deg': current.get('wind_deg'),  # degrees
        'weather_id': current.get('weather', [{}])[0].get('id'),  # Main weather condition ID
        'weather_main': current.get('weather', [{}])[0].get('main'),  # Group of weather parameters
        'weather_description': current.get('weather', [{}])[0].get('description'),  # Weather condition within the group
    }
    # Add rain/snow if available
    if 'rain' in current and '1h' in current['rain']:
        processed_data['current']['rain_1h'] = current['rain']['1h']
    if 'snow' in current and '1h' in current['snow']:
        processed_data['current']['snow_1h'] = current['snow']['1h']

    hourly_forecasts = []
    for h_data in raw_data.get('hourly', [])[:48]:  # Take up to 48 hours
        hourly_forecasts.append({
            'timestamp_utc': h_data.get('dt'),
            'temp': h_data.get('temp'),
            'feels_like': h_data.get('feels_like'),
            'pressure': h_data.get('pressure'),
            'humidity': h_data.get('humidity'),
            'dew_point': h_data.get('dew_point'),
            'uvi': h_data.get('uvi'),
            'clouds': h_data.get('clouds'),
            'visibility': h_data.get('visibility'),
            'wind_speed': h_data.get('wind_speed'),
            'wind_deg': h_data.get('wind_deg'),
            'pop': h_data.get('pop'),  # Probability of precipitation
            'weather_id': h_data.get('weather', [{}])[0].get('id'),
            'weather_main': h_data.get('weather', [{}])[0].get('main'),
            'weather_description': h_data.get('weather', [{}])[0].get('description'),
        })
        if 'rain' in h_data and '1h' in h_data['rain']:
            hourly_forecasts[-1]['rain_1h'] = h_data['rain']['1h']
        if 'snow' in h_data and '1h' in h_data['snow']:
            hourly_forecasts[-1]['snow_1h'] = h_data['snow']['1h']
    processed_data['hourly_forecasts'] = hourly_forecasts

    daily_forecasts = []
    for d_data in raw_data.get('daily', [])[:8]:  # Take up to 8 days
        daily_forecasts.append({
            'timestamp_utc': d_data.get('dt'),
            'sunrise_utc': d_data.get('sunrise'),
            'sunset_utc': d_data.get('sunset'),
            'moonrise_utc': d_data.get('moonrise'),
            'moonset_utc': d_data.get('moonset'),
            'moon_phase': d_data.get('moon_phase'),
            'temp_day': d_data.get('temp', {}).get('day'),
            'temp_min': d_data.get('temp', {}).get('min'),
            'temp_max': d_data.get('temp', {}).get('max'),
            'temp_night': d_data.get('temp', {}).get('night'),
            'temp_eve': d_data.get('temp', {}).get('eve'),
            'temp_morn': d_data.get('temp', {}).get('morn'),
            'feels_like_day': d_data.get('feels_like', {}).get('day'),
            'feels_like_night': d_data.get('feels_like', {}).get('night'),
            'feels_like_eve': d_data.get('feels_like', {}).get('eve'),
            'feels_like_morn': d_data.get('feels_like', {}).get('morn'),
            'pressure': d_data.get('pressure'),
            'humidity': d_data.get('humidity'),
            'dew_point': d_data.get('dew_point'),
            'wind_speed': d_data.get('wind_speed'),
            'wind_deg': d_data.get('wind_deg'),
            'clouds': d_data.get('clouds'),
            'pop': d_data.get('pop'),
            'uvi': d_data.get('uvi'),
            'weather_id': d_data.get('weather', [{}])[0].get('id'),
            'weather_main': d_data.get('weather', [{}])[0].get('main'),
            'weather_description': d_data.get('weather', [{}])[0].get('description'),
        })
        if 'rain' in d_data:
            daily_forecasts[-1]['rain'] = d_data['rain']  # Total precipitation for the day
        if 'snow' in d_data:
            daily_forecasts[-1]['snow'] = d_data['snow']
    processed_data['daily_forecasts'] = daily_forecasts

    return processed_data


if __name__ == "__main__":
    print("Waiting 15 seconds before connecting to Kafka...")
    time.sleep(15)
    print(
        f"Starting weather data processor. Consuming from '{RAW_WEATHER_TOPIC}' and producing to '{PROCESSED_WEATHER_TOPIC}'...")
    try:
        for message in consumer:
            raw_data = message.value
            location_name = raw_data.get('location_name', 'UNKNOWN_LOCATION')
            print(f"Received raw data for {location_name}. Processing...")

            try:
                processed_data = process_weather_data(raw_data)
                producer.send(PROCESSED_WEATHER_TOPIC, key=location_name.encode('utf-8'), value=processed_data)
                print(f"Published processed data for {location_name} to '{PROCESSED_WEATHER_TOPIC}'.")
            except Exception as e:
                print(f"Error processing data for {location_name}: {e}")

    except KeyboardInterrupt:
        print("Processor stopped manually.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        consumer.close()
        producer.close()
        print("Kafka consumer and producer closed.")

