import os
import json
import time
from threading import Thread
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from flask_cors import CORS  # To allow frontend to make requests
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
PROCESSED_WEATHER_TOPIC = "processed-weather-data"

# InfluxDB Configuration
# Replace with your InfluxDB details (sensitive info should come from Kubernetes Secrets in prod)
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token")  # Replace with your actual token
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "home")  # Replace with your actual organization
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "weather_data")  # Replace with your actual bucket

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# InfluxDB Client setup
influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
query_api = influxdb_client.query_api()

# Kafka Consumer setup (will be run in a separate thread)
kafka_consumer = KafkaConsumer(
    PROCESSED_WEATHER_TOPIC,
    bootstrap_servers=[KAFKA_BROKER_URL],
    group_id='dashboard-backend-consumer-group',
    auto_offset_reset='latest',  # Start consuming from the latest message
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def kafka_consumer_thread():
    """
    Background thread to continuously consume messages from Kafka
    and write them to InfluxDB.
    """
    print(f"Starting Kafka consumer thread for topic: {PROCESSED_WEATHER_TOPIC}...")
    while True:
        try:
            for message in kafka_consumer:
                processed_data = message.value
                location_name = processed_data.get('location_name', 'unknown')

                print(f"Backend received processed data for {location_name} at {datetime.now()}.")

                # --- Write Current Weather Data to InfluxDB ---
                current = processed_data.get('current', {})
                if current:
                    try:
                        point_current = (
                            Point("current_weather")
                            .tag("location", location_name)
                            .field("temperature", current.get('temp'))
                            .field("feels_like", current.get('feels_like'))
                            .field("pressure", current.get('pressure'))
                            .field("humidity", current.get('humidity'))
                            .field("dew_point", current.get('dew_point'))
                            .field("uvi", current.get('uvi'))
                            .field("clouds", current.get('clouds'))
                            .field("visibility", current.get('visibility'))
                            .field("wind_speed", current.get('wind_speed'))
                            .field("wind_deg", current.get('wind_deg'))
                            .field("weather_id", current.get('weather_id'))
                            .field("weather_main", current.get('weather_main'))
                            .field("weather_description", current.get('weather_description'))
                            .field("sunrise_utc", current.get('sunrise_utc'))
                            .field("sunset_utc", current.get('sunset_utc'))
                            # Use the timestamp from the current data point
                            .time(datetime.fromtimestamp(current.get('timestamp_utc'), tz=timezone.utc))
                        )
                        if 'rain_1h' in current:
                            point_current.field("rain_1h", current['rain_1h'])
                        if 'snow_1h' in current:
                            point_current.field("snow_1h", current['snow_1h'])

                        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point_current)
                        # print(f"Wrote current weather for {location_name} to InfluxDB.")
                    except Exception as e:
                        print(f"Error writing current weather to InfluxDB for {location_name}: {e}")

                # --- Write Hourly Forecasts to InfluxDB ---
                hourly_forecasts = processed_data.get('hourly_forecasts', [])
                for h_data in hourly_forecasts:
                    try:
                        point_hourly = (
                            Point("hourly_forecast")
                            .tag("location", location_name)
                            .field("temperature", h_data.get('temp'))
                            .field("feels_like", h_data.get('feels_like'))
                            .field("pressure", h_data.get('pressure'))
                            .field("humidity", h_data.get('humidity'))
                            .field("pop", h_data.get('pop'))  # Probability of precipitation
                            .field("weather_id", h_data.get('weather_id'))
                            .field("weather_main", h_data.get('weather_main'))
                            .field("weather_description", h_data.get('weather_description'))
                            # Use the forecast timestamp as the measurement time
                            .time(datetime.fromtimestamp(h_data.get('timestamp_utc'), tz=timezone.utc))
                        )
                        if 'rain_1h' in h_data:
                            point_hourly.field("rain_1h", h_data['rain_1h'])
                        if 'snow_1h' in h_data:
                            point_hourly.field("snow_1h", h_data['snow_1h'])

                        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point_hourly)
                    except Exception as e:
                        print(f"Error writing hourly forecast to InfluxDB for {location_name}: {e}")

                # --- Write Daily Forecasts to InfluxDB ---
                daily_forecasts = processed_data.get('daily_forecasts', [])
                for d_data in daily_forecasts:
                    try:
                        point_daily = (
                            Point("daily_forecast")
                            .tag("location", location_name)
                            .field("temp_min", d_data.get('temp_min'))
                            .field("temp_max", d_data.get('temp_max'))
                            .field("pop", d_data.get('pop'))  # Probability of precipitation
                            .field("weather_id", d_data.get('weather_id'))
                            .field("weather_main", d_data.get('weather_main'))
                            .field("weather_description", d_data.get('weather_description'))
                            .field("sunrise_utc", d_data.get('sunrise_utc'))
                            .field("sunset_utc", d_data.get('sunset_utc'))
                            # Use the forecast timestamp as the measurement time
                            .time(datetime.fromtimestamp(d_data.get('timestamp_utc'), tz=timezone.utc))
                        )
                        if 'rain' in d_data:
                            point_daily.field("rain", d_data['rain'])  # Total for the day
                        if 'snow' in d_data:
                            point_daily.field("snow", d_data['snow'])

                        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point_daily)
                    except Exception as e:
                        print(f"Error writing daily forecast to InfluxDB for {location_name}: {e}")

        except Exception as e:
            print(f"Kafka consumer thread encountered an error: {e}. Retrying in 5 seconds...")
            time.sleep(5)


# --- Flask API Endpoints ---

@app.route('/api/locations', methods=['GET'])
def get_locations():
    """
    Returns a list of all unique locations found in the current_weather data.
    """
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -30d) // Look back 30 days to find locations
        |> filter(fn: (r) => r._measurement == "current_weather")
        |> distinct(column: "location")
    '''
    try:
        result = query_api.query(query, org=INFLUXDB_ORG)
        locations = []
        for table in result:
            for record in table.records:
                locations.append(record.get_value())
        return jsonify(sorted(list(set(locations))))  # Ensure unique and sorted
    except Exception as e:
        print(f"Error fetching locations from InfluxDB: {e}")
        return jsonify({"error": "Could not fetch locations"}), 500


@app.route('/api/current/<location_name>', methods=['GET'])
def get_current_weather(location_name):
    """
    Retrieves the latest current weather data for a given location.
    """
    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -1h) // Look back 1 hour for the latest data
        |> filter(fn: (r) => r._measurement == "current_weather" and r.location == "{location_name}")
        |> last() // Get the most recent record
    '''
    try:
        result = query_api.query(query, org=INFLUXDB_ORG)
        if result and result[0].records:
            record = result[0].records[0]
            current_data = {
                "timestamp": record.get_time().isoformat(),
                "temperature": record.get_field("temperature"),
                "feels_like": record.get_field("feels_like"),
                "pressure": record.get_field("pressure"),
                "humidity": record.get_field("humidity"),
                "wind_speed": record.get_field("wind_speed"),
                "wind_deg": record.get_field("wind_deg"),
                "weather_description": record.get_field("weather_description"),
                "sunrise_utc": record.get_field("sunrise_utc"),
                "sunset_utc": record.get_field("sunset_utc"),
                "rain_1h": record.get_field("rain_1h"),
                "snow_1h": record.get_field("snow_1h")
            }
            return jsonify(current_data)
        return jsonify({"message": "No current weather data found for this location."}), 404
    except Exception as e:
        print(f"Error fetching current weather for {location_name}: {e}")
        return jsonify({"error": "Could not fetch current weather"}), 500


@app.route('/api/hourly/<location_name>', methods=['GET'])
def get_hourly_forecast(location_name):
    query_latest_timestamp = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -1h) // Look back 1 hour to find the very latest current data
        |> filter(fn: (r) => r._measurement == "current_weather" and r.location == "{location_name}")
        |> last()
        |> keep(columns: ["_time"])
    '''
    latest_time = None
    try:
        result = query_api.query(query_latest_timestamp, org=INFLUXDB_ORG)
        if result and result[0].records:
            latest_time = result[0].records[0].get_time()
        else:
            return jsonify({"message": "No recent current weather data to anchor forecast."}), 404
    except Exception as e:
        print(f"Error fetching latest current weather timestamp for hourly forecast {location_name}: {e}")
        return jsonify({"error": "Could not fetch forecast anchor"}), 500

    future_24_hours_ts = int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())

    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -2h) // Look for hourly forecasts generated in the last 2 hours
        |> filter(fn: (r) => r._measurement == "hourly_forecast" and r.location == "{location_name}")
        |> group(columns: ["_time"]) // Group by the forecast timestamp to get distinct points
        |> sort(columns: ["_time"], desc: false) // Sort by forecast time
        |> unique(column: "_time") // Get unique forecast points by time
        |> filter(fn: (r) => r._time >= now()) // Only future forecasts
        |> limit(n: 24) // Limit to the first 24 hours
    '''
    try:
        result = query_api.query(query, org=INFLUXDB_ORG)
        hourly_data = []
        for table in result:
            for record in table.records:
                hourly_data.append({
                    "timestamp": record.get_time().isoformat(),
                    "temperature": record.get_field("temperature"),
                    "pop": record.get_field("pop"),
                })
        # Sort again by timestamp to ensure correct order
        hourly_data.sort(key=lambda x: x['timestamp'])
        return jsonify(hourly_data)
    except Exception as e:
        print(f"Error fetching hourly forecast for {location_name}: {e}")
        return jsonify({"error": "Could not fetch hourly forecast"}), 500


@app.route('/api/daily/<location_name>', methods=['GET'])
def get_daily_forecast(location_name):


    query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -2h) // Look for daily forecasts generated in the last 2 hours
        |> filter(fn: (r) => r._measurement == "daily_forecast" and r.location == "{location_name}")
        |> group(columns: ["_time"]) // Group by the forecast timestamp to get distinct points
        |> sort(columns: ["_time"], desc: false) // Sort by forecast time
        |> unique(column: "_time") // Get unique forecast points by time
        |> filter(fn: (r) => r._time >= now()) // Only future forecasts
        |> limit(n: 8) // Limit to the first 8 days (OWM onecall max)
    '''
    try:
        result = query_api.query(query, org=INFLUXDB_ORG)
        daily_data = []
        for table in result:
            for record in table.records:
                daily_data.append({
                    "timestamp": record.get_time().isoformat(),
                    "temp_min": record.get_field("temp_min"),
                    "temp_max": record.get_field("temp_max"),
                    "pop": record.get_field("pop"),
                })
        # Sort again by timestamp to ensure correct order
        daily_data.sort(key=lambda x: x['timestamp'])
        return jsonify(daily_data)
    except Exception as e:
        print(f"Error fetching daily forecast for {location_name}: {e}")
        return jsonify({"error": "Could not fetch daily forecast"}), 500


# --- Application Startup ---
if __name__ == '__main__':
    # Start the Kafka consumer in a separate thread
    consumer_thread = Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()

    # Start the Flask API
    print("Starting Flask API...")
    # Use 0.0.0.0 to make it accessible from outside the container
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    # use_reloader=False because we're running a separate thread manually
