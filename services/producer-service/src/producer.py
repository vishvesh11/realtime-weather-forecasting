import requests
import time
import os
from kafka import KafkaProducer
import json
import sys # Import sys for flushing stdout

# --- Configuration ---
OWM_API_KEY = os.getenv("OWM_API_KEY")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "realtime-weather-app-kafka:9092")
RAW_WEATHER_TOPIC = "raw-weather-data"

LOCATIONS = {
    "indore-palasia": {"lat": 22.7237, "lon": 75.9050},
    "pune-wakad": {"lat": 18.6076, "lon": 73.7415},
    "bangalore-hsr": {"lat": 12.9126, "lon": 77.6387}
}

def create_kafka_producer():
    print("Producer service: Waiting 15 seconds before connecting to Kafka...")
    sys.stdout.flush()
    time.sleep(15)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        print(f"Producer connected to Kafka at {KAFKA_BROKER_URL}")
        sys.stdout.flush()
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.stdout.flush()
        sys.exit(1)

producer = create_kafka_producer()

if producer is None:
    sys.exit(1)

def fetch_and_publish_weather(location_name, lat, lon):
    print(f"DEBUG: Entering fetch_and_publish_weather for {location_name}") # DEBUG
    sys.stdout.flush()

    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={OWM_API_KEY}&units=metric"
    print(f"DEBUG: API URL: {url}") # DEBUG PRINT
    sys.stdout.flush()

    try:
        response = requests.get(url, timeout=10)
        print(f"DEBUG: API Response Status Code: {response.status_code}") # DEBUG
        sys.stdout.flush()

        if response.status_code == 200:
            data = response.json()
            data['location_name'] = location_name
            data['lat'] = lat
            data['lon'] = lon


            producer.send(RAW_WEATHER_TOPIC, key=location_name.encode('utf-8'), value=data)
            print(f"Published weather for {location_name} to Kafka.")
            sys.stdout.flush()
        else:
            print(f"Error fetching weather for {location_name}: {response.status_code} - {response.text}")
            sys.stdout.flush()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Request to OWM API failed for {location_name}: {e}")
        sys.stdout.flush()
    except json.JSONDecodeError as e: # Catch JSON parsing errors
        print(f"ERROR: Failed to decode JSON response from OWM for {location_name}: {e}. Response text: {response.text}")
        sys.stdout.flush()
    except Exception as e: # Catch any other unexpected errors
        print(f"ERROR: An unexpected error occurred in fetch_and_publish_weather for {location_name}: {e}")
        sys.stdout.flush()

if __name__ == "__main__":
    print(f"Starting producer service. Publishing to topic: {RAW_WEATHER_TOPIC}")
    sys.stdout.flush()

    while True:
        print("DEBUG: Entering main loop iteration.") # DEBUG
        sys.stdout.flush()
        for loc_name, coords in LOCATIONS.items():
            print(f"DEBUG: Processing location: {loc_name}") # DEBUG
            sys.stdout.flush()
            fetch_and_publish_weather(loc_name, coords['lat'], coords['lon'])
        producer.flush() # Ensure all messages are sent
        print("Waiting 10 minutes for next poll...")
        sys.stdout.flush()
        time.sleep(60 * 10)
