import requests
import time
import os
from kafka import KafkaProducer
import json

# Replace with your actual values
OWM_API_KEY = os.getenv("OWM_API_KEY")
KAFKA_BROKER_URL = "realtime-weather-app-kafka:9092"
RAW_WEATHER_TOPIC = "raw-weather-data"

LOCATIONS = {
    "indore-palasia": {"lat": 22.7237, "lon": 75.9050}, # Example coords
    "pune-wakad": {"lat": 18.6076, "lon": 73.7415},   # Example coords
    "bangalore-hsr": {"lat": 12.9126, "lon": 77.6387} # Example coords
}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_publish_weather(location_name, lat, lon):
    print(f"Fetching weather for {location_name}...")
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={OWM_API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Add location metadata to the data
        data['location_name'] = location_name
        data['lat'] = lat
        data['lon'] = lon

        # Use location_name as key to ensure ordering for that location
        producer.send(RAW_WEATHER_TOPIC, key=location_name.encode('utf-8'), value=data)
        print(f"Published weather for {location_name} to Kafka.")
    else:
        print(f"Error fetching weather for {location_name}: {response.status_code} - {response.text}")

if __name__ == "__main__":
    while True:
        for loc_name, coords in LOCATIONS.items():
            fetch_and_publish_weather(loc_name, coords['lat'], coords['lon'])
        producer.flush() # Ensure all messages are sent
        print("Waiting 10 minutes for next poll...")
        time.sleep(60 * 10) # Poll every 10 minutes