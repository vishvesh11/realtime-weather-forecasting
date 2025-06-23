import requests
import json
import time
from datetime import datetime, timedelta
import os
import math

# --- Configuration ---
# Replace with your actual Weatherbit.io API Key
WEATHERBIT_API_KEY = os.getenv("WEATHERBIT_API_KEY")

# Define the target location's latitude and longitude
TARGET_LOCATION = {
    "name": "Bangalore-hsr",
    "lat": 12.9126,
    "lon": 77.6387
}


FIXED_START_DATE_STR = "2015-05-20"


DAYS_PER_REQUEST_CHUNK = 10


INITIAL_DAYS_OFFSET = 3650


API_BASE_URL = "https://api.weatherbit.io/v2.0/history/hourly"


OUTPUT_FILENAME = f"historical_weather_hourly_{TARGET_LOCATION['name'].replace('-', '_')}.jsonl"


REQUEST_DELAY_SECONDS = 0.1


# --- Script Logic ---

def fetch_historical_hourly_data(location_info, api_key, fixed_start_date_str, chunk_size, offset_days, output_file):

    total_records_collected = 0
    today = datetime.now()

    effective_end_date = today - timedelta(days=offset_days)

    fixed_start_date = datetime.strptime(fixed_start_date_str, '%Y-%m-%d')


    if fixed_start_date > effective_end_date:
        print(
            f"Error: FIXED_START_DATE ({fixed_start_date_str}) is later than the effective end date ({effective_end_date.strftime('%Y-%m-%d')} calculated with offset).")
        print("Please adjust FIXED_START_DATE or INITIAL_DAYS_OFFSET.")
        return


    total_days_to_fetch = (effective_end_date - fixed_start_date).days + 1

    num_requests = math.ceil(total_days_to_fetch / chunk_size)

    print(
        f"Starting to fetch approximately {total_days_to_fetch} days ({num_requests} requests) of historical HOURLY data for {location_info['name']}...")
    print(f"From {fixed_start_date_str} up to {effective_end_date.strftime('%Y-%m-%d')}.")
    print(f"Data will be continuously saved to: {output_file} (JSON Lines format)")

    with open(output_file, 'a') as f:
        for i in range(num_requests):
            current_chunk_start_date = fixed_start_date + timedelta(days=i * chunk_size)
            current_chunk_end_date = current_chunk_start_date + timedelta(days=chunk_size - 1)

            if current_chunk_end_date > effective_end_date:
                current_chunk_end_date = effective_end_date

            if current_chunk_start_date > current_chunk_end_date:
                break

            start_date_str = current_chunk_start_date.strftime('%Y-%m-%d')
            end_date_str = current_chunk_end_date.strftime('%Y-%m-%d')

            print(f"Fetching chunk {i + 1}/{num_requests}: {start_date_str} to {end_date_str}...")

            params = {
                "key": api_key,
                "lat": location_info["lat"],
                "lon": location_info["lon"],
                "start_date": start_date_str,
                "end_date": end_date_str,
                "units": "M",  # Metric units (Celsius, m/s, mm)
                "tz": "local"  # Request data in local timezone
            }

            try:
                response = requests.get(API_BASE_URL, params=params)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
                data = response.json()

                if data and 'data' in data and len(data['data']) > 0:
                    for hourly_record in data['data']:
                        hourly_record['location_name'] = location_info['name']
                        hourly_record['lat'] = location_info['lat']
                        hourly_record['lon'] = location_info['lon']

                        # Write each record as a separate JSON line
                        f.write(json.dumps(hourly_record) + '\n')
                        total_records_collected += 1
                    print(
                        f"Successfully fetched {len(data['data'])} hourly records for this chunk. Total records collected: {total_records_collected}")
                else:
                    print(f"No hourly data returned for {start_date_str} to {end_date_str}. Response: {data}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred for {start_date_str} to {end_date_str}: {http_err}")
                if response.status_code == 429:  # Too Many Requests
                    print("Rate limit hit. Please consider increasing delay or checking your plan limits.")
                    break  # Stop fetching if rate limit is hit
            except requests.exceptions.ConnectionError as conn_err:
                print(f"Connection error occurred for {start_date_str} to {end_date_str}: {conn_err}")
            except requests.exceptions.Timeout as timeout_err:
                print(f"Timeout error occurred for {start_date_str} to {end_date_str}: {timeout_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"An unexpected error occurred for {start_date_str} to {end_date_str}: {req_err}")
            except json.JSONDecodeError:
                print(f"Failed to decode JSON for {start_date_str} to {end_date_str}. Response text: {response.text}")

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\nFinished data collection. Total records collected: {total_records_collected}")
    print(f"Data saved to {os.path.abspath(output_file)}")


if __name__ == "__main__":
    if WEATHERBIT_API_KEY == "WEATHERBIT_API_KEY":
        print(
            "WARNING: Please check 'YOUR_WEATHERBIT_API_KEY' ")
        print("You can get your free API key after signing up on weatherbit.io.")
    else:
        fetch_historical_hourly_data(TARGET_LOCATION, WEATHERBIT_API_KEY, FIXED_START_DATE_STR, DAYS_PER_REQUEST_CHUNK,
                                     INITIAL_DAYS_OFFSET, OUTPUT_FILENAME)


