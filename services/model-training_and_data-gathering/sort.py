import json


# List of input JSON files
input_files = ["historical_weather_hourly_Indore_Palasia.jsonl", "historical_weather_hourly_Indore_Palasia2016to2020.jsonl", "historical_weather_hourly_Indore_Palasia2020-04to 2020-01.jsonl","historical_weather_hourly_Indore_Palasia-2021-05-26.jsonl"]  # replace with actual file names
unique_records = {}

# Load, parse, and deduplicate records
for file in input_files:
    with open(file, 'r') as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                ts = record.get("ts")
                if ts is not None:
                    unique_records[ts] = record  # If ts already exists, it will be overwritten
            except json.JSONDecodeError:
                print(f"Invalid JSON in file {file}, skipping.")

# Sort records by timestamp descending
sorted_records = sorted(unique_records.values(), key=lambda x: x["ts"], reverse=True)

# Save to output file
output_file = "historical_weather_hourly_Indore_Palasia.jsonl"
with open(output_file, 'w') as out:
    for record in sorted_records:
        json.dump(record, out)
        out.write('\n')

print(f"Deduplicated and sorted data written to: {output_file}")