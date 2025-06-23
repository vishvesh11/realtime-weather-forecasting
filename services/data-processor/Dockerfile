
FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/processor.py /app/src/processor.py

ENV KAFKA_BROKER_URL=localhost:9092
ENV RAW_WEATHER_TOPIC=raw-weather-data
ENV PROCESSED_WEATHER_TOPIC=processed-weather-data

CMD ["python", "/app/src/processor.py"]