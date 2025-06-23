FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/src/ml-models
COPY src/predictor.py /app/src/predictor.py
COPY src/ml-models/ /app/src/ml-models/
# Copy all .joblib files
ENV KAFKA_BROKER_URL=localhost:9092
ENV PROCESSED_WEATHER_TOPIC=processed-weather-data
ENV PREDICTIONS_TOPIC=weather-predictions

CMD ["python", "/app/src/predictor.py"]