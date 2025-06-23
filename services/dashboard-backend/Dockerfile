# services/dashboard-backend/Dockerfile
FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/app.py /app/src/app.py

ENV KAFKA_BROKER_URL=localhost:9092
ENV PREDICTIONS_TOPIC=weather-predictions
ENV FLASK_APP=/app/src/app.py

# InfluxDB env vars - these will be set by Kubernetes secret
ENV INFLUXDB_URL=http://localhost:8086
ENV INFLUXDB_TOKEN=my-super-secret-token
ENV INFLUXDB_ORG=my-organization
ENV INFLUXDB_BUCKET=weather_data

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "src.app:app"]