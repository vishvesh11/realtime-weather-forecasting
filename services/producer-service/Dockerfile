
FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/producer.py /app/src/producer.py

ENV KAFKA_BROKER_URL=localhost:9092
ENV RAW_WEATHER_TOPIC=raw-weather-data
ENV OWM_API_KEY=API_KEY

CMD ["python", "/app/src/producer.py"]