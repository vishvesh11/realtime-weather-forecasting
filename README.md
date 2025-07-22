# Realtime Weather Forecasting Application

## Overview

This project implements an end-to-end real-time weather forecasting system. It collects current weather data, processes it, feeds it into machine learning models for hourly and daily predictions, and visualizes both real-time conditions and forecasts through a dynamic web dashboard. The entire application is designed for cloud-native deployment using Kubernetes and Helm.

## Features

  * **Real-time Data Ingestion:** Polls OpenWeatherMap API for current weather data for multiple locations.

  * **Data Aggregation & Streaming:** Utilizes Kafka as a central message broker for raw and processed weather data.

  * **Data Processing:** Dedicated service to clean, flatten, and prepare raw weather data for consumption by ML models.

  * **Machine Learning Models:**

      * **Hourly Forecast Model:** Predicts weather conditions up to 24 hours in the future (using LightGBM).

      * **Daily Forecast Model:** Predicts weather conditions up to 14 days in the future (using LightGBM).

      * Models are stored using `joblib` and are location-specific.

  * **Persistent Storage:** Forecast data is stored in InfluxDB, optimized for time-series data.

  * **Interactive Dashboard:** A Next.js/React frontend displays current weather conditions, hourly temperature/precipitation forecasts, and daily min/max temperature and precipitation chances.

  * **Containerized Deployment:** All services are containerized using Docker.

  * **Orchestration with Kubernetes & Helm:** The entire application stack is deployed and managed using Kubernetes, with Helm charts for easy installation and configuration.

## Architecture

The application follows a microservices architecture, leveraging Kafka for inter-service communication:

```
+-------------------+      +-----------------+      +-----------------+
| Producer Service  |----->|                 |----->| Data Processor  |
| (OpenWeatherMap)  |      |                 |      | (Raw -> Processed)|
+-------------------+      |   Kafka         |      +-----------------+
                           |   (raw-data)    |             |
                           |                 |<------------+
                           |   (processed-data)|
                           |                 |
                           |   (hourly-forecasts)|
                           |                 |
                           |   (daily-forecasts)|
                           +-----------------+
                                   |
                                   |
                +------------------+------------------+
                |                                     |
                |                                     |
+-------------------+                       +-------------------+
| ML Model - Hourly |                       | ML Model - Daily  |
| (Consumes Processed) |                       | (Consumes Processed)|
| (Publishes Hourly Forecasts) |                       | (Publishes Daily Forecasts) |
+-------------------+                       +-------------------+
        |                                     |
        | (Kafka)                             | (Kafka)
        v                                     v
+-------------------+      +-------------------+      +-------------------+
| Dashboard Backend |----->|                 |----->| Dashboard Frontend|
| (Consumes Forecasts) |      |   InfluxDB      |      | (Next.js/React)   |
| (Serves API)      |      |                 |      |                   |
+-------------------+      +-------------------+      +-------------------+
```

**Key Technologies Used:**

  * **Data Ingestion:** Python, `requests`, OpenWeatherMap API

  * **Streaming:** Apache Kafka

  * **Data Processing:** Python, `kafka-python`

  * **Machine Learning:** Python, LightGBM, `joblib`

  * **Backend API:** Python, Flask, `influxdb-client-python`

  * **Time-Series Database:** InfluxDB

  * **Frontend:** Next.js, React, Tailwind CSS, Recharts, Moment.js, Lucide React

  * **Containerization:** Docker

  * **Orchestration:** Kubernetes, Helm

## Directory Structure

```
.
├── LICENSE
├── README.md
├── docker-compose/          # (Optional) For local development with Docker Compose
├── frontend/                # Next.js/React dashboard application
│   ├── src/
│   │   ├── app/
│   │   │   └── page.tsx     # Main dashboard page
│   │   └── ...
│   ├── package.json
│   ├── next.config.ts
│   └── Dockerfile
├── kubernetes/              # Raw Kubernetes manifests (pre-Helm)
├── services/
│   ├── dashboard-backend/
│   │   ├── src/
│   │   │   └── app.py       # Flask API for dashboard
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── data-processor/      # New service for data processing
│   │   ├── src/
│   │   │   └── processor.py # Kafka consumer/producer for processing
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── ml-model-daily/      # Daily forecast ML service
│   │   ├── src/
│   │   │   └── ...          # ML model inference logic
│   │   └── Dockerfile
│   ├── ml-model-hourly/     # Hourly forecast ML service
│   │   ├── src/
│   │   │   └── ...          # ML model inference logic
│   │   └── Dockerfile
│   ├── model-training_and_data-gathering/ # For training scripts and historical data
│   ├── producer-service/
│   │   ├── src/
│   │   │   └── producer.py  # OpenWeatherMap data ingestion
│   │   ├── requirements.txt
│   │   └── Dockerfile
├── shared/                  # (Potentially for common utilities, config)
├── venv/                    # Python virtual environment
└── weather-forecast-app/    # Helm Chart root directory
    ├── Chart.yaml
    ├── values.yaml
    └── templates/           # Kubernetes manifest templates
        ├── _helpers.tpl
        ├── kafka/
        ├── zookeeper/
        ├── influxdb/
        ├── producer-service/
        ├── data-processor/
        ├── ml-model-hourly/
        ├── ml-model-daily/
        ├── dashboard-backend/
        ├── dashboard-frontend/
        └── secrets.yaml
```

## Getting Started

### Prerequisites

  * Docker (for building images)

  * Kubernetes cluster (Minikube, Kind, GKE, EKS, AKS, etc.)

  * `kubectl` (Kubernetes command-line tool)

  * Helm (Kubernetes package manager)

  * Python 3.9+

  * Node.js 18+ & npm/yarn

### Setup Steps

1.  **Obtain API Keys:**

      * Get an API key from [OpenWeatherMap](https://openweathermap.org/api).
      * Ensure your InfluxDB instance is set up and you have an organization, bucket, and an API token with write/read permissions.

2.  **Build Docker Images:**
    Navigate to each service directory (`services/producer-service`, `services/data-processor`, `services/dashboard-backend`, `frontend`, and later `services/ml-model-hourly`, `services/ml-model-daily`) and build their respective Docker images. Replace `your-registry` with your actual Docker registry (e.g., Docker Hub username, GCR, ECR).

    ```bash
    # Example for producer-service
    cd services/producer-service
    docker build -t your-registry/producer-service:latest .
    docker push your-registry/producer-service:latest

    # Repeat for data-processor, dashboard-backend, frontend
    # And later for ml-model-hourly, ml-model-daily when ready
    ```

3.  **Configure Helm Chart:**
    Edit `weather-forecast-app/values.yaml`:

      * Update `secrets.openWeatherMapApiKey`, `secrets.influxdb.token`, `secrets.influxdb.adminPassword` with your actual values.

      * Update the `image` fields for all services (`producerService.image`, `dataProcessor.image`, `dashboardBackend.image`, `dashboardFrontend.image`, `mlModelHourly.image`, `mlModelDaily.image`) to point to your pushed Docker images (e.g., `your-registry/producer-service:latest`).

      * Adjust `influxdb.org` and `influxdb.bucket` to match your InfluxDB setup.

      * Decide on the `dashboardFrontend.service.type` (e.g., `LoadBalancer` for cloud, `NodePort` for local testing).

### Deployment

1.  **Navigate to the Helm chart directory:**

    ```bash
    cd weather-forecast-app

    ```

2.  **Install the Helm chart:**

    ```bash
    helm install weather-app . --namespace weather-ns --create-namespace

    ```

    This command will deploy all enabled components (Kafka, Zookeeper, InfluxDB, Producer, Data Processor, Dashboard Backend, Dashboard Frontend). ML models are disabled by default; enable them in `values.yaml` when ready.

3.  **Verify Deployment:**
    Check the status of your pods and services:

    ```bash
    kubectl get pods -n weather-ns
    kubectl get services -n weather-ns

    ```

    Look for the external IP or NodePort of the `weather-app-dashboard-frontend` service to access your dashboard in a web browser.

## Configuration

All configurable parameters, including API keys, image names, resource limits, and service types, are managed through `weather-forecast-app/values.yaml`. Modify this file to customize your deployment.

## Future Enhancements

  * **ML Model Deployment:** Implement the `ml-model-hourly` and `ml-model-daily` services to consume processed data, perform predictions, and publish forecasts to Kafka.

  * **Model Serving:** Integrate a model serving framework (e.g., BentoML, MLflow, Seldon Core) for more robust model deployment and versioning.

  * **Monitoring & Alerting:** Set up Prometheus and Grafana (already indicated in `kubernetes/monitoring`) to monitor application health, Kafka metrics, and InfluxDB performance.

  * **CI/CD Pipeline:** Automate building Docker images, updating Helm charts, and deploying to Kubernetes.

  * **Scalability:** Implement horizontal pod autoscaling (HPA) for services based on CPU/memory usage or Kafka consumer lag.

  * **Data Retention Policies:** Configure InfluxDB retention policies for historical data.

  * **Improved Error Handling & Dead Letter Queues:** For Kafka messages that fail processing.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
