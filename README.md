# 📊 NYC 311 Service Request Intelligence Platform (AWS + Databricks Edition)

## 🎯 Project Objective

Build a **real-time, production-grade MLOps pipeline** on AWS and Databricks that:

- Ingests NYC 311 data daily from the API
- Follows a Medallion Architecture: Bronze → Silver → Gold
- Forecasts service request volumes using Prophet
- Flags anomalies based on model thresholds
- Tracks experiments using MLflow (Databricks-native)
- Visualizes insights via Streamlit
- Is containerized and CI/CD-enabled for deployment

---

## 🧱 Project Architecture Overview

| Layer               | Technology Used                                         |
|--------------------|----------------------------------------------------------|
| Ingestion          | Python, S3, Databricks Auto Loader                       |
| Transformation     | PySpark, Delta Lake (Bronze → Silver → Gold)             |
| Forecasting        | Prophet, Optuna (on Databricks Jobs)                     |
| Anomaly Detection  | Prophet thresholds, Z-score (PySpark)                    |
| Experiment Tracking| MLflow (Databricks-native)                               |
| Orchestration      | Databricks Workflows or Prefect on AWS                   |
| Dashboard          | Streamlit hosted via ECS or EC2                          |
| CI/CD              | GitHub Actions, Docker, AWS ECS, Databricks REST API     |
| Testing            | Pytest (unit + integration against Delta tables)         |

---

## 🔁 Medallion Architecture Flow

API → S3 (raw) → Databricks Auto Loader → Bronze Delta
→ Silver Delta (cleaned & normalized)
→ Gold Delta (aggregated for forecasting & dashboard)



---

## 🔧 Component Directory Structure

nyc-311-mlops/
├── ingestion/ # API to S3, Auto Loader setup
├── transformation/ # Bronze → Silver → Gold Delta jobs
├── ml/ # Forecasting + anomaly detection
├── orchestration/ # Databricks Workflows or Prefect flows
├── dashboards/ # Streamlit app
├── tests/ # Unit and integration tests
├── docker-compose.yml # Local testing
├── requirements.txt
├── README.md # This file
└── .github/workflows/ci.yml # CI/CD pipeline



---

## 🚀 How to Run the Pipeline (Dev Mode)

### 1. Ingest Data to S3
python ingestion/fetch_api_data.py

### 2. Load Raw Data to Databricks (Bronze)
Handled automatically by Databricks Auto Loader.

### 3. Transform: Bronze → Silver → Gold
Run notebook jobs or Databricks Workflows via UI or API.

### 4. Forecast & Flag Anomalies
python ml/generate_forecasts.py

### 5. Track ML Experiments
Logged automatically to Databricks MLflow.

### 6. View Dashboard
streamlit run dashboards/app.py

## 📈 Deliverables to Showcase

| Deliverable                      | Value                                               |
| -------------------------------- | --------------------------------------------------- |
| 📊 Forecast + anomaly dashboard  | Demonstrates real-time insights for stakeholders    |
| 🧪 MLflow + Optuna tuning logs   | Showcases experimentation and model reproducibility |
| 🧊 Delta Lake Medallion pipeline | Scalable, modular data engineering architecture     |
| ⚙️ Databricks Workflows          | Reliable automation of daily flows                  |
| 🐳 Docker + CI/CD pipeline       | Proves deployment readiness and DevOps maturity     |
| ✅ Tests (unit + integration)     | Ensures production-grade quality assurance          |

## 🔐 Deployment
- Streamlit App: Deployed via Docker to AWS ECS Fargate or EC2
- Forecasting Pipeline: Scheduled via Databricks Workflows
- CI/CD: GitHub Actions → Docker Build → Deploy to ECS & Databricks Jobs via API

## 🧪 Testing
Run unit and integration tests locally:
pytest tests/

To test against Delta tables in Databricks:
- Use the Databricks SQL Connector or pytest-databricks-connect
