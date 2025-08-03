# 🚀 NYC 311 Service Request Intelligence Platform

A production-grade MLOps pipeline for ingesting, processing, forecasting, and detecting anomalies in NYC 311 service request data.

---

## 📦 Project Overview

This project builds a real-time data pipeline that:
- Ingests daily NYC 311 service request data from the public API
- Stores raw data in a **MySQL Bronze layer**
- Transforms data into clean Silver and aggregated Gold layers
- Forecasts request volume using **Prophet + Optuna**
- Detects anomalies in complaint trends
- Tracks model performance with **MLflow**
- Supports orchestration using **Prefect**
- Visualizes outputs via dashboards (optional)

---

## 🛠️ Tech Stack

- **Python 3.11**
- **Docker + Docker Compose**
- **MySQL 8.x**
- **Prophet**, **Optuna**, **scikit-learn**
- **Prefect 2.x** for orchestration
- **MLflow** for experiment tracking
- **Tableau / Streamlit** (optional) for dashboarding

---

## 🧱 Medallion Architecture

| Layer  | Description                                      |
|--------|--------------------------------------------------|
| Bronze | Raw JSON from the NYC 311 API                    |
| Silver | Cleaned, deduplicated data with standard schema  |
| Gold   | Aggregated complaint counts with derived features|

---

## 📂 Project Structure

```text
├── ingestion/                  # API fetching + Bronze layer
├── transformation/            # Bronze → Silver → Gold logic
├── ml/                        # Forecast + anomaly detection
├── orchestration/             # Prefect pipeline flows
├── tests/                     # Unit + integration tests
├── scripts/                   # CLI scripts for dev/test runs
├── data/                      # DB utils and SQL helpers
├── logs/                      # Pipeline logs
├── requirements.txt
├── docker-compose.yml
├── Makefile
└── README.md


