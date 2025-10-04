# 🗽 NYC 311 Service Request Intelligence Platform

A **production-grade data pipeline** that ingests NYC 311 service request data daily, processes it using the **Medallion Architecture** (Bronze → Silver → Gold), applies **forecasting** and **anomaly detection**, and visualizes insights via a **Streamlit dashboard**.

---

## 💼 Business Case: Why Anomaly Detection?

NYC’s 311 service provides a critical channel for citizens to report non-emergency issues — from noise complaints to infrastructure failures.  
However, due to the large volume and variety of requests, **sudden spikes or anomalies** often go undetected or are flagged too late.

### Gaps in the Current Process
- ❌ No automated mechanism to detect abnormal increases in specific complaint types or boroughs  
- ❌ Operational teams remain **reactive**, responding only after complaints surge  
- ❌ Delays in identifying anomalies lead to **service degradation**, inefficiencies, and poor resource allocation  

### This Project Solves
- ✅ **Timely detection** of unusual spikes in 311 requests using model-based thresholds  
- ✅ **Predictive insights** to forecast demand and guide resource planning  
- ✅ A **transparent, reproducible ML system** with full lineage, auditability, and visualization  

---

## 🚀 Features

- **Daily Data Ingestion** from NYC Open Data API  
- **Bronze → Silver → Gold** data processing via Delta Lake  
- **Forecasting** future service volumes with Prophet + Optuna hyperparameter tuning  
- **Anomaly Detection** using Prophet model thresholds  
- **Experiment Tracking** with MLflow  
- **Streamlit Dashboard** for stakeholder-friendly insights  
- **CI/CD** with GitHub Actions  
- **Fully Containerized** for deployment across Databricks or local environments  

---

## 🏗 Architecture Diagram

```text
       ┌──────────────────────────┐
       │   NYC Open Data API      │
       └────────────┬─────────────┘
                    │
                    ▼
        ┌────────────────────┐
        │     Bronze Layer    │
        │ Raw 311 API ingest  │
        │ (Delta Table)       │
        └────────┬────────────┘
                 │
                 ▼
        ┌────────────────────┐
        │    Silver Layer     │
        │ Clean + Deduped     │
        │ Normalized Schema   │
        └────────┬────────────┘
                 │
                 ▼
        ┌────────────────────┐
        │     Gold Layer      │
        │ Aggregated Views    │
        │ Forecasts & Anoms   │
        └────────┬────────────┘
                 │
                 ▼
        ┌────────────────────┐
        │ Streamlit Dashboard │
        │  + MLflow Tracking  │
        └────────────────────┘

---

## Repository Structure
nyc-311-mlops/
│
├── src/
│   ├── nyc311/
│   │   ├── ingestion/        # API → Bronze
│   │   ├── transforms/       # Bronze → Silver → Gold
│   │   ├── forecasting/      # Prophet + anomaly detection
│   │   ├── common/           # Configs, schemas, logging
│   │   └── jobs/             # Job entrypoints
│
├── notebooks/                # Databricks notebook runners
├── conf/                     # Environment configs (dev, acc, prod)
├── tests/                    # Unit and Spark integration tests
├── workflows/                # Databricks job/workflow definitions
├── databricks.yml            # Bundle configuration
├── Makefile                  # Developer commands
└── README.md                 # Documentation (you’re here)

