# 🗽 NYC 311 Service Request Intelligence Platform - PoC

A **cost-optimized, production-grade data pipeline** for NYC 311 service request analysis using Databricks, AWS, and the Medallion Architecture. Built to stay **under $100** while demonstrating enterprise ML engineering practices.

---

## 📊 Project Overview

This platform ingests NYC 311 service request data, processes it through Bronze → Silver → Gold layers, applies ML-based forecasting and anomaly detection, and delivers insights via an interactive dashboard.

### 🎯 Business Value
- **Proactive Operations**: Detect anomalous spikes in service requests before they become critical
- **Resource Optimization**: Forecast demand patterns for better workforce allocation
- **Cost Efficiency**: 95% cost reduction vs. traditional 24/7 cluster approaches
- **Transparency**: Full ML lineage, auditability, and explainable insights

---

## 💰 Cost Optimization Strategy (Target: <$100)

| Component | Strategy | Est. Cost |
|-----------|----------|-----------|
| **Databricks Compute** | Job clusters (auto-terminate) + Community Edition for dev | $30-40 |
| **AWS S3 Storage** | Lifecycle policies, compressed Delta tables | $5-10 |
| **Data Volume** | Sample last 90 days only (vs. 10+ years full dataset) | $0 |
| **API Calls** | Cached responses, incremental loads | $0 |
| **Development** | Local Spark for testing, CI/CD optimizations | $5-10 |
| **Monitoring** | Built-in Databricks metrics (no external tools) | $0 |
| **Total Estimate** | | **$40-60** |

### 🔧 Cost Control Measures
- ✅ **Job Clusters**: Spin up only when needed, terminate after 5 min idle
- ✅ **Spot Instances**: 70% discount on AWS spot for batch workloads
- ✅ **Data Sampling**: Process 90-day rolling window (not full 10+ year dataset)
- ✅ **Incremental Processing**: Delta Lake change data capture
- ✅ **Auto-scaling**: Min 1 worker, max 3 workers
- ✅ **Scheduled Jobs**: Run daily during off-peak hours (3 AM ET)
- ✅ **Local Development**: Unit tests run on local Spark (no cluster costs)

---

## 🏗️ Architecture

```
┌─────────────────┐
│  NYC Open Data  │
│   311 API       │
└────────┬────────┘
         │ Daily Ingestion (Scheduled Job)
         ▼
┌─────────────────────────────────────────┐
│         BRONZE LAYER (Raw Data)          │
│  • Raw JSON from API                     │
│  • Append-only Delta tables              │
│  • Partition by ingestion_date           │
└────────┬────────────────────────────────┘
         │ Data Quality + Deduplication
         ▼
┌─────────────────────────────────────────┐
│      SILVER LAYER (Cleaned Data)         │
│  • Standardized schema                   │
│  • Type casting + validation             │
│  • Business rules applied                │
└────────┬────────────────────────────────┘
         │ Aggregation + Feature Engineering
         ▼
┌─────────────────────────────────────────┐
│    GOLD LAYER (Analytics-Ready Data)     │
│  • Daily aggregates by complaint_type    │
│  • Time series features                  │
│  • Ready for ML + BI tools               │
└────────┬────────────────────────────────┘
         │
         ├─────────────────┬────────────────┐
         ▼                 ▼                ▼
    ┌─────────┐    ┌──────────────┐  ┌──────────┐
    │ Prophet │    │   Anomaly    │  │Streamlit │
    │Forecast │    │  Detection   │  │Dashboard │
    │ Model   │    │  (Threshold) │  │          │
    └─────────┘    └──────────────┘  └──────────┘
         │                 │
         └────────┬────────┘
                  ▼
          ┌──────────────┐
          │    MLflow    │
          │  Experiment  │
          │   Tracking   │
          └──────────────┘
```

---

## 📁 Project Structure

```
nyc-311-platform/
│
├── .databricks/                 # Databricks configuration
│   └── bundle.yml              # Databricks Asset Bundle config
│
├── .github/
│   └── workflows/
│       ├── ci.yml              # CI/CD pipeline
│       └── deploy.yml          # Deployment automation
│
├── conf/
│   ├── dev.yaml                # Development environment config
│   ├── prod.yaml               # Production environment config
│   └── cluster_config.yaml     # Cost-optimized cluster specs
│
├── notebooks/
│   ├── 00_setup_and_exploration.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_aggregation.py
│   ├── 04_ml_forecasting.py
│   ├── 05_anomaly_detection.py
│   └── 99_monitoring_dashboard.py
│
├── src/
│   ├── nyc311/
│   │   ├── __init__.py
│   │   ├── ingestion/
│   │   │   ├── __init__.py
│   │   │   └── api_client.py
│   │   ├── transformations/
│   │   │   ├── __init__.py
│   │   │   ├── bronze_to_silver.py
│   │   │   └── silver_to_gold.py
│   │   ├── ml/
│   │   │   ├── __init__.py
│   │   │   ├── forecasting.py
│   │   │   └── anomaly_detection.py
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   ├── config.py
│   │   │   ├── logger.py
│   │   │   └── delta_helpers.py
│   │   └── monitoring/
│   │       ├── __init__.py
│   │       └── metrics.py
│   └── setup.py
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_api_client.py
│   │   ├── test_transformations.py
│   │   └── test_ml_models.py
│   └── integration/
│       └── test_pipeline_e2e.py
│
├── app/
│   ├── streamlit_app.py        # Dashboard application
│   ├── requirements.txt
│   └── Dockerfile
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf             # AWS S3 + IAM setup
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── scripts/
│       ├── setup_workspace.sh
│       └── cost_monitor.sh
│
├── docs/
│   ├── architecture.md
│   ├── cost_analysis.md
│   └── runbook.md
│
├── .env.example                # Environment variables template
├── .gitignore
├── databricks.yml              # Databricks CLI config
├── requirements.txt            # Python dependencies
├── requirements-dev.txt        # Development dependencies
├── pytest.ini
├── setup.py
└── README.md
```

---

## 🚀 Quick Start (Under $100 Budget)

### Prerequisites
- AWS Account (free tier eligible)
- Databricks Account (Community Edition or trial)
- Python 3.9+
- Databricks CLI

### 1️⃣ Initial Setup (5 minutes, $0)

```bash
# Clone repository
git clone <your-repo-url>
cd nyc-311-platform

# Install dependencies locally (for development)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements-dev.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### 2️⃣ Infrastructure Setup (10 minutes, ~$5)

```bash
# Setup AWS resources (S3 buckets with lifecycle policies)
cd infrastructure/terraform
terraform init
terraform plan
terraform apply -var="environment=dev"

# Configure Databricks workspace
databricks configure --token
databricks bundle deploy --target dev
```

### 3️⃣ Run Initial Pipeline (30 minutes, ~$15)

```bash
# Option A: Via Databricks CLI (recommended for cost control)
databricks jobs run-now --job-id <job-id>

# Option B: Via Python (local development)
python -m pytest tests/unit  # Runs locally, $0
```

### 4️⃣ Deploy Dashboard (5 minutes, ~$5/month)

```bash
cd app
docker build -t nyc311-dashboard .
# Deploy to AWS ECS Fargate (or run locally)
streamlit run streamlit_app.py
```

---

## 🎓 ML Engineering Best Practices

### ✅ Code Quality
- **Type Hints**: Full static typing with `mypy`
- **Linting**: `ruff` for fast, modern Python linting
- **Formatting**: `black` for consistent code style
- **Testing**: 85%+ coverage with `pytest`

### ✅ Data Engineering
- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Incremental Processing**: Only process new/changed data
- **Data Quality**: Automated expectations with Great Expectations
- **Partitioning**: Optimized by `ingestion_date` for pruning

### ✅ ML Operations
- **Experiment Tracking**: MLflow for all model training
- **Model Registry**: Versioned models with staging/production aliases
- **Hyperparameter Tuning**: Optuna for efficient search
- **Feature Store**: Databricks Feature Store for reusability

### ✅ Observability
- **Pipeline Monitoring**: Success/failure rates, duration metrics
- **Data Quality Alerts**: Automated anomaly detection on data drift
- **Cost Tracking**: Daily spend alerts (AWS CloudWatch + SNS)
- **Model Performance**: Prediction accuracy, MAPE, residuals

---

## 📊 Data Pipeline Details

### Bronze Layer (Raw Ingestion)
```python
# Daily incremental load (last 7 days with 1-day overlap)
# Estimated: 50K-100K records/day
# Storage: ~10MB/day compressed (Delta)
# Cost: $0.02/day S3 storage
```

### Silver Layer (Cleaned Data)
```python
# Apply data quality rules
# - Remove duplicates (unique_key deduplication)
# - Validate schema (complaint_type not null)
# - Standardize dates (UTC timezone)
# - Enrich with borough lookup
# Cost: 2 min job runtime = $0.30
```

### Gold Layer (Aggregates)
```python
# Daily aggregations by:
# - complaint_type
# - borough
# - agency
# Features: rolling averages, lag features
# Cost: 1 min job runtime = $0.15
```

---

## 🤖 ML Model Details

### Forecasting Model (Prophet)
- **Objective**: Predict next 30 days of service requests
- **Features**: Trend, weekly seasonality, holidays
- **Training**: Weekly on 90-day rolling window
- **Hyperparameters**: Tuned via Optuna (20 trials)
- **Metrics**: MAPE, MAE, RMSE
- **Cost**: 10 min training = $1.50/week

### Anomaly Detection
- **Method**: Statistical thresholds from Prophet uncertainty intervals
- **Logic**: Flag if actual > upper_bound (95% confidence)
- **Alerts**: Triggered for 3+ consecutive anomalies
- **Cost**: 1 min inference = $0.15/day

---

## 📈 Success Metrics

### Technical KPIs
- **Pipeline SLA**: 99.5% daily completion rate
- **Data Freshness**: <2 hours lag from API to Gold layer
- **Model Accuracy**: MAPE <15% on 7-day forecast
- **False Positive Rate**: <5% on anomaly detection

### Business KPIs
- **Early Detection**: Identify spikes 2-3 days before peak
- **Resource Optimization**: 20% improvement in crew scheduling
- **Cost Efficiency**: Maintain <$140/month operational cost (3 environments)
- **Cost Efficiency (Optimized)**: <$80/month with on-demand QA

---

## 🔒 Security & Compliance

- **Secrets Management**: AWS Secrets Manager (no hardcoded credentials)
- **IAM Roles**: Least-privilege access for Databricks → S3
- **Data Encryption**: At-rest (S3 SSE-S3) and in-transit (TLS 1.2+)
- **Audit Logging**: Delta Lake transaction logs + CloudTrail
- **PII Handling**: No PII in 311 data, but架构 supports masking if needed

---

## 🐛 Troubleshooting

### Issue: Job fails with "Out of Memory"
**Solution**: Increase worker node size or optimize data partitioning
```python
# Re-partition Silver table
spark.read.table("silver.requests").repartition(10).write.mode("overwrite").saveAsTable("silver.requests")
```

### Issue: Forecast model poor accuracy
**Solution**: Check data quality, extend training window
```python
# Validate no missing dates in training data
df.groupBy(f.window("created_date", "1 day")).count().orderBy("window")
```

### Issue: Exceeding $100 budget
**Solution**: Review cluster usage, implement auto-termination
```bash
# Check spend
databricks jobs list --output JSON | jq '.jobs[] | select(.settings.max_concurrent_runs > 1)'
```

---

## 🎯 Roadmap

### Phase 1: PoC (Current - Week 4)
- ✅ Basic medallion architecture
- ✅ Prophet forecasting
- ✅ Threshold-based anomaly detection
- ✅ Streamlit dashboard

### Phase 2: Production Hardening (Week 5-8)
- ⬜ Advanced anomaly detection (Isolation Forest, LSTM)
- ⬜ Real-time streaming (Kinesis + Structured Streaming)
- ⬜ Automated retraining pipeline
- ⬜ A/B testing framework

### Phase 3: Scale & Expand (Week 9-12)
- ⬜ Multi-city expansion (Chicago, LA, Boston)
- ⬜ Predictive maintenance for city assets
- ⬜ Citizen-facing mobile app integration
- ⬜ Advanced NLP on complaint descriptions

---

## 👥 Contributing

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes, add tests
pytest tests/

# Submit PR with cost impact analysis
# Include: "Estimated cost change: +$X/month"
```

---

## 📚 References

- [NYC 311 Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)
- [Databricks Cost Optimization](https://docs.databricks.com/optimizations/cost-optimization.html)
- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)
- [Prophet Documentation](https://facebook.github.io/prophet/)
- [MLflow Guide](https://mlflow.org/docs/latest/index.html)

---

## 📝 License

MIT License - See LICENSE file

---





