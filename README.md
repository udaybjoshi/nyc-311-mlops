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

## 📂 Root Directory Structure

```
nyc-311-platform/
│
├── .databricks/                    # Databricks Asset Bundle configuration
├── .github/                        # GitHub Actions CI/CD workflows
├── app/                           # Streamlit dashboard application
├── conf/                          # Configuration files
├── docs/                          # Documentation
├── infrastructure/                # Infrastructure as Code (Terraform)
├── notebooks/                     # Databricks notebooks
├── src/                           # Python source code
├── tests/                         # Test suites
├── .env.example                   # Environment variables template
├── .gitignore                     # Git ignore rules
├── databricks.yml                 # Databricks CLI configuration
├── pytest.ini                     # Pytest configuration
├── README.md                      # Main project documentation
├── requirements.txt               # Python dependencies
├── requirements-dev.txt           # Development dependencies
└── setup.py                       # Package setup configuration
```

---

## 📋 Detailed File Reference

### 🔧 Configuration Files

#### `.env.example`
**Purpose**: Template for environment variables  
**Key Contents**:
- AWS credentials
- Databricks connection details
- NYC API tokens
- Feature flags
- Cost thresholds

**Usage**:
```bash
cp .env.example .env
# Edit .env with your credentials
```

#### `databricks.yml`
**Purpose**: Databricks Asset Bundle configuration  
**Key Contents**:
- Job definitions (data ingestion, ML training)
- Cluster configurations
- Deployment targets (dev/prod)
- Schedules and triggers

**Usage**:
```bash
databricks bundle deploy -t dev
databricks bundle run -t dev daily_ingestion
```

#### `conf/cluster_config.yaml`
**Purpose**: Detailed cluster specifications  
**Key Contents**:
- Cost-optimized instance types
- Auto-termination settings
- Spark configurations
- Spot instance policies

**Cost Impact**: **Critical** - Proper configuration saves 70%+ on compute costs

#### `conf/dev.yaml` / `conf/prod.yaml`
**Purpose**: Environment-specific configurations  
**Key Contents**:
- Catalog/schema names
- Storage paths
- Batch sizes
- Quality thresholds

---

### 📓 Notebooks (Databricks)

#### `notebooks/00_setup_and_exploration.py`
**Purpose**: Initial setup and data exploration  
**When to Run**: First time setup  
**Outputs**: Understanding of data schema and volume  
**Cost**: ~$0.10 (5 min on single-node cluster)

#### `notebooks/01_bronze_ingestion.py`
**Purpose**: Ingest raw data from NYC Open Data API  
**Schedule**: Daily at 3 AM ET  
**Processing**:
- Fetch data via Socrata API
- Write to Bronze Delta table
- Partition by ingestion_date

**Key Features**:
- Incremental loading (reduces API calls)
- Error handling and retries
- Data quality checks

**Cost**: ~$0.30 per run (30 min)

#### `notebooks/02_silver_transformation.py`
**Purpose**: Clean and standardize data  
**Schedule**: After Bronze ingestion  
**Transformations**:
- Deduplication
- Schema enforcement
- Type casting
- Borough standardization
- Derived features (day_of_week, is_weekend, etc.)

**Cost**: ~$0.30 per run (20 min)

#### `notebooks/03_gold_aggregation.py`
**Purpose**: Create analytics-ready aggregations  
**Schedule**: After Silver transformation  
**Outputs**:
- Daily aggregates by complaint_type
- Borough-level statistics
- Temporal patterns

**Cost**: ~$0.15 per run (10 min)

#### `notebooks/04_ml_forecasting.py`
**Purpose**: Train Prophet forecasting models  
**Schedule**: Weekly on Sunday at 4 AM  
**Features**:
- Hyperparameter tuning with Optuna
- MLflow experiment tracking
- Model registration
- Cross-validation

**Cost**: ~$1.50 per run (30 min on ML cluster)

#### `notebooks/05_anomaly_detection.py`
**Purpose**: Detect unusual spikes in service requests  
**Schedule**: After forecasting  
**Method**: Statistical thresholds from Prophet uncertainty intervals  
**Outputs**: Anomaly alerts and scores

**Cost**: ~$0.15 per run (10 min)

#### `notebooks/99_monitoring_dashboard.py`
**Purpose**: Monitoring and debugging notebook  
**Usage**: Ad-hoc analysis and troubleshooting  
**Cost**: Variable (interactive use)

---

### 🐍 Python Source Code (`src/`)

#### `src/nyc311/__init__.py`
**Purpose**: Package initialization  
**Contents**: Version info, package metadata

#### `src/nyc311/ingestion/api_client.py`
**Purpose**: NYC Open Data API client  
**Key Classes**:
- `NYC311APIClient`: Main API interaction class
- `DataQualityValidator`: Validates fetched data
- `APIConfig`: Configuration dataclass

**Features**:
- Automatic pagination
- Rate limiting
- Retry logic with exponential backoff
- Type hints for IDE support

**Usage Example**:
```python
from src.nyc311.ingestion.api_client import NYC311APIClient

client = NYC311APIClient(app_token="your_token")
records = client.fetch_incremental(days_back=7)
```

#### `src/nyc311/transformations/bronze_to_silver.py`
**Purpose**: Silver layer transformation logic  
**Key Functions**:
- `deduplicate_records()`
- `standardize_schema()`
- `validate_data_quality()`
- `derive_temporal_features()`

#### `src/nyc311/transformations/silver_to_gold.py`
**Purpose**: Gold layer aggregation logic  
**Key Functions**:
- `aggregate_by_date()`
- `calculate_metrics()`
- `create_features()`

#### `src/nyc311/ml/forecasting.py`
**Purpose**: ML forecasting utilities  
**Key Functions**:
- `train_prophet_model()`
- `tune_hyperparameters()`
- `evaluate_model()`
- `generate_forecast()`

#### `src/nyc311/ml/anomaly_detection.py`
**Purpose**: Anomaly detection logic  
**Methods**:
- Threshold-based detection
- Z-score calculation
- Seasonal decomposition

#### `src/nyc311/utils/config.py`
**Purpose**: Configuration management  
**Features**:
- Load from environment variables
- Validate required settings
- Type-safe configuration objects

#### `src/nyc311/utils/logger.py`
**Purpose**: Centralized logging  
**Features**:
- Structured logging (JSON format)
- Log levels management
- Cloud logging integration

#### `src/nyc311/utils/delta_helpers.py`
**Purpose**: Delta Lake utility functions  
**Key Functions**:
- `optimize_table()`
- `vacuum_table()`
- `get_table_stats()`
- `merge_records()`

---

### 🧪 Tests (`tests/`)

#### `tests/unit/test_api_client.py`
**Purpose**: Unit tests for API client  
**Coverage**:
- API request handling
- Pagination logic
- Error scenarios
- Data validation

**Run**: `pytest tests/unit/test_api_client.py -v`

#### `tests/unit/test_transformations.py`
**Purpose**: Unit tests for transformation logic  
**Coverage**:
- Bronze → Silver transformation
- Silver → Gold aggregation
- Data quality rules

#### `tests/unit/test_ml_models.py`
**Purpose**: Unit tests for ML components  
**Coverage**:
- Model training
- Prediction generation
- Metric calculation

#### `tests/integration/test_pipeline_e2e.py`
**Purpose**: End-to-end pipeline tests  
**Coverage**:
- Full data flow (Bronze → Silver → Gold)
- ML training pipeline
- Dashboard data loading

**Run**: `pytest tests/integration/ -v --run-integration`

#### `pytest.ini`
**Purpose**: Pytest configuration  
**Settings**:
- Test discovery patterns
- Coverage reporting
- Markers for integration tests

---

### 📊 Dashboard (`app/`)

#### `app/streamlit_app.py`
**Purpose**: Interactive Streamlit dashboard  
**Features**:
- Real-time metrics
- Time series visualizations
- Forecast vs actual comparisons
- Anomaly alerts
- Borough breakdowns

**Run Locally**:
```bash
cd app
streamlit run streamlit_app.py
```

#### `app/Dockerfile`
**Purpose**: Containerize dashboard for deployment  
**Optimizations**:
- Multi-stage build (reduces image size)
- Non-root user (security)
- Health checks

**Build & Run**:
```bash
docker build -t nyc311-dashboard .
docker run -p 8501:8501 --env-file .env nyc311-dashboard
```

#### `app/requirements.txt`
**Purpose**: Dashboard-specific dependencies  
**Key Packages**:
- streamlit
- plotly
- databricks-sql-connector

#### `app/.streamlit/config.toml`
**Purpose**: Streamlit configuration  
**Settings**:
- Theme customization
- Server configuration
- Caching policies

---

### 🏗️ Infrastructure (`infrastructure/`)

#### `infrastructure/terraform/main.tf`
**Purpose**: AWS infrastructure as code  
**Resources Created**:
- S3 buckets (data, logs, config)
- IAM roles and policies
- CloudWatch alarms
- SNS topics for alerts

**Usage**:
```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

#### `infrastructure/terraform/variables.tf`
**Purpose**: Terraform input variables  
**Variables**:
- Environment name
- Region
- Bucket names
- Cost thresholds

#### `infrastructure/terraform/outputs.tf`
**Purpose**: Terraform outputs  
**Outputs**:
- S3 bucket ARNs
- IAM role ARNs
- SNS topic ARNs

#### `infrastructure/scripts/setup_workspace.sh`
**Purpose**: Automated workspace setup  
**Actions**:
- Create Databricks secrets
- Upload notebooks
- Configure cluster policies

#### `infrastructure/scripts/cost_monitor.sh`
**Purpose**: Daily cost monitoring  
**Actions**:
- Query AWS Cost Explorer
- Compare against thresholds
- Send alerts if over budget

---

### 🔄 CI/CD (`.github/`)

#### `.github/workflows/ci.yml`
**Purpose**: Continuous Integration pipeline  
**Jobs**:
1. **Code Quality**: Linting, formatting, type checking
2. **Unit Tests**: Run all unit tests with coverage
3. **Integration Tests**: End-to-end tests
4. **Security Scan**: Dependency vulnerabilities
5. **Build Docker**: Create dashboard image
6. **Deploy Dev**: Auto-deploy to dev environment
7. **Deploy Prod**: Manual approval for production

**Triggers**:
- Push to main/dev branches
- Pull requests
- Manual dispatch

#### `.github/workflows/deploy.yml`
**Purpose**: Deployment workflow  
**Actions**:
- Databricks bundle deployment
- Dashboard deployment to ECS
- Smoke tests

---

### 📚 Documentation (`docs/`)

#### `docs/architecture.md`
**Purpose**: System architecture documentation  
**Contents**:
- Architecture diagrams
- Data flow descriptions
- Component interactions
- Design decisions

#### `docs/cost_analysis.md`
**Purpose**: Detailed cost breakdown and optimization  
**Contents**:
- Per-component cost estimates
- Optimization strategies
- Monthly cost projections
- Cost monitoring setup

#### `docs/runbook.md`
**Purpose**: Operational procedures  
**Contents**:
- Deployment steps
- Troubleshooting guide
- Monitoring procedures
- Incident response

#### `docs/api.md`
**Purpose**: API documentation  
**Contents**:
- Function signatures
- Usage examples
- Best practices

---

## 📦 Dependencies

### `requirements.txt`
**Purpose**: Production Python dependencies  
**Key Packages**:
- `pyspark==3.5.0`: Spark processing
- `delta-spark==3.0.0`: Delta Lake support
- `prophet==1.1.5`: Time series forecasting
- `mlflow==2.9.2`: Experiment tracking
- `streamlit==1.29.0`: Dashboard framework

**Install**: `pip install -r requirements.txt`

### `requirements-dev.txt`
**Purpose**: Development/testing dependencies  
**Key Packages**:
- `pytest==7.4.3`: Testing framework
- `ruff==0.1.9`: Fast linter
- `black==23.12.1`: Code formatter
- `mypy==1.7.1`: Type checker

**Install**: `pip install -r requirements-dev.txt`

---

## 🎯 Quick Reference: Common Tasks

### Start Development
```bash
git clone <repo>
cd nyc-311-platform
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
# Edit .env with credentials
```

### Run Tests
```bash
pytest tests/unit -v                    # Unit tests
pytest tests/integration -v --run-integration  # Integration tests
pytest --cov=src tests/                # With coverage
```

### Deploy to Databricks
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev daily_ingestion
```

### Run Dashboard Locally
```bash
cd app
streamlit run streamlit_app.py
# Access at http://localhost:8501
```

### Monitor Costs
```bash
./infrastructure/scripts/cost_monitor.sh
aws ce get-cost-and-usage --time-period Start=2025-10-01,End=2025-10-06 --granularity DAILY --metrics UnblendedCost
```

---

## 📊 File Size Estimates

| Directory | File Count | Total Size | Notes |
|-----------|------------|------------|-------|
| `notebooks/` | 6 | ~50 KB | Lightweight notebooks |
| `src/` | ~20 | ~200 KB | Python source code |
| `tests/` | ~10 | ~100 KB | Test files |
| `app/` | 3 | ~30 KB | Dashboard code |
| `infrastructure/` | ~10 | ~50 KB | IaC files |
| `docs/` | ~5 | ~100 KB | Documentation |
| **Total** | **~60** | **~500 KB** | Excluding dependencies |

With dependencies:
- `venv/`: ~500 MB (Python packages)
- `.git/`: ~10 MB (version control)

---

## 🔐 Security Considerations

### Sensitive Files (Never Commit)
- `.env` - Environment variables with credentials
- `*.pem` - SSH keys
- `*.key` - API keys stored as files
- `secrets/` - Any secrets directory

### Protected by `.gitignore`
- All sensitive files listed above
- `venv/`, `__pycache__/`, `*.pyc`
- `.pytest_cache/`, `.coverage`
- `htmlcov/`, `*.egg-info/`

---

## 📝 Maintenance

### Regular Updates
- **Weekly**: Review and merge dependabot PRs
- **Monthly**: Update documentation, review costs
- **Quarterly**: Rotate credentials, security audit

### Version Control
- Use semantic versioning (MAJOR.MINOR.PATCH)
- Tag releases: `git tag v1.0.0`
- Maintain CHANGELOG.md

---


