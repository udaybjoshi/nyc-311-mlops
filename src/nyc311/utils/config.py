"""Env-aware config loader (dev/qa/prod) with pydantic validation."""

from __future__ import annotations
import os, yaml
from pydantic import BaseModel, Field

class AwsCfg(BaseModel):
    """AWS configuration settings."""
    region: str
    s3_bucket: str
    s3_prefix: str

class DbcCfg(BaseModel):
    """Databricks Unity Catalog and scheduling settings."""
    uc_catalog: str
    uc_bronze: str
    uc_silver: str
    uc_gold: str
    schedule_cron: str

class PipelinesCfg(BaseModel):
    """Pipeline execution and cost-control parameters."""
    sample_days: int = Field(90, ge=7, le=365)
    forecast_horizon_days: int = Field(14, ge=7, le=60)
    overlap_days: int = Field(1, ge=0, le=7)
    min_workers: int = 1
    max_workers: int = 3
    auto_terminate_minutes: int = 5
    use_spot: bool = True

class AppCfg(BaseModel):
    """Top-level application configuration."""
    env: str
    aws: AwsCfg
    databricks: DbcCfg
    pipelines: PipelinesCfg

def load_config(env: str | None = None) -> AppCfg:
    """Load and validate environment-specific configuration.

    Args:
      env: Target environment name; if None, uses ENV env var or 'dev'.

    Returns:
      AppCfg: Validated configuration object.

    Raises:
      FileNotFoundError: If the env YAML file is missing.
      ValidationError: If pydantic validation fails.
    """
    env = env or os.getenv("ENV", "dev")
    path = os.path.join("conf", f"{env}.yaml")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return AppCfg(**raw)
