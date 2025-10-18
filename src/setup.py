from setuptools import setup, find_packages

setup(
    name="nyc311",
    version="0.1.0",
    description="NYC 311 MLOps pipelines (Databricks + AWS)",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[],  # keep empty; use requirements*.txt to manage deps
    entry_points={
        "console_scripts": [
            "bronze_ingest=nyc311.entrypoints.bronze_ingest:main",
            "silver_transform=nyc311.entrypoints.silver_transform:main",
            "gold_aggregate=nyc311.entrypoints.gold_aggregate:main",
            "forecast_train=nyc311.entrypoints.forecast_train:main",
            "forecast_infer=nyc311.entrypoints.forecast_infer:main",
            "anomaly_detect=nyc311.entrypoints.anomaly_detect:main",
            "forecast_tune=nyc311.entrypoints.forecast_tune:main",
            "registry_promote=nyc311.entrypoints.registry_promote:main",
        ]
    },
    zip_safe=False,
)

