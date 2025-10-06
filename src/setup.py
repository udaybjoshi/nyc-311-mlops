from setuptools import setup, find_packages

setup(
    name="nyc311",
    version="0.1.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[],
    entry_points={
        "console_scripts": [
            "bronze_ingest=nyc311.entrypoints.bronze_ingest:main",
            "silver_transform=nyc311.entrypoints.silver_transform:main",
            "gold_aggregate=nyc311.entrypoints.gold_aggregate:main",
            "forecast_train=nyc311.entrypoints.forecast_train:main",
            "anomaly_detect=nyc311.entrypoints.anomaly_detect:main",
        ]
    },
)
