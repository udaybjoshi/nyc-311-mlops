import pytest
from transformation.transform_to_silver import bronze_to_silver_task

def test_bronze_to_silver_runs():
    bronze_to_silver_task()