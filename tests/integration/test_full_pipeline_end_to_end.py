from orchestration.full_pipeline_flow import medallion_pipeline

def test_pipeline_runs_end_to_end():
    # WARNING: Requires real DB connection and API access
    medallion_pipeline(target_date="2025-08-01")