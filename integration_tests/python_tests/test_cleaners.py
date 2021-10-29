import os
import pytest
from .utils.run import run_dbt

def test_cleaners(db, debug=True):

    print (f"Running setup and tests for {db}")

    run_dbt('dbt seed --full-refresh --select sample_user_data expected_sample_user_data', db)
    run_dbt('dbt run --full-refresh --select sanitized_user_data+', db)
    run_dbt('dbt test --models sanitized_user_data --greedy', db)

    print (f"Running tests completed for {db}")
