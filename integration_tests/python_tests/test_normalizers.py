import os
import pytest
from .utils.run import run_dbt

def test_normalizers(db, debug=True):

    print (f"Running setup and tests for {db}")

    run_dbt('dbt seed --full-refresh --select us_states_normalization expected_us_states_normalization', db)
    run_dbt('dbt run --full-refresh --select us_states_normalized+', db)
    run_dbt('dbt test --models us_states_normalized --greedy', db)

    print (f"Running tests completed for {db}")
