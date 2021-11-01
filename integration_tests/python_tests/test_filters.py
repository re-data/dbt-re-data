import os
import pytest
from .utils.run import run_dbt

def test_deduplication(db, debug=True):

    print (f"Running setup and tests for {db}")

    run_dbt('dbt seed --full-refresh --select duplicated expected_deduplicated', db)
    run_dbt('dbt run --full-refresh --select duplicated+', db)
    run_dbt('dbt test --store-failures --models deduplicated --greedy', db)

    print (f"Running tests completed for {db}")
