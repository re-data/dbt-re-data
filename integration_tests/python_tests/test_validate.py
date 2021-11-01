import os
import pytest
from .utils.run import run_dbt

def test_validate_regex(db, debug=True):

    print (f"Running setup and tests for {db}")

    run_dbt('dbt seed --full-refresh --select '
            'public_macros.validating expected.validating'
        , db)
    run_dbt('dbt run --full-refresh --select public_macros.validating', db)
    run_dbt('dbt test --store-failures --select public_macros.validating --greedy', db)

    print (f"Running tests completed for {db}")