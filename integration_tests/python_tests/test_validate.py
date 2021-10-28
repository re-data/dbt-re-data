import os
import pytest
from .utils.run import run_dbt

@pytest.mark.parametrize('db', ['postgres'])
def test_validate_regex(db, debug=True):

    print (f"Running setup and tests for {db}")

    run_dbt('dbt seed --full-refresh --select validate_email', db)
    run_dbt('dbt run --full-refresh --select validate_email+', db)

    print (f"Running tests completed for {db}")