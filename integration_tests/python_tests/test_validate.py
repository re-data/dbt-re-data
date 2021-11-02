import os
import pytest
from .utils.run import dbt_seed, dbt_run, dbt_test

def test_validate_regex(db, debug=True):

    print (f"Running setup and tests for {db}")

    dbt_seed(
        '--select public_macros.validating expected.validating', db
    )

    dbt_run('--select public_macros.validating', db)
    dbt_test('--select public_macros.validating', db)

    print (f"Running tests completed for {db}")