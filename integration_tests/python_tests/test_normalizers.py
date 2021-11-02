import os
import pytest
from .utils.run import dbt_seed, dbt_run, dbt_test

def test_normalizers(db, debug=True):

    print (f"Running setup and tests for {db}")

    dbt_seed('--select us_states_normalization expected_us_states_normalized', db)
    dbt_run('--select us_states_normalized+', db)
    dbt_test('--models us_states_normalized', db)

    print (f"Running tests completed for {db}")
