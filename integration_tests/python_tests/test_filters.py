import os
import pytest
from .utils.run import dbt_seed, dbt_run, dbt_test

def test_deduplication(db, debug=True):

    print (f"Running setup and tests for {db}")

    dbt_seed('--select duplicated expected_deduplicated', db)
    dbt_run('--select deduplicated', db)
    dbt_test('--select deduplicated', db)

    print (f"Running tests completed for {db}")
