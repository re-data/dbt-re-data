from .utils.run import dbt_seed, dbt_run, dbt_test

def test_deduplication(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }

    print (f"Running setup and tests for {db}")

    dbt_seed(f'--select public_macros.filtering', db, dbt_vars)
    dbt_run(f'--select deduplicated', db, dbt_vars)
    dbt_test(f'--select deduplicated', db, dbt_vars)

def test_get_duplicates(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }

    print (f"Running setup and tests for {db}")

    dbt_seed(f'--select public_macros.filtering', db, dbt_vars)
    dbt_run(f'--select duplicates', db, dbt_vars)
    dbt_test(f'--select duplicates', db, dbt_vars)

    print (f"Running tests completed for {db}")
