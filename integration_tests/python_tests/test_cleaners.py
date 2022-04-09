from .utils.run import dbt_seed, dbt_run, dbt_test

def test_cleaners(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }

    print (f"Running setup and tests for {db}")

    dbt_seed(f'--select public_macros.cleaning', db, dbt_vars)
    dbt_run(f'--select sanitized_user_data+', db, dbt_vars)
    dbt_test(f'--select sanitized_user_data', db, dbt_vars)

    print (f"Running tests completed for {db}")
