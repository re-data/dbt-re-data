from .utils.run import dbt_seed, dbt_run, dbt_test

def test_normalizers(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }

    print (f"Running setup and tests for {db}")

    dbt_seed(f'--select public_macros.normalizing', db, dbt_vars)
    dbt_run(f'--select us_states_normalized+', db, dbt_vars)
    dbt_test(f'--models us_states_normalized', db, dbt_vars)

    print (f"Running tests completed for {db}")
