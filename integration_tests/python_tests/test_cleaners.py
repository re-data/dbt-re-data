from .utils.run import dbt_seed, dbt_run, dbt_test

def test_cleaners(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }

    print (f"Running setup and tests for {db}")

    dbt_seed('--select sample_user_data expected_sample_user_data customers_to_impute expected_customers_imputed', db, dbt_vars)
    
    dbt_run('--select sanitized_user_data+', db, dbt_vars)
    dbt_test('--select sanitized_user_data', db, dbt_vars)
    
    dbt_run('--select imputed_customers_data+', db, dbt_vars)
    dbt_test('--select imputed_customers_data', db, dbt_vars)
  
    print (f"Running tests completed for {db}")
