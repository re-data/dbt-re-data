from .utils.run import dbt_seed, dbt_run, dbt_test

def test_cleaners(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }

    print (f"Running setup and tests for {db}")
    seeds_cmd = '--select sample_user_data expected_sample_user_data customers_to_impute expected_customers_imputed'
    print('seeds cmd :',seeds_cmd)

    dbt_seed(seeds_cmd, db)
    
    dbt_run('--select sanitized_user_data+', db)
    dbt_test('--select sanitized_user_data', db)
    
    dbt_run('--select imputed_customers_data+', db)
    dbt_test('--select imputed_customers_data', db)
  
    print (f"Running tests completed for {db}")
