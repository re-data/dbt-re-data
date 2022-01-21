import yaml
from .utils.run import dbt_seed, dbt_run, dbt_test

def test_deduplication(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }
    yaml_vars = yaml.dump(dbt_vars)

    print (f"Running setup and tests for {db}")

    dbt_seed(f'--select duplicated expected_deduplicated --vars "{yaml_vars}"', db)
    dbt_run(f'--select deduplicated --vars "{yaml_vars}"', db)
    dbt_test(f'--select deduplicated --vars "{yaml_vars}"', db)

def test_get_duplicates(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }
    yaml_vars = yaml.dump(dbt_vars)

    print (f"Running setup and tests for {db}")

    dbt_seed(f'--select duplicates expected_duplicates --vars "{yaml_vars}"', db)
    dbt_run(f'--select duplicates --vars "{yaml_vars}"', db)
    dbt_test(f'--select duplicates --vars "{yaml_vars}"', db)

    print (f"Running tests completed for {db}")
