import yaml
from .utils.run import dbt_seed, dbt_run, dbt_test

def test_validate_regex(db, source_schema, debug=True):
    dbt_vars = {
        'source_schema': source_schema
    }
    yaml_vars = yaml.dump(dbt_vars)

    print (f"Running setup and tests for {db}")

    dbt_seed(
        f'--select public_macros.validating expected.validating --vars "{yaml_vars}"', db
    )

    dbt_run(f'--select public_macros.validating --vars "{yaml_vars}"', db)
    dbt_test(f'--select public_macros.validating --vars "{yaml_vars}"', db)

    print (f"Running tests completed for {db}")