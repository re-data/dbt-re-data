import os
import copy
import yaml
from datetime import datetime, timedelta
from .utils.run import dbt_seed, dbt_run, dbt_test, dbt_command

RUN_TIME = datetime(2021, 5, 2, 0, 0, 0)

DBT_VARS = {
    're_data:time_window_start': (RUN_TIME - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
    're_data:time_window_end': RUN_TIME.strftime("%Y-%m-%d %H:%M:%S"),
}

def test_monitoring(db):

    load_deps = 'dbt deps'
    assert os.system(load_deps) == 0

    dbt_vars = copy.deepcopy(DBT_VARS)
    
    print (f"Running setup and tests for {db}")

    profile_part = f' --profile re_data_{db}'

    dbt_seed('--vars "{}"'.format(yaml.dump(dbt_vars)), db)
    dbt_run('--models transformed', db)

    print (f"Computing re_data metrics for {db}") 
    dbt_run('--exclude transformed  --vars "{}"'.format(yaml.dump(dbt_vars)), db)

    # updat dbts_vars to run dbt for next day of data
    dbt_vars['re_data:time_window_start'] = dbt_vars['re_data:time_window_end']
    dbt_vars['re_data:time_window_end'] = (RUN_TIME + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    dbt_command(
        'dbt run --exclude transformed --fail-fast --vars "{}"'.format(yaml.dump(dbt_vars)),
        db
    )

    dbt_test('--vars "{}"'.format(yaml.dump(dbt_vars)), db)

    print (f"Running tests completed for {db}")
