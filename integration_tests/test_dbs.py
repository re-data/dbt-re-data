import os
import copy
import yaml
from datetime import datetime, timedelta

RUN_TIME = datetime(2021, 5, 2, 0, 0, 0)

DBT_VARS = {
    're_data:schemas': ['dq_raw', 'dq'],
    're_data:time_window_start': (RUN_TIME - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
    're_data:time_window_end': RUN_TIME.strftime("%Y-%m-%d %H:%M:%S"),
}

def _test_generic(db, dbt_vars=None, debug=True):

    load_deps = 'dbt deps'
    assert os.system(load_deps) == 0

    if not dbt_vars:
        dbt_vars = copy.deepcopy(DBT_VARS)
    
    print (f"Running setup and tests for {db}")

    profile_part = f' --profile re_data_{db}'

    print (f"Running init seed for {db}") 
    init_seeds = 'dbt seed --threads 4 --full-refresh {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
    os.system(init_seeds)
    print (f"Init seed completed for {db}") 
    
    re_dq_project = 'dbt run --models transformed --threads 4 -x --full-refresh {}'.format(profile_part)
    os.system(re_dq_project)
    print (f"Run dq_project expect re_data completed for {db}") 

    print (f"Computing re_data metrics for {db}") 
    run_re_data = 'dbt run --exclude transformed --threads 4 -x --full-refresh {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
    if debug:
        run_re_data = 'DBT_MACRO_DEBUGGING=1 ' + run_re_data

    print (f"Running for first day of data")
    assert os.system(run_re_data) == 0

    # updat dbts_vars to run dbt for next day of data
    dbt_vars['re_data:time_window_start'] = dbt_vars['re_data:time_window_end']
    dbt_vars['re_data:time_window_end'] = (RUN_TIME + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    re_data_next_day = 'dbt run --exclude transformed --threads 4 -x {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
    if debug:
        re_data_next_day = 'DBT_MACRO_DEBUGGING=1 ' + re_data_next_day
    
    print (f"Running for second day of data")
    assert os.system(re_data_next_day) == 0

    print (f"Running tests for {db}")
    test_re_data = 'dbt test --threads 4 --store-failures -x {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
    assert os.system(test_re_data) == 0

    print (f"Running tests completed for {db}")


def test_postgres():
    _test_generic('postgres')

def test_snowflake():
    _test_generic('snowflake')

def test_redshift():
    _test_generic('redshift')

def test_bigquery():
    _test_generic('bigquery')