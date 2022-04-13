import os
import copy
import yaml
import json
from datetime import datetime, timedelta
from .utils.run import dbt_seed, dbt_run, dbt_test, dbt_command

RUN_TIME = datetime(2021, 5, 2, 0, 0, 0)

DBT_VARS = {
    're_data:time_window_start': (RUN_TIME - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
    're_data:time_window_end': RUN_TIME.strftime("%Y-%m-%d %H:%M:%S"),
    're_data:save_test_history': True
}

def test_monitoring(db, source_schema):
    DBT_VARS.update({'source_schema': source_schema})

    load_deps = 'dbt deps'
    assert os.system(load_deps) == 0

    dbt_vars = copy.deepcopy(DBT_VARS)
    
    print (f"Running setup and tests for {db}")

    dbt_seed('--select monitoring', db, dbt_vars)
    dbt_run('--models transformed', db, dbt_vars)
    dbt_command(
        f'dbt run-operation create_test_source_tables',
        db, dbt_vars
    )

    print (f"Computing re_data metrics for {db}") 
    dbt_run('--select package:re_data', db, dbt_vars)

    dbt_command(
        f'dbt run-operation schema_change_buy_events_add_column',
        db, dbt_vars
    )

    # update dbts_vars to run dbt for next day of data
    dbt_vars['re_data:time_window_start'] = dbt_vars['re_data:time_window_end']
    dbt_vars['re_data:time_window_end'] = (RUN_TIME + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    dbt_command(
        'dbt run --select package:re_data --fail-fast',
        db, dbt_vars
    )

    dbt_command(
        'dbt run --select monitoring.*', db, dbt_vars
    )

    dbt_test('--select test_re_data_anomalies test_re_data_metrics test_re_data_z_score re_data_metrics transformed', db, dbt_vars)

    # tests test_history seperately, because those are actually added to DB after running
    # dbt test command
    dbt_test('--select test_re_data_test_history', db, dbt_vars)

    op_vars = {
        'start_date': RUN_TIME.strftime("%Y-%m-%d"),
        'end_date': (RUN_TIME + timedelta(days=1)).strftime("%Y-%m-%d"),
        'interval': 'days:1'
    }
    op_vars = yaml.dump(op_vars)
    
    dbt_command(
        f'dbt run-operation generate_overview --args "{op_vars}"',
        db, dbt_vars
    )

    overview = json.load(open(f'../target/re_data/overview.json'))
    expected_types = ['metric', 'schema_change', 'schema', 'alert', 'anomaly']
    all_types = set()

    # some simple check for now
    for obj in overview:
        all_types.add(obj['type'])
        assert obj['table_name']
        assert 'column_name' in obj
        assert 'computed_on' in obj

    assert len(overview) > 100
    assert sorted(all_types) == sorted(expected_types)

    print (f"Running tests completed for {db}")
