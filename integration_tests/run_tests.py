import os
import copy
import yaml
from datetime import datetime, timedelta

CHECK_DBS = ['postgres', 'snowflake', 'redshift', 'bigquery']

RUN_TIME = datetime(2021, 5, 2, 0, 0, 0)

DBT_VARS = {
    're_data:alerting_z_score': 3,
    're_data:schemas': ['re_data_raw'],
    're_data:time_window_start': (RUN_TIME - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
    're_data:time_window_end': RUN_TIME.strftime("%Y-%m-%d %H:%M:%S"),
    're_data:anomaly_detection_window_start': (RUN_TIME - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S"),
    're_data:actively_monitored_by_default': True
}

def test_dbt(debug=True):

    load_deps = 'dbt deps'
    os.system(load_deps)

    for db in CHECK_DBS:
        dbt_vars = copy.deepcopy(DBT_VARS)
        print (f"Running setup and tests for {db}")

        profile_part = f' --profile re_data_{db}'

        print (f"Running init seed for {db}") 
        init_seeds = 'dbt seed -x --full-refresh {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
        os.system(init_seeds)
        print (f"Init seed completed for {db}") 
        
        print (f"Computing re_data metrics for {db}") 
        if db == 'snowflake':
            schemas = dbt_vars['re_data:schemas']
            schemas = [el.upper() for el in schemas]
            dbt_vars['re_data:schemas'] = schemas

        run_re_data = 'dbt run -x --full-refresh {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
        if debug:
            run_re_data = 'DBT_MACRO_DEBUGGING=1 ' + run_re_data

        print (f"Running for first day of data")
        os.system(run_re_data)

        # updat dbts_vars to run dbt for next day of data
        dbt_vars['re_data:time_window_start'] = dbt_vars['re_data:time_window_end']
        dbt_vars['re_data:time_window_end'] = (RUN_TIME + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        re_data_next_day = 'dbt run -x {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
        if debug:
            re_data_next_day = 'DBT_MACRO_DEBUGGING=1 ' + re_data_next_day
        
        print (f"Running for second day of data")
        os.system(re_data_next_day)

        print (f"Running tests for {db}")
        test_re_data = 'dbt test -x {} --vars "{}"'.format(profile_part, yaml.dump(dbt_vars))
        os.system(test_re_data)

        print (f"Running tests completed for {db}")


if __name__ == "__main__":
    test_dbt()