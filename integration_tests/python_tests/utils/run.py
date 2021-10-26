import os

def run_dbt(command, for_db):
    debug = 'DBT_MACRO_DEBUGGING=1 '
    profile_part = f' --profile re_data_{for_db}'
    assert os.system(debug + command + profile_part) == 0
