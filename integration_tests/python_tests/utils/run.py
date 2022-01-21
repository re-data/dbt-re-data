import os
import yaml

def dbt_command(command, for_db, dbt_vars, threads=None):
    debug = 'DBT_MACRO_DEBUGGING=1 '
    profile_part = f' --profile re_data_{for_db}'
    yaml_vars = yaml.dump(dbt_vars)
    cmd = f'{debug} {command} --vars "{yaml_vars}" {profile_part}'
    if threads:
        cmd += f' --threads {threads}'
    assert os.system(cmd) == 0

def dbt_seed(args, for_db, dbt_vars):
    dbt_command(f'dbt seed --full-refresh {args}', for_db, dbt_vars, threads=4)

def dbt_run(args, for_db, dbt_vars):
    dbt_command(f'dbt run --full-refresh --fail-fast {args}', for_db, dbt_vars, threads=4)

def dbt_test(args, for_db, dbt_vars):
    dbt_command(f'dbt test --store-failures --fail-fast {args}', for_db, dbt_vars, threads=4)