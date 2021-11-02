pytest --db postgres $@ & 
pytest --db snowflake $@ &
pytest --db bigquery $@ &
pytest --db redshift $@ &
wait 
