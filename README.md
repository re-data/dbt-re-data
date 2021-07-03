# dbt-re-data

dbt_re_data is dbt package for [re_data](https://github.com/redata-team/redata), data quality library build on top of dbt.

# Install

dbt_re_data is mostly intended to be used with `re_data` python package. But you can also add only dbt part to your project.

# Schema
All models created by re_data have custom schema with `re` suffix. First part of schema is taken as usual from your project profile configuration

## Variables
The following variables need to be defined in your dbt_project.yml:

```yaml
vars:
  re_data:schemas:
    - schema_to_monitor
    - another_schema_to_monitor
```

## Models
### Information about monitored tables

- [re_data_tables](#re_data_tables-source)
- [re_data_columns](#re_data_columns-source)

### Metrics computed

#### Table metrics
Contain metrics for whole table changes.

- [re_data_freshness](#re_data_freshness-source)
- [re_data_row_count](#re_data_row_count-source)

#### Column metrics
Contain metrics for specifc column in the table.
Currently stats are computed for numeric and text columns.

- [re_data_count_nulls](#re_data_count_nulls-source)
- [re_data_count_missing](#re_data_count_missing-source)
- [re_data_min](#re_data_min-source)
- [re_data_max](#re_data_max-source)
- [re_data_avg](#re_data_avg-source)
- [re_data_min_length](#re_data_min_length-source)
- [re_data_max_length](#re_data_max_length-source)
- [re_data_avg_length](#re_data_avg_length-source)
- [re_data_base_metrics](#re_data_base_metrics-source)

### Anomalies & Alerts
- [re_data_z_score](#re_data_z_score-source)
- [re_data_alerting](#re_data_alerting-source)

 #### re_data_tables ([source](models/meta/re_data_tables.sql))
 Information about all monitored tables. This is currently only table which is supposed to be edited (you can think of it as a configuration table) 
 2 columns can be changed there:
  - Change `actively_monitored` to `true`/`false` to start/stop monitoring table and computing stats for it, `(default: false)`
  - Change `time_filter` to name of column you would like to use as time filter
    Time filter is important thing in `re_data`, it's used in all filters computing metrics (to filter records added in a given day)
    One the start some educated guess :) is assigned as this field, but quite often it may require to be changed. `(default: first timestamp type column)`
  
**Important: default behaviour for newly discovered tables**
 
   By default (assuming env variable `re_data:actively_monitored_by_default` hasn't been changed tables are not monitored. So first `re_data`
   run should just find tables to monitor but don't actually compute metrics. This is for you so that you can check confirmation and run it for tables you wish.
   You can obviously just update all `actively_monitored` parameters to true if you want to run it for all tables or even set `re_data:actively_monitored_by_default` to true.
 
 
 #### re_data_columns ([source](models/meta/re_data_columns.sql))
 Information about all monitored columns, this contains information about columns similar to this
 what you can find in `information_schema`. This table is not supposed to be edited and new columns will be added and old removed
 in case of schema changes for your tables.
 
 #### re_data_freshness ([source](models/metrics_queries/re_data_base_metrics.sql))
 Information about time (in seconds) since last data was added to each table. `time_filter` column is used to find about
 time record was added. If `time_filter` column is updated, update time will also be taken into account, but be warned that in this case
 all stats computed will also take into account updated time (This maybe good or bad thing depeneding on your use case).
 
 #### re_data_row_count ([source](models/final_metrics/re_data_row_count.sql))
 Numbers of rows added to table in specific time range. `re_data` time range is currently one day period.
 
 #### re_data_count_nulls ([source](models/final_metrics/re_data_count_nulls.sql))
 Number of nulls in a given column for specific time range.
 
 #### re_data_count_missing ([source](models/final_metrics/re_data_count_missing.sql))
 Number of nulls and empty string values in a given column for specific time range.
 
 #### re_data_min ([source](models/final_metrics/re_data_min.sql))
 Minimal value appearing in a given column for specific time range.
 
 #### re_data_max ([source](models/final_metrics/re_data_max.sql))
 Maximal value appearing in a given column for specific time range.
 
 #### re_data_avg ([source](models/final_metrics/re_data_avg.sql))
 Average of all values appearing in a given column for specific time range.
 
 #### re_data_min_length ([source](models/final_metrics/re_data_min_length.sql))
 Minimal length of all strings appearing in a given column for specific time range.
 
 #### re_data_max_length ([source](models/final_metrics/re_data_max_length.sql))
 Maximal length of all strings appearing in a given column for specific time range.
 
 #### re_data_avg_length ([source](models/final_metrics/re_data_avg_length.sql))
 Average length of all strings appearing in a given column for specific time range.
 
 #### re_data_base_metrics ([source](models/metrics_queries/re_data_base_metrics.sql))
 Internal table containing most of described metrics (apart from `re_data_freshness`). To really access
 metrics it's usually better to use view for specific metric.
 
 #### re_data_z_score ([source](models/anomalies/re_data_z_score.sql))
 Computed z_score for metric. `re_data` looks back on what where metrics values in last 30 days and compute z_score for newest value.
 
 #### re_data_alerting ([source](models/final_metrics/re_data_alerting.sql))
 View computed on top of `re_data_z_score` table to contain metrics which look alerting. Alerting threshold is controled by var `re_data:alerting_z_score`
 which is equal to 3 by default, but can be changed and adjusted.
 
