# dbt_re_data

dbt_re_data is dbt package for [re_data](https://github.com/redata-team/redata), data quality library build on top of dbt.

# Install

dbt_re_data is mostly intended to be used with `re_data` python package. But you can also add only dbt part to your project.

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

- [re_monitored_tables](#re_monitored_tables-source)
- [re_monitored_columns](#re_monitored_columns-source)

### Metrics computed

#### Table metrics
Contain metrics for whole table changes.

- [re_freshness](#re_freshness-source)
- [re_row_count](#re_row_count-source)

#### Column metrics
Contain metrics for specifc column in the table.
Currently stats are computed for numeric and text columns.

- [re_count_nulls](#re_count_nulls-source)
- [re_count_missing](#re_count_missing-source)
- [re_min](#re_min-source)
- [re_max](#re_max-source)
- [re_avg](#re_avg-source)
- [re_min_length](#re_min_length-source)
- [re_max_length](#re_max_length-source)
- [re_avg_length](#re_avg_length-source)
- [re_base_metrics](#re_base_metrics-source)

### Anomalies & Alerts
- [re_z_score](#re_z_score-source)
- [re_alerting](#re_alerting-source)

 #### re_monitored_tables ([source](models/meta/re_monitored_tables.sql))
 Information about all monitored tables. You can edit it in 2 ways:
  - Change `actively_monitored` to false to stop monitor table and stop computing stats for it
  - Change `time_filter` to name of column you would like to use as time filter.
    Time filter is important thing in `re_data`, it's used in all filters computing metrics (to filter records added in a given day)
    One the start some educated guess :) is assigned as this field, but quite often it may require to be changed.
  
**Important: default behaviour for newly discovered tables**
 
   By default (assuming env variable `re_data:activey_monitored_by_default` hasn't been changed tables are not monitored. So first `re_data`
   run should just find tables to monitor but don't actually compute metrics. This is for you so that you can check confirmation and run it for tables you wish.
   You can obviously just update all `actively_monitored` parameters to true if you want to run it for all tables or set  `re_data:activey_monitored_by_default` to true.
 
 
 #### re_monitored_columns ([source](models/meta/re_monitored_columns.sql))
 Information about all monitored columns, this contains information about columns similar to this
 what you can find in `information_schema`. This table is not supposed to be edited and new columns will be added and old removed
 in case of schema changes for your tables.
 
 #### re_freshness ([source](models/metrics_queries/re_base_metrics.sql))
 Information about time (in seconds) since last data was added to each table. `time_filter` column is used to find about
 time record was added. If `time_filter` column is updated, update time will also be taken into account, but be warned that in this case
 all stats computed will also take into account updated time (This maybe good or bad thing depeneding on your use case).
 
 #### re_row_count ([source](models/final_metrics/re_row_count.sql))
 Numbers of rows added to table in specific time range. `re_data` time range is currently one day period.
 
 #### re_count_nulls ([source](models/final_metrics/re_count_nulls.sql))
 Number of nulls in a given column for specific time range.
 
 #### re_count_missing ([source](models/final_metrics/re_count_missing.sql))
 Number of nulls and empty string values in a given column for specific time range.
 
 #### re_min ([source](models/final_metrics/re_min.sql))
 Minimal value appearing in a given column for specific time range.
 
 #### re_max ([source](models/final_metrics/re_max.sql))
 Maximal value appearing in a given column for specific time range.
 
 #### re_avg ([source](models/final_metrics/re_avg.sql))
 Average of all values appearing in a given column for specific time range.
 
 #### re_min_length ([source](models/final_metrics/re_min_length.sql))
 Minimal length of all strings appearing in a given column for specific time range.
 
 #### re_max_length ([source](models/final_metrics/re_max_length.sql))
 Maximal length of all strings appearing in a given column for specific time range.
 
 #### re_avg_length ([source](models/final_metrics/re_avg_length.sql))
 Average length of all strings appearing in a given column for specific time range.
 
 #### re_base_metrics ([source](models/metrics_queries/re_base_metrics.sql))
 Internal table containing most of described metrics (apart from `re_freshness`). To really access
 metrics it's usually better to use view for specific metric.
 
 #### re_z_score ([source](models/anomalies/re_z_score.sql))
 Computed z_score for metric. `re_data` looks back on what where metrics values in last 30 days and compute z_score for newest value.
 
 #### re_alerting ([source](models/final_metrics/re_alerting.sql))
 View computed on top of `re_z_score` table to contain metrics which look alerting. Alerting threshold is controled by var `re_data:alerting_z_score`
 which is equal to 3 by default, but can be changed and adjusted.
 
