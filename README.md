# dbt_re_data

dbt_re_data is dbt package for [re_data](https://github.com/redata-team/redata), data quality library build on top of dbt.

# Install

dbt_re_data is mostly intended to be used with `re_data` python package. But you can also add only dbt part to your project.

## Variables
The following variables need to be defined in your dbt_project.yml:

```yaml
vars:
     redata:schemas:
         - schema_to_monitor
         - another_schema_to_monitor
```

## Models
### Information about monitored tables

- [re_monitored_columns](#re_monitored_columns)
- [re_monitored_tables](#re_monitored_tables)

### Metrics computed

- [re_freshness](#re_freshness)
- [re_row_count](#re_row_count)
- [re_count_nulls](#re_count_nulls)
- [re_count_missing](#re_count_missing)
- [re_min](#re_min)
- [re_max](#re_max)
- [re_avg](#re_avg)
- [re_min_length](#re_min_length)
- [re_max_length](#re_max_length)
- [re_avg_length](#re_avg_length)
- [re_base_metrics](#re_base_metrics)

### Anomalies & Alerts
- [re_z_score](#re_z_score)
- [re_alerting](#re_alerting)


 ### [re_monitored_columns](models/meta/re_monitored_columns.sql)
 ### [re_monitored_tables](models/meta/re_monitored_tables.sql)
 ### [re_freshness](models/metrics_queries/re_base_metrics.sql)
 ### [re_row_count](models/final_metrics/re_row_count.sql)
 ### [re_count_nulls](models/final_metrics/re_count_nulls.sql)
 ### [re_count_missing](models/final_metrics/re_count_missing.sql)
 ### [re_min](models/final_metrics/re_min.sql)
 ### [re_max](models/final_metrics/re_max.sql)
 ### [re_avg](models/final_metrics/re_avg.sql)
 ### [re_min_length](models/final_metrics/re_min_length.sql)
 ### [re_max_length](models/final_metrics/re_max_length.sql)
 ### [re_avg_length](models/final_metrics/re_avg_length.sql)
 ### [re_base_metrics](models/final_metrics/re_base_metrics.sql)
 
 ### [re_z_score](models/final_metrics/re_z_score.sql)
 ### [re_alerting](models/final_metrics/re_alerting.sql)
 
