# re_data

re_data is data quality framework. It lets you do queries similar to those:

```sql title="Your Data Warehouse"
select * from anomalies_in_row_counts;

select * from recent_schema_changes;

select * from all_tables_freshness order by last_update_time;

select * from daily_null_percent where table = 'X' and col = 'Y';
```

in your Snowflake, Redshift, BigQuery, Postgres DB.

Build as dbt-package & optional python lib. 

It let's you know what's happening in your data.

And you can visualize it, any way you want in your favorite BI tool.

# Getting stated

Check out [docs :notebook_with_decorative_cover:  :notebook_with_decorative_cover:](https://re-data.github.io/re-data/docs/introduction/whatis)

# Python package

Check out more information about python-package [here](https://github.com/re-data/re-data)

# Community

Say, hi to us on! 🙂

- [Slack](https://www.getre.io/slack)
- [Twitter](https://twitter.com/re_data_labs)

