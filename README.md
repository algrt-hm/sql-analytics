# SQL analytics

This repo shows analytics using pure SQL only. 

 - The output could be used directly in dashboarding/data viz software such as Tableau
 - There are some advantages to a pure-SQL solution particularly for a smaller organisation; if a view is queried data is from tables that are used operationally, which will always be up to date

### Specification

An ed-tech startup seeks the below outputs in a table or view, which comprise key metrics, used by management to run the business.

 - 1) A view which provides the following information for each individual apprentice as of the last day of each month during their apprenticeship
    - a. Cumulative live days ON programme as of that day
    - b. Cumulative targeted training hours as of that day
    - c. Cumulative logged training hours as of that day
    - d. % of cumulative targeted training hours that have been logged as of that day
    - e. Most recent feedback score in that month

 - 2) A view which provides the following information for each programme (i.e. group of apprentices enrolled in the same programme) as of 31st December 2020
    - a. Total number of 'live' apprentices that were ON programme as of that day
    - b. Number of live apprentices that had 80%+ of their cumulative targeted training hours logged as of that day
    - c. % of total live apprentices that had 80%+ of their cumulative targeted training hours logged as of that day
    - d. Number of live apprentices that had an all-time average feedback score of 8+ as of that day
    - e. % of total live apprentices that had an all-time average feedback score of 8+ as of that day

 - Further context
    - Apprenticeships are not considered 'live' during any 'break' periods  
    - Targeted weekly training hours are 'paused' during any 'break' periods
    - Targeted weekly training hours should be adjusted for annual holiday allowances as follows:
        - Career Starters have 4 weeks of holiday per year (i.e. when their targeted weekly training hours are not required)
        - Career Builders have 5 weeks of holiday per year (i.e. when their targeted weekly training hours are not required)
    - Apprenticeships can end either as 'completions' or as 'withdrawals'
    - The previous version of the feedback form asked the exact same question as the new form

### My solution and example outputs

My approach can be found [here](sql_analytics.sql). It produces the below outputs when run.

| id | month | cum\_days | cum\_target | cum\_logged | pct\_tgt\_lgd | response |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 356 | 202009 | 1 | 0.8491549518243564 | 2.5 | 294 | NULL |
| 356 | 202010 | 32 | 27.172958458379405 | 26.33 | 97 | NULL |
| 356 | 202011 | 62 | 52.64760701311009 | 60.08 | 114 | 7 |
| 356 | 202012 | 80 | 67.9323961459485 | 80.08 | 118 | NULL |
| 356 | 202101 | 80 | 67.9323961459485 | 80.08 | 118 | 7 |
| 359 | 202011 | 17 | 16.49786763544464 | 14.15 | 86 | 8 |
| 359 | 202012 | 48 | 46.582214500078976 | 43.4 | 93 | 8 |
| 359 | 202101 | 79 | 76.66656136471332 | 61.9 | 81 | 8 |
| 360 | 202011 | 14 | 13.303427578581582 | 12.5 | 94 | 9 |
| 360 | 202012 | 45 | 42.76101721686937 | 44.75 | 105 | NULL |
| 360 | 202101 | 76 | 72.21860685515716 | 49.75 | 69 | NULL |
| 362 | 202011 | 17 | 15.466750908229347 | 9 | 58 | NULL |
| 362 | 202012 | 48 | 43.67082609382404 | 43.9 | 101 | 6 |
| 362 | 202101 | 79 | 71.87490127941874 | 43.9 | 61 | NULL |
| 366 | 202009 | 14 | 12.737324277365346 | 12 | 94 | NULL |

| programme\_name | date | n\_live | n\_eighty\_pct\_plus\_logged | pct\_eighty\_pct\_plus\_logged | n\_avg\_eight\_plus | pct\_avg\_eight\_plus |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Accounting L3 | 2020-12-31 | 19 | 17 | 89 | 16 | 84 |
| Accounting L4 | 2020-12-31 | 16 | 15 | 94 | 12 | 75 |
| Business Associate L3 | 2020-12-31 | 85 | 62 | 73 | 73 | 86 |
| Data Fellowship L4 | 2020-12-31 | 383 | 232 | 61 | 251 | 66 |
| Digital Marketing L3 | 2020-12-31 | 78 | 59 | 76 | 58 | 74 |
| Diploma | 2020-12-31 | 2 | 0 | 0 | 0 | 0 |
| People Leadership L3 | 2020-12-31 | 207 | 148 | 71 | 109 | 53 |
| Project Management L4 | 2020-12-31 | 146 | 73 | 50 | 67 | 46 |
| Software Engineering L4 | 2020-12-31 | 34 | 30 | 88 | 28 | 82 |

### Running the SQL

Should you wish to run the example SQL, you will need PostgreSQL installed and running on a machine you have access to.

The database would need to be created before it can be loaded:

```sql
CREATE DATABASE database_name;
```

Secondly, tables and data would be loaded from the shell:

```sh
psql -U username database_name < sql_analytics_db.pgsql
```

Note that `sql_analytics_db.pgsql` can be found [here](sql_analytics_db.pgsql)
