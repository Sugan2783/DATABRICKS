# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev_catalog.bronze.dim_date AS
# MAGIC WITH dates AS (
# MAGIC   SELECT sequence(
# MAGIC     to_date('2020-01-01'),
# MAGIC     to_date('2030-12-31'),
# MAGIC     interval 1 day
# MAGIC   ) AS d
# MAGIC )
# MAGIC SELECT
# MAGIC   date AS DateID,
# MAGIC   year(date) AS Year,
# MAGIC   month(date) AS Month,
# MAGIC   day(date) AS Day,
# MAGIC   dayofweek(date) AS DayofWeek,
# MAGIC   weekofyear(date) AS WeekofYear,
# MAGIC   quarter(date) AS Quarter,
# MAGIC   date_format(date, 'MMMM') AS MonthName,
# MAGIC   date_format(date, 'EEEE') AS DayName,
# MAGIC   CASE WHEN dayofweek(date) IN (1,7) THEN 1 ELSE 0 END AS IsWeekend
# MAGIC FROM dates
# MAGIC LATERAL VIEW explode(d) AS date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*CREATE OR REPLACE TABLE dim_date (
# MAGIC         DateID DATE,
# MAGIC         Year INT,
# MAGIC         Month INT,
# MAGIC         Day INT,
# MAGIC         Quarter INT,
# MAGIC         WeekdayName STRING,
# MAGIC         IsWeekend STRING
# MAGIC     );
# MAGIC
# MAGIC     WITH date_range AS (
# MAGIC         SELECT sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day) AS dates
# MAGIC     )
# MAGIC     INSERT INTO dim_date
# MAGIC     SELECT 
# MAGIC         date AS DateID,
# MAGIC         year(date) AS Year,
# MAGIC         month(date) AS Month,
# MAGIC         day(date) AS Day,
# MAGIC         quarter(date) AS Quarter,
# MAGIC         date_format(date, 'EEEE') AS WeekdayName,
# MAGIC         CASE WHEN date_format(date, 'E') IN ('Sat', 'Sun') THEN 'Yes' ELSE 'No' END AS IsWeekend
# MAGIC     FROM date_range
# MAGIC     LATERAL VIEW explode(dates) AS date;*/

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp();

# COMMAND ----------


dbutils.fs.ls ('abfss://output@insurancemedallion.dfs.core.windows.net/')
