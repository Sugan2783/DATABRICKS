# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG DEV_CATALOG;
# MAGIC USE SCHEMA SILVER;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_catalog.silver.departments_dim;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

departments_bronze = spark.read.table("dev_catalog.bronze.departments")


# COMMAND ----------

from delta.tables import DeltaTable

departments_silver = DeltaTable.forName(spark, "dev_catalog.silver.departments_dim")

(
    departments_silver.alias("tgt")
    .merge(
        departments_bronze.alias("src"),
        "tgt.DeptID = src.DeptID"
    )
    .whenMatchedUpdate(set={
        "Name" : "src.Name"       
    })
    .whenNotMatchedInsert(values={
        "DeptID": "src.DeptID",
        "Name": "src.Name",
        "InsertDate_Silver": current_timestamp(),
        "UpdateDate_Silver": current_timestamp()
    })
    .execute()
)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.departments_dim;
