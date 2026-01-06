# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.providers_dim;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

providers_bronze = spark.read.table("dev_catalog.bronze.providers")
providers_bronze.display()

# COMMAND ----------


from delta.tables import DeltaTable

providers_silver = DeltaTable.forName(spark, "dev_catalog.silver.providers_dim")

(
    providers_silver.alias("tgt").merge(
        providers_bronze.alias("src"),
        "tgt.ProviderID = src.ProviderID"   
    )
    .whenMatchedUpdate(set={
        "FirstName": "src.FirstName",
        "LastName": "src.LastName",
        "Specialization": "src.Specialization",
        "DeptID": "src.DeptID",
        "NPI": "src.NPI"
    })
    .whenNotMatchedInsert(values={
        "ProviderID" : "src.ProviderID",
        "FirstName": "src.FirstName",
        "LastName": "src.LastName",
        "Specialization": "src.Specialization",
        "DeptID": "src.DeptID",
        "NPI": "src.NPI",
        "InsertDate_silver": current_timestamp(),
        "UpdateDate_silver": current_timestamp()     
    })
    .execute()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.providers_dim;
