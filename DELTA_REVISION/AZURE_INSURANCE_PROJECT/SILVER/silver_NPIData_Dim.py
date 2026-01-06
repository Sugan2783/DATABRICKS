# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG DEV_CATALOG;
# MAGIC USE SCHEMA SILVER;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

npidata_bronze = spark.read.table("dev_catalog.bronze.npi_data")
npidata_bronze.display()

# COMMAND ----------

from delta.tables import DeltaTable

npidata_silver = DeltaTable.forName(spark,"dev_catalog.silver.npi_data_dim")

(
    npidata_silver.alias("tgt")
 .merge(
   npidata_bronze.alias("src"),
   "tgt.npi_id = src.npi_id")
 .whenMatchedUpdate(set={
     "first_name": "src.first_name",
     "last_name": "src.last_name",
     "position" : "src.position",
     "organisation_name" : "src.organisation_name",
     "last_updated": "src.last_updated"
 })     
 .whenNotMatchedInsert(values={
     "npi_id": "src.npi_id",
     "first_name": "src.first_name",
     "last_name" : "src.last_name",
     "position" : "src.position",
     "organisation_name":"src.organisation_name",
     "last_updated": "src.last_updated",
     "InsertDate_silver" : current_timestamp(),
     "UpdateDate_silver" : current_timestamp()  
 })
.execute()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.npi_data_dim;
