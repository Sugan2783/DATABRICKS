# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG DEV_CATALOG;
# MAGIC USE SCHEMA SILVER;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

icdcodedata_bronze = spark.read.table("dev_catalog.bronze.icd_code_data")
icdcodedata_bronze.display()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Deduplicate source DataFrame by icd_code, keeping the latest updated_date
icdcodedata_bronze_deduped = (
    icdcodedata_bronze
    .orderBy("updated_date", ascending=False)
    .dropDuplicates(["icd_code"])
)

icdcode_data_silver = DeltaTable.forName(
    spark, 
    "dev_catalog.silver.icdcode_data_dim"
)

(
    icdcode_data_silver.alias("tgt").merge(
        icdcodedata_bronze_deduped.alias("src"),
        "tgt.icd_code = src.icd_code"
    )
    .whenMatchedUpdate(set={
        "icd_code_type": "src.icd_code_type",
        "code_description": "src.code_description",
        "inserted_date": "src.inserted_date",
        "updated_date": "src.updated_date",
        "is_current_flag": "src.is_current_flag"
    })
    .whenNotMatchedInsert(
        values={
            "icd_code": "src.icd_code",
            "icd_code_type": "src.icd_code_type",
            "code_description": "src.code_description",
            "inserted_date": "src.inserted_date",
            "updated_date": "src.updated_date",
            "is_current_flag": "src.is_current_flag",
            "InsertDate_silver": current_timestamp(),
            "UpdateDate_silver": current_timestamp()
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.icdcode_data_dim;
