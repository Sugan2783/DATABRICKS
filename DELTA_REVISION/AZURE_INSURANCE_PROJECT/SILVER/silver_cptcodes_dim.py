# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG DEV_CATALOG;
# MAGIC USE SCHEMA SILVER;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

cptcode_bronze = spark.read.table("dev_catalog.bronze.cptcodes")
cptcode_bronze.display()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

cptcodes_silver = DeltaTable.forName(spark, "dev_catalog.silver.cptcodes_dim")

(
    cptcodes_silver.alias("tgt").merge(
        cptcode_bronze.alias("src"),
        "tgt.CPT_Codes = src.CPT_Codes"
    )
    .whenMatchedUpdate(
        set={
            "Procedure_Code_Category": "src.Procedure_Code_Category",
            "Procedure_Code_Descriptions": "src.Procedure_Code_Descriptions",
            "Code_Status": "src.Code_Status",
            "UpdateDate_silver": current_timestamp()
        }
    )
    .whenNotMatchedInsert(
        values={
            "CPT_Codes": "src.CPT_Codes",
            "Procedure_Code_Category": "src.Procedure_Code_Category",
            "Procedure_Code_Descriptions": "src.Procedure_Code_Descriptions",
            "Code_Status": "src.Code_Status",
            "InsertDate_silver": current_timestamp(),
            "UpdateDate_silver": current_timestamp()
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.cptcodes_dim;
