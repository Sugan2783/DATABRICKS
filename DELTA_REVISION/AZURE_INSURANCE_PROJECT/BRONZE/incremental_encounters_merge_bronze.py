# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_encounter = spark.read.table("dev_catalog.bronze.encounters")
df_encounter.display()

# COMMAND ----------

df_encounters_src = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/ENCOUNTERS.csv")
df_encounters_src.display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField("EncounterID", StringType(), True),
    StructField("PatientID", StringType(), True),
    StructField("EncounterDate",DateType(), True),
    StructField("EncounterType", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("DepartmentID", StringType(), True),
    StructField("ProcedureCode", IntegerType(), True),
    StructField("InsertedDate", DateType(), True),
    StructField("ModifiedDate", DateType(), True),
    StructField("InsertDate_bronze", DateType(), True),
    StructField("UpdateDate_bronze", DateType(), True) 
])

df_encounter_src = spark.read.schema(schema).csv("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/ENCOUNTERS.csv")
df_encounter_src.display()

# COMMAND ----------

from pyspark.sql.functions import lit

df_encounter_tgt = spark.read.table("dev_catalog.bronze.encounters")
df_encounter_tgt = df_encounter_tgt.withColumn("InsertDate_bronze", lit(None).cast(TimestampType())).withColumn("UpdateDate_bronze", lit(None).cast(TimestampType()))


df_encounter_tgt.display()

# COMMAND ----------

df_encounter_tgt.write.format("delta").option("overwriteSchema", True).mode("overwrite").saveAsTable("dev_catalog.bronze.encounters")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table dev_catalog.bronze.encounters;

# COMMAND ----------

df_encounter_src.createOrReplaceTempView("encounters_src")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO DEV_CATALOG.BRONZE.ENCOUNTERS ET
# MAGIC USING ENCOUNTERS_SRC ES
# MAGIC ON ET.EncounterID = ES.EncounterID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET ET.PatientID = ES.PatientID,
# MAGIC ET.EncounterDate = ES.EncounterDate,
# MAGIC ET.EncounterType = ES.EncounterType,
# MAGIC ET.ProviderID = ES.ProviderID,
# MAGIC ET.DepartmentID = ES.DepartmentID,
# MAGIC ET.ProcedureCode = ES.ProcedureCode,
# MAGIC ET.ModifiedDate = ES.ModifiedDate,
# MAGIC ET.InsertedDate = ES.InsertedDate,
# MAGIC ET.InsertDate_bronze = ES.InsertDate_bronze
# MAGIC    
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (ET.EncounterID,ET.PatientID,ET.EncounterDate,ET.EncounterType,ET.ProviderID,ET.DepartmentID)
# MAGIC VALUES (ES.EncounterID,ES.PatientID,ES.EncounterDate,ES.EncounterType,ES.ProviderID,ES.DepartmentID)

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

encounter_tgt = DeltaTable.forName(spark, "dev_catalog.bronze.encounters")
encounter_src = df_encounter_src

(    encounter_tgt.alias("tgt")
          .merge(encounter_src.alias("src"),
          "tgt.encounterID = src.encounterID"  
    )
    .whenMatchedUpdate(set={
    "PatientID": "src.PatientID",
    "EncounterDate": "src.EncounterDate",
    "EncounterType": "src.EncounterType",
    "ProviderID": "src.ProviderID",
    "DepartmentID": "src.DepartmentID",
    "ProcedureCode": "src.ProcedureCode",
    "InsertedDate": "src.InsertedDate",
    "ModifiedDate": "src.ModifiedDate",
    "InsertDate_Bronze": current_timestamp(),
    "UpdateDate_Bronze": current_timestamp()
}
).whenNotMatchedInsert(values={
    "EncounterID": "src.EncounterID",
    "PatientID": "src.PatientID",
    "EncounterDate": "src.EncounterDate",
    "EncounterType": "src.EncounterType",
    "ProviderID": "src.ProviderID",
    "DepartmentID": "src.DepartmentID",
    "ProcedureCode": "src.ProcedureCode",
    "InsertedDate": "src.InsertedDate",
    "ModifiedDate": "src.ModifiedDate",
    "InsertDate_Bronze": "src.InsertDate_Bronze",
    "UpdateDate_Bronze": "src.UpdateDate_Bronze"
}
).execute()
)
