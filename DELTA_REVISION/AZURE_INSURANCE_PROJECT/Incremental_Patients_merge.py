# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_patients_tgt = spark.read.table("patients")

# COMMAND ----------

df_patients_src = spark.read.format("csv").option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/PATIENTS.csv")

# COMMAND ----------

from pyspark.sql.types import *
from datetime import date

schema = StructType([
    StructField("PatientID", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("MiddleName", StringType(), True),
    StructField("SSN", StringType(), True),
    StructField("PhoneNumber", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("DOB", DateType(), True),
    StructField("Address", StringType(), True),
    StructField("ModifiedDate", DateType(), True),
    StructField("InsertDate_bronze", DateType(), True),
    StructField("UpdateDate_bronze", DateType(), True) 
])

df_patients_src = spark.read.format("csv").option("header", True).schema(schema).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/PATIENTS.csv")
df_patients_src.display()



# COMMAND ----------

# %sql
# ALTER TABLE dev_catalog.bronze.patients 
# SET TBLPROPERTIES (
#   'delta.columnMapping.mode' = 'name'
# );

# ALTER TABLE dev_catalog.bronze.patients 
# DROP COLUMNS (InsertDate, UpdateDate);

# COMMAND ----------

from pyspark.sql.functions import lit


df_patients_tgt = spark.read.table("dev_catalog.bronze.patients")
df_patients_tgt = (
   df_patients_tgt
    .withColumn("InsertDate_bronze", lit(None).cast(TimestampType()))
    .withColumn("UpdateDate_bronze", lit(None).cast(TimestampType()))
)


# COMMAND ----------

display(df_patients_tgt)

# COMMAND ----------

df_patients_tgt.write.format("delta").option("overwriteSchema", True).mode("overwrite").saveAsTable("dev_catalog.bronze.patients");

# COMMAND ----------

# MAGIC %sql
# MAGIC df_patients_src.createOrReplaceTempView("patients_src")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dev_catalog.bronze.claims AS tgt
# MAGIC
# MAGIC USING df_patients_src AS src
# MAGIC ON tgt.ClaimID = src.ClaimID
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC tgt.ClaimID = src.ClaimID,
# MAGIC tgt.TransactionID = src.TransactionID,
# MAGIC tgt.PatientID = src.PatientID,
# MAGIC tgt.EncounterID = src.EncounterID,
# MAGIC tgt.ProviderID = src.ProviderID,
# MAGIC tgt.DeptID = src.DeptID,
# MAGIC tgt.ServiceDate = src.ServiceDate,
# MAGIC tgt.ClaimDate = src.ClaimDate,
# MAGIC tgt.PayorID = src.PayorID,
# MAGIC tgt.ClaimAmount = src.ClaimAmount,
# MAGIC tgt.PaidAmount = src.PaidAmount,
# MAGIC tgt.ClaimStatus = src.ClaimStatus,
# MAGIC tgt.PayorType = src.PayorType,
# MAGIC tgt.Deductible = src.Deductible,
# MAGIC tgt.Coinsurance = src.Coinsurance,
# MAGIC tgt.Copay = src.Copay,
# MAGIC tgt.InsertDate = src.InsertDate,
# MAGIC tgt.ModifiedDate = src.ModifiedDate,
# MAGIC tgt.UpdateDate_bronze = current_timestamp()
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT
# MAGIC (tgt.ClaimID,tgt.TransactionID,tgt.PatientID,tgt.EncounterID,tgt.ProviderID,tgt.DeptID,tgt.ServiceDate,tgt.ClaimDate,tgt.PayorID,tgt.ClaimAmount,tgt.PaidAmount,tgt.ClaimStatus,tgt.PayorType,tgt.Deductible,tgt.Coinsurance,tgt.Copay,tgt.InsertDate,tgt.ModifiedDate,tgt.InsertDate_bronze,tgt.UpdateDate_bronze)
# MAGIC VALUES
# MAGIC (src.ClaimID,src.TransactionID,src.PatientID,src.EncounterID,src.ProviderID,src.DeptID,src.ServiceDate,src.ClaimDate,src.PayorID,src.ClaimAmount,src.PaidAmount,src.ClaimStatus,src.PayorType,src.Deductible,src.Coinsurance,src.Copay,src.InsertDate,src.ModifiedDate,current_timestamp(),current_timestamp());

# COMMAND ----------

from delta.tables import DeltaTable

# Reference the Delta table using the correct forName signature
delta_table_tgt = DeltaTable.forName(spark, "dev_catalog.bronze.claims")

# Your incoming incremental DataFrame
source_df = df_claims_src

# Perform MERGE (Upsert)

(
    delta_table_tgt.alias("tgt")
        .merge(
        source_df.alias("src"),
        "tgt.ClaimID = src.ClaimID"
)

.whenMatchedUpdate(set=
{
    "claimid": "src.ClaimID",
    "transactionid": "src.TransactionID",
    "patientid": "src.PatientID",
    "encounterid": "src.EncounterID",
    "providerid": "src.ProviderID",
    "deptid": "src.DeptID",
    "servicedate": "src.ServiceDate",
    "claimdate": "src.ClaimDate",
    "payorid": "src.PayorID",
    "claimamount": "src.ClaimAmount",
    "paidamount": "src.PaidAmount",
    "claimstatus": "src.ClaimStatus",
    "payortype": "src.PayorType",
    "deductible": "src.Deductible",
    "coinsurance": "src.Coinsurance",
    "copay": "src.Copay",
    "insertdate": "src.InsertDate",
    "modifieddate": "src.ModifiedDate",
    "updatedate_bronze": current_timestamp()
})

.whenNotMatchedInsert(values={
    "claimid": "src.ClaimID",
    "transactionid": "src.TransactionID",
    "patientid": "src.PatientID",
    "encounterid": "src.EncounterID",
    "providerid": "src.ProviderID",
    "deptid": "src.DeptID",
    "servicedate": "src.ServiceDate",
    "claimdate": "src.ClaimDate",
    "payorid": "src.PayorID",
    "claimamount": "src.ClaimAmount",
    "paidamount": "src.PaidAmount",
    "claimstatus": "src.ClaimStatus",
    "payortype": "src.PayorType",
    "deductible": "src.Deductible",
    "coinsurance": "src.Coinsurance",
    "copay": "src.Copay",
    "insertdate": "src.InsertDate",
    "modifieddate": "src.ModifiedDate",
    "insertdate_bronze": current_timestamp(),
    "updatedate_bronze": current_timestamp()
})

.execute()
)
