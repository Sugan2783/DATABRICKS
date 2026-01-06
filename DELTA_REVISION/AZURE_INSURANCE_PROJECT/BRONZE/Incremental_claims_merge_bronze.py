# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_claims_tgt = spark.read.table("dev_catalog.bronze.claims")
df_claims_tgt.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table claims;

# COMMAND ----------

from pyspark.sql.types import *
from datetime import date

schema =StructType([
    StructField("ClaimID", StringType(), True),
    StructField("TransactionID", StringType(), True),
    StructField("PatientID", StringType(), True),
    StructField("EncounterID", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("DeptID", StringType(), True),
    StructField("ServiceDate", DateType(), True),
    StructField("ClaimDate", DateType(), True),
    StructField("PayorID", StringType(), True),
    StructField("ClaimAmount", DoubleType(), True),
    StructField("PaidAmount", DoubleType(), True)    ,
    StructField("ClaimStatus", StringType(), True),
    StructField("PayorType", StringType(), True),
    StructField("Deductible", DoubleType(), True),
    StructField("Coinsurance", DoubleType(), True),
    StructField("Copay", DoubleType(), True),
    StructField("InsertDate", DateType(), True),
    StructField("ModifiedDate", DateType(), True),
    StructField("InsertDate_bronze", DateType(), True),
    StructField("UpdateDate_bronze", DateType(), True)
])
    
    
df_claims_src = spark.read.format("csv").option("header", True).schema(schema).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/CLAIMS.csv")
df_claims_src.display()

# COMMAND ----------

from pyspark.sql.functions import lit


df_claims_tgt = spark.read.table("dev_catalog.bronze.claims")
df_claims_tgt = (
   df_claims_tgt
    .withColumn("InsertDate_bronze", lit(None).cast(TimestampType()))
    .withColumn("UpdateDate_bronze", lit(None).cast(TimestampType())))


# COMMAND ----------

df_claims_tgt.write.format("delta").option("overwriteSchema", True).mode("overwrite").saveAsTable("dev_catalog.bronze.claims");

# COMMAND ----------

df_claims_src.createOrReplaceTempView("claims_src")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dev_catalog.bronze.claims AS tgt
# MAGIC USING claims_src AS src
# MAGIC ON tgt.ClaimID = src.ClaimID
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
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT
# MAGIC (tgt.ClaimID,tgt.TransactionID,tgt.PatientID,tgt.EncounterID,tgt.ProviderID,tgt.DeptID,tgt.ServiceDate,tgt.ClaimDate,tgt.PayorID,tgt.ClaimAmount,tgt.PaidAmount,tgt.ClaimStatus,tgt.PayorType,tgt.Deductible,tgt.Coinsurance,tgt.Copay,tgt.InsertDate,tgt.ModifiedDate,tgt.InsertDate_bronze,tgt.UpdateDate_bronze)
# MAGIC VALUES
# MAGIC (src.ClaimID,src.TransactionID,src.PatientID,src.EncounterID,src.ProviderID,src.DeptID,src.ServiceDate,src.ClaimDate,src.PayorID,src.ClaimAmount,src.PaidAmount,src.ClaimStatus,src.PayorType,src.Deductible,src.Coinsurance,src.Copay,src.InsertDate,src.ModifiedDate,current_timestamp(),current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.claims where claimid = 'CLAIM000001';

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
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
