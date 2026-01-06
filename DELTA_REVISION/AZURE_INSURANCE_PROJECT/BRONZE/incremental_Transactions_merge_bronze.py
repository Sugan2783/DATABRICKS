# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_transactions_tgt = spark.read.table("transactions")
display(df_transactions_tgt)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table transactions;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("TransactionID", StringType(), True),
    StructField("EncounterID", StringType(), True),
    StructField("PatientID", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("DeptID", StringType(), True),
    StructField("VisitDate", TimestampType(), True),
    StructField("ServiceDate", TimestampType(), True),
    StructField("PaidDate", TimestampType(), True),
    StructField("VisitType", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("AmountType", StringType(), True),
    StructField("PaidAmount", DoubleType(), True),
    StructField("ClaimID", StringType(), True),
    StructField("PayorID", StringType(), True),
    StructField("ProcedureCode", IntegerType(), True),
    StructField("ICDCode", StringType(), True),
    StructField("LineOfBusiness", StringType(), True),
    StructField("MedicaidID", StringType(), True),
    StructField("MedicareID", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("InsertDate_bronze", TimestampType(), True),
    StructField("UpdateDate_bronze", TimestampType(), True)
])

df_transactions_src = spark.read.format("csv").schema(schema).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/ENCOUNTERS.csv")
df_transactions_src.display()

# COMMAND ----------

df_transactions_src.createOrReplaceTempView("transactions_src")

# COMMAND ----------

df_transactions_tgt = spark.read.table("dev_catalog.bronze.transactions")

df_transactions_tgt = df_transactions_tgt.withColumn("InsertDate_bronze", lit(None).cast(TimestampType())).withColumn("UpdateDate_bronze", lit(None).cast(TimestampType()))

df_transactions_tgt.display()

# COMMAND ----------

# %sql
# ALTER TABLE dev_catalog.bronze.transactions 
# SET TBLPROPERTIES (
#   'delta.columnMapping.mode' = 'name'
# );

# ALTER TABLE dev_catalog.bronze.transactions 
# DROP COLUMNS (InsertDate, UpdateDate);

# COMMAND ----------

df_transactions_tgt.write.format("delta").option("overwriteSchema", True).mode("overwrite").saveAsTable("dev_catalog.bronze.transactions")


# COMMAND ----------

# MAGIC %sql
# MAGIC describe dev_catalog.bronze.transactions;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.types import *
from pyspark.sql.functions import *

transactions_tgt = DeltaTable.forName(spark, "dev_catalog.bronze.transactions")
transactions_src = df_transactions_src

(
transactions_tgt.alias("tgt")
    .merge(
        transactions_src.alias("src"),
        "tgt.TransactionID = src.TransactionId"
        )
    .whenMatchedUpdate(set= {
        "TransactionID": "src.TransactionID",
        "EncounterID": "src.EncounterID",
        "PatientID": "src.PatientID",
        "ProviderID": "src.ProviderID",
        "DeptID": "src.DeptID",
        "VisitDate": "src.VisitDate",
        "ServiceDate": "src.ServiceDate",
        "PaidDate": "src.PaidDate",
        "VisitType": "src.VisitType",
        "Amount": "src.Amount",
        "AmountType": "src.AmountType",
        "PaidAmount": "src.PaidAmount",
        "ClaimID": "src.ClaimID",
        "PayorID": "src.PayorID",
        "ProcedureCode": "src.ProcedureCode",
        "ICDCode": "src.ICDCode",
        "LineOfBusiness": "src.LineOfBusiness",
        "MedicaidID": "src.MedicaidID",
        "MedicareID": "src.MedicareID",
        "ModifiedDate": "src.ModifiedDate",
        "InsertDate_bronze": "src.InsertDate",
        "UpdateDate_bronze": "src.ModifiedDate"
        })
    .whenNotMatchedInsert(values={
        "TransactionID" : "src.TransactionID",
        "EncounterID" : "src.EncounterID",
        "PatientID" : "src.PatientID",
        "ProviderID" : "src.ProviderID",
        "DeptID" : "src.DeptID",
        "VisitDate" : "src.VisitDate",
        "ServiceDate" : "src.ServiceDate",
        "PaidDate" : "src.PaidDate",
        "VisitType" : "src.VisitType",
        "Amount" : "src.Amount",
        "AmountType" : "src.AmountType",
        "PaidAmount" : "src.PaidAmount",
        "ClaimID" : "src.ClaimID",
        "PayorID" : "src.PayorID",
        "ProcedureCode" : "src.ProcedureCode",
        "ICDCode" : "src.ICDCode",
        "LineOfBusiness" : "src.LineOfBusiness",
        "MedicaidID" : "src.MedicaidID",
        "MedicareID" : "src.MedicareID",
        "ModifiedDate" : "src.ModifiedDate",
        "InsertDate_bronze" : "src.InsertDate",
        "UpdateDate_bronze" : "src.ModifiedDate"
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select * from dev_catalog.bronze.transactions --where cast(ProcedureCode as string) = '0585T';
