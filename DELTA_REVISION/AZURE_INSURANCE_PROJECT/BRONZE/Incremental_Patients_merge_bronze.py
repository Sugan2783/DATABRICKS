# Databricks notebook source
# MAGIC %md
# MAGIC **PATIENTS SCD1**

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_patients_tgt = spark.read.table("patients")
df_patients_tgt.display()

# COMMAND ----------

display(dbutils.fs.ls('abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/'))

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

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.patients;

# COMMAND ----------

from pyspark.sql.functions import lit


df_patients_tgt = spark.read.table("dev_catalog.bronze.patients")
df_patients_tgt = (
   df_patients_tgt
    .withColumn("InsertDate_bronze", lit(None).cast(TimestampType()))
    .withColumn("UpdateDate_bronze", lit(None).cast(TimestampType()))
)


display(df_patients_tgt)

# COMMAND ----------

df_patients_tgt.write.format("delta").option("overwriteSchema", True).mode("overwrite").saveAsTable("dev_catalog.bronze.patients")

# COMMAND ----------

# %sql
# ALTER TABLE dev_catalog.bronze.patients 
# SET TBLPROPERTIES (
#   'delta.columnMapping.mode' = 'name'
# );

# ALTER TABLE dev_catalog.bronze.patients 
# DROP COLUMNS (InsertDate, UpdateDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe dev_catalog.bronze.patients

# COMMAND ----------

df_patients_src.createOrReplaceTempView("patients_src")


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO dev_catalog.bronze.patients AS tgt
# MAGIC USING patients_src AS src
# MAGIC ON tgt.PatientID = src.PatientID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC tgt.FirstName = src.FirstName,
# MAGIC tgt.LastName = src.LastName,
# MAGIC tgt.MiddleName = src.MiddleName,
# MAGIC tgt.SSN = src.SSN,
# MAGIC tgt.PhoneNumber = src.PhoneNumber,
# MAGIC tgt.Gender = src.Gender,
# MAGIC tgt.DOB = src.DOB,
# MAGIC tgt.Address = src.Address,
# MAGIC tgt.ModifiedDate = src.ModifiedDate,
# MAGIC --tgt.InsertDate_bronze = src.InsertDate_bronze,
# MAGIC tgt.UpdateDate_bronze = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT
# MAGIC (tgt.PatientID,tgt.FirstName,tgt.LastName,tgt.MiddleName,tgt.SSN,tgt.PhoneNumber,tgt.Gender,tgt.DOB,tgt.Address,
# MAGIC  tgt.ModifiedDate,tgt.InsertDate_bronze,tgt.UpdateDate_bronze)
# MAGIC VALUES
# MAGIC (src.PatientID,src.FirstName,src.LastName,src.MiddleName,src.SSN,src.PhoneNumber,src.Gender,src.DOB,src.Address,
# MAGIC  src.ModifiedDate,current_timestamp(),current_timestamp());

# COMMAND ----------

# PatientID and ModifiedDate are null because the MERGE statement does not insert values for these columns.
# The INSERT clause in your MERGE only specifies other columns, so PatientID and ModifiedDate are left as NULL for new rows.

# To fix this, include PatientID and ModifiedDate in the INSERT columns and VALUES in your MERGE statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.patients where FirstName = 'Balamurugan';

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# Reference the Delta table using the correct forName signature
delta_table_tgt = DeltaTable.forName(spark, "dev_catalog.bronze.patients")
   

# Your incoming incremental DataFrame
source_df = df_patients_src

# Perform MERGE (Upsert)
(
    delta_table_tgt.alias("tgt")
    .merge(
        source_df.alias("src"),
        "tgt.patientID = src.patientID"
    )
    .whenMatchedUpdate(set={
        "firstname": "src.FirstName",
        "lastname": "src.LastName",
        "middlename": "src.MiddleName",
        "ssn": "src.SSN",
        "phonenumber": "src.PhoneNumber",
        "gender": "src.Gender",
        "dob": "src.DOB",
        "address": "src.Address",
        "modifieddate": "src.ModifiedDate",
        "InsertDate_Bronze": current_timestamp(),
        "UpdateDate_Bronze": current_timestamp()

    })
    .whenNotMatchedInsert(values={
        "firstname": "src.FirstName",
        "lastname": "src.LastName",
        "middlename": "src.MiddleName",
        "ssn": "src.SSN",
        "phonenumber": "src.PhoneNumber",
        "gender": "src.Gender",
        "dob": "src.DOB",
        "address": "src.Address",
        "modifieddate": "src.ModifiedDate",
        "InsertDate_Bronze": "src.InsertDate_Bronze",
        "UpdateDate_Bronze": "src.UpdateDate_Bronze"
    })
    .execute()
)


   
