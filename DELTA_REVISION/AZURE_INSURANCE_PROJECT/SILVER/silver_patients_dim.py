# Databricks notebook source
df_patirnts_val = spark.read.table("dev_catalog.silver.patients_dim")
df_patirnts_val.createOrReplaceTempView("patients_soda")

# COMMAND ----------

# MAGIC %pip install soda-core
# MAGIC %pip install soda-core-spark-df

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


from soda.scan import Scan


scan = Scan()
scan.add_spark_session(
    spark,
    data_source_name="patients"
)
scan.set_data_source_name("patients")
scan.add_sodacl_yaml_str(
    """
    checks for patients_soda:
      - row_count > 0
      - missing_count(PatientID) = 0
        
    """
)

exit_code = scan.execute()
if exit_code != 0:
    raise Exception("Data Quality Check Failed")
else:
    print("Data Quality Check Passed")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG DEV_CATALOG;
# MAGIC USE SCHEMA SILVER;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

patients_bronze = spark.read.table("dev_catalog.bronze.patients")
patients_bronze = patients_bronze.drop("InsertDate_bronze","UpdateDate_bronze")

# COMMAND ----------


from delta.tables import DeltaTable

patients_silver = DeltaTable.forName(spark,"dev_catalog.silver.patients_dim")

(
    patients_silver.alias("tgt").merge(
        patients_bronze.alias("src"),
        "tgt.PatientID = src.PatientID"
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
        "InsertDate_silver": current_timestamp(),
        "UpdateDate_silver": current_timestamp()

    })
    .whenNotMatchedInsert(values={
        "PatientID": "src.PatientID",
        "firstname": "src.FirstName",
        "lastname": "src.LastName",
        "middlename": "src.MiddleName",
        "ssn": "src.SSN",
        "phonenumber": "src.PhoneNumber",
        "gender": "src.Gender",
        "dob": "src.DOB",
        "address": "src.Address",
        "modifieddate": "src.ModifiedDate",
        "InsertDate_silver": current_timestamp(),
        "UpdateDate_silver": current_timestamp()
    })
    .execute()
)




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DEV_CATALOG.SILVER.patients_dim ORDER BY Patient_dim_key asc;
