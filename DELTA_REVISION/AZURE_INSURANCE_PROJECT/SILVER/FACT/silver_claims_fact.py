# Databricks notebook source
# MAGIC %pip install soda-core
# MAGIC %pip install soda-core-spark-df

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema silver;

# COMMAND ----------

df_claims_soda = spark.read.table("dev_catalog.bronze.claims")
df_claims_soda.createOrReplaceTempView("claim_soda")

# COMMAND ----------

from soda.scan import Scan

scan = Scan()
scan.add_spark_session(
    spark,
    data_source_name="claims"
)
scan.set_data_source_name("claims")
scan.add_sodacl_yaml_str(
    """
    checks for claim_soda:
      - row_count > 0
      - missing_count(ClaimID) = 0
      - duplicate_count(ClaimID) = 0
      - min(ClaimAmount) >= 0
      - min(PaidAmount) >= 0
      - invalid_count(ClaimStatus) = 0:
          valid values: ['Paid','Pending','Denied','Rejected','Approved']
          """
)
exit_code = scan.execute()
# Print detailed scan results to debug which checks failed
import json
print(json.dumps(scan.get_scan_results(), indent=2))
if exit_code != 0:
    raise Exception("Claims data quality checks failed")
else:
    print("Claims data quality checks passed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.claims;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Temp view for encounters updated in the last 12 months
# MAGIC CREATE OR REPLACE TEMP VIEW vw_last12_claims  AS
# MAGIC SELECT *
# MAGIC FROM dev_catalog.bronze.claims
# MAGIC WHERE
# MAGIC UpdateDate_bronze >= add_months(current_date(), -12);

# COMMAND ----------

# MAGIC %sql
# MAGIC --Delete matching records from silver fact
# MAGIC DELETE FROM dev_catalog.silver.fact_claims
# MAGIC WHERE EncounterID IN (SELECT EncounterID FROM vw_last12_claims);    

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.fact_claims 
# MAGIC select src.ClaimID,
# MAGIC        src.TransactionID,
# MAGIC        coalesce(p.Patient_Dim_Key,0) as Patient_Dim_Key,
# MAGIC        src.EncounterID,
# MAGIC        coalesce(pr.Provider_Dim_Key,0) as Provider_Dim_Key,
# MAGIC        coalesce(d.Dept_Dim_Key,0) as Dept_Dim_Key,
# MAGIC        src.ServiceDate,
# MAGIC        src.ClaimDate,
# MAGIC        src.PayorID,
# MAGIC        src.ClaimAmount,
# MAGIC        src.PaidAmount,
# MAGIC        src.ClaimStatus,
# MAGIC        src.PayorType,
# MAGIC        src.Deductible,
# MAGIC        src.Coinsurance,
# MAGIC        src.Copay,
# MAGIC        src.InsertDate,
# MAGIC        src.ModifiedDate
# MAGIC from dev_catalog.bronze.claims src
# MAGIC left join dev_catalog.silver.patients_dim p
# MAGIC on src.PatientID = p.PatientID
# MAGIC left join dev_catalog.silver.providers_dim pr
# MAGIC on src.ProviderID = pr.ProviderID
# MAGIC left join dev_catalog.silver.departments_dim d
# MAGIC on src.DeptID = d.DeptID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from dev_catalog.silver.fact_claims
# MAGIC where ClaimID in ( select ClaimID from dev_catalog.bronze.claims 
# MAGIC                     where ModifiedDate >= add_months(current_date(), -12))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.fact_claims 
# MAGIC select src.ClaimID,
# MAGIC        src.TransactionID,
# MAGIC        coalesce(p.Patient_Dim_Key, 0) as Patient_Dim_Key,
# MAGIC        src.EncounterID,
# MAGIC        coalesce(pr.Provider_Dim_Key, 0) as Provider_Dim_Key,
# MAGIC        coalesce(d.Dept_Dim_Key, 0) as Dept_Dim_Key,
# MAGIC        src.ServiceDate,
# MAGIC        src.ClaimDate,
# MAGIC        src.PayorID,
# MAGIC        src.ClaimAmount,
# MAGIC        src.PaidAmount,
# MAGIC        src.ClaimStatus,
# MAGIC        src.PayorType,
# MAGIC        src.Deductible,
# MAGIC        src.Coinsurance,
# MAGIC        src.Copay,
# MAGIC        src.InsertDate,
# MAGIC        src.ModifiedDate
# MAGIC from dev_catalog.bronze.claims src
# MAGIC left join dev_catalog.silver.patients_dim p
# MAGIC on src.PatientID = p.PatientID
# MAGIC left join dev_catalog.silver.providers_dim pr
# MAGIC on src.ProviderID = pr.ProviderID
# MAGIC left join dev_catalog.silver.departments_dim d
# MAGIC on src.DeptID = d.DeptID
# MAGIC where src.ModifiedDate >= add_months(current_date(), -12);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.fact_claims;

# COMMAND ----------

# from soda.scan import Scan

# scan = Scan()
# scan.add_spark_session(
#     spark,
#     data_source_name="claims"
# )
# scan.set_data_source_name("claims")
# scan.add_sodacl_yaml_str(
#     """
#     checks for claim_soda:
#       - row_count > 0
#       - missing_count(ClaimID) = 0
#       - duplicate_count(ClaimID) = 0
#       - min(ClaimAmount) >= 0
#       - min(PaidAmount) >= 0
#       - invalid_count(ClaimStatus) = 0:
#           valid values: ['Paid','Pending','Denied','Rejected','Approved']
#       - failed rows:
#           name: PaidAmount cannot exceed ClaimAmount
#           fail condition: PaidAmount > ClaimAmount
#     """
# )
# exit_code = scan.execute()
# # Print detailed scan results to debug which checks failed
# import json
# print(json.dumps(scan.get_scan_results(), indent=2))
# if exit_code != 0:
#     raise Exception("Claims data quality checks failed")
# else:
#     print("Claims data quality checks passed")
