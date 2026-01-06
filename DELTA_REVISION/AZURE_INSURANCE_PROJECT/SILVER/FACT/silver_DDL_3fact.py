# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG dev_catalog;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.fact_claims
# MAGIC (
# MAGIC   ClaimID STRING,
# MAGIC   TransactionID STRING,
# MAGIC   Patient_dim_key BIGINT, 
# MAGIC   EncounterID STRING,
# MAGIC   Provider_Dim_Key BIGINT, 
# MAGIC   Dept_Dim_Key BIGINT,
# MAGIC   ServiceDate TIMESTAMP,
# MAGIC   ClaimDate TIMESTAMP,
# MAGIC   PayorID STRING,
# MAGIC   ClaimAmount DECIMAL(10,2),
# MAGIC   PaidAmount DECIMAL(10,2),
# MAGIC   ClaimStatus STRING,
# MAGIC   PayorType STRING,
# MAGIC   Deductible DECIMAL(10,2),
# MAGIC   Coinsurance DECIMAL(10,2),
# MAGIC   Copay DECIMAL(10,2),
# MAGIC   InsertDate TIMESTAMP,
# MAGIC   ModifiedDate TIMESTAMP
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_catalog.silver.fact_encounters;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dev_catalog.silver.fact_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.silver.fact_transactions (
# MAGIC     TransactionID STRING,
# MAGIC     EncounterID STRING,
# MAGIC     Patient_Dim_Key BIGINT,
# MAGIC     Provider_Dim_key BIGINT,
# MAGIC     Dept_Dim_Key BIGINT,
# MAGIC     VisitDate TIMESTAMP,
# MAGIC     ServiceDate TIMESTAMP,
# MAGIC     PaidDate TIMESTAMP,
# MAGIC     VisitType STRING,
# MAGIC     Amount DECIMAL(10,2),
# MAGIC     AmountType STRING,
# MAGIC     PaidAmount DECIMAL(10,2),
# MAGIC     ClaimID STRING,
# MAGIC     PayorID STRING,
# MAGIC     CPT_Dim_Key BIGINT,
# MAGIC     icdcode_dim_key BIGINT,
# MAGIC     LineOfBusiness STRING,
# MAGIC     MedicaidID STRING,
# MAGIC     MedicareID STRING,
# MAGIC     InsertDate TIMESTAMP,
# MAGIC     ModifiedDate TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.fact_encounters (
# MAGIC   EncounterID STRING,
# MAGIC   Patient_Dim_Key BIGINT,       -- FK to dim_patients.PatientKey
# MAGIC   EncounterDate TIMESTAMP,
# MAGIC   EncounterType STRING,
# MAGIC   Provider_Dim_Key BIGINT,     
# MAGIC   Dept_Dim_Key BIGINT,          
# MAGIC   ProcedureCode INT,
# MAGIC   InsertedDate TIMESTAMP,
# MAGIC   ModifiedDate TIMESTAMP
# MAGIC );

# COMMAND ----------


