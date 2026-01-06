# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.fact_transactions
# MAGIC select 
# MAGIC src.TransactionID,
# MAGIC src.EncounterID,
# MAGIC coalesce(p.Patient_Dim_Key, 0) as Patient_Dim_Key,
# MAGIC coalesce(pr.Provider_Dim_Key, 0) as Provider_Dim_Key,
# MAGIC coalesce(dept.Dept_Dim_Key, 0) as Dept_Dim_Key,
# MAGIC src.VisitDate,
# MAGIC src.ServiceDate,
# MAGIC src.PaidDate,
# MAGIC src.VisitType,
# MAGIC src.Amount,
# MAGIC src.AmountType,
# MAGIC src.PaidAmount,
# MAGIC src.ClaimID,
# MAGIC src.PayorID,
# MAGIC coalesce(cpt.CPT_Dim_Key, 0) as CPT_Dim_Key,
# MAGIC coalesce(icd.icdcode_dim_key, 0 ) as icdcode_dim_key,
# MAGIC src.LineOfBusiness,
# MAGIC src.MedicaidID,
# MAGIC src.MedicareID,
# MAGIC current_timestamp() as InsertDate,
# MAGIC src.ModifiedDate
# MAGIC from dev_catalog.bronze.transactions src
# MAGIC left join dev_catalog.silver.patients_dim p on src.PatientID = p.PatientID
# MAGIC left join dev_catalog.silver.providers_dim pr on src.ProviderID = pr.ProviderID
# MAGIC left join dev_catalog.silver.departments_dim dept on src.DeptID = dept.DeptID
# MAGIC left join dev_catalog.silver.cptcodes_dim cpt on src.ProcedureCode = cpt.CPT_Codes
# MAGIC left join dev_catalog.silver.icdcode_data_dim icd on src.ICDCode = icd.icd_code;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM dev_catalog.silver.fact_transactions
# MAGIC WHERE TransactionID in (
# MAGIC      SELECT TransactionID
# MAGIC      FROM dev_catalog.bronze.transactions
# MAGIC      WHERE ModifiedDate >= add_months(current_date(), -12))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.silver.fact_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.fact_transactions
# MAGIC select 
# MAGIC src.TransactionID,
# MAGIC src.EncounterID,
# MAGIC coalesce(p.Patient_Dim_Key, 0) as Patient_Dim_Key,
# MAGIC coalesce(pr.Provider_Dim_Key, 0) as Provider_Dim_Key,
# MAGIC coalesce(dept.Dept_Dim_Key, 0) as Dept_Dim_Key,
# MAGIC src.VisitDate,
# MAGIC src.ServiceDate,
# MAGIC src.PaidDate,
# MAGIC src.VisitType,
# MAGIC src.Amount,
# MAGIC src.AmountType,
# MAGIC src.PaidAmount,
# MAGIC src.ClaimID,
# MAGIC src.PayorID,
# MAGIC coalesce(cpt.CPT_Dim_Key, 0) as CPT_Dim_Key,
# MAGIC coalesce(icd.icdcode_dim_key, 0 ) as icdcode_dim_key,
# MAGIC src.LineOfBusiness,
# MAGIC src.MedicaidID,
# MAGIC src.MedicareID,
# MAGIC current_timestamp() as InsertDate,
# MAGIC src.ModifiedDate
# MAGIC from dev_catalog.bronze.transactions src
# MAGIC left join dev_catalog.silver.patients_dim p on src.PatientID = p.PatientID
# MAGIC left join dev_catalog.silver.providers_dim pr on src.ProviderID = pr.ProviderID
# MAGIC left join dev_catalog.silver.departments_dim dept on src.DeptID = dept.DeptID
# MAGIC left join dev_catalog.silver.cptcodes_dim cpt on src.ProcedureCode = cpt.CPT_Codes
# MAGIC left join dev_catalog.silver.icdcode_data_dim icd on src.ICDCode = icd.icd_code
# MAGIC where src.ModifiedDate >= add_months(current_date(), -12);
