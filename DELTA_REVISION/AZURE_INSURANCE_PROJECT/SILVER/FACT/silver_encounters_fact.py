# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.fact_encounters
# MAGIC select 
# MAGIC src.EncounterID,
# MAGIC coalesce(p.Patient_Dim_Key, 0) as Patient_Dim_Key,
# MAGIC src.EncounterDate,
# MAGIC src.EncounterType,
# MAGIC coalesce(pr.Provider_Dim_Key ,0) as Provider_Dim_Key,
# MAGIC coalesce(dept.Dept_Dim_Key, 0) as Dept_Dim_Key,
# MAGIC src.ProcedureCode,
# MAGIC src.InsertedDate,
# MAGIC src.modifiedDate
# MAGIC from dev_catalog.bronze.encounters src
# MAGIC left join dev_catalog.silver.patients_dim p on src.PatientID = p.PatientID
# MAGIC left join dev_catalog.silver.providers_dim pr on src.ProviderID = pr.ProviderID
# MAGIC left join dev_catalog.silver.departments_dim dept on src.DepartmentID = dept.DeptID

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM dev_catalog.silver.fact_encounters
# MAGIC WHERE EncounterID IN (
# MAGIC SELECT EncounterID
# MAGIC FROM dev_catalog.bronze.encounters
# MAGIC WHERE ModifiedDate >= add_months(current_date(), -12))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.fact_encounters
# MAGIC select 
# MAGIC src.EncounterID,
# MAGIC coalesce(p.Patient_Dim_Key, 0) as Patient_Dim_Key,
# MAGIC src.EncounterDate,
# MAGIC src.EncounterType,
# MAGIC coalesce(pr.Provider_Dim_Key ,0) as Provider_Dim_Key,
# MAGIC coalesce(dept.Dept_Dim_Key, 0) as Dept_Dim_Key,
# MAGIC src.ProcedureCode,
# MAGIC src.InsertedDate,
# MAGIC src.modifiedDate
# MAGIC from dev_catalog.bronze.encounters src
# MAGIC left join dev_catalog.silver.patients_dim p on src.PatientID = p.PatientID
# MAGIC left join dev_catalog.silver.providers_dim pr on src.ProviderID = pr.ProviderID
# MAGIC left join dev_catalog.silver.departments_dim dept on src.DepartmentID = dept.DeptID
# MAGIC where src.ModifiedDate >= add_months(current_date(), -12);
