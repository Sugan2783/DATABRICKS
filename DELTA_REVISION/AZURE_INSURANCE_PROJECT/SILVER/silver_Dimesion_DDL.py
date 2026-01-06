# Databricks notebook source
# MAGIC %md
# MAGIC PATIENTS DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DEV_CATALOG.SILVER.patients_dim
# MAGIC (
# MAGIC Patient_dim_key BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 0 INCREMENT BY 1),
# MAGIC PatientID  STRING,	
# MAGIC FirstName	STRING,
# MAGIC LastName	STRING,
# MAGIC MiddleName	STRING,
# MAGIC SSN	 STRING,
# MAGIC PhoneNumber STRING,	
# MAGIC Gender	STRING,
# MAGIC DOB	TIMESTAMP,
# MAGIC Address	STRING,
# MAGIC ModifiedDate TIMESTAMP,
# MAGIC InsertDate_silver TIMESTAMP,
# MAGIC UpdateDate_silver TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev_catalog.silver.patients_dim(PatientID, FirstName, LastName, MiddleName, SSN, PhoneNumber, Gender, DOB, Address, ModifiedDate)
# MAGIC VALUES("dwunclassified", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC PROVIDER DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev_catalog.silver.providers_dim (
# MAGIC     Provider_Dim_Key BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 0 INCREMENT BY 1),
# MAGIC     ProviderID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Specialization STRING,
# MAGIC     DeptID STRING,
# MAGIC     NPI BIGINT,
# MAGIC     InsertDate_Silver TIMESTAMP,
# MAGIC     UpdateDate_Silver TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev_catalog.silver.providers_dim (PROVIDERID,FIRSTNAME,LASTNAME, SPECIALIZATION, DEPTID, NPI)
# MAGIC VALUES("dwunclassified", NULL, NULL, NULL, NULL,NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC DEPARTMENTS DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev_catalog.silver.departments_dim
# MAGIC (
# MAGIC   Dept_Dim_Key BIGINT generated always as identity(start with 0 increment by 1),
# MAGIC   DeptID STRING,
# MAGIC   Name STRING,
# MAGIC   InsertDate_silver TIMESTAMP,
# MAGIC   UpdateDate_silver timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev_catalog.silver.departments_dim(DeptID, Name) VALUES("dwunclassified", NULL);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC CPTCODES DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev_catalog.silver.cptcodes_dim
# MAGIC (
# MAGIC   CPT_Dim_Key BIGINT generated always as identity(start with 0 increment by 1),
# MAGIC    Procedure_Code_Category string,
# MAGIC    CPT_Codes string,
# MAGIC    Procedure_Code_Descriptions  string,
# MAGIC    Code_Status string,
# MAGIC    InsertDate_silver timestamp,
# MAGIC    UpdateDate_silver timestamp
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.cptcodes_dim
# MAGIC (
# MAGIC CPT_Codes, Procedure_Code_Category, Procedure_Code_Descriptions,
# MAGIC  Code_Status) values ("dwunclassified", NULL, NULL, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC ICD_CODE_DATA DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev_catalog.silver.icdcode_data_dim
# MAGIC (
# MAGIC   icdcode_dim_key bigint generated always as identity(start with 0 increment by 1),
# MAGIC   icd_code string,
# MAGIC   icd_code_type  string,
# MAGIC   code_description string,
# MAGIC   inserted_date timestamp,
# MAGIC   updated_date timestamp,
# MAGIC   is_current_flag boolean,
# MAGIC   InsertDate_silver timestamp,
# MAGIC   UpdateDate_silver timestamp 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.icdcode_data_dim
# MAGIC (
# MAGIC   icd_code, icd_code_type, code_description, inserted_date,  updated_date, is_current_flag, InsertDate_silver,
# MAGIC   UpdateDate_silver
# MAGIC )
# MAGIC values("dwunclassified", NULL, NULL, NULL, NULL, NULL, NULL, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC NPI_DATA DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev_catalog.silver.npi_data_dim
# MAGIC (
# MAGIC   npi_dim_key bigint generated always as identity(start with 0 increment by 1),
# MAGIC   npi_id bigint,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   position string,
# MAGIC   organisation_name string,
# MAGIC   last_updated timestamp,
# MAGIC   insertDate_silver timestamp,
# MAGIC   UpdateDate_silver timestamp  
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev_catalog.silver.npi_data_dim
# MAGIC (
# MAGIC   npi_id, first_name, last_name, position, organisation_name,
# MAGIC   last_updated) values (0, NULL,NULL, NULL, NULL, NULL);
