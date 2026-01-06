# Databricks notebook source
dbutils.notebook.run(
    '/Workspace/Users/suganthibala2701@gmail.com/dbnew_Azure_Project/BRONZE/Incremental_claims_merge_bronze',
    60
)

# COMMAND ----------

dbutils.notebook.run(
    '/Workspace/Users/suganthibala2701@gmail.com/dbnew_Azure_Project/BRONZE/incremental_encounters_merge_bronze',
    60
)

# COMMAND ----------

dbutils.notebook.run(
    '/Workspace/Users/suganthibala2701@gmail.com/dbnew_Azure_Project/BRONZE/Incremental_Patients_merge_bronze',
    60
)

# COMMAND ----------

dbutils.notebook.run(
    '/Workspace/Users/suganthibala2701@gmail.com/dbnew_Azure_Project/BRONZE/incremental_Transactions_merge_bronze',
    60
)
