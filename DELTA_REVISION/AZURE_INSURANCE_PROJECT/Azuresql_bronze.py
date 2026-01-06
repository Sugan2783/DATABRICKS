# Databricks notebook source
display(dbutils.fs.ls('abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb'))

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_claim = spark.read.format("csv").option("inferSchema", True).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/CLAIMS.csv")

df_claim.display()

df_claim.write.format("delta").mode("overwrite").saveAsTable("claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims;

# COMMAND ----------

df_dept = spark.read.format("csv").option("inferSchema", True).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/DEPARTMENTS.csv")

df_dept.display()

df_dept.write.format("delta").mode("overwrite").saveAsTable("departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from departments;

# COMMAND ----------

df_provider = spark.read.format("csv").option("inferSchema", True).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/PROVIDERS.csv")

df_provider.display()

df_provider.write.format("delta").mode("overwrite").saveAsTable("providers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from providers;

# COMMAND ----------

# %sql
# drop table dev_catalog.bronze.patients;

# COMMAND ----------

df_dept = spark.read.format("csv").option("inferSchema", True).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/PATIENTS.csv")

df_dept.display()

df_dept.write.format("delta").mode("overwrite").saveAsTable("patients")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from patients;

# COMMAND ----------

df_dept = spark.read.format("csv").option("inferSchema", True).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/TRANSACTIONS.csv")

df_dept = df_dept.withColumn("ProcedureCode", df_dept.ProcedureCode.cast("string"))

df_dept.display()

# COMMAND ----------

df_dept.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("dev_catalog.bronze.transactions")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transactions;

# COMMAND ----------

df_dept = spark.read.format("csv").option("inferSchema", True).option("header", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/azuresqldb/ENCOUNTERS.csv")

df_dept.display()

df_dept.write.format("delta").mode("overwrite").saveAsTable("encounters")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from encounters;
