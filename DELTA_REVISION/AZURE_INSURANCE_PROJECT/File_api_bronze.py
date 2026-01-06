# Databricks notebook source
display(dbutils.fs.ls("abfss://output@insurancemedallion.dfs.core.windows.net/FILE_API/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_icdcode = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/FILE_API/ICD_code_data.csv")

df_icdcode.display()

df_icdcode.write.format("delta").mode("overwrite").saveAsTable("icd_code_data")



# COMMAND ----------


from pyspark.sql import DataFrame

def replace_spaces_with_underscores(df_cptcodes: DataFrame) -> DataFrame:
    # Use a clearer name for readability
    renamed_df = df_cptcodes
    for old_col_name in df_cptcodes.columns:
        new_col_name = old_col_name.replace(" ", "_")
        if old_col_name != new_col_name:
            renamed_df = renamed_df.withColumnRenamed(old_col_name, new_col_name)
    return renamed_df

df_cleaned_cptcodes = replace_spaces_with_underscores(df_cptcodes)

print("\n--- Cleaned Schema ---")


# COMMAND ----------



df_cptcodes = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/FILE_API/cptcodes.csv")


# df_cptcodes = df_cptcodes.toDF(*[col.replace(' ', '_') for col in df_cptcodes.columns])df_cptcodes.write.format("delta").mode("overwrite").saveAsTable("cptcodes")



# COMMAND ----------

# df_cleaned_cptcodes = df_cleaned_cptcodes.withColumn("CPT_Codes", df_cleaned_cptcodes["CPT_Codes"].cast("string"))

# COMMAND ----------

df_cleaned_cptcodes.write.format("delta").mode("overwrite").saveAsTable("dev_catalog.bronze.cptcodes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.cptcodes;

# COMMAND ----------

df_npi = spark.read.format("csv").option("header", True).option("inferSchema", True).load("abfss://output@insurancemedallion.dfs.core.windows.net/FILE_API/NPI_data.csv") 

df_npi.display()

df_npi.write.format("delta").mode("overwrite").saveAsTable("npi_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from npi_data;
