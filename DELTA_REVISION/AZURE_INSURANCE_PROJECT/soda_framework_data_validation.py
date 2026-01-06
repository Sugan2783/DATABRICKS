# Databricks notebook source
# MAGIC %pip install soda-core
# MAGIC %pip install soda-core-spark-df

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from soda.scan import Scan

scan = Scan()
scan.add_spark_session(
    spark,
    data_source_name="provider"
)
scan.set_data_source_name("provider")
scan.add_sodacl_yaml_file("path")


exit_code = scan.execute()
if exit_code != 0:
    raise Exception("Data Quality Check Failed")
else:
    print("Data Quality Check Passed")
