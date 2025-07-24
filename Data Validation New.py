# Databricks notebook source
import pandas as pd

df_claims = ['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status']
df_icd_ref = ['cpt_code', 'cpt_description']
df_providers = ['provider_id', 'provider_name', 'provider_address', 'provider_city', 'provider_state', 'provider_zip']

def validate_claims(self,df_claims, df_icd_ref, df_providers):
    errors = []

    # 1. Schema check
    expected_cols = ['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status']
    missing_cols = set(expected_cols) - set(df_claims.columns)
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")

    # 2. Null checks
    if df_claims['member_id'].isnull().any():
        errors.append("Nulls found in member_id")
    if df_claims['date_of_service'].isnull().any():
        errors.append("Nulls found in date_of_service")

    # 3. Amount range check
    if (df_claims['amount_paid'] <= 0).any():
        errors.append("Amount paid <= 0 found")

    # 4. Code validation (ICD codes)
    invalid_icd = df_claims.loc[~df_claims['cpt_code'].isin(df_icd_ref['cpt_code'])]
    if not invalid_icd.empty:
        errors.append(f"Invalid CPT codes found: {invalid_icd['cpt_code'].unique()}")

    # 5. Referential integrity (provider IDs)
    invalid_providers = df_claims.loc[~df_claims['provider_id'].isin(df_providers['provider_id'])]
    if not invalid_providers.empty:
        errors.append(f"Invalid provider IDs found: {invalid_providers['provider_id'].unique()}")

    # 6. Date consistency
    invalid_dates = df_claims.loc[df_claims['date_of_service'] > pd.Timestamp.today()]
    if not invalid_dates.empty:
        errors.append("Date of service is in the future")

    return errors

# Example usage
# df_claims, df_icd_ref, df_providers loaded from data sources

validation_errors = validate_claims(df_claims, df_icd_ref, df_providers)
if validation_errors:
    print("Validation Errors Found:")
    for e in validation_errors:
        print("-", e)
else:
    print("All validations passed.")

# COMMAND ----------


df_claims = pd.read_csv('claims')
df_icd_ref = pd.read_csv('icd_reference')
df_providers = pd.read_csv('providers')

# COMMAND ----------

df_claims = pd.read_excel('claims.xlsx', sheet_name='Claims')
df_icd_ref = pd.read_excel('codes.xlsx', sheet_name='ICD')
df_providers = pd.read_excel('providers.xlsx')

# COMMAND ----------

df_claims = spark.createDataFrame(data=[], schema=['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status'])
df_icd_ref = spark.createDataFrame (data=[], schema=['cpt_code', 'cpt_description'])
df_providers = spark.createDataFrame(data=[], schema=['provider_id', 'provider_name', 'provider_address', 'provider_city', 'provider_state', 'provider_zip'])

df_claims.write.mode('overwrite').saveAsTable('claims')
df_icd_ref.write.mode('overwrite').saveAsTable('icd_ref')
df_providers.write.mode('overwrite').saveAsTable('providers')





# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType

# Define schema for df_claims
schema_claims = StructType([
    StructField('claim_id', StringType(), True),
    StructField('member_id', StringType(), True),
    StructField('date_of_service', DateType(), True),
    StructField('amount_paid', FloatType(), True),
    StructField('cpt_code', StringType(), True),
    StructField('provider_id', StringType(), True),
    StructField('claim_status', StringType(), True)
])

# Define schema for df_icd_ref
schema_icd_ref = StructType([
    StructField('cpt_code', StringType(), True),
    StructField('cpt_description', StringType(), True)
])

# Define schema for df_providers
schema_providers = StructType([
    StructField('provider_id', StringType(), True),
    StructField('provider_name', StringType(), True),
    StructField('provider_address', StringType(), True),
    StructField('provider_city', StringType(), True),
    StructField('provider_state', StringType(), True),
    StructField('provider_zip', StringType(), True)
])

# Create DataFrames with defined schemas
df_claims = spark.createDataFrame(data=[], schema=schema_claims)
df_icd_ref = spark.createDataFrame(data=[], schema=schema_icd_ref)
df_providers = spark.createDataFrame(data=[], schema=schema_providers)

# Write DataFrames to tables
df_claims.write.mode('overwrite').saveAsTable('claims')
df_icd_ref.write.mode('overwrite').saveAsTable('icd_ref')
df_providers.write.mode('overwrite').saveAsTable('providers')

# COMMAND ----------

import pandas as pd

# Create DataFrames from the lists
df_claims = pd.DataFrame(columns=['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status'])
df_icd_ref = pd.DataFrame(columns=['cpt_code', 'cpt_description'])
df_providers = pd.DataFrame(columns=['provider_id', 'provider_name', 'provider_address', 'provider_city', 'provider_state', 'provider_zip'])

def validate_claims(df_claims, df_icd_ref, df_providers):
    errors = []

    # 1. Schema check
    expected_cols = ['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status']
    missing_cols = set(expected_cols) - set(df_claims.columns)
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")

    # 2. Null checks
    if df_claims['member_id'].isnull().any():
        errors.append("Nulls found in member_id")
    if df_claims['date_of_service'].isnull().any():
        errors.append("Nulls found in date_of_service")

    # 3. Amount range check
    if (df_claims['amount_paid'] <= 0).any():
        errors.append("Amount paid <= 0 found")

    # 4. Code validation (ICD codes)
    invalid_icd = df_claims.loc[~df_claims['cpt_code'].isin(df_icd_ref['cpt_code'])]
    if not invalid_icd.empty:
        errors.append(f"Invalid CPT codes found: {invalid_icd['cpt_code'].unique()}")

    # 5. Referential integrity (provider IDs)
    invalid_providers = df_claims.loc[~df_claims['provider_id'].isin(df_providers['provider_id'])]
    if not invalid_providers.empty:
        errors.append(f"Invalid provider IDs found: {invalid_providers['provider_id'].unique()}")

    # 6. Date consistency
    invalid_dates = df_claims.loc[df_claims['date_of_service'] > pd.Timestamp.today()]
    if not invalid_dates.empty:
        errors.append("Date of service is in the future")

    return errors

# Example usage
# df_claims, df_icd_ref, df_providers loaded from data sources

validation_errors = validate_claims(df_claims, df_icd_ref, df_providers)
if validation_errors:
    print("Validation Errors Found:")
    for e in validation_errors:
        print("-", e)
else:
    print("All validations passed.")

# COMMAND ----------

import pandas as pd

# Create DataFrames from the lists
df_claims = pd.DataFrame(columns=['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status'])
df_icd_ref = pd.DataFrame(columns=['cpt_code', 'cpt_description'])
df_providers = pd.DataFrame(columns=['provider_id', 'provider_name', 'provider_address', 'provider_city', 'provider_state', 'provider_zip'])

def validate_claims(df_claims, df_icd_ref, df_providers):
    errors = []

    # 1. Schema check
    expected_cols = ['claim_id', 'member_id', 'date_of_service', 'amount_paid', 'cpt_code', 'provider_id', 'claim_status']
    missing_cols = set(expected_cols) - set(df_claims.columns)
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")

    # 2. Null checks
    if df_claims['member_id'].isnull().any():
        errors.append("Nulls found in member_id")
    if df_claims['date_of_service'].isnull().any():
        errors.append("Nulls found in date_of_service")

    # 3. Amount range check
    if (df_claims['amount_paid'] <= 0).any():
        errors.append("Amount paid <= 0 found")

    # 4. Code validation (ICD codes)
    invalid_icd = df_claims.loc[~df_claims['cpt_code'].isin(df_icd_ref['cpt_code'])]
    if not invalid_icd.empty:
        errors.append(f"Invalid CPT codes found: {invalid_icd['cpt_code'].unique()}")

    # 5. Referential integrity (provider IDs)
    invalid_providers = df_claims.loc[~df_claims['provider_id'].isin(df_providers['provider_id'])]
    if not invalid_providers.empty:
        errors.append(f"Invalid provider IDs found: {invalid_providers['provider_id'].unique()}")

    # 6. Date consistency
    invalid_dates = df_claims.loc[df_claims['date_of_service'] > pd.Timestamp.today()]
    if not invalid_dates.empty:
        errors.append("Date of service is in the future")

    return errors

# Example usage
# df_claims, df_icd_ref, df_providers loaded from data sources

validation_errors = validate_claims(df_claims, df_icd_ref, df_providers)
if validation_errors:
    print("Validation Errors Found:")
    for e in validation_errors:
        print("-", e)
else:
    print("All validations passed.")

# COMMAND ----------

df = pd.DataFrame({'code': ['A', 'B', 'C', 'D']})
valid_codes = ['A', 'B']

# Shows which codes are valid
print(df['code'].isin(valid_codes))
# Output: [True, True, False, False]

# Use ~ to get the invalid ones
print(~df['code'].isin(valid_codes))
# Output: [False, False, True, True]

# Filter for invalid
df_invalid = df[~df['code'].isin(valid_codes)]
print(df_invalid)
# Output: C and D rows

# COMMAND ----------

import pandas as pd

claims = pd.dataFrame({'claim_id': [1, 2, 3], 'member_id': [101, 102, 103]})
icd_ref =pd.dataFrame({"cpt_code": ['A', 'B', 'C', 'D'], 'cpt_description': ['desc1', 'desc2', 'desc3', 'desc4']})



# COMMAND ----------

emp_df = pd.read_parquet("/Volumes/workspace/default/emp/emp_parquet_salt/")

# COMMAND ----------

emp_df = pd.read_parquet(
    "/Volumes/workspace/default/emp/emp_parquet_salt/",
    engine='fastparquet'
)

# COMMAND ----------

emp_df = pd.read_parquet("/Volumes/workspace/default/emp/Badrec/")
emp_df.display()

# COMMAND ----------

emp_delta = pd.read_parquet("workspace.default.budget_pivot")

# COMMAND ----------

df_claims = pd.read_parquet('s3://my-bucket/claims.parquet', storage_options={"key": "XXX", "secret": "YYY"})


# COMMAND ----------

from sqlalchemy import create_engine
import pandas as pd
claim_df = pd.read_sql("SELECT * FROM CALIMS", engine)

# COMMAND ----------

# MAGIC %pip install sqlalchemy
# MAGIC
# MAGIC from sqlalchemy import create_engine
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Assuming 'engine' is defined somewhere in your code
# MAGIC claim_df = pd.read_sql("SELECT * FROM CALIMS",con)

# COMMAND ----------

from sqlalchemy import create_engine
import pandas as pd

# Example: PostgreSQL
engine = create_engine("postgresql+psycopg2://username:password@host:port/database")

df_claims = pd.read_sql("SELECT * FROM claims", engine)
df_icd_ref = pd.read_sql("SELECT * FROM icd_codes", engine)
df_providers = pd.read_sql("SELECT * FROM providers", engine)


# COMMAND ----------

# MAGIC %md
# MAGIC **SNOWFLAKE DATABASE**

# COMMAND ----------


#df_claims = pd.read_sql("SELECT * FROM CLAIMS", engine)


# COMMAND ----------


)
#df_claims = pd.read_sql("SELECT * FROM CLAIMS", engine)

# COMMAND ----------


}

# COMMAND ----------

# emp_df = pd.read_parquet("/Volumes/workspace/default/emp/Badrec/")
# emp_df.display()
emp_df = spark.read.format("csv").option("inferSchema",True).option("header", "true").csv("/Volumes/workspace/default/emp/Badrec/").write.format("snowflake").options(**options).mode("append").option("DBTable", "EMP").save()


# COMMAND ----------


