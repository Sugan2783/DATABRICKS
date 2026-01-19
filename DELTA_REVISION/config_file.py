#azure_sql

INGESTION_CONFIG = [
  {
    "source":"azure_sql",
    "path": "/Volumes/prod/bronze/projectdata/claims.csv",
    "table":"prod.bronze.claims"
  },
  {
    "source":"azure_sql",
    "path": "/Volumes/prod/bronze/projectdata/departments",
    "table":"prod.bronze.departments"
  },
  {
    "source":"azure_sql",
    "path": "/Volumes/prod/bronze/projectdata/encounter_data.csv",
    "table":"prod.bronze.encounters"
  },
  {
    "source":"azure_sql",
    "path": "/Volumes/prod/bronze/projectdata/patients.csv",
    "table":"prod.bronze.patients"
  },
  {
    "source":"azure_sql",
    "path": "/Volumes/prod/bronze/projectdata/providers.csv",
    "table":"prod.bronze.providers"
  },
  {
    "source":"azure_sql",
    "path": "/Volumes/prod/bronze/projectdata/transactions.csv",
    "table":"prod.bronze.transactions"
  },
  {
    "source":"api_data",
    "path": "/Volumes/prod/bronze/api_data/ICD_code_data.csv",
    "table":"prod.bronze.ICD_code_data"
  },
  {
    "source":"api_data",
    "path": "/Volumes/prod/bronze/api_data/NPI_data.csv",
    "table":"prod.bronze.NPI_data"
  },
  {
      "source":"api_data",
    "path": "/Volumes/prod/bronze/api_data/cptcodes.csv",
    "table":"prod.bronze.cptcodes"
  }]
  




