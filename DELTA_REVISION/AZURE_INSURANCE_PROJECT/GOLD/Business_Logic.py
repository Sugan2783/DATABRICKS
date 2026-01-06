# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dev_catalog;
# MAGIC use schema gold;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. What is the total claim amount vs. total paid amount by month?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT
# MAGIC    SUM(c.ClaimAmount) AS claimamt,
# MAGIC    SUM(c.PaidAmount) AS paidamt,
# MAGIC    SUM(c.ClaimAmount) - SUM(c.PaidAmount) AS `amount to be collected`,
# MAGIC    YEAR(t.PaidDate) AS paid_year,
# MAGIC    MONTH(t.PaidDate) AS paid_month
# MAGIC  FROM dev_catalog.silver.fact_claims c
# MAGIC  LEFT JOIN dev_catalog.silver.fact_transactions t
# MAGIC    ON c.TransactionID = t.TransactionID
# MAGIC  GROUP BY
# MAGIC    YEAR(t.PaidDate),
# MAGIC    MONTH(t.PaidDate)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.total_claim_and_paid_monthly AS
# MAGIC  (SELECT
# MAGIC    SUM(c.ClaimAmount) AS claimamt,
# MAGIC    SUM(c.PaidAmount) AS paidamt,
# MAGIC    SUM(c.ClaimAmount) - SUM(c.PaidAmount) AS `amount to be collected`,
# MAGIC    YEAR(t.PaidDate) AS paid_year,
# MAGIC    MONTH(t.PaidDate) AS paid_month
# MAGIC  FROM dev_catalog.silver.fact_claims c
# MAGIC  LEFT JOIN dev_catalog.silver.fact_transactions t
# MAGIC    ON c.TransactionID = t.TransactionID
# MAGIC  GROUP BY
# MAGIC    YEAR(t.PaidDate),
# MAGIC    MONTH(t.PaidDate)
# MAGIC  );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  2 What is the average claim amount by patient age group 18 to 40 years?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    FLOOR(AVG(c.ClaimAmount)) AS claimamt,
# MAGIC    FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), p.DOB) / 12) AS age
# MAGIC  FROM dev_catalog.silver.fact_claims c
# MAGIC  LEFT JOIN dev_catalog.silver.patients_dim p
# MAGIC    ON c.Patient_Dim_Key = p.Patient_Dim_Key
# MAGIC  WHERE FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), p.DOB) / 12) BETWEEN 18 AND 40
# MAGIC  GROUP BY age;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.claim_amt_byAge AS
# MAGIC  SELECT 
# MAGIC    FLOOR(AVG(c.ClaimAmount)) AS claimamt,
# MAGIC    FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), p.DOB) / 12) AS age
# MAGIC  FROM dev_catalog.silver.fact_claims c
# MAGIC  LEFT JOIN dev_catalog.silver.patients_dim p
# MAGIC    ON c.Patient_Dim_Key = p.Patient_Dim_Key
# MAGIC  WHERE FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), p.DOB) / 12) BETWEEN 18 AND 40
# MAGIC  GROUP BY age
# MAGIC  ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  3. Which providers see the most Medicare vs Medicaid patients?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  select 
# MAGIC  pr.ProviderID,
# MAGIC  c.PayorID,
# MAGIC  count(case when c.PayorID = 'Medicaid' THEN 1
# MAGIC  when c.PayorID = 'Medicare' THEN 2 end )as count_PayorType
# MAGIC  from dev_catalog.silver.providers_dim pr
# MAGIC  LEFT JOIN dev_catalog.silver.fact_claims c
# MAGIC  ON pr.Provider_Dim_Key = c.Provider_Dim_Key
# MAGIC  ---where PayorID = 'Medicare' and PayorID = 'Medicaid'
# MAGIC  where PayorID = 'Medicare' or PayorID = 'Medicaid'
# MAGIC  group by
# MAGIC  c.PayorID,
# MAGIC  pr.ProviderID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view dev_catalog.gold.medicare_medicaid_provider AS
# MAGIC  (
# MAGIC  select 
# MAGIC  pr.ProviderID,
# MAGIC  c.PayorID,
# MAGIC  count(case when c.PayorID = 'Medicaid' THEN 1
# MAGIC  when c.PayorID = 'Medicare' THEN 2 end )as count_PayorType
# MAGIC  from dev_catalog.silver.providers_dim pr
# MAGIC  LEFT JOIN dev_catalog.silver.fact_claims c
# MAGIC  ON pr.Provider_Dim_Key = c.Provider_Dim_Key
# MAGIC  ---where PayorID = 'Medicare' and PayorID = 'Medicaid'
# MAGIC  where PayorID = 'Medicare' or PayorID = 'Medicaid'
# MAGIC  group by
# MAGIC  c.PayorID,
# MAGIC  pr.ProviderID);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT
# MAGIC    pr.ProviderID,
# MAGIC    pr.FirstName,
# MAGIC    pr.LastName,
# MAGIC    CASE
# MAGIC      WHEN c.PayorID IN ('MEDICARE') THEN 'Medicare'
# MAGIC      WHEN c.PayorID IN ('MEDICAID') THEN 'Medicaid'
# MAGIC    END AS payor_type,
# MAGIC    COUNT(DISTINCT c.Provider_Dim_Key) AS Provider_count
# MAGIC  FROM dev_catalog.silver.providers_dim pr
# MAGIC  JOIN dev_catalog.silver.fact_claims c
# MAGIC    ON pr.Provider_Dim_Key = c.Provider_Dim_Key
# MAGIC  WHERE c.PayorID IN ('MEDICARE', 'MEDICAID')
# MAGIC  GROUP BY
# MAGIC    pr.ProviderID,
# MAGIC    pr.FirstName,
# MAGIC    pr.LastName,
# MAGIC    CASE
# MAGIC      WHEN c.PayorID IN ('MEDICARE') THEN 'Medicare'
# MAGIC      WHEN c.PayorID IN ('MEDICAID') THEN 'Medicaid'
# MAGIC    END
# MAGIC  ORDER BY Provider_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view medicare_medicaid_provider AS
# MAGIC  SELECT
# MAGIC    pr.ProviderID,
# MAGIC    pr.FirstName,
# MAGIC    pr.LastName,
# MAGIC    CASE
# MAGIC      WHEN c.PayorID IN ('MEDICARE') THEN 'Medicare'
# MAGIC      WHEN c.PayorID IN ('MEDICAID') THEN 'Medicaid'
# MAGIC    END AS payor_type,
# MAGIC    COUNT(DISTINCT c.Provider_Dim_Key) AS Provider_count
# MAGIC  FROM dev_catalog.silver.providers_dim pr
# MAGIC  JOIN dev_catalog.silver.fact_claims c
# MAGIC    ON pr.Provider_Dim_Key = c.Provider_Dim_Key
# MAGIC  WHERE c.PayorID IN ('MEDICARE', 'MEDICAID')
# MAGIC  GROUP BY
# MAGIC    pr.ProviderID,
# MAGIC    pr.FirstName,
# MAGIC    pr.LastName,
# MAGIC    CASE
# MAGIC      WHEN c.PayorID IN ('MEDICARE') THEN 'Medicare'
# MAGIC      WHEN c.PayorID IN ('MEDICAID') THEN 'Medicaid'
# MAGIC    END
# MAGIC  ORDER BY Provider_count DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  4. Which providers are linked to high-cost procedures?

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT
# MAGIC    ProviderID,
# MAGIC    FirstName,
# MAGIC    Specialization,
# MAGIC    ProcedureCode,
# MAGIC    Amount
# MAGIC  FROM (
# MAGIC    SELECT
# MAGIC      p.ProviderID,
# MAGIC      p.FirstName,
# MAGIC      p.Specialization,
# MAGIC      e.ProcedureCode,
# MAGIC      t.Amount,
# MAGIC      RANK() OVER (
# MAGIC        PARTITION BY p.ProviderID
# MAGIC        ORDER BY t.Amount DESC
# MAGIC      ) AS rnk
# MAGIC    FROM dev_catalog.silver.fact_encounters e
# MAGIC    JOIN dev_catalog.silver.providers_dim p
# MAGIC      ON e.Provider_Dim_Key = p.Provider_Dim_Key
# MAGIC    join dev_catalog.silver.fact_transactions t 
# MAGIC    on t.Provider_Dim_Key = p.Provider_Dim_Key
# MAGIC  ) 
# MAGIC  WHERE rnk <= 1;
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC  create or replace view dev_catalog.gold.top_procedures AS
# MAGIC  SELECT
# MAGIC    ProviderID,
# MAGIC    FirstName,
# MAGIC    Specialization,
# MAGIC    ProcedureCode,
# MAGIC    Amount
# MAGIC  FROM (
# MAGIC    SELECT
# MAGIC      p.ProviderID,
# MAGIC      p.FirstName,
# MAGIC      p.Specialization,
# MAGIC      e.ProcedureCode,
# MAGIC      t.Amount,
# MAGIC      RANK() OVER (
# MAGIC        PARTITION BY p.ProviderID
# MAGIC        ORDER BY t.Amount DESC
# MAGIC      ) AS rnk
# MAGIC    FROM dev_catalog.silver.fact_encounters e
# MAGIC    JOIN dev_catalog.silver.providers_dim p
# MAGIC      ON e.Provider_Dim_Key = p.Provider_Dim_Key
# MAGIC    join dev_catalog.silver.fact_transactions t 
# MAGIC    on t.Provider_Dim_Key = p.Provider_Dim_Key
# MAGIC     ) 
# MAGIC  WHERE rnk <= 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
