-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Bronze Layer Validation
-- MAGIC
-- MAGIC **Objective:** Validate Bronze tables are complete and correctly structured.
-- MAGIC
-- MAGIC **Exam Topics:** SQL queries, Delta Lake table inspection

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Record Counts

-- COMMAND ----------

-- Verify expected record counts
SELECT 'raw_orders' AS table_name, COUNT(*) AS record_count
FROM delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_orders`
UNION ALL
SELECT 'raw_customers', COUNT(*)
FROM delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_customers`
UNION ALL
SELECT 'raw_products', COUNT(*)
FROM delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_products`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Check Delta Table Properties
-- MAGIC
-- MAGIC **EXAM TIP:** `DESCRIBE DETAIL` shows Delta table metadata including file count, size, and format.

-- COMMAND ----------

DESCRIBE DETAIL delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Metadata Columns

-- COMMAND ----------

-- Confirm metadata columns are populated
SELECT
    _datasource,
    _ingest_timestamp,
    _input_file_name,
    COUNT(*) AS records
FROM delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_orders`
GROUP BY _datasource, _ingest_timestamp, _input_file_name
ORDER BY _input_file_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preview Data Quality Issues
-- MAGIC
-- MAGIC These will be addressed in Phase 2 (Silver layer).

-- COMMAND ----------

-- Null customer IDs
SELECT COUNT(*) AS null_customer_ids
FROM delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_orders`
WHERE customer_id IS NULL

-- COMMAND ----------

-- Null order dates
SELECT COUNT(*) AS null_order_dates
FROM delta.`dbfs:/FileStore/ecommerce_project/bronze/raw_orders`
WHERE order_date IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Takeaway
-- MAGIC
-- MAGIC Bronze tables preserve ALL raw data, including bad records.
-- MAGIC Data quality enforcement happens in Silver, not Bronze.
-- MAGIC This is a core principle of medallion architecture tested on the exam.
