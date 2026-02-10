-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Query Performance Analysis
-- MAGIC
-- MAGIC **Objective:** Read and interpret query execution plans.
-- MAGIC
-- MAGIC **Exam Topics:** EXPLAIN, data skipping, partition pruning

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## EXPLAIN: View the Query Plan
-- MAGIC
-- MAGIC **EXAM TIP:** EXPLAIN shows how Spark will execute a query.

-- COMMAND ----------

-- Basic explain
EXPLAIN
SELECT * FROM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
WHERE order_date > '2025-01-01'

-- COMMAND ----------

-- Extended explain (shows more detail)
EXPLAIN EXTENDED
SELECT * FROM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
WHERE order_date > '2025-01-01'
  AND shipping_state = 'CA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Skipping
-- MAGIC
-- MAGIC **EXAM TIP:** Delta Lake stores min/max statistics per file.
-- MAGIC When you filter on a column, Delta skips files where the value can't exist.
-- MAGIC
-- MAGIC Z-ORDER improves data skipping by co-locating similar values in the same files.

-- COMMAND ----------

-- This query benefits from data skipping on order_date
-- After Z-ORDER BY (order_date), only files containing matching dates are read
SELECT COUNT(*)
FROM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Exam Takeaways
-- MAGIC
-- MAGIC 1. `EXPLAIN` shows the logical and physical plan
-- MAGIC 2. Delta Lake automatically tracks min/max stats per file per column
-- MAGIC 3. Data skipping uses these stats to skip irrelevant files
-- MAGIC 4. Z-ORDER improves data skipping effectiveness
-- MAGIC 5. Partition pruning skips entire partitions (similar concept)
-- MAGIC 6. Photon acceleration speeds up SQL workloads (if enabled)
