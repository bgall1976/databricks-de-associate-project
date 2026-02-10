-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - VACUUM, History, and Time Travel
-- MAGIC
-- MAGIC **Objective:** Manage Delta table history and query previous versions.
-- MAGIC
-- MAGIC **EXAM TIP:** Time travel, VACUUM, and DESCRIBE HISTORY are all heavily tested.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DESCRIBE HISTORY: View Table Versions
-- MAGIC
-- MAGIC **EXAM TIP:** Every write operation creates a new version in the transaction log.

-- COMMAND ----------

-- See all operations performed on the table
DESCRIBE HISTORY delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time Travel: Query Previous Versions
-- MAGIC
-- MAGIC **EXAM TIP:** Two syntaxes for time travel:
-- MAGIC 1. `VERSION AS OF n` - query a specific version number
-- MAGIC 2. `TIMESTAMP AS OF 'datetime'` - query as of a specific time

-- COMMAND ----------

-- Query the original version (before any updates/optimizations)
-- EXAM TIP: Version 0 is the initial write
SELECT COUNT(*) AS v0_count
FROM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
VERSION AS OF 0

-- COMMAND ----------

-- Query as of a specific timestamp
-- EXAM TIP: Useful for auditing or recovering from bad writes
SELECT COUNT(*) AS historical_count
FROM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
TIMESTAMP AS OF '2025-01-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time Travel in PySpark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # EXAM TIP: Both syntaxes work in PySpark
-- MAGIC
-- MAGIC # By version
-- MAGIC df_v0 = (
-- MAGIC     spark.read
-- MAGIC     .format("delta")
-- MAGIC     .option("versionAsOf", 0)
-- MAGIC     .load("dbfs:/FileStore/ecommerce_project/silver/cleaned_orders")
-- MAGIC )
-- MAGIC print(f"Version 0 count: {df_v0.count()}")
-- MAGIC
-- MAGIC # By timestamp
-- MAGIC # df_ts = (
-- MAGIC #     spark.read
-- MAGIC #     .format("delta")
-- MAGIC #     .option("timestampAsOf", "2025-01-01")
-- MAGIC #     .load("dbfs:/FileStore/ecommerce_project/silver/cleaned_orders")
-- MAGIC # )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## RESTORE: Roll Back to a Previous Version
-- MAGIC
-- MAGIC **EXAM TIP:** RESTORE reverts a table to a previous state.

-- COMMAND ----------

-- Restore to a specific version
-- RESTORE TABLE delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
-- TO VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VACUUM: Clean Up Old Files
-- MAGIC
-- MAGIC **EXAM TIP:** This is critical. Know exactly what VACUUM does and does not do.
-- MAGIC
-- MAGIC - VACUUM removes files older than the retention period (default: 168 hours / 7 days)
-- MAGIC - After VACUUM, you CANNOT time travel to versions older than the retention period
-- MAGIC - VACUUM does NOT remove the transaction log entries, only the data files
-- MAGIC - You cannot set retention below 168 hours without disabling a safety check

-- COMMAND ----------

-- Dry run: see what VACUUM would delete (does not actually delete)
-- VACUUM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders` DRY RUN

-- COMMAND ----------

-- Actually vacuum (removes files older than 168 hours)
-- VACUUM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`

-- COMMAND ----------

-- To vacuum with a shorter retention (DANGEROUS - disables time travel):
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- VACUUM delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
-- RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Exam Takeaways
-- MAGIC
-- MAGIC 1. `DESCRIBE HISTORY` shows all operations and version numbers
-- MAGIC 2. `VERSION AS OF n` queries a specific version
-- MAGIC 3. `TIMESTAMP AS OF 'datetime'` queries as of a time
-- MAGIC 4. `RESTORE TABLE TO VERSION AS OF n` rolls back
-- MAGIC 5. `VACUUM` removes old files; default retention is 7 days (168 hours)
-- MAGIC 6. After VACUUM, time travel to vacuumed versions FAILS
-- MAGIC 7. VACUUM does NOT delete transaction log entries
-- MAGIC 8. Never set retention below 7 days in production
