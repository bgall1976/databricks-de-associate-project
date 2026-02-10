# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - MERGE / Upsert Patterns
# MAGIC
# MAGIC **Objective:** Learn Delta Lake MERGE for handling incremental updates.
# MAGIC
# MAGIC **Exam Topics:** MERGE INTO, upserts, SCD Type 1, conditional updates
# MAGIC
# MAGIC **EXAM TIP:** MERGE is one of the most heavily tested topics. Know the syntax cold.

# COMMAND ----------

SILVER_BASE_PATH = "dbfs:/FileStore/ecommerce_project/silver"
ORDERS_SILVER = f"{SILVER_BASE_PATH}/cleaned_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 1: Basic Upsert with SQL
# MAGIC
# MAGIC **EXAM TIP:** This is the most common MERGE pattern on the exam.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, register the Silver table for SQL access
# MAGIC CREATE TABLE IF NOT EXISTS silver_orders
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/ecommerce_project/silver/cleaned_orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXAM TIP: Know this syntax pattern exactly
# MAGIC -- This example shows how you would merge new/updated orders
# MAGIC
# MAGIC -- MERGE INTO silver_orders AS target
# MAGIC -- USING new_orders AS source
# MAGIC -- ON target.order_id = source.order_id
# MAGIC -- WHEN MATCHED THEN
# MAGIC --   UPDATE SET *              -- Update all columns
# MAGIC -- WHEN NOT MATCHED THEN
# MAGIC --   INSERT *                  -- Insert all columns
# MAGIC
# MAGIC -- The * syntax means "all columns" - both tables must have matching schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 2: Conditional MERGE
# MAGIC
# MAGIC **EXAM TIP:** You can add conditions to WHEN MATCHED and WHEN NOT MATCHED.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only update if the source record is newer
# MAGIC -- MERGE INTO silver_orders AS target
# MAGIC -- USING new_orders AS source
# MAGIC -- ON target.order_id = source.order_id
# MAGIC -- WHEN MATCHED AND source.order_date > target.order_date THEN
# MAGIC --   UPDATE SET
# MAGIC --     target.status = source.status,
# MAGIC --     target.total_amount = source.total_amount,
# MAGIC --     target._silver_timestamp = current_timestamp()
# MAGIC -- WHEN NOT MATCHED THEN
# MAGIC --   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 3: MERGE with PySpark (Delta Lake API)
# MAGIC
# MAGIC **EXAM TIP:** Know both SQL and Python MERGE syntax.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Reference the existing Silver Delta table
silver_table = DeltaTable.forPath(spark, ORDERS_SILVER)

# Simulate incoming updates: take 100 existing orders and change their status
updates = (
    spark.read.format("delta").load(ORDERS_SILVER)
    .limit(100)
    .withColumn("status", lit("updated_status"))
)

# EXAM TIP: Python MERGE syntax
silver_table.alias("target").merge(
    updates.alias("source"),
    "target.order_id = source.order_id"  # Match condition
).whenMatchedUpdateAll(                   # Update ALL columns when matched
).whenNotMatchedInsertAll(                # Insert ALL columns when not matched
).execute()

print("MERGE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 4: Deduplication with MERGE
# MAGIC
# MAGIC Use MERGE to insert only new records (skip duplicates).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert-only MERGE (skip existing records)
# MAGIC -- MERGE INTO silver_orders AS target
# MAGIC -- USING new_orders AS source
# MAGIC -- ON target.order_id = source.order_id
# MAGIC -- WHEN NOT MATCHED THEN
# MAGIC --   INSERT *
# MAGIC -- Note: No WHEN MATCHED clause = existing records are untouched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 5: DELETE with MERGE
# MAGIC
# MAGIC **EXAM TIP:** MERGE can also delete records.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Soft delete: mark records as deleted instead of removing them
# MAGIC -- MERGE INTO silver_orders AS target
# MAGIC -- USING deleted_orders AS source
# MAGIC -- ON target.order_id = source.order_id
# MAGIC -- WHEN MATCHED THEN
# MAGIC --   DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Exam Takeaways
# MAGIC
# MAGIC 1. `MERGE INTO target USING source ON condition` is the base syntax
# MAGIC 2. `WHEN MATCHED THEN UPDATE SET *` updates all columns
# MAGIC 3. `WHEN NOT MATCHED THEN INSERT *` inserts all columns
# MAGIC 4. You can add conditions: `WHEN MATCHED AND condition THEN ...`
# MAGIC 5. MERGE can UPDATE, INSERT, and DELETE in a single statement
# MAGIC 6. In Python: use `DeltaTable.forPath()` then `.merge().whenMatched...().execute()`
# MAGIC 7. MERGE is the correct answer for upserts, deduplication, and SCD Type 1
