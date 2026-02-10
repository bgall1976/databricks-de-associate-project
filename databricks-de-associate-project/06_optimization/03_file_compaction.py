# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - File Compaction Strategies
# MAGIC
# MAGIC **Objective:** Understand file management in Delta Lake.
# MAGIC
# MAGIC **Exam Topics:** Small file problem, auto-compaction, optimized writes

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Small File Problem
# MAGIC
# MAGIC Streaming workloads and frequent appends create many small files.
# MAGIC Small files = slow reads because Spark must open each file individually.
# MAGIC
# MAGIC **Solutions:**
# MAGIC 1. OPTIMIZE (manual compaction)
# MAGIC 2. Auto Compaction (automatic, triggered after writes)
# MAGIC 3. Optimized Writes (automatic, during writes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Compaction
# MAGIC
# MAGIC **EXAM TIP:** Auto compaction runs OPTIMIZE automatically after each write.

# COMMAND ----------

# Enable auto compaction on a table
spark.sql("""
    ALTER TABLE delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
    SET TBLPROPERTIES (
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimized Writes
# MAGIC
# MAGIC **EXAM TIP:** Optimized writes reduce the number of files created during writes.
# MAGIC Enabled by default on Databricks Runtime 9.1+.

# COMMAND ----------

# Enable optimized writes on a table
spark.sql("""
    ALTER TABLE delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the properties were set
# MAGIC SHOW TBLPROPERTIES delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Exam Takeaways
# MAGIC
# MAGIC 1. Small files = performance problem
# MAGIC 2. OPTIMIZE = manual compaction (you run it)
# MAGIC 3. Auto Compaction = runs OPTIMIZE after writes (table property)
# MAGIC 4. Optimized Writes = reduces file count during writes (table property)
# MAGIC 5. Both auto-features are set via `ALTER TABLE SET TBLPROPERTIES`
