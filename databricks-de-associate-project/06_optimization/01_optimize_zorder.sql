-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - OPTIMIZE and Z-ORDER
-- MAGIC
-- MAGIC **Objective:** Compact small files and co-locate data for faster queries.
-- MAGIC
-- MAGIC **EXAM TIP:** Know when and why to use OPTIMIZE and Z-ORDER.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Check Current File Count
-- MAGIC
-- MAGIC **EXAM TIP:** DESCRIBE DETAIL shows the number of files and total size.

-- COMMAND ----------

DESCRIBE DETAIL delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OPTIMIZE: Compact Small Files
-- MAGIC
-- MAGIC **EXAM TIP:**
-- MAGIC - OPTIMIZE merges small files into larger files (target ~1GB per file)
-- MAGIC - Does NOT change the data, only the file layout
-- MAGIC - Safe to run at any time (non-destructive)
-- MAGIC - Readers can continue querying during OPTIMIZE

-- COMMAND ----------

-- Basic OPTIMIZE
OPTIMIZE delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`

-- COMMAND ----------

-- Check file count after optimization (should be fewer, larger files)
DESCRIBE DETAIL delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Z-ORDER: Co-locate Data by Column
-- MAGIC
-- MAGIC **EXAM TIP:**
-- MAGIC - Z-ORDER sorts data within files by the specified columns
-- MAGIC - Best for columns frequently used in WHERE clauses
-- MAGIC - Choose columns with HIGH cardinality (many distinct values)
-- MAGIC - You can Z-ORDER by multiple columns (but effectiveness decreases with each)
-- MAGIC - Always combined with OPTIMIZE (Z-ORDER triggers OPTIMIZE)

-- COMMAND ----------

-- EXAM TIP: This is the most common pattern
OPTIMIZE delta.`dbfs:/FileStore/ecommerce_project/silver/cleaned_orders`
ZORDER BY (order_date, customer_id)

-- COMMAND ----------

-- Z-ORDER the order items table by product_id (common filter column)
OPTIMIZE delta.`dbfs:/FileStore/ecommerce_project/silver/order_items`
ZORDER BY (product_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## When to Z-ORDER by Which Columns
-- MAGIC
-- MAGIC | Column | Cardinality | Good Z-ORDER candidate? |
-- MAGIC |---|---|---|
-- MAGIC | order_date | High (many dates) | Yes |
-- MAGIC | customer_id | High (many customers) | Yes |
-- MAGIC | status | Low (6 values) | No (use partition instead) |
-- MAGIC | region | Low (5 values) | No |
-- MAGIC | product_id | Medium-High | Yes |
-- MAGIC
-- MAGIC **EXAM TIP:** Z-ORDER works best with high-cardinality columns.
-- MAGIC Low-cardinality columns are better handled with partitioning.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Exam Takeaways
-- MAGIC
-- MAGIC 1. `OPTIMIZE table_name` compacts small files
-- MAGIC 2. `OPTIMIZE table_name ZORDER BY (col)` compacts AND co-locates
-- MAGIC 3. Z-ORDER is for high-cardinality filter columns
-- MAGIC 4. OPTIMIZE is non-destructive and safe to run anytime
-- MAGIC 5. Old files remain until VACUUM removes them
-- MAGIC 6. OPTIMIZE does NOT remove data or change query results
