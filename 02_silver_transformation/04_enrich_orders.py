# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Enrich Orders
# MAGIC
# MAGIC **Objective:** Join orders with customers and products to create an enriched Silver table.
# MAGIC
# MAGIC **Exam Topics:** Joins, CTAS, SQL UDFs

# COMMAND ----------

SILVER_BASE_PATH = "dbfs:/FileStore/ecommerce_project/silver"

orders = spark.read.format("delta").load(f"{SILVER_BASE_PATH}/cleaned_orders")
customers = spark.read.format("delta").load(f"{SILVER_BASE_PATH}/cleaned_customers")
order_items = spark.read.format("delta").load(f"{SILVER_BASE_PATH}/order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Enriched Orders
# MAGIC
# MAGIC **EXAM TIP:** Joins in Spark SQL and DataFrames produce the same result.
# MAGIC The exam tests both syntaxes.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# Join orders with customer information
enriched_orders = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"  # Left join: keep all orders even if customer is missing
    )
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.order_date"),
        col("o.status"),
        col("o.payment_method"),
        col("o.total_amount"),
        col("o.shipping_state"),
        col("c.first_name"),
        col("c.last_name"),
        col("c.email"),
        col("c.region"),
        col("c.loyalty_tier"),
    )
    .withColumn("_silver_timestamp", current_timestamp())
)

enriched_orders.write.format("delta").mode("overwrite").save(
    f"{SILVER_BASE_PATH}/enriched_orders"
)

print(f"Enriched orders: {enriched_orders.count()}")
display(enriched_orders.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same Join in SQL (CTAS Pattern)
# MAGIC
# MAGIC **EXAM TIP:** `CREATE TABLE AS SELECT (CTAS)` is the SQL way to create a Delta table from a query.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register Silver tables for SQL access
# MAGIC CREATE TABLE IF NOT EXISTS silver_cleaned_orders
# MAGIC USING DELTA LOCATION 'dbfs:/FileStore/ecommerce_project/silver/cleaned_orders';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_cleaned_customers
# MAGIC USING DELTA LOCATION 'dbfs:/FileStore/ecommerce_project/silver/cleaned_customers';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_order_items
# MAGIC USING DELTA LOCATION 'dbfs:/FileStore/ecommerce_project/silver/order_items';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXAM TIP: CTAS creates a new Delta table from a query result
# MAGIC CREATE OR REPLACE TABLE silver_enriched_items AS
# MAGIC SELECT
# MAGIC     oi.order_id,
# MAGIC     oi.product_id,
# MAGIC     oi.quantity,
# MAGIC     oi.unit_price,
# MAGIC     oi.line_total,
# MAGIC     o.customer_id,
# MAGIC     o.order_date,
# MAGIC     o.status,
# MAGIC     o.shipping_state,
# MAGIC     c.region,
# MAGIC     c.loyalty_tier
# MAGIC FROM silver_order_items oi
# MAGIC LEFT JOIN silver_cleaned_orders o ON oi.order_id = o.order_id
# MAGIC LEFT JOIN silver_cleaned_customers c ON o.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a SQL UDF
# MAGIC
# MAGIC **EXAM TIP:** SQL UDFs are testable. Know how to CREATE FUNCTION.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a UDF to categorize order size
# MAGIC CREATE OR REPLACE FUNCTION categorize_order(amount DOUBLE)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC     WHEN amount < 50 THEN 'Small'
# MAGIC     WHEN amount < 150 THEN 'Medium'
# MAGIC     WHEN amount < 500 THEN 'Large'
# MAGIC     ELSE 'Enterprise'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the UDF
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     total_amount,
# MAGIC     categorize_order(total_amount) AS order_category
# MAGIC FROM silver_cleaned_orders
# MAGIC LIMIT 10
