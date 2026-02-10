# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Clean Orders
# MAGIC
# MAGIC **Objective:** Transform raw Bronze orders into clean Silver records.
# MAGIC
# MAGIC **Exam Topics:** Filtering, type casting, null handling, explode, withColumn

# COMMAND ----------

BRONZE_BASE_PATH = "dbfs:/FileStore/ecommerce_project/bronze"
SILVER_BASE_PATH = "dbfs:/FileStore/ecommerce_project/silver"

ORDERS_BRONZE = f"{BRONZE_BASE_PATH}/raw_orders"
ORDERS_SILVER = f"{SILVER_BASE_PATH}/cleaned_orders"
ORDER_ITEMS_SILVER = f"{SILVER_BASE_PATH}/order_items"

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, explode, row_number, current_timestamp,
    when, abs as spark_abs
)
from pyspark.sql.window import Window

# Read Bronze orders
bronze_orders = spark.read.format("delta").load(ORDERS_BRONZE)
print(f"Bronze records: {bronze_orders.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Remove Records with Null Required Fields
# MAGIC
# MAGIC **EXAM TIP:** Silver layer enforces data quality. Null primary keys and dates get filtered out.

# COMMAND ----------

# Filter out records with null required fields
orders_not_null = bronze_orders.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("order_date").isNotNull()
)

removed_count = bronze_orders.count() - orders_not_null.count()
print(f"Removed {removed_count} records with null required fields")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplicate
# MAGIC
# MAGIC **EXAM TIP:** Use `row_number()` with a Window function to pick the most recent record per key.

# COMMAND ----------

# Deduplicate: keep the latest record per order_id based on ingest timestamp
window_spec = Window.partitionBy("order_id").orderBy(col("_ingest_timestamp").desc())

orders_deduped = (
    orders_not_null
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

dedup_removed = orders_not_null.count() - orders_deduped.count()
print(f"Removed {dedup_removed} duplicate records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cast Data Types and Standardize
# MAGIC
# MAGIC **EXAM TIP:** Use `to_timestamp()` for datetime conversions, `col().cast()` for type changes.

# COMMAND ----------

orders_typed = (
    orders_deduped
    .withColumn("order_date", to_timestamp("order_date"))
    .withColumn("total_amount", col("total_amount").cast("double"))
    .withColumn("subtotal", col("subtotal").cast("double"))
    .withColumn("tax_amount", col("tax_amount").cast("double"))
    .withColumn("shipping_cost", col("shipping_cost").cast("double"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Filter Invalid Records
# MAGIC
# MAGIC Remove orders with future dates and negative totals.

# COMMAND ----------

from pyspark.sql.functions import current_date

orders_valid = orders_typed.filter(
    (col("order_date") <= current_timestamp()) &  # No future dates
    (col("total_amount") >= 0)                     # No negative totals
)

filtered_count = orders_typed.count() - orders_valid.count()
print(f"Filtered {filtered_count} invalid records (future dates or negative totals)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Extract Shipping Address (Flatten Struct)
# MAGIC
# MAGIC **EXAM TIP:** Access STRUCT fields with dot notation and promote them to top-level columns.

# COMMAND ----------

orders_flat = (
    orders_valid
    .withColumn("shipping_street", col("shipping_address.street"))
    .withColumn("shipping_city", col("shipping_address.city"))
    .withColumn("shipping_state", col("shipping_address.state"))
    .withColumn("shipping_zip", col("shipping_address.zip_code"))
    .drop("shipping_address")  # Remove the nested struct
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Select Final Silver Columns and Write

# COMMAND ----------

silver_orders = (
    orders_flat
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "payment_method",
        "subtotal",
        "tax_amount",
        "shipping_cost",
        "total_amount",
        "shipping_street",
        "shipping_city",
        "shipping_state",
        "shipping_zip",
        "_ingest_timestamp",
        "_input_file_name",
    )
    .withColumn("_silver_timestamp", current_timestamp())
)

# Write to Silver as Delta
silver_orders.write.format("delta").mode("overwrite").save(ORDERS_SILVER)
print(f"Silver orders written: {silver_orders.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Explode Order Items into Separate Table
# MAGIC
# MAGIC **EXAM TIP:** `explode()` converts an array into rows. This is a heavily tested topic.

# COMMAND ----------

# Explode items array from the valid (pre-flattened) orders
order_items = (
    orders_valid
    .select(
        "order_id",
        "customer_id",
        "order_date",
        explode("items").alias("item"),
    )
    .select(
        "order_id",
        "customer_id",
        "order_date",
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        col("item.discount_pct").alias("discount_pct"),
        col("item.line_total").alias("line_total"),
    )
    # Filter out negative quantities (bad data)
    .filter(col("quantity") > 0)
    .withColumn("_silver_timestamp", current_timestamp())
)

order_items.write.format("delta").mode("overwrite").save(ORDER_ITEMS_SILVER)
print(f"Silver order items written: {order_items.count()}")

# COMMAND ----------

# Verify
display(spark.read.format("delta").load(ORDERS_SILVER).limit(5))

# COMMAND ----------

display(spark.read.format("delta").load(ORDER_ITEMS_SILVER).limit(10))
