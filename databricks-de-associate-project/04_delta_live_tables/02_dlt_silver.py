# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Silver Layer with Expectations
# MAGIC
# MAGIC **IMPORTANT:** Run as part of a DLT pipeline, not interactively.
# MAGIC
# MAGIC **Exam Topics:** DLT expectations, `dlt.read_stream()`, data quality enforcement

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, explode, initcap, trim, lower, to_date
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Orders
# MAGIC
# MAGIC **EXAM TIP:** Expectations enforce data quality at the Silver layer.
# MAGIC - `expect` = warn (keep record, log violation)
# MAGIC - `expect_or_drop` = remove bad records silently
# MAGIC - `expect_or_fail` = halt pipeline on violation

# COMMAND ----------

@dlt.table(
    name="silver_cleaned_orders",
    comment="Cleaned and validated order data"
)
# EXAM TIP: Multiple expectations can be applied to one table
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
@dlt.expect_or_drop("no_future_dates", "order_date <= current_timestamp()")
@dlt.expect_or_drop("positive_total", "total_amount >= 0")
def silver_cleaned_orders():
    return (
        dlt.read_stream("bronze_raw_orders")
        .withColumn("order_date", to_timestamp("order_date"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("subtotal", col("subtotal").cast("double"))
        .withColumn("tax_amount", col("tax_amount").cast("double"))
        .withColumn("shipping_cost", col("shipping_cost").cast("double"))
        # Flatten shipping address struct
        .withColumn("shipping_street", col("shipping_address.street"))
        .withColumn("shipping_city", col("shipping_address.city"))
        .withColumn("shipping_state", col("shipping_address.state"))
        .withColumn("shipping_zip", col("shipping_address.zip_code"))
        .drop("shipping_address", "items")  # Drop nested fields
        .withColumn("_silver_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Order Items (Exploded)

# COMMAND ----------

@dlt.table(
    name="silver_order_items",
    comment="Individual order line items exploded from orders"
)
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect("valid_product", "product_id IS NOT NULL")
def silver_order_items():
    return (
        dlt.read_stream("bronze_raw_orders")
        .select(
            "order_id",
            "customer_id",
            to_timestamp("order_date").alias("order_date"),
            explode("items").alias("item"),
        )
        .select(
            "order_id",
            "customer_id",
            "order_date",
            col("item.product_id").alias("product_id"),
            col("item.quantity").cast("int").alias("quantity"),
            col("item.unit_price").cast("double").alias("unit_price"),
            col("item.discount_pct").cast("double").alias("discount_pct"),
            col("item.line_total").cast("double").alias("line_total"),
        )
        .withColumn("_silver_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Customers

# COMMAND ----------

@dlt.table(
    name="silver_cleaned_customers",
    comment="Cleaned and standardized customer data"
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email IS NOT NULL")
def silver_cleaned_customers():
    return (
        dlt.read_stream("bronze_raw_customers")
        .withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name", initcap(trim(col("last_name"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("date_of_birth", to_date("date_of_birth"))
        .withColumn("registration_date", to_date("registration_date"))
        .withColumn("_silver_timestamp", current_timestamp())
    )
