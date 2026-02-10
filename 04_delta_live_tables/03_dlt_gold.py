# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Gold Layer - Materialized Views
# MAGIC
# MAGIC **IMPORTANT:** Run as part of a DLT pipeline, not interactively.
# MAGIC
# MAGIC **EXAM TIP:** Gold DLT tables use `dlt.read()` (not `dlt.read_stream()`)
# MAGIC because they perform full recomputation of aggregates.
# MAGIC This makes them materialized views, not streaming tables.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max, round as spark_round, countDistinct, to_date

# COMMAND ----------

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue metrics excluding cancelled and refunded orders"
)
def gold_daily_revenue():
    orders = dlt.read("silver_cleaned_orders")  # dlt.read() = materialized view
    return (
        orders
        .filter(~col("status").isin("cancelled", "refunded"))
        .withColumn("order_day", to_date("order_date"))
        .groupBy("order_day")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        )
        .orderBy(col("order_day").desc())
    )

# COMMAND ----------

@dlt.table(
    name="gold_top_products",
    comment="Product performance metrics"
)
def gold_top_products():
    items = dlt.read("silver_order_items")
    orders = dlt.read("silver_cleaned_orders")

    valid_items = (
        items.join(orders.select("order_id", "status"), "order_id", "left")
        .filter(~col("status").isin("cancelled", "refunded"))
    )

    return (
        valid_items
        .groupBy("product_id")
        .agg(
            countDistinct("order_id").alias("order_count"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_round(spark_sum("line_total"), 2).alias("total_revenue"),
            spark_round(avg("unit_price"), 2).alias("avg_selling_price"),
        )
        .orderBy(col("total_revenue").desc())
    )

# COMMAND ----------

@dlt.table(
    name="gold_regional_sales",
    comment="Sales aggregated by geographic region"
)
def gold_regional_sales():
    orders = dlt.read("silver_cleaned_orders")
    customers = dlt.read("silver_cleaned_customers")

    return (
        orders
        .filter(~col("status").isin("cancelled", "refunded"))
        .join(customers.select("customer_id", "region"), "customer_id", "left")
        .groupBy("region", "shipping_state")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        )
        .orderBy(col("total_revenue").desc())
    )
