# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Layer
# MAGIC
# MAGIC **IMPORTANT:** This notebook must be run as part of a DLT pipeline, not interactively.
# MAGIC
# MAGIC **Exam Topics:** `@dlt.table`, `dlt.read_stream()`, `cloudFiles`, streaming tables

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# Configuration - update these for your environment
RAW_ORDERS_PATH = "dbfs:/FileStore/ecommerce_project/raw/orders"
RAW_CUSTOMERS_PATH = "dbfs:/FileStore/ecommerce_project/raw/customers"
RAW_PRODUCTS_PATH = "dbfs:/FileStore/ecommerce_project/raw/products"

# COMMAND ----------

# EXAM TIP: @dlt.table defines a DLT table
# Using spark.readStream with cloudFiles makes this a STREAMING table
# DLT manages the checkpoint automatically (you don't set checkpointLocation)

@dlt.table(
    name="bronze_raw_orders",
    comment="Raw order data ingested from JSON files via Auto Loader"
)
def bronze_raw_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_ORDERS_PATH)
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_input_file_name", input_file_name())
        .withColumn("_datasource", lit("ecommerce_orders"))
    )

# COMMAND ----------

@dlt.table(
    name="bronze_raw_customers",
    comment="Raw customer data ingested from CSV files"
)
def bronze_raw_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(RAW_CUSTOMERS_PATH)
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_input_file_name", input_file_name())
    )

# COMMAND ----------

@dlt.table(
    name="bronze_raw_products",
    comment="Raw product catalog ingested from CSV files"
)
def bronze_raw_products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(RAW_PRODUCTS_PATH)
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_input_file_name", input_file_name())
    )
