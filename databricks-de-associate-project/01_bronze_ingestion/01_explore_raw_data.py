# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Explore Raw Data
# MAGIC
# MAGIC **Objective:** Understand the structure of raw data before ingesting into Bronze.
# MAGIC
# MAGIC **Exam Topics:** Extracting data from files, schema definition, complex data types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update these paths to match your workspace setup.

# COMMAND ----------

# Base path - update for your environment
# Community Edition: "dbfs:/FileStore/ecommerce_project/raw"
# Trial workspace: your cloud storage path
RAW_BASE_PATH = "dbfs:/FileStore/ecommerce_project/raw"

ORDERS_PATH = f"{RAW_BASE_PATH}/orders"
CUSTOMERS_PATH = f"{RAW_BASE_PATH}/customers"
PRODUCTS_PATH = f"{RAW_BASE_PATH}/products"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Order Data (JSON)
# MAGIC Orders are stored as newline-delimited JSON with nested structures.

# COMMAND ----------

# List files in the orders directory
display(dbutils.fs.ls(ORDERS_PATH))

# COMMAND ----------

# Read a single order file to understand the schema
# EXAM TIP: spark.read.json() can read a single file or a directory
orders_sample = spark.read.json(f"{ORDERS_PATH}/")
orders_sample.printSchema()

# COMMAND ----------

# EXAM TIP: Understand how to work with complex types
# The 'items' field is an ARRAY of STRUCTS
# The 'shipping_address' field is a STRUCT
display(orders_sample.limit(5))

# COMMAND ----------

# EXAM TIP: Access nested fields using dot notation (structs) and explode (arrays)
from pyspark.sql.functions import col, explode

# Access struct fields with dot notation
orders_sample.select(
    "order_id",
    "shipping_address.city",      # Dot notation for struct fields
    "shipping_address.state",
).show(5)

# COMMAND ----------

# Explode array fields to flatten
# EXAM TIP: explode() creates one row per array element
orders_exploded = orders_sample.select(
    "order_id",
    explode("items").alias("item"),  # Each item becomes its own row
)

# Then access the struct fields within the exploded column
orders_exploded.select(
    "order_id",
    "item.product_id",
    "item.quantity",
    "item.unit_price",
).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Customer Data (CSV)

# COMMAND ----------

customers_sample = spark.read.option("header", "true").csv(f"{CUSTOMERS_PATH}/")
customers_sample.printSchema()

# COMMAND ----------

# Note: CSV reads everything as strings by default
# EXAM TIP: Use inferSchema=true to auto-detect types, or define schema manually
customers_typed = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{CUSTOMERS_PATH}/")
)
customers_typed.printSchema()

# COMMAND ----------

display(customers_typed.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Product Data (CSV)

# COMMAND ----------

products_sample = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{PRODUCTS_PATH}/")
)
products_sample.printSchema()

# COMMAND ----------

display(products_sample.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Quick Check
# MAGIC Identify issues before building the ingestion pipeline.

# COMMAND ----------

# Check for nulls in critical order fields
from pyspark.sql.functions import count, when, isnull

orders_sample.select(
    count("*").alias("total_orders"),
    count(when(isnull("order_id"), True)).alias("null_order_id"),
    count(when(isnull("customer_id"), True)).alias("null_customer_id"),
    count(when(isnull("order_date"), True)).alias("null_order_date"),
    count(when(isnull("total_amount"), True)).alias("null_total_amount"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways for the Exam
# MAGIC
# MAGIC 1. `spark.read.json()` automatically infers schema for JSON files
# MAGIC 2. CSV files need `header=true` and `inferSchema=true` for proper reading
# MAGIC 3. Nested JSON creates STRUCT and ARRAY types in Spark
# MAGIC 4. Use dot notation for STRUCT access, `explode()` for ARRAY access
# MAGIC 5. Always check data quality before building pipelines
