# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Auto Loader: Ingest Customers (CSV)
# MAGIC
# MAGIC **Objective:** Ingest CSV customer data using Auto Loader.
# MAGIC
# MAGIC **Exam Topics:** Auto Loader with CSV, schema hints, header handling

# COMMAND ----------

RAW_BASE_PATH = "dbfs:/FileStore/ecommerce_project/raw"
BRONZE_BASE_PATH = "dbfs:/FileStore/ecommerce_project/bronze"
CHECKPOINT_BASE_PATH = "dbfs:/FileStore/ecommerce_project/checkpoints"

CUSTOMERS_SOURCE = f"{RAW_BASE_PATH}/customers"
CUSTOMERS_BRONZE = f"{BRONZE_BASE_PATH}/raw_customers"
CUSTOMERS_CHECKPOINT = f"{CHECKPOINT_BASE_PATH}/customers"
CUSTOMERS_SCHEMA_LOCATION = f"{CHECKPOINT_BASE_PATH}/customers_schema"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# EXAM TIP: For CSV files, you need additional cloudFiles options
customers_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", CUSTOMERS_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")                              # CSV-specific: first row is header
    .option("cloudFiles.schemaHints", "customer_id STRING, zip_code STRING")  # Force types
    .load(CUSTOMERS_SOURCE)
)

# Add metadata columns
customers_bronze = (
    customers_stream
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_input_file_name", input_file_name())
    .withColumn("_datasource", lit("ecommerce_customers"))
)

# COMMAND ----------

# Write to Bronze
query = (
    customers_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CUSTOMERS_CHECKPOINT)
    .trigger(availableNow=True)
    .start(CUSTOMERS_BRONZE)
)

query.awaitTermination()

# COMMAND ----------

# Validate
bronze_customers = spark.read.format("delta").load(CUSTOMERS_BRONZE)
print(f"Total customer records in Bronze: {bronze_customers.count()}")
display(bronze_customers.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Differences: JSON vs CSV Auto Loader
# MAGIC
# MAGIC | Setting | JSON | CSV |
# MAGIC |---|---|---|
# MAGIC | `cloudFiles.format` | `json` | `csv` |
# MAGIC | Header handling | N/A | `header=true` required |
# MAGIC | Schema hints | Rarely needed | Useful for zip codes, IDs |
# MAGIC | Type inference | Good by default | Can misinterpret numeric strings |
# MAGIC
# MAGIC **EXAM TIP:** `schemaHints` lets you override inferred types without defining a full schema.
