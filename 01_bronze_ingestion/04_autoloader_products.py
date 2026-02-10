# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Auto Loader: Ingest Products (CSV)
# MAGIC
# MAGIC **Objective:** Ingest product catalog using Auto Loader.

# COMMAND ----------

RAW_BASE_PATH = "dbfs:/FileStore/ecommerce_project/raw"
BRONZE_BASE_PATH = "dbfs:/FileStore/ecommerce_project/bronze"
CHECKPOINT_BASE_PATH = "dbfs:/FileStore/ecommerce_project/checkpoints"

PRODUCTS_SOURCE = f"{RAW_BASE_PATH}/products"
PRODUCTS_BRONZE = f"{BRONZE_BASE_PATH}/raw_products"
PRODUCTS_CHECKPOINT = f"{CHECKPOINT_BASE_PATH}/products"
PRODUCTS_SCHEMA_LOCATION = f"{CHECKPOINT_BASE_PATH}/products_schema"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

products_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", PRODUCTS_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(PRODUCTS_SOURCE)
)

products_bronze = (
    products_stream
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_input_file_name", input_file_name())
    .withColumn("_datasource", lit("ecommerce_products"))
)

# COMMAND ----------

query = (
    products_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", PRODUCTS_CHECKPOINT)
    .trigger(availableNow=True)
    .start(PRODUCTS_BRONZE)
)

query.awaitTermination()

# COMMAND ----------

bronze_products = spark.read.format("delta").load(PRODUCTS_BRONZE)
print(f"Total product records in Bronze: {bronze_products.count()}")
display(bronze_products.limit(5))
