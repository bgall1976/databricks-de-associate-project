# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Auto Loader: Ingest Orders (JSON)
# MAGIC
# MAGIC **Objective:** Use Auto Loader to incrementally ingest JSON order files into a Bronze Delta table.
# MAGIC
# MAGIC **Exam Topics:** Auto Loader, structured streaming, Delta Lake, metadata columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

RAW_BASE_PATH = "dbfs:/FileStore/ecommerce_project/raw"
BRONZE_BASE_PATH = "dbfs:/FileStore/ecommerce_project/bronze"
CHECKPOINT_BASE_PATH = "dbfs:/FileStore/ecommerce_project/checkpoints"

ORDERS_SOURCE = f"{RAW_BASE_PATH}/orders"
ORDERS_BRONZE = f"{BRONZE_BASE_PATH}/raw_orders"
ORDERS_CHECKPOINT = f"{CHECKPOINT_BASE_PATH}/orders"
ORDERS_SCHEMA_LOCATION = f"{CHECKPOINT_BASE_PATH}/orders_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Ingestion
# MAGIC
# MAGIC Auto Loader uses `cloudFiles` as the streaming source format.
# MAGIC It automatically tracks which files have been processed via a checkpoint.
# MAGIC
# MAGIC **EXAM TIP:** Auto Loader is the preferred method for ingesting files from cloud storage.
# MAGIC It scales to millions of files and supports schema evolution.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# EXAM TIP: This is the core Auto Loader pattern you need to know
orders_stream = (
    spark.readStream
    .format("cloudFiles")                                    # Auto Loader format
    .option("cloudFiles.format", "json")                     # Source file format
    .option("cloudFiles.schemaLocation", ORDERS_SCHEMA_LOCATION)  # Schema tracking
    .option("cloudFiles.inferColumnTypes", "true")           # Infer types
    .load(ORDERS_SOURCE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata Columns
# MAGIC
# MAGIC Bronze tables should always include metadata for lineage and debugging.
# MAGIC
# MAGIC **EXAM TIP:** `input_file_name()` is a Spark function that returns the source file path.

# COMMAND ----------

orders_bronze = (
    orders_stream
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_input_file_name", input_file_name())
    .withColumn("_datasource", lit("ecommerce_orders"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table
# MAGIC
# MAGIC **EXAM TIP:** Key writeStream options:
# MAGIC - `checkpointLocation`: Required for exactly-once processing
# MAGIC - `trigger(availableNow=True)`: Process all available files, then stop
# MAGIC - `trigger(processingTime="5 minutes")`: Micro-batch every 5 minutes
# MAGIC - No trigger: Continuous processing

# COMMAND ----------

# Write as a Delta table with a checkpoint
# Using availableNow=True to process all current files and stop
# In production, you might use processingTime or continuous mode
query = (
    orders_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", ORDERS_CHECKPOINT)
    .trigger(availableNow=True)
    .start(ORDERS_BRONZE)
)

# Wait for the stream to finish processing
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the Bronze Table

# COMMAND ----------

# Read back the Bronze table and verify
bronze_orders = spark.read.format("delta").load(ORDERS_BRONZE)
print(f"Total records in Bronze: {bronze_orders.count()}")
bronze_orders.printSchema()

# COMMAND ----------

# Check the metadata columns
display(
    bronze_orders.select(
        "order_id",
        "customer_id",
        "order_date",
        "total_amount",
        "_ingest_timestamp",
        "_input_file_name",
    ).limit(10)
)

# COMMAND ----------

# Verify all source files were processed
display(
    bronze_orders
    .groupBy("_input_file_name")
    .count()
    .orderBy("_input_file_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register as a Table (Optional)
# MAGIC
# MAGIC **EXAM TIP:** You can register a Delta path as a table for SQL access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table pointing to the Bronze Delta location
# MAGIC CREATE TABLE IF NOT EXISTS bronze_raw_orders
# MAGIC USING DELTA
# MAGIC LOCATION '${ORDERS_BRONZE}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Test Incremental Processing
# MAGIC
# MAGIC 1. Add a new JSON batch file to the orders source directory
# MAGIC 2. Re-run the Auto Loader stream (the cell with `writeStream`)
# MAGIC 3. Verify that ONLY the new file was processed (check `_input_file_name`)
# MAGIC 4. Confirm the total record count increased by the expected amount
# MAGIC
# MAGIC This demonstrates Auto Loader's incremental processing via checkpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Exam Takeaways
# MAGIC
# MAGIC 1. Auto Loader format is `cloudFiles`, NOT `autoloader`
# MAGIC 2. `cloudFiles.schemaLocation` is required for schema inference/evolution
# MAGIC 3. `checkpointLocation` enables exactly-once processing
# MAGIC 4. `trigger(availableNow=True)` processes all available data then stops
# MAGIC 5. Auto Loader creates a streaming DataFrame → use `writeStream`, not `write`
# MAGIC 6. Always add metadata columns (`_ingest_timestamp`, `input_file_name()`) to Bronze
