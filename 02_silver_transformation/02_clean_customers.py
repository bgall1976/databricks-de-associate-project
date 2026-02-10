# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Clean Customers
# MAGIC
# MAGIC **Objective:** Clean and standardize customer data for the Silver layer.

# COMMAND ----------

BRONZE_BASE_PATH = "dbfs:/FileStore/ecommerce_project/bronze"
SILVER_BASE_PATH = "dbfs:/FileStore/ecommerce_project/silver"

CUSTOMERS_BRONZE = f"{BRONZE_BASE_PATH}/raw_customers"
CUSTOMERS_SILVER = f"{SILVER_BASE_PATH}/cleaned_customers"

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, initcap, to_date, current_timestamp, row_number
)
from pyspark.sql.window import Window

bronze_customers = spark.read.format("delta").load(CUSTOMERS_BRONZE)
print(f"Bronze customer records: {bronze_customers.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and Standardize

# COMMAND ----------

# Deduplicate by customer_id
window_spec = Window.partitionBy("customer_id").orderBy(col("_ingest_timestamp").desc())

silver_customers = (
    bronze_customers
    # Deduplicate
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
    # Standardize names
    .withColumn("first_name", initcap(trim(col("first_name"))))
    .withColumn("last_name", initcap(trim(col("last_name"))))
    # Standardize email
    .withColumn("email", lower(trim(col("email"))))
    # Cast dates
    .withColumn("date_of_birth", to_date("date_of_birth"))
    .withColumn("registration_date", to_date("registration_date"))
    # Add Silver metadata
    .withColumn("_silver_timestamp", current_timestamp())
    # Select final columns
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "zip_code",
        "region",
        "date_of_birth",
        "registration_date",
        "loyalty_tier",
        "_silver_timestamp",
    )
)

# COMMAND ----------

silver_customers.write.format("delta").mode("overwrite").save(CUSTOMERS_SILVER)
print(f"Silver customers written: {silver_customers.count()}")
display(silver_customers.limit(5))
