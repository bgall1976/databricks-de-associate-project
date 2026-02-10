-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Unity Catalog Setup
-- MAGIC
-- MAGIC **Objective:** Create the three-level namespace for our project.
-- MAGIC
-- MAGIC **EXAM TIP:** Know the hierarchy: CATALOG > SCHEMA > TABLE/VIEW
-- MAGIC
-- MAGIC **Requires:** Trial or paid workspace with Unity Catalog enabled.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Create the Catalog
-- MAGIC
-- MAGIC A catalog is the top-level container. Think of it as a database server.

-- COMMAND ----------

-- EXAM TIP: Catalogs are the top level of the Unity Catalog namespace
CREATE CATALOG IF NOT EXISTS ecommerce_project;

-- Set as default for this session
USE CATALOG ecommerce_project;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Create Schemas
-- MAGIC
-- MAGIC Schemas organize tables within a catalog. We create one per medallion layer.

-- COMMAND ----------

-- EXAM TIP: Schemas are the second level of the namespace
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingested data';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned and conformed data';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-level aggregates';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Create Managed Tables
-- MAGIC
-- MAGIC **EXAM TIP:** Managed tables have their data managed by Unity Catalog.
-- MAGIC When you DROP a managed table, the data is deleted.
-- MAGIC External tables point to data you manage yourself (data persists after DROP).

-- COMMAND ----------

USE SCHEMA silver;

-- EXAM TIP: Managed table (no LOCATION clause)
CREATE TABLE IF NOT EXISTS cleaned_orders (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    status STRING,
    payment_method STRING,
    subtotal DOUBLE,
    tax_amount DOUBLE,
    shipping_cost DOUBLE,
    total_amount DOUBLE,
    shipping_street STRING,
    shipping_city STRING,
    shipping_state STRING,
    shipping_zip STRING,
    _ingest_timestamp TIMESTAMP,
    _silver_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Cleaned order data with quality enforcement applied';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS cleaned_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    region STRING,
    date_of_birth DATE,
    registration_date DATE,
    loyalty_tier STRING,
    _silver_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Cleaned customer data with standardized formats';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Verify the Namespace

-- COMMAND ----------

-- List all schemas in the catalog
SHOW SCHEMAS IN ecommerce_project;

-- COMMAND ----------

-- List all tables in the silver schema
SHOW TABLES IN ecommerce_project.silver;

-- COMMAND ----------

-- Describe a table to see its full path
DESCRIBE TABLE EXTENDED ecommerce_project.silver.cleaned_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Exam Takeaways
-- MAGIC
-- MAGIC 1. Three-level namespace: `catalog.schema.table`
-- MAGIC 2. `CREATE CATALOG` → `CREATE SCHEMA` → `CREATE TABLE`
-- MAGIC 3. Managed tables: Unity Catalog controls the storage (no LOCATION)
-- MAGIC 4. External tables: You control storage (specify LOCATION)
-- MAGIC 5. DROP managed table = data deleted. DROP external table = data preserved.
-- MAGIC 6. `USE CATALOG` and `USE SCHEMA` set defaults for the session
