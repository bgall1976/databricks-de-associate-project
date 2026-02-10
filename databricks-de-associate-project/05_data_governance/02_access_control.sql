-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Access Control
-- MAGIC
-- MAGIC **Objective:** Practice granting and revoking permissions on Unity Catalog objects.
-- MAGIC
-- MAGIC **EXAM TIP:** Know the GRANT/REVOKE syntax and which privileges exist.
-- MAGIC
-- MAGIC **Note:** You need admin or ownership privileges to run these commands.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Available Privileges
-- MAGIC
-- MAGIC | Privilege | Applies To | Description |
-- MAGIC |---|---|---|
-- MAGIC | SELECT | Table, View | Read data |
-- MAGIC | MODIFY | Table | Insert, update, delete data |
-- MAGIC | CREATE TABLE | Schema | Create tables in schema |
-- MAGIC | CREATE SCHEMA | Catalog | Create schemas in catalog |
-- MAGIC | ALL PRIVILEGES | Any | All available privileges |
-- MAGIC | USAGE | Catalog, Schema | Required to access child objects |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grant Permissions
-- MAGIC
-- MAGIC **EXAM TIP:** USAGE is required on parent objects to access child objects.

-- COMMAND ----------

-- Grant a user the ability to read from silver tables
-- EXAM TIP: Must grant USAGE on catalog AND schema before SELECT on table
GRANT USAGE ON CATALOG ecommerce_project TO `data_analyst@example.com`;
GRANT USAGE ON SCHEMA ecommerce_project.gold TO `data_analyst@example.com`;
GRANT SELECT ON SCHEMA ecommerce_project.gold TO `data_analyst@example.com`;

-- COMMAND ----------

-- Grant a group permissions
-- EXAM TIP: Groups are the recommended way to manage access
GRANT USAGE ON CATALOG ecommerce_project TO `data_engineers`;
GRANT USAGE ON SCHEMA ecommerce_project.silver TO `data_engineers`;
GRANT SELECT, MODIFY ON SCHEMA ecommerce_project.silver TO `data_engineers`;
GRANT CREATE TABLE ON SCHEMA ecommerce_project.silver TO `data_engineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revoke Permissions

-- COMMAND ----------

-- Remove SELECT from a specific table
REVOKE SELECT ON TABLE ecommerce_project.silver.cleaned_customers
FROM `data_analyst@example.com`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View Current Grants

-- COMMAND ----------

-- EXAM TIP: SHOW GRANTS shows who has access to what
SHOW GRANTS ON SCHEMA ecommerce_project.silver;

-- COMMAND ----------

SHOW GRANTS ON TABLE ecommerce_project.silver.cleaned_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Exam Takeaways
-- MAGIC
-- MAGIC 1. USAGE on parent is REQUIRED before any access to children
-- MAGIC 2. `GRANT SELECT ON SCHEMA` applies to all current AND future tables in that schema
-- MAGIC 3. `GRANT SELECT ON TABLE` applies only to that specific table
-- MAGIC 4. Use groups, not individual users, for access management
-- MAGIC 5. `SHOW GRANTS` to audit permissions
-- MAGIC 6. Ownership is the highest privilege and allows managing grants
