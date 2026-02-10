-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 - Dynamic Views for PII Masking
-- MAGIC
-- MAGIC **Objective:** Create views that mask sensitive data based on the requesting user.
-- MAGIC
-- MAGIC **EXAM TIP:** Dynamic views are the Unity Catalog approach to column-level
-- MAGIC and row-level security. This is a heavily tested topic.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Column-Level Security: Mask PII
-- MAGIC
-- MAGIC **EXAM TIP:** `current_user()` returns the email of the user running the query.
-- MAGIC `is_member()` checks if the user belongs to a group.

-- COMMAND ----------

USE CATALOG ecommerce_project;
USE SCHEMA silver;

-- COMMAND ----------

-- Dynamic view that masks email and phone for non-admin users
-- EXAM TIP: This is the exact pattern the exam tests
CREATE OR REPLACE VIEW v_customers_masked AS
SELECT
    customer_id,
    first_name,
    last_name,
    -- Column-level security: mask email for non-admins
    CASE
        WHEN is_member('pii_admins') THEN email
        ELSE CONCAT(LEFT(email, 2), '***@***.com')
    END AS email,
    -- Column-level security: mask phone for non-admins
    CASE
        WHEN is_member('pii_admins') THEN phone
        ELSE CONCAT('***-***-', RIGHT(phone, 4))
    END AS phone,
    city,
    state,
    region,
    loyalty_tier,
    registration_date
FROM cleaned_customers;

-- COMMAND ----------

-- Test the view (what you see depends on your group membership)
SELECT * FROM v_customers_masked LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Row-Level Security: Filter by Region
-- MAGIC
-- MAGIC **EXAM TIP:** Dynamic views can also implement row-level security.

-- COMMAND ----------

-- Only show orders from the user's assigned region
-- Regional managers see their region; admins see everything
CREATE OR REPLACE VIEW v_orders_by_region AS
SELECT o.*
FROM cleaned_orders o
LEFT JOIN cleaned_customers c ON o.customer_id = c.customer_id
WHERE
    is_member('admin_group')
    OR (is_member('northeast_team') AND c.region = 'Northeast')
    OR (is_member('southeast_team') AND c.region = 'Southeast')
    OR (is_member('midwest_team') AND c.region = 'Midwest')
    OR (is_member('southwest_team') AND c.region = 'Southwest')
    OR (is_member('west_team') AND c.region = 'West');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grant Access to Views (Not Underlying Tables)
-- MAGIC
-- MAGIC **EXAM TIP:** Grant SELECT on the VIEW, not the table.
-- MAGIC Users access data through the view, which enforces the masking/filtering.

-- COMMAND ----------

-- Analysts get access to the masked view, NOT the raw table
GRANT SELECT ON VIEW v_customers_masked TO `data_analysts`;

-- Do NOT grant access to the underlying table
-- REVOKE SELECT ON TABLE cleaned_customers FROM `data_analysts`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify: Check What current_user() Returns

-- COMMAND ----------

-- Useful for testing dynamic view logic
SELECT current_user() AS my_user;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Key Exam Takeaways
-- MAGIC
-- MAGIC 1. `current_user()` returns the current user's email
-- MAGIC 2. `is_member('group_name')` checks group membership
-- MAGIC 3. Column-level security: use CASE WHEN in the SELECT clause
-- MAGIC 4. Row-level security: use conditions in the WHERE clause
-- MAGIC 5. Grant SELECT on the VIEW, not the underlying table
-- MAGIC 6. Dynamic views are the recommended approach for PII masking in Unity Catalog
