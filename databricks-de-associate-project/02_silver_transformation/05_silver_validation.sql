-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Silver Layer Validation

-- COMMAND ----------

-- Record counts
SELECT 'cleaned_orders' AS table_name, COUNT(*) AS records FROM silver_cleaned_orders
UNION ALL
SELECT 'cleaned_customers', COUNT(*) FROM silver_cleaned_customers
UNION ALL
SELECT 'order_items', COUNT(*) FROM silver_order_items

-- COMMAND ----------

-- Verify no null primary keys in Silver (should all be 0)
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_dates
FROM silver_cleaned_orders

-- COMMAND ----------

-- Verify no duplicate order_ids in Silver (should be 0)
SELECT COUNT(*) AS duplicate_count
FROM (
    SELECT order_id, COUNT(*) AS cnt
    FROM silver_cleaned_orders
    GROUP BY order_id
    HAVING cnt > 1
)

-- COMMAND ----------

-- Verify no negative quantities in order items (should be 0)
SELECT COUNT(*) AS negative_quantities
FROM silver_order_items
WHERE quantity <= 0

-- COMMAND ----------

-- Verify no future dates (should be 0)
SELECT COUNT(*) AS future_dates
FROM silver_cleaned_orders
WHERE order_date > current_timestamp()
