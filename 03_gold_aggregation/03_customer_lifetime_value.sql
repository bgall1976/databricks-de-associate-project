-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 - Customer Lifetime Value
-- MAGIC
-- MAGIC **Objective:** Calculate customer lifetime value from order history.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_customer_lifetime_value AS
SELECT
    o.customer_id,
    c.first_name,
    c.last_name,
    c.region,
    c.loyalty_tier,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.total_amount), 2) AS lifetime_spend,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) AS customer_tenure_days
FROM silver_cleaned_orders o
LEFT JOIN silver_cleaned_customers c ON o.customer_id = c.customer_id
WHERE o.status NOT IN ('cancelled', 'refunded')
GROUP BY o.customer_id, c.first_name, c.last_name, c.region, c.loyalty_tier
ORDER BY lifetime_spend DESC

-- COMMAND ----------

SELECT * FROM gold_customer_lifetime_value LIMIT 20

-- COMMAND ----------

-- CLV by loyalty tier
SELECT
    loyalty_tier,
    COUNT(*) AS customer_count,
    ROUND(AVG(lifetime_spend), 2) AS avg_lifetime_spend,
    ROUND(AVG(total_orders), 1) AS avg_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM gold_customer_lifetime_value
GROUP BY loyalty_tier
ORDER BY avg_lifetime_spend DESC
