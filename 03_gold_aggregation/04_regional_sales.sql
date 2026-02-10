-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Regional Sales
-- MAGIC
-- MAGIC **Objective:** Aggregate sales by geographic region.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_regional_sales AS
SELECT
    c.region,
    o.shipping_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(SUM(o.total_amount), 2) AS total_revenue,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value
FROM silver_cleaned_orders o
LEFT JOIN silver_cleaned_customers c ON o.customer_id = c.customer_id
WHERE o.status NOT IN ('cancelled', 'refunded')
GROUP BY c.region, o.shipping_state
ORDER BY total_revenue DESC

-- COMMAND ----------

-- Revenue by region (summary)
SELECT
    region,
    SUM(total_orders) AS total_orders,
    SUM(unique_customers) AS unique_customers,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM gold_regional_sales
WHERE region IS NOT NULL
GROUP BY region
ORDER BY total_revenue DESC

-- COMMAND ----------

-- Top 5 states by revenue
SELECT shipping_state, total_revenue, total_orders
FROM gold_regional_sales
ORDER BY total_revenue DESC
LIMIT 5
