-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Top Products
-- MAGIC
-- MAGIC **Objective:** Identify best-selling products by revenue and quantity.
-- MAGIC
-- MAGIC **Exam Topics:** GROUP BY, HAVING, subqueries, window functions

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_top_products AS
SELECT
    oi.product_id,
    COUNT(DISTINCT oi.order_id) AS order_count,
    SUM(oi.quantity) AS total_quantity_sold,
    ROUND(SUM(oi.line_total), 2) AS total_revenue,
    ROUND(AVG(oi.unit_price), 2) AS avg_selling_price,
    ROUND(AVG(oi.discount_pct), 4) AS avg_discount
FROM silver_order_items oi
LEFT JOIN silver_cleaned_orders o ON oi.order_id = o.order_id
WHERE o.status NOT IN ('cancelled', 'refunded')
GROUP BY oi.product_id
ORDER BY total_revenue DESC

-- COMMAND ----------

-- Top 10 products by revenue
SELECT * FROM gold_top_products LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revenue Ranking with RANK()
-- MAGIC
-- MAGIC **EXAM TIP:** Know the difference between ROW_NUMBER(), RANK(), and DENSE_RANK().

-- COMMAND ----------

-- EXAM TIP: RANK() leaves gaps, DENSE_RANK() does not, ROW_NUMBER() is always unique
SELECT
    product_id,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS dense_revenue_rank
FROM gold_top_products
LIMIT 20
