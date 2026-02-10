-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Daily Revenue
-- MAGIC
-- MAGIC **Objective:** Create a Gold table with daily revenue metrics.
-- MAGIC
-- MAGIC **Exam Topics:** Aggregations, GROUP BY, CTAS, date functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Gold Daily Revenue Table

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_daily_revenue AS
SELECT
    DATE(order_date) AS order_day,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(MIN(total_amount), 2) AS min_order_value,
    ROUND(MAX(total_amount), 2) AS max_order_value
FROM silver_cleaned_orders
WHERE status NOT IN ('cancelled', 'refunded')
GROUP BY DATE(order_date)
ORDER BY order_day DESC

-- COMMAND ----------

-- Verify
SELECT * FROM gold_daily_revenue LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rolling 7-Day Average Revenue
-- MAGIC
-- MAGIC **EXAM TIP:** Window functions over ordered data are tested frequently.

-- COMMAND ----------

SELECT
    order_day,
    total_revenue,
    ROUND(AVG(total_revenue) OVER (
        ORDER BY order_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7day_avg
FROM gold_daily_revenue
ORDER BY order_day DESC
LIMIT 30
