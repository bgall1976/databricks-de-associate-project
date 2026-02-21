# Phase 3: Gold Layer Aggregation

## Learning Objectives

- Build business-level aggregate tables from Silver data
- Create materialized views and summary tables
- Understand the Gold layer's role in the medallion architecture

## Exam Topics Covered

- **Databricks Lakehouse Platform (24%):** Medallion architecture (Gold layer)
- **ELT with Spark SQL and Python (29%):** Aggregations, GROUP BY, window functions

## Key Concept

Gold tables are consumption-ready. They serve specific business questions and are optimized for query performance. Each Gold table typically answers one business question.

## Notebooks

1. **01_daily_revenue.sql** - Daily revenue aggregation
2. **02_top_products.sql** - Top products by revenue and quantity
3. **03_customer_lifetime_value.sql** - Customer lifetime value calculation
4. **04_regional_sales.sql** - Sales by region

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** A business analyst asks a data engineer to create a table that shows total revenue per day, refreshed each morning. The table should be queried by downstream dashboards for fast reads. In the medallion architecture, where does this table belong?

A) Bronze — because it ingests daily data  
B) Silver — because it's cleaned and aggregated  
C) Gold — because it's a business-level aggregate optimized for consumption  
D) A separate reporting database outside the lakehouse  

<details><summary>Answer</summary>

**C** — Gold tables serve specific business questions and are optimized for consumption by dashboards, reports, and BI tools. Daily revenue is a business-level aggregate, which is the defining characteristic of Gold-layer tables.

</details>

---

**Scenario 2:** A data engineer needs to calculate the top 10 products by total revenue using the Silver `enriched_orders` table. Which SQL approach is correct?

A) `SELECT product_name, SUM(quantity * unit_price) AS total_revenue FROM silver.enriched_orders GROUP BY product_name ORDER BY total_revenue DESC LIMIT 10`  
B) `SELECT TOP 10 product_name, SUM(quantity * unit_price) FROM silver.enriched_orders`  
C) `SELECT product_name, RANK() OVER (ORDER BY quantity) FROM silver.enriched_orders LIMIT 10`  
D) `SELECT FIRST(product_name, 10) FROM silver.enriched_orders`  

<details><summary>Answer</summary>

**A** — Standard SQL with `GROUP BY`, `ORDER BY ... DESC`, and `LIMIT 10` is the correct approach. Spark SQL does not support `TOP N` syntax. Option C ranks by quantity (not revenue) and doesn't aggregate. This tests basic aggregation patterns frequently seen on the exam.

</details>

---

**Scenario 3:** A data engineer creates a Gold table for customer lifetime value (CLV). The underlying Silver table receives new orders daily. The CLV Gold table should recompute from all historical orders each time it refreshes. In a DLT pipeline, which approach is appropriate?

A) Define the Gold table as a streaming table using `dlt.read_stream()`  
B) Define the Gold table as a materialized view using `dlt.read()`  
C) Use a MERGE statement to incrementally update CLV  
D) Use `INSERT INTO` to append new CLV calculations  

<details><summary>Answer</summary>

**B** — Gold aggregation tables that need to recompute from the full dataset should use `dlt.read()` (materialized view). A materialized view fully recomputes on each pipeline run, ensuring the aggregation is correct. Streaming tables (`dlt.read_stream()`) only process new/incremental data, which doesn't work for recomputing lifetime totals.

</details>

---

**Scenario 4:** A team has built a Gold table `gold.daily_revenue` using `CREATE OR REPLACE TABLE ... AS SELECT`. A business user reports that yesterday's numbers look wrong due to a bad batch of Silver data that has since been corrected. What should the data engineer do?

A) Use `RESTORE TABLE gold.daily_revenue TO VERSION AS OF` to go back  
B) Simply re-run the `CREATE OR REPLACE TABLE` statement — it will recompute from corrected Silver data  
C) Manually edit the rows in the Gold table  
D) Drop the Gold table and recreate it with a new name  

<details><summary>Answer</summary>

**B** — Since the Gold table is defined as `CREATE OR REPLACE TABLE ... AS SELECT` from Silver, re-running the statement will fully recompute the table from the (now corrected) Silver data. RESTORE would go back to a version that still had bad data. This pattern is one of the benefits of the medallion architecture — corrections flow downstream.

</details>

---

**Scenario 5:** A data engineer needs to compute a running total of revenue per customer, ordered by order date. Which SQL feature should they use?

A) `GROUP BY` with `SUM()`  
B) A window function: `SUM(revenue) OVER (PARTITION BY customer_id ORDER BY order_date)`  
C) A self-join on the orders table  
D) A recursive CTE  

<details><summary>Answer</summary>

**B** — Window functions with `SUM() OVER (PARTITION BY ... ORDER BY ...)` compute running/cumulative totals. `GROUP BY` would collapse rows into single aggregates per group, losing the row-level detail. Window functions are a key exam topic under the ELT with Spark SQL domain.

</details>
