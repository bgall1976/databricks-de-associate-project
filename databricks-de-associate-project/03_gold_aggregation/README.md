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
