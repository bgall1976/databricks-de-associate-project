# Phase 2: Silver Layer Transformation

## Learning Objectives

After completing this phase, you will be able to:

- Clean and deduplicate data using Delta Lake MERGE
- Enforce data quality constraints
- Flatten nested JSON structures
- Join multiple tables to create enriched datasets
- Use CTAS and INSERT patterns

## Exam Topics Covered

- **ELT with Spark SQL and Python (29%):** MERGE, CTAS, filtering, UDFs, complex types
- **Databricks Lakehouse Platform (24%):** Medallion architecture (Silver layer), schema enforcement
- **Incremental Data Processing (16%):** Streaming deduplication

## Key Concepts

### MERGE (Upsert) Pattern

```sql
MERGE INTO target_table t
USING source_table s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**EXAM TIP:** MERGE is the correct answer for:
- Deduplication with late-arriving data
- Slowly Changing Dimensions (SCD Type 1)
- Upserting streaming data into a Delta table

### Silver Layer Responsibilities

- Remove duplicates
- Enforce data types and constraints
- Handle null values
- Flatten nested structures
- Standardize field names and formats
- Join related tables for enrichment

## Notebooks in This Phase

1. **01_clean_orders.py** - Deduplicate, cast types, filter bad records
2. **02_clean_customers.py** - Deduplicate customers, standardize fields
3. **03_merge_upserts.py** - MERGE patterns for incremental updates
4. **04_enrich_orders.py** - Join orders with customers and products
5. **05_silver_validation.sql** - Validate Silver tables

## Exercises

1. Add a new batch of orders with updated statuses for existing order IDs, then run the MERGE to see upserts in action
2. Create a SQL UDF to categorize orders by total amount (small/medium/large)
3. Write a query that uses `WHEN MATCHED AND` conditions in MERGE (conditional updates)
