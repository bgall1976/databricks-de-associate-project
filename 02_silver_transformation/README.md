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

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** An e-commerce company's Bronze table contains order records, and some orders appear multiple times due to duplicate file deliveries. The data engineer needs to deduplicate these records and keep only the latest version of each order (based on a `last_updated` timestamp). Which approach is most appropriate?

A) Use `SELECT DISTINCT *` to remove duplicates  
B) Use `MERGE INTO` with a match condition on `order_id`, updating when the source `last_updated` is newer  
C) Delete duplicates manually with `DELETE FROM`  
D) Use `GROUP BY order_id` and pick one record randomly  

<details><summary>Answer</summary>

**B** — `MERGE INTO` is the correct approach for deduplication with late-arriving data. It matches on the business key (`order_id`), updates when the source has a newer timestamp, and inserts new records. `SELECT DISTINCT` wouldn't handle records that differ in non-key columns. This is the standard SCD Type 1 pattern.

</details>

---

**Scenario 2:** A Silver table needs to be created from a query that joins Bronze orders with Bronze customers. If the Silver table already exists, it should be completely replaced with fresh results. Which SQL statement achieves this?

A) `INSERT INTO silver.enriched_orders SELECT ...`  
B) `CREATE TABLE IF NOT EXISTS silver.enriched_orders AS SELECT ...`  
C) `CREATE OR REPLACE TABLE silver.enriched_orders AS SELECT ...`  
D) `MERGE INTO silver.enriched_orders USING (...) ...`  

<details><summary>Answer</summary>

**C** — `CREATE OR REPLACE TABLE ... AS SELECT` (CRAS) drops the existing table and recreates it from the query results. `INSERT INTO` would append (creating duplicates). `CREATE TABLE IF NOT EXISTS` would skip if the table exists. `MERGE` is for incremental updates, not full replacement.

</details>

---

**Scenario 3:** A data engineer has a Bronze orders table where the `order_details` column is a struct containing `product_id`, `quantity`, and `unit_price`. They need to extract `product_id` into its own column. Which syntax is correct?

A) `SELECT order_details['product_id'] FROM bronze_orders`  
B) `SELECT order_details.product_id FROM bronze_orders`  
C) `SELECT EXPLODE(order_details.product_id) FROM bronze_orders`  
D) `SELECT FLATTEN(order_details).product_id FROM bronze_orders`  

<details><summary>Answer</summary>

**B** — Dot notation (`struct_col.field`) is used to access fields within a struct. Bracket notation (`map_col['key']`) is for map types. `EXPLODE()` is for converting array elements into rows. This distinction is frequently tested on the exam.

</details>

---

**Scenario 4:** A Bronze table has an `items` column that is an array of structs. Each element contains `product_id`, `quantity`, and `price`. A data engineer needs to create one row per item. Which function should they use?

A) `FLATTEN(items)`  
B) `UNPACK(items)`  
C) `EXPLODE(items)`  
D) `UNNEST(items)`  

<details><summary>Answer</summary>

**C** — `EXPLODE()` converts each element of an array into a separate row. After exploding, you can use dot notation to access the struct fields within each element (e.g., `col.product_id`). This is a core Spark SQL function and very commonly tested.

</details>

---

**Scenario 5:** A data engineer runs the following MERGE statement:

```sql
MERGE INTO silver.customers t
USING bronze.customers s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

What does this statement do?

A) It updates all matching records and inserts all non-matching records  
B) It only updates matching records where the source has a newer timestamp, and inserts new records  
C) It fails because you can't use AND conditions in WHEN MATCHED  
D) It deletes matched records and inserts all source records  

<details><summary>Answer</summary>

**B** — The `WHEN MATCHED AND s.updated_at > t.updated_at` clause adds a condition so that only records with a newer timestamp are updated. Records that match on `customer_id` but have an older or equal timestamp are left unchanged. Non-matching records are inserted. This is the standard pattern for SCD Type 1 with conditional updates.

</details>

---

**Scenario 6:** A data engineer needs to create a reusable SQL function that converts a status code (1, 2, 3) into a human-readable label ("pending", "shipped", "delivered"). Which approach should they use?

A) Create a Python UDF with `@udf` decorator  
B) Create a SQL UDF with `CREATE FUNCTION`  
C) Use a lookup table with a JOIN  
D) Both B and C are valid; B is preferred for simple mappings  

<details><summary>Answer</summary>

**D** — A SQL UDF (`CREATE FUNCTION status_label(code INT) RETURNS STRING RETURN CASE WHEN code = 1 THEN 'pending' ...`) is ideal for simple mappings. A lookup table with JOIN also works. For the exam, know the `CREATE FUNCTION` syntax — SQL UDFs are simpler, have no serialization overhead, and are preferred over Python UDFs when possible.

</details>

---

**Scenario 7:** A data engineer needs to insert data into a partitioned Silver table, replacing only the data in the `2024-01` partition while keeping all other partitions intact. Which approach is correct?

A) `INSERT OVERWRITE silver.orders SELECT * FROM source WHERE month = '2024-01'`  
B) `DELETE FROM silver.orders WHERE month = '2024-01'` then `INSERT INTO`  
C) `MERGE INTO silver.orders` with partition filter  
D) Both A and B work, but A is preferred for atomicity  

<details><summary>Answer</summary>

**D** — `INSERT OVERWRITE` with dynamic partition overwrite replaces only the affected partitions atomically. The delete-then-insert approach also works but is not atomic (a failure between steps could leave the table in an inconsistent state). For the exam, `INSERT OVERWRITE` is the standard answer for partition-level replacement.

</details>
