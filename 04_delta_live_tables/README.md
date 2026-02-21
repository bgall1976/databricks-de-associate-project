# Phase 4: Delta Live Tables (DLT)

## Learning Objectives

After completing this phase, you will be able to:

- Define DLT pipelines using the `@dlt.table` and `@dlt.view` decorators
- Use `dlt.read()` and `dlt.read_stream()` to reference upstream tables
- Implement data quality expectations with `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`
- Configure and deploy a DLT pipeline
- Understand streaming tables vs materialized views in DLT

## Exam Topics Covered

- **Incremental Data Processing (16%):** DLT syntax, expectations, streaming tables
- **Production Pipelines (16%):** DLT pipeline configuration and deployment

## Key Concepts

### DLT Table Types

| Type | Decorator | Use Case |
|---|---|---|
| Streaming table | `@dlt.table` with `dlt.read_stream()` | Incremental/append-only data |
| Materialized view | `@dlt.table` with `dlt.read()` | Aggregations, complete recomputation |
| View | `@dlt.view` | Intermediate transformations (not persisted) |

### Expectations (Data Quality)

```python
@dlt.expect("description", "constraint")              # Warn but keep record
@dlt.expect_or_drop("description", "constraint")      # Drop failing records
@dlt.expect_or_fail("description", "constraint")      # Fail the pipeline
```

**EXAM TIP:** Know which expectation to use:
- `expect` = log the violation, keep the row (monitoring)
- `expect_or_drop` = silently remove bad rows (quarantine)
- `expect_or_fail` = halt the pipeline (critical constraint)

## Important Notes

- DLT notebooks cannot be run interactively (they must be run as a DLT pipeline)
- DLT requires a trial workspace or paid workspace (not available on Community Edition)
- To test: create a DLT pipeline in the Workflows UI and attach these notebooks

## Notebooks

1. **01_dlt_bronze.py** - Bronze layer as DLT streaming tables
2. **02_dlt_silver.py** - Silver layer with expectations
3. **03_dlt_gold.py** - Gold layer as materialized views
4. **dlt_pipeline_config.json** - Pipeline configuration reference

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** A data engineering team wants to build a declarative ETL pipeline that automatically manages dependencies between tables, enforces data quality rules, and tracks lineage. They want to avoid writing manual orchestration logic. Which Databricks feature should they use?

A) Databricks Workflows with multiple notebook tasks  
B) Delta Live Tables (DLT)  
C) Structured Streaming with manual checkpoints  
D) Apache Airflow integration  

<details><summary>Answer</summary>

**B** — Delta Live Tables is a declarative pipeline framework that automatically manages table dependencies, provides built-in data quality expectations, and tracks lineage. Unlike Workflows, DLT handles the execution graph declaratively — you define the tables, and DLT figures out the execution order. This is a key selling point tested on the exam.

</details>

---

**Scenario 2:** A data engineer is building a DLT Silver table from a Bronze streaming table. The engineer wants to silently drop any records where `customer_id IS NULL` without failing the pipeline. Which DLT expectation should they use?

A) `@dlt.expect("valid_customer", "customer_id IS NOT NULL")`  
B) `@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")`  
C) `@dlt.expect_or_fail("valid_customer", "customer_id IS NOT NULL")`  
D) `@dlt.expect_or_quarantine("valid_customer", "customer_id IS NOT NULL")`  

<details><summary>Answer</summary>

**B** — `@dlt.expect_or_drop` silently removes records that violate the constraint. `@dlt.expect` would log a warning but keep the bad record. `@dlt.expect_or_fail` would halt the pipeline. There is no `expect_or_quarantine` decorator (though quarantine tables can be built manually).

</details>

---

**Scenario 3:** A DLT pipeline has a Gold aggregation table that computes daily revenue. This table needs to recompute from the full Silver dataset on each pipeline run (not incrementally). How should the data engineer define this table?

A) Use `@dlt.table` with `dlt.read_stream("silver_orders")`  
B) Use `@dlt.table` with `dlt.read("silver_orders")`  
C) Use `@dlt.view` with `dlt.read_stream("silver_orders")`  
D) Use `@dlt.table` with `spark.read.format("delta")`  

<details><summary>Answer</summary>

**B** — `dlt.read()` performs a full (batch) read of the referenced table, creating a materialized view that is fully recomputed on each run. `dlt.read_stream()` creates a streaming table that only processes new/incremental data. For aggregations that must consider the full dataset, `dlt.read()` is the correct choice.

</details>

---

**Scenario 4:** A data engineer deploys a DLT pipeline and notices that some records in the Silver table have null `email` values. They want the pipeline to continue processing but track how many records violate this constraint for monitoring. Which expectation should they use?

A) `@dlt.expect("valid_email", "email IS NOT NULL")`  
B) `@dlt.expect_or_drop("valid_email", "email IS NOT NULL")`  
C) `@dlt.expect_or_fail("valid_email", "email IS NOT NULL")`  
D) No expectation — add a WHERE clause to filter nulls  

<details><summary>Answer</summary>

**A** — `@dlt.expect` logs the constraint violation (making it visible in the DLT pipeline metrics) but keeps the record. This is ideal for monitoring scenarios where you want to track data quality issues without modifying the data or stopping the pipeline.

</details>

---

**Scenario 5:** A financial services company has a DLT pipeline that processes transactions. Regulatory requirements mandate that no transaction can have a negative amount. If a negative amount is detected, the pipeline must immediately stop and alert the team. Which expectation enforces this?

A) `@dlt.expect("positive_amount", "amount >= 0")`  
B) `@dlt.expect_or_drop("positive_amount", "amount >= 0")`  
C) `@dlt.expect_or_fail("positive_amount", "amount >= 0")`  
D) Add a WHERE clause: `WHERE amount >= 0`  

<details><summary>Answer</summary>

**C** — `@dlt.expect_or_fail` halts the pipeline when the constraint is violated. This is the correct choice when the business cannot tolerate bad data flowing downstream — such as regulatory or compliance requirements. The pipeline stops, preventing corrupt data from reaching consumers.

</details>

---

**Scenario 6:** A data engineer attempts to run a DLT notebook interactively using the "Run All" button in the Databricks notebook interface. The notebook fails with errors. Why?

A) DLT notebooks require a GPU cluster  
B) DLT notebooks can only be executed as part of a DLT pipeline, not interactively  
C) The notebook is missing import statements  
D) DLT notebooks only work with SQL, not Python  

<details><summary>Answer</summary>

**B** — DLT notebooks cannot be run interactively. They must be executed as part of a DLT pipeline configured in the Databricks Workflows UI. The `dlt` module and `@dlt.table` decorators are only available within the DLT runtime environment. This is a common gotcha on the exam.

</details>

---

**Scenario 7:** In a DLT pipeline, a data engineer defines a transformation that filters and cleans data but does not need to persist the result to storage — it's only used as an intermediate step consumed by downstream DLT tables. Which DLT object type should they use?

A) `@dlt.table` — a streaming table  
B) `@dlt.table` — a materialized view  
C) `@dlt.view` — a DLT view (not persisted)  
D) A temporary view using `CREATE TEMP VIEW`  

<details><summary>Answer</summary>

**C** — `@dlt.view` creates a logical view within the DLT pipeline that is not persisted to storage. It's ideal for intermediate transformations that are only consumed by other DLT tables. Using `@dlt.table` would unnecessarily write the intermediate data to storage. Standard `CREATE TEMP VIEW` is not part of the DLT declarative framework.

</details>
