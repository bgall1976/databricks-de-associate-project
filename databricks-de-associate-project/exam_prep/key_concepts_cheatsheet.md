# Key Concepts Cheatsheet

Quick reference for exam day. Print this or review it the morning of.

---

## Delta Lake

| Concept | Syntax / Detail |
|---|---|
| Create table | `CREATE TABLE t USING DELTA` |
| CTAS | `CREATE OR REPLACE TABLE t AS SELECT ...` |
| Insert | `INSERT INTO t VALUES (...)` / `INSERT OVERWRITE t SELECT ...` |
| MERGE | `MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` |
| Time travel (SQL) | `SELECT * FROM t VERSION AS OF 3` / `TIMESTAMP AS OF '...'` |
| Time travel (Python) | `.option("versionAsOf", 3)` / `.option("timestampAsOf", "...")` |
| Restore | `RESTORE TABLE t TO VERSION AS OF 3` |
| History | `DESCRIBE HISTORY t` |
| Detail | `DESCRIBE DETAIL t` |
| OPTIMIZE | `OPTIMIZE t` / `OPTIMIZE t ZORDER BY (col)` |
| VACUUM | `VACUUM t` / `VACUUM t RETAIN 168 HOURS` |
| Schema evolution | `.option("mergeSchema", "true")` on write |
| Add column | `ALTER TABLE t ADD COLUMN col_name TYPE` |
| Table properties | `ALTER TABLE t SET TBLPROPERTIES ('key' = 'value')` |
| Auto compaction | `'delta.autoOptimize.autoCompact' = 'true'` |
| Optimized writes | `'delta.autoOptimize.optimizeWrite' = 'true'` |

---

## Auto Loader

```python
spark.readStream
    .format("cloudFiles")                              # NOT "autoloader"
    .option("cloudFiles.format", "json")               # or csv, parquet
    .option("cloudFiles.schemaLocation", path)          # REQUIRED
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
```

| Key Fact | Detail |
|---|---|
| Format name | `cloudFiles` |
| Schema tracking | `cloudFiles.schemaLocation` (required) |
| Schema hints | `cloudFiles.schemaHints` (override specific columns) |
| Rescued data | `_rescued_data` column for unmatched schema columns |
| vs COPY INTO | Auto Loader scales better, handles schema evolution |

---

## Structured Streaming Triggers

| Trigger | Behavior |
|---|---|
| No trigger | Continuous micro-batch |
| `trigger(processingTime="5 minutes")` | Micro-batch every 5 min |
| `trigger(availableNow=True)` | Process all available, then stop |
| `trigger(once=True)` | Process one micro-batch, then stop |

---

## Delta Live Tables (DLT)

| Concept | Syntax |
|---|---|
| Define table | `@dlt.table` |
| Define view | `@dlt.view` |
| Read (full) | `dlt.read("table_name")` → materialized view |
| Read (stream) | `dlt.read_stream("table_name")` → streaming table |
| Expect (warn) | `@dlt.expect("desc", "constraint")` |
| Expect (drop) | `@dlt.expect_or_drop("desc", "constraint")` |
| Expect (fail) | `@dlt.expect_or_fail("desc", "constraint")` |

---

## Unity Catalog

| Concept | Detail |
|---|---|
| Namespace | `catalog.schema.table` |
| Create catalog | `CREATE CATALOG name` |
| Create schema | `CREATE SCHEMA name` |
| Use catalog | `USE CATALOG name` |
| Use schema | `USE SCHEMA name` |
| Grant | `GRANT SELECT ON TABLE t TO user` |
| Revoke | `REVOKE SELECT ON TABLE t FROM user` |
| Show grants | `SHOW GRANTS ON TABLE t` |
| USAGE required | Must grant USAGE on catalog AND schema before table access |
| Dynamic view PII | `CASE WHEN is_member('group') THEN col ELSE '***' END` |
| current_user() | Returns email of current user |
| is_member() | Checks group membership |
| Managed table | DROP deletes data |
| External table | DROP preserves data |

---

## Widgets (Parameterized Notebooks)

| Action | Syntax |
|---|---|
| Create text | `dbutils.widgets.text("name", "default", "label")` |
| Create dropdown | `dbutils.widgets.dropdown("name", "default", ["a","b"], "label")` |
| Get value | `dbutils.widgets.get("name")` |
| Remove one | `dbutils.widgets.remove("name")` |
| Remove all | `dbutils.widgets.removeAll()` |

---

## Production Workflows

| Concept | Detail |
|---|---|
| Job cluster | Created per job, auto-terminates, cheaper |
| All-purpose cluster | Long-running, shared, more expensive |
| Task dependencies | `depends_on` controls execution order |
| Parallel tasks | Tasks without depends_on run simultaneously |
| Notebook orchestration | `dbutils.notebook.run(path, timeout, args)` |
| Exit value | `dbutils.notebook.exit("value")` |
| Git integration | Databricks Repos |

---

## Medallion Architecture

| Layer | Purpose | Data Quality | Example |
|---|---|---|---|
| Bronze | Raw ingestion | None (preserve all) | raw_orders |
| Silver | Clean + conform | Enforce constraints | cleaned_orders |
| Gold | Business aggregates | N/A (derived) | daily_revenue |

---

## Common Traps

1. Auto Loader format is `cloudFiles`, NOT `autoloader`
2. VACUUM default is 168 hours (7 days), NOT 24 or 72
3. After VACUUM, time travel to old versions FAILS
4. USAGE on parent is required BEFORE granting table access
5. `dlt.read()` = materialized view, `dlt.read_stream()` = streaming table
6. MERGE can UPDATE, INSERT, and DELETE in one statement
7. Job clusters (not all-purpose) for production
8. `availableNow=True` processes ALL data; `once=True` processes ONE micro-batch
