# Phase 6: Delta Lake Optimization

## Learning Objectives

- Use OPTIMIZE and Z-ORDER to improve query performance
- Understand and use VACUUM to clean up old files
- Query historical data with time travel
- Analyze query plans for performance tuning

## Exam Topics Covered

- **Databricks Lakehouse Platform (24%):** Delta Lake features (OPTIMIZE, VACUUM, time travel)
- **ELT with Spark SQL and Python (29%):** Query optimization

## Key Concepts

### OPTIMIZE
Compacts small files into larger ones. Delta Lake accumulates many small files from streaming and frequent writes. OPTIMIZE merges them for faster reads.

### Z-ORDER
Co-locates related data within files based on specified columns. Dramatically speeds up queries that filter on those columns.

### VACUUM
Removes files older than the retention period (default 7 days). Required for storage cost management but prevents time travel beyond the retention period.

### Time Travel
Query previous versions of a table by version number or timestamp. Essential for auditing and recovering from bad writes.

## Notebooks

1. **01_optimize_zorder.sql** - OPTIMIZE and Z-ORDER commands
2. **02_vacuum_history.sql** - VACUUM, DESCRIBE HISTORY, time travel
3. **03_file_compaction.py** - File compaction strategies
4. **04_query_performance.sql** - Query plan analysis

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** A Delta table receives thousands of small streaming writes per day, resulting in thousands of small Parquet files. Query performance has degraded significantly. What should the data engineer do first?

A) Drop and recreate the table  
B) Run `OPTIMIZE` on the table to compact small files  
C) Run `VACUUM` to remove old files  
D) Increase the cluster size  

<details><summary>Answer</summary>

**B** — `OPTIMIZE` compacts many small files into fewer, larger files, which significantly improves read performance. This is the standard solution for the "small file problem" in Delta Lake. `VACUUM` removes old files but doesn't compact current ones. Always OPTIMIZE before VACUUM.

</details>

---

**Scenario 2:** A data engineer runs `OPTIMIZE orders ZORDER BY (customer_id)`. After the optimization, they want to reclaim storage by removing the old, now-redundant data files. Which command should they run?

A) `DELETE FROM orders WHERE _file_name IN (old_files)`  
B) `VACUUM orders`  
C) `DROP TABLE orders` and recreate it  
D) `REFRESH TABLE orders`  

<details><summary>Answer</summary>

**B** — `VACUUM` removes data files that are no longer referenced by the current version of the table and are older than the retention period (default 168 hours / 7 days). After `OPTIMIZE`, the old small files are replaced by compacted files, and `VACUUM` cleans them up.

</details>

---

**Scenario 3:** A data engineer runs `VACUUM orders RETAIN 0 HOURS` to immediately clean up all old files. After the command succeeds, a colleague tries to run `SELECT * FROM orders VERSION AS OF 5` (an older version). What happens?

A) The query succeeds — VACUUM doesn't affect time travel  
B) The query fails — VACUUM removed the files needed for that version  
C) The query returns partial results  
D) The query automatically reads the latest version instead  

<details><summary>Answer</summary>

**B** — VACUUM removes old data files beyond the retention period. With `RETAIN 0 HOURS`, all old files are removed. Time travel requires those old files to reconstruct previous versions — once they're gone, time travel to those versions fails. This is why the default retention is 7 days and reducing it below that requires setting `delta.retentionDurationCheck.enabled = false`.

</details>

---

**Scenario 4:** A Delta table `orders` is frequently queried with filters on `order_date` and `customer_id`. Both columns have high cardinality. The data engineer wants to optimize the physical layout of the data for these query patterns. What should they do?

A) Partition the table by both `order_date` and `customer_id`  
B) Run `OPTIMIZE orders ZORDER BY (order_date, customer_id)`  
C) Create indexes on both columns  
D) Sort the table manually with `ORDER BY`  

<details><summary>Answer</summary>

**B** — Z-ORDER co-locates related data in the same files based on the specified columns, enabling data skipping for queries that filter on those columns. Partitioning by high-cardinality columns (like `customer_id`) would create too many small partitions. Delta Lake doesn't use traditional indexes. Z-ORDER with high-cardinality filter columns is the recommended approach.

</details>

---

**Scenario 5:** A data engineer needs to investigate what operations were performed on a Delta table over the last week. They want to see the version numbers, timestamps, operations, and who performed them. Which command should they use?

A) `DESCRIBE DETAIL orders`  
B) `DESCRIBE HISTORY orders`  
C) `SHOW TBLPROPERTIES orders`  
D) `SELECT * FROM orders._delta_log`  

<details><summary>Answer</summary>

**B** — `DESCRIBE HISTORY` returns the operation log for a Delta table, including version numbers, timestamps, operation types (WRITE, MERGE, OPTIMIZE, etc.), and user information. `DESCRIBE DETAIL` shows table-level metadata like file count, size, and location — not operation history. Reading `_delta_log` directly is not the recommended approach.

</details>

---

**Scenario 6:** A data engineer accidentally ran an `UPDATE` statement that corrupted data in a Silver table. They need to revert the table to its state before the bad update (version 12). Which command undoes the change?

A) `SELECT * FROM silver.orders VERSION AS OF 12` — and write the result to a new table  
B) `RESTORE TABLE silver.orders TO VERSION AS OF 12`  
C) `DELETE FROM silver.orders WHERE version > 12`  
D) `ROLLBACK silver.orders TO VERSION 12`  

<details><summary>Answer</summary>

**B** — `RESTORE TABLE ... TO VERSION AS OF` reverts the table to a previous version. This creates a new version of the table that matches the old state. Option A would read the old data but not actually revert the table. There is no `ROLLBACK` command in Delta Lake. `RESTORE` is the exam-expected answer for "undo a bad write."

</details>

---

**Scenario 7:** What is the default retention period for `VACUUM` in Delta Lake?

A) 24 hours  
B) 72 hours (3 days)  
C) 168 hours (7 days)  
D) 720 hours (30 days)  

<details><summary>Answer</summary>

**C** — The default VACUUM retention period is 168 hours (7 days). Files newer than this threshold are kept even if they are no longer referenced by the latest table version — this ensures time travel and concurrent readers continue to work. This is one of the most commonly tested facts on the exam.

</details>

---

**Scenario 8:** A data engineer wants to enable automatic file compaction so that small files are compacted after every write without manually running `OPTIMIZE`. Which table properties should they set?

A) `delta.autoOptimize.optimizeWrite = true` and `delta.autoOptimize.autoCompact = true`  
B) `delta.optimize.enabled = true`  
C) `delta.compaction.auto = true`  
D) `delta.vacuum.autoRun = true`  

<details><summary>Answer</summary>

**A** — Setting `delta.autoOptimize.optimizeWrite` to `true` optimizes the file sizes during writes, and `delta.autoOptimize.autoCompact` to `true` runs a mini-OPTIMIZE after each write to compact small files. These are set via `ALTER TABLE t SET TBLPROPERTIES (...)`.

</details>
