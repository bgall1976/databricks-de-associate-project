# Common Mistakes and Exam Gotchas

These are the most frequently missed topics based on community feedback from exam takers.

---

## 1. Auto Loader Format Name

**Wrong:** `format("autoloader")`
**Right:** `format("cloudFiles")`

This trips up a surprising number of people. The format name is `cloudFiles`, always.

---

## 2. VACUUM Retention

**Wrong assumption:** VACUUM default is 24 or 72 hours
**Reality:** Default retention is **168 hours (7 days)**

Also remember: after VACUUM, you CANNOT time travel to versions older than the retention period. The data files are gone.

---

## 3. trigger(once=True) vs trigger(availableNow=True)

**once=True:** Processes ONE micro-batch of data, then stops. If there are 100 files, it might only process a subset.

**availableNow=True:** Processes ALL available data across multiple micro-batches, then stops. This is the preferred option for batch-style processing.

The exam may test this distinction specifically.

---

## 4. DLT Read Functions

**dlt.read("table"):** Full read → creates a materialized view (recomputed fully each run)
**dlt.read_stream("table"):** Streaming read → creates a streaming table (incremental processing)

Gold aggregation tables typically use `dlt.read()` because they need to recompute aggregates from the full dataset.

---

## 5. Unity Catalog USAGE Requirement

To SELECT from `catalog.schema.table`, you need:
1. USAGE on the catalog
2. USAGE on the schema
3. SELECT on the table

Forgetting USAGE on the parent objects is a common source of "permission denied" errors and a common exam question.

---

## 6. Managed vs External Tables

**Managed table** (no LOCATION clause):
- Unity Catalog manages the storage
- DROP deletes both metadata AND data

**External table** (with LOCATION clause):
- You manage the storage
- DROP deletes only metadata, data files remain

The exam loves to test this distinction.

---

## 7. MERGE Syntax

**Wrong:** `MERGE target USING source`
**Right:** `MERGE INTO target USING source`

The `INTO` keyword is required. Also remember:
- `WHEN MATCHED THEN UPDATE SET *` (update all columns)
- `WHEN NOT MATCHED THEN INSERT *` (insert all columns)
- You CAN have both WHEN MATCHED and WHEN NOT MATCHED in the same MERGE

---

## 8. Schema Evolution Options

**For writes:** `.option("mergeSchema", "true")` — adds new columns automatically
**For Auto Loader:** `cloudFiles.schemaLocation` — tracks schema changes
**ALTER TABLE:** `ALTER TABLE t ADD COLUMN col TYPE` — manual approach

The exam may present a scenario where new columns appear in source data and ask how to handle it.

---

## 9. Job Cluster vs All-Purpose Cluster

The exam almost always expects **Job cluster** as the answer for production workloads. Key differences:
- Job clusters are cheaper (no idle charges)
- Job clusters auto-terminate
- Job clusters are dedicated to a single job
- All-purpose clusters are for development only (in production contexts)

---

## 10. DESCRIBE HISTORY vs DESCRIBE DETAIL

**DESCRIBE HISTORY:** Shows operation history (versions, timestamps, operations)
**DESCRIBE DETAIL:** Shows table metadata (file count, size, location, format)

These are different commands that return different information. The exam may use one when the other is needed.

---

## 11. DLT Expectations

| Expectation | On Violation | Use Case |
|---|---|---|
| `@dlt.expect` | Warn + keep record | Monitoring/alerting |
| `@dlt.expect_or_drop` | Silently remove record | Data quality filtering |
| `@dlt.expect_or_fail` | Halt pipeline | Critical constraints |

The exam will present a scenario and ask which expectation is appropriate. Think about the business impact of the violation to choose correctly.

---

## 12. COPY INTO vs Auto Loader Decision

**Use COPY INTO when:**
- Loading a few hundred to a few thousand files
- One-time or infrequent loads
- Simple schemas that don't change

**Use Auto Loader when:**
- Continuous or frequent ingestion
- Millions of files
- Schema may evolve
- Need exactly-once guarantees

When in doubt on the exam, Auto Loader is usually the correct answer.

---

## 13. Streaming Limitations

Structured streaming does NOT support:
- Full sorts (ORDER BY on unbounded streams)
- Full outer joins
- Aggregations without windowing on non-append streams

The exam may present code that looks correct but includes an unsupported operation.

---

## 14. Z-ORDER Column Selection

Z-ORDER is for **high-cardinality** columns frequently used in WHERE clauses.

**Good Z-ORDER candidates:** date, customer_id, product_id
**Bad Z-ORDER candidates:** status (6 values), boolean flags, region (5 values)

Low-cardinality columns should use partitioning instead.

---

## 15. RESTORE vs Time Travel

**Time travel** (`VERSION AS OF`): Reads old data without changing the current table
**RESTORE**: Actually reverts the table to a previous version (creates a new version that looks like the old one)

The exam may ask which to use when you need to "undo" a bad write (answer: RESTORE).
