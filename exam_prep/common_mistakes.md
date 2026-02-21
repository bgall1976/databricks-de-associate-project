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

---

## 16. Choosing Between CTAS, INSERT INTO, and INSERT OVERWRITE

| Command | Behavior | Use When |
|---|---|---|
| `CREATE OR REPLACE TABLE ... AS SELECT` | Drops and recreates the table | Full refresh from scratch |
| `INSERT INTO` | Appends rows to existing table | Adding new data (may create duplicates) |
| `INSERT OVERWRITE` | Replaces partitions/data in table | Replacing specific partitions |
| `MERGE INTO` | Upserts (update + insert) | Dedup, SCD Type 1, incremental loads |

The exam often presents a scenario and asks which command is appropriate. Pay attention to whether the question says "replace," "append," "update existing," or "create."

---

## 17. Confusing Struct, Array, and Map Access Syntax

This is one of the most common traps:

| Data Type | Access Syntax | Example |
|---|---|---|
| **Struct** | Dot notation | `address.city` |
| **Map** | Bracket notation | `address['city']` |
| **Array** | Index or EXPLODE | `items[0]` or `EXPLODE(items)` |

The exam will give you a column and ask how to access a nested field. You must know the column's data type to choose the correct syntax. Dot notation on a map or bracket notation on a struct will fail.

---

## 18. VACUUM + Time Travel Interaction

**Critical sequence to understand:**

1. Table has versions 1-20
2. You run `VACUUM RETAIN 168 HOURS` (default)
3. Files for versions older than 7 days are deleted
4. Time travel to those old versions NOW FAILS

**Exam trap:** A question may describe running VACUUM and then ask if a time travel query will succeed. If the version is older than the retention period, the answer is NO.

**Double trap:** Running `VACUUM RETAIN 0 HOURS` requires setting `delta.retentionDurationCheck.enabled = false` first — Databricks won't let you vacuum below the safety threshold by default.

---

## 19. DLT Pipeline Development vs Execution

**Cannot do interactively:**
- Run DLT notebooks with the "Run" button
- Test `@dlt.table` decorators in a regular notebook

**Can do:**
- Define DLT tables in notebooks, then run as a DLT pipeline via Workflows UI
- View DLT pipeline results and data quality metrics in the pipeline UI

The exam may present a scenario where someone tries to test a DLT notebook interactively and ask why it fails.

---

## 20. Auto Loader: Schema Location vs Checkpoint Location

These are TWO DIFFERENT things:

| Setting | Purpose | Configured Via |
|---|---|---|
| **Schema location** | Stores inferred/evolved schema | `.option("cloudFiles.schemaLocation", path)` |
| **Checkpoint location** | Stores streaming progress state | `.option("checkpointLocation", path)` on writeStream |

Both are required. Schema location is an Auto Loader option (on the read side). Checkpoint location is a streaming option (on the write side). Confusing these is a common exam mistake.

---

## 21. "When in Doubt" Rules for the Exam

If you're stuck on a question, these heuristics align with how Databricks designs exam answers:

1. **Ingestion at scale** → Auto Loader (not COPY INTO, not spark.read)
2. **Production cluster** → Job cluster (not all-purpose)
3. **Upsert/dedup** → MERGE INTO (not INSERT or DELETE+INSERT)
4. **Data quality in pipeline** → DLT expectations (not manual WHERE clauses)
5. **PII masking** → Dynamic views with `is_member()` (not separate tables)
6. **Undo bad write** → `RESTORE TABLE` (not time travel SELECT)
7. **File compaction** → `OPTIMIZE` (not repartition or manual rewrite)
8. **Declarative pipeline** → DLT (not manual Workflow orchestration)
9. **Parameterize notebook** → `dbutils.widgets` (not environment variables)
10. **Git integration** → Databricks Repos (not CLI or Connect)
