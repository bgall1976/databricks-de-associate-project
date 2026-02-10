# Practice Questions

Work through these questions after completing the project. Each question maps to a specific exam domain and project phase. Answers are in expandable sections.

---

## Section 1: Databricks Lakehouse Platform

**Q1.** What is the primary advantage of a data lakehouse over a traditional data lake?

A) Lower storage costs
B) ACID transactions and schema enforcement on top of cloud storage
C) Faster networking between nodes
D) Compatibility with on-premise hardware

<details><summary>Answer</summary>B — The lakehouse combines data lake flexibility with warehouse reliability (ACID, schema enforcement).</details>

**Q2.** In the medallion architecture, which layer enforces data quality constraints?

A) Bronze  B) Silver  C) Gold  D) Platinum

<details><summary>Answer</summary>B — Silver handles cleaning, deduplication, and quality enforcement. Bronze preserves raw data.</details>

**Q3.** What is the underlying file format of Delta Lake tables?

A) CSV  B) JSON  C) Parquet  D) Avro

<details><summary>Answer</summary>C — Delta Lake = Parquet files + transaction log (_delta_log).</details>

**Q4.** What type of cluster should be used for production Workflows?

A) All-purpose cluster  B) Job cluster  C) SQL warehouse  D) Serverless cluster

<details><summary>Answer</summary>B — Job clusters auto-terminate after the job and are cheaper.</details>

**Q5.** Which statement about managed vs external tables is correct?

A) DROP managed table preserves data; DROP external table deletes data
B) DROP managed table deletes data; DROP external table preserves data
C) Both DROP operations delete data
D) Neither DROP operation deletes data

<details><summary>Answer</summary>B — Managed table DROP deletes data. External table DROP only removes metadata.</details>

---

## Section 2: ELT with Spark SQL and Python

**Q6.** How do you access a field `city` inside a struct column `address`?

A) `address['city']`  B) `address.city`  C) `EXPLODE(address).city`  D) `FLATTEN(address.city)`

<details><summary>Answer</summary>B — Dot notation for structs. Bracket notation is for maps.</details>

**Q7.** Which function converts an array column into multiple rows?

A) FLATTEN()  B) UNPACK()  C) EXPLODE()  D) UNNEST()

<details><summary>Answer</summary>C — EXPLODE() creates one row per array element.</details>

**Q8.** In a MERGE statement, which clause inserts new records?

A) WHEN MATCHED THEN INSERT  B) WHEN NOT MATCHED THEN INSERT  C) WHEN NOT MATCHED THEN UPDATE  D) WHEN MATCHED THEN APPEND

<details><summary>Answer</summary>B — WHEN NOT MATCHED handles source records not found in the target.</details>

**Q9.** How do you read version 5 of a Delta table in SQL?

A) `SELECT * FROM t AT VERSION 5`  B) `SELECT * FROM t VERSION AS OF 5`  C) `SELECT * FROM t@v5`  D) `SELECT * FROM t ROLLBACK TO 5`

<details><summary>Answer</summary>B — VERSION AS OF is the SQL time travel syntax.</details>

**Q10.** How do you read version 3 in PySpark?

A) `spark.read.format("delta").option("version", 3).load(path)`
B) `spark.read.format("delta").option("versionAsOf", 3).load(path)`
C) `spark.read.format("delta").version(3).load(path)`
D) `spark.read.format("delta").load(path, version=3)`

<details><summary>Answer</summary>B — The option name is `versionAsOf`.</details>

**Q11.** What does CREATE OR REPLACE TABLE ... AS SELECT do?

A) Updates matching rows  B) Drops and recreates the table  C) Appends results  D) Fails if table exists

<details><summary>Answer</summary>B — CTAS with OR REPLACE drops and recreates from the query.</details>

---

## Section 3: Incremental Data Processing

**Q12.** What is the streaming source format for Auto Loader?

A) `autoloader`  B) `cloudFiles`  C) `delta`  D) `fileStream`

<details><summary>Answer</summary>B — Auto Loader uses `cloudFiles` as the format.</details>

**Q13.** Which option is REQUIRED for Auto Loader schema inference?

A) `cloudFiles.schemaLocation`  B) `cloudFiles.checkpointLocation`  C) `cloudFiles.schemaPath`  D) `cloudFiles.inferSchema`

<details><summary>Answer</summary>A — schemaLocation stores the inferred schema.</details>

**Q14.** What is the primary advantage of Auto Loader over COPY INTO?

A) More file formats  B) Scales to millions of files  C) Less expensive  D) Supports batch processing

<details><summary>Answer</summary>B — Auto Loader uses file notification to handle millions of files.</details>

**Q15.** Which trigger processes all available data then stops?

A) `trigger(once=True)`  B) `trigger(availableNow=True)`  C) `trigger(processingTime="0 seconds")`  D) `trigger(continuous=True)`

<details><summary>Answer</summary>B — availableNow=True processes all available data across multiple micro-batches then stops.</details>

**Q16.** What does `@dlt.expect_or_drop("valid", "id IS NOT NULL")` do?

A) Fails the pipeline  B) Logs a warning, keeps records  C) Removes records with null id  D) Replaces nulls with default

<details><summary>Answer</summary>C — expect_or_drop silently removes violating records.</details>

**Q17.** In DLT, what is the difference between `dlt.read()` and `dlt.read_stream()`?

A) read() is external, read_stream() is DLT  B) read() = materialized view, read_stream() = streaming table  C) read() is faster  D) No difference

<details><summary>Answer</summary>B — read() does full reads (materialized view), read_stream() reads incrementally (streaming table).</details>

**Q18.** Which DLT expectation halts the pipeline on violation?

A) @dlt.expect  B) @dlt.expect_or_drop  C) @dlt.expect_or_fail  D) @dlt.expect_or_halt

<details><summary>Answer</summary>C — expect_or_fail stops the pipeline.</details>

---

## Section 4: Production Pipelines

**Q19.** How do you retrieve a widget value?

A) `dbutils.widgets.value("name")`  B) `dbutils.widgets.get("name")`  C) `dbutils.getWidget("name")`  D) `spark.conf.get("widget.name")`

<details><summary>Answer</summary>B — dbutils.widgets.get("name").</details>

**Q20.** What does `dbutils.notebook.run()` return?

A) DataFrame  B) Status code  C) String from dbutils.notebook.exit()  D) Nothing

<details><summary>Answer</summary>C — Returns the string passed to exit() in the called notebook.</details>

**Q21.** Which feature provides Git integration in the Databricks workspace?

A) Databricks Connect  B) Databricks Repos  C) Databricks CLI  D) Databricks Notebooks

<details><summary>Answer</summary>B — Repos provides in-workspace Git integration.</details>

---

## Section 5: Data Governance

**Q22.** What is the three-level namespace in Unity Catalog?

A) database.schema.table  B) catalog.schema.table  C) workspace.database.table  D) account.catalog.table

<details><summary>Answer</summary>B — catalog.schema.table.</details>

**Q23.** Which privilege is required on a catalog before accessing its schemas?

A) SELECT  B) ALL PRIVILEGES  C) USAGE  D) READ

<details><summary>Answer</summary>C — USAGE on parent objects is required first.</details>

**Q24.** Which function checks group membership in dynamic views?

A) has_role()  B) is_member()  C) belongs_to()  D) in_group()

<details><summary>Answer</summary>B — is_member('group_name').</details>

**Q25.** How do you implement column-level security in Unity Catalog?

A) Column-level GRANT  B) Dynamic views with CASE WHEN and is_member()  C) Column masking policies  D) Encrypted columns

<details><summary>Answer</summary>B — Dynamic views with CASE WHEN is_member() are the primary approach.</details>

---

## Mixed / Advanced

**Q26.** After OPTIMIZE ZORDER BY, what reclaims storage from old files?

A) DROP TABLE  B) VACUUM  C) REFRESH TABLE  D) TRUNCATE TABLE

<details><summary>Answer</summary>B — VACUUM removes old files.</details>

**Q27.** Default VACUUM retention period?

A) 24 hours  B) 48 hours  C) 72 hours  D) 168 hours (7 days)

<details><summary>Answer</summary>D — 168 hours (7 days).</details>

**Q28.** 10 million JSON files need daily ingestion. Best method?

A) COPY INTO  B) Auto Loader  C) spark.read.json()  D) dbutils.fs.ls() + manual

<details><summary>Answer</summary>B — Auto Loader scales to millions of files.</details>

**Q29.** Which command shows Delta table operation history?

A) SHOW HISTORY  B) DESCRIBE HISTORY  C) SELECT * FROM _delta_log  D) AUDIT TABLE

<details><summary>Answer</summary>B — DESCRIBE HISTORY.</details>

**Q30.** How do you restore a Delta table to version 3?

A) ROLLBACK TABLE t TO VERSION 3  B) RESTORE TABLE t TO VERSION AS OF 3  C) ALTER TABLE t RESTORE VERSION 3  D) UNDO TABLE t TO VERSION 3

<details><summary>Answer</summary>B — RESTORE TABLE ... TO VERSION AS OF.</details>

---

## Scoring

- 27-30 correct: You are ready for the exam
- 22-26 correct: Review the topics you missed, then retake
- Below 22: Work through the project phases again before attempting the exam
