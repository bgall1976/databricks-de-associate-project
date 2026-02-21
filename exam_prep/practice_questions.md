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

---

## Section 6: Scenario-Based Questions (Exam-Style)

The questions below are scenario-based — the format most commonly used on the actual Databricks Data Engineer Associate exam. Each question presents a real-world situation and asks you to choose the best Databricks-native solution.

---

### Lakehouse Platform Scenarios

**Q31.** A company currently stores data in a data lake (S3/ADLS) using Parquet files and also maintains a separate data warehouse for BI queries. They want to eliminate the dual architecture and have a single system that supports both raw data storage and SQL analytics with ACID transactions. What should they implement?

A) Migrate everything to the data warehouse  
B) Add Apache Hive on top of the data lake  
C) Implement a lakehouse architecture using Delta Lake on their existing cloud storage  
D) Add a streaming layer between the lake and warehouse  

<details><summary>Answer</summary>C — The lakehouse architecture combines data lake flexibility (cheap cloud storage, schema-on-read) with warehouse features (ACID transactions, schema enforcement, SQL analytics). Delta Lake provides the transaction layer on top of existing Parquet files in cloud storage, eliminating the need for a separate warehouse.</details>

**Q32.** A data engineer joins a new team and is told that all tables are stored as "Delta tables." They inspect the storage location and find Parquet files plus a `_delta_log/` directory. Which statement is true?

A) The files should be in Delta format, not Parquet — something is wrong  
B) Delta Lake tables ARE Parquet files plus a transaction log (`_delta_log`) — this is expected  
C) The `_delta_log` directory is temporary and can be deleted  
D) Parquet files are the backup; the real data is in `_delta_log`  

<details><summary>Answer</summary>B — Delta Lake = Parquet data files + a transaction log directory (`_delta_log`). The transaction log records all changes (commits) and enables ACID transactions, time travel, and schema enforcement. The `_delta_log` must never be manually deleted.</details>

**Q33.** A data engineer is asked to explain the medallion architecture to a new team member. Which description is correct?

A) Bronze = aggregated data, Silver = raw data, Gold = cleaned data  
B) Bronze = raw ingested data, Silver = cleaned/conformed data, Gold = business-level aggregates  
C) Bronze = external data, Silver = internal data, Gold = archived data  
D) Bronze = streaming data, Silver = batch data, Gold = real-time data  

<details><summary>Answer</summary>B — Bronze preserves raw data as-is, Silver handles cleaning/deduplication/conforming, and Gold provides business-level aggregates for consumption. This is also called the "multi-hop" architecture.</details>

**Q34.** Which Databricks cluster type should be used by a data analyst who needs to run ad-hoc SQL queries and explore data interactively?

A) Job cluster  
B) All-purpose cluster  
C) DLT pipeline cluster  
D) GPU cluster  

<details><summary>Answer</summary>B — All-purpose clusters are designed for interactive exploration, ad-hoc queries, and development. Job clusters are for automated production workloads. SQL warehouses are another valid option for SQL-only workloads, but among these choices, all-purpose is correct.</details>

---

### ELT with Spark SQL Scenarios

**Q35.** A data engineer has a Delta table that was updated with incorrect data 2 hours ago. They want to query the table as it was before the bad update to verify what the data looked like. The bad update was version 15, so they want to see version 14. Which query is correct?

A) `SELECT * FROM orders ROLLBACK TO VERSION 14`  
B) `SELECT * FROM orders VERSION AS OF 14`  
C) `SELECT * FROM orders AT VERSION 14`  
D) `SELECT * FROM orders@v14`  

<details><summary>Answer</summary>B — `VERSION AS OF` is the correct Delta Lake time travel SQL syntax. `TIMESTAMP AS OF` is the alternative using a timestamp. This only reads the old version — it does not change the current table state. To actually revert, use `RESTORE TABLE`.</details>

**Q36.** A data engineer needs to write PySpark code to read version 8 of a Delta table located at `/mnt/data/orders`. Which code is correct?

A) `spark.read.format("delta").option("version", 8).load("/mnt/data/orders")`  
B) `spark.read.format("delta").option("versionAsOf", 8).load("/mnt/data/orders")`  
C) `spark.read.format("delta").version(8).load("/mnt/data/orders")`  
D) `spark.read.format("delta").load("/mnt/data/orders", version=8)`  

<details><summary>Answer</summary>B — The option name is `versionAsOf` (not `version`). Similarly, for timestamp-based time travel, the option is `timestampAsOf`. This is a frequently tested syntax detail.</details>

**Q37.** A data engineer has a Bronze table where the `address` column is a map type with keys like `"city"`, `"state"`, `"zip"`. How do they extract the city value?

A) `SELECT address.city FROM bronze_customers`  
B) `SELECT address['city'] FROM bronze_customers`  
C) `SELECT EXPLODE(address).city FROM bronze_customers`  
D) `SELECT MAP_GET(address, 'city') FROM bronze_customers`  

<details><summary>Answer</summary>B — Bracket notation (`map_col['key']`) is used for map types. Dot notation (`struct_col.field`) is for structs. This is a critical distinction on the exam — know when to use dot vs bracket notation.</details>

**Q38.** A data engineer is asked to load data from an external CSV file into a Delta table. The table may or may not already exist. They should only process files that haven't been loaded before, and the operation should be idempotent. Which command is most appropriate?

A) `spark.read.csv(path).write.mode("append").saveAsTable("target")`  
B) `COPY INTO target FROM source FILEFORMAT = CSV`  
C) `INSERT INTO target SELECT * FROM csv.\`path\``  
D) `MERGE INTO target USING csv.\`path\` s ON ...`  

<details><summary>Answer</summary>B — `COPY INTO` is designed for idempotent file loading — it tracks which files have been processed and won't re-load them. It's the correct answer for simple, infrequent file loads where idempotency is important. For continuous/high-volume scenarios, Auto Loader would be preferred instead.</details>

---

### Incremental Data Processing Scenarios

**Q39.** A data engineering team builds a streaming pipeline with Auto Loader. After running successfully for a week, the pipeline is stopped for maintenance. When restarted, the pipeline automatically picks up from where it left off without reprocessing old files. What makes this possible?

A) Auto Loader re-scans the directory to find new files  
B) The checkpoint location stores the processing state, enabling exactly-once recovery  
C) The source files are deleted after processing  
D) Delta Lake's transaction log tracks which files were read  

<details><summary>Answer</summary>B — The checkpoint location persists the state of the streaming query, including which files have been processed. On restart, the stream reads the checkpoint and resumes from the last committed position, providing exactly-once processing guarantees.</details>

**Q40.** A data engineer is building a DLT pipeline with three layers: Bronze, Silver, and Gold. The Bronze layer ingests from cloud storage incrementally. The Silver layer cleans and deduplicates. The Gold layer computes daily revenue. Which combination of DLT read functions is correct?

A) Bronze: `dlt.read_stream()`, Silver: `dlt.read_stream()`, Gold: `dlt.read_stream()`  
B) Bronze: `dlt.read_stream()`, Silver: `dlt.read_stream()`, Gold: `dlt.read()`  
C) Bronze: `dlt.read()`, Silver: `dlt.read()`, Gold: `dlt.read()`  
D) Bronze: `dlt.read()`, Silver: `dlt.read_stream()`, Gold: `dlt.read_stream()`  

<details><summary>Answer</summary>B — Bronze and Silver process data incrementally (streaming), so they use `dlt.read_stream()`. Gold computes aggregates that need the full dataset (not just incremental changes), so it uses `dlt.read()` to create a materialized view that fully recomputes each run.</details>

**Q41.** A structured streaming job processes data from a Bronze Delta table to a Silver Delta table. The data engineer notices that the streaming job supports `append` and `complete` output modes. For writing cleaned records to a Silver table, which output mode should they use?

A) `complete` — overwrites the entire table each micro-batch  
B) `append` — adds new records to the table each micro-batch  
C) `update` — updates only changed records  
D) `overwrite` — replaces the table each run  

<details><summary>Answer</summary>B — For ETL pipelines that clean and write records to Silver, `append` output mode is standard. `complete` mode would rewrite the entire table on each micro-batch, which is only appropriate for aggregation queries. `update` mode is for updating in-place (not commonly used with Delta sink).</details>

**Q42.** A data engineer needs to explain the difference between `trigger(once=True)` and `trigger(availableNow=True)` to their team. Which explanation is correct?

A) They are identical — both process all available data  
B) `once=True` processes one micro-batch (may not include all data); `availableNow=True` processes all available data across multiple micro-batches  
C) `once=True` runs faster because it uses a single batch  
D) `availableNow=True` runs continuously; `once=True` runs once  

<details><summary>Answer</summary>B — `trigger(once=True)` processes exactly one micro-batch, which may not include all available data if there's a large backlog. `trigger(availableNow=True)` processes all available data by running as many micro-batches as needed, then stops. For batch-style streaming workloads, `availableNow=True` is preferred.</details>

---

### Production Pipeline Scenarios

**Q43.** A data engineer creates a Databricks Workflow with 5 tasks. Task 1 loads data, Tasks 2-4 process different data domains in parallel (all depend on Task 1), and Task 5 runs a final quality check (depends on Tasks 2, 3, and 4). What happens if Task 3 fails?

A) All tasks stop immediately  
B) Tasks 2 and 4 continue; Task 5 does not run because one of its dependencies (Task 3) failed  
C) Task 5 runs anyway with partial data  
D) The entire workflow restarts from Task 1  

<details><summary>Answer</summary>B — Databricks Workflows respect dependency graphs. Tasks 2 and 4 can continue since they don't depend on Task 3. Task 5 will not run because it depends on Task 3 (which failed). The workflow will be marked as failed, and error notifications will trigger if configured.</details>

**Q44.** A data engineer wants to pass a date parameter to a notebook when it's called from a Databricks Workflow. Inside the notebook, how should they receive this parameter?

A) `import sys; date = sys.argv[1]`  
B) `date = dbutils.widgets.get("date")`  
C) `date = spark.conf.get("date")`  
D) `date = os.environ["date"]`  

<details><summary>Answer</summary>B — When a Workflow passes parameters to a notebook task, the parameters are received as widget values. The notebook should first define the widget (`dbutils.widgets.text("date", "")`) and then retrieve the value with `dbutils.widgets.get("date")`. Workflows automatically populate widget values from the task configuration.</details>

---

### Data Governance Scenarios

**Q45.** An auditor asks a data engineer to prove that a Gold reporting table was derived from specific source tables and transformations. Which Unity Catalog feature provides this information without manual documentation?

A) Table properties (`SHOW TBLPROPERTIES`)  
B) Operation history (`DESCRIBE HISTORY`)  
C) Automated data lineage (visible in the Databricks UI Lineage tab)  
D) Table comments (`COMMENT ON TABLE`)  

<details><summary>Answer</summary>C — Unity Catalog automatically tracks data lineage, recording which tables and columns were used to create derived tables. The Lineage tab in the Databricks UI shows upstream sources and downstream consumers. This is maintained automatically — no manual documentation required.</details>

---

## Scoring (Updated)

- 40-45 correct: **Exam ready** — you have strong command of all domains
- 34-39 correct: **Almost there** — review the topics you missed, focus on scenario-based reasoning
- 27-33 correct: **Needs work** — revisit the project phases for missed topics, then retake
- Below 27: **Significant review needed** — work through all project phases before attempting the exam
