# Phase 1: Bronze Layer Ingestion

## Learning Objectives

After completing this phase, you will be able to:

- Use Auto Loader (`cloudFiles`) to incrementally ingest data
- Understand the difference between Auto Loader and COPY INTO
- Create Delta Lake tables from raw data
- Add metadata columns for lineage tracking
- Configure checkpoints for exactly-once processing

## Exam Topics Covered

- **Incremental Data Processing (16%):** Auto Loader, structured streaming, COPY INTO
- **Databricks Lakehouse Platform (24%):** Delta Lake format, medallion architecture (Bronze layer)
- **ELT with Spark SQL and Python (29%):** Extracting data from files, schema definition

## Key Concepts

### Auto Loader vs COPY INTO

| Feature | Auto Loader | COPY INTO |
|---|---|---|
| Scalability | Scales to millions of files | Better for thousands of files |
| Schema evolution | Built-in support | Manual |
| File tracking | Automatic (checkpoint) | Manual (tracks processed files in table metadata) |
| When to use | Continuous/frequent ingestion | One-time or infrequent loads |
| Exam tip | Preferred answer for most ingestion scenarios | Correct answer for simple, one-time loads |

### Auto Loader Key Parameters

```python
spark.readStream
    .format("cloudFiles")                          # Auto Loader format
    .option("cloudFiles.format", "json")           # Source file format
    .option("cloudFiles.schemaLocation", path)     # Where to store inferred schema
    .option("cloudFiles.inferColumnTypes", "true") # Infer data types
    .load(source_path)
```

### Metadata Columns for Bronze Tables

Always add these to your Bronze tables:

- `_datasource`: Where the data came from
- `_ingest_timestamp`: When the data was ingested
- `_input_file_name`: Which file the record came from (use `input_file_name()`)

## Notebooks in This Phase

1. **01_explore_raw_data.py** - Explore raw files before ingestion
2. **02_autoloader_orders.py** - Auto Loader for JSON order files
3. **03_autoloader_customers.py** - Auto Loader for CSV customer files
4. **04_autoloader_products.py** - Auto Loader for CSV product files
5. **05_bronze_validation.sql** - Validate that Bronze tables are correct

## Exercises

After completing the notebooks:

1. Drop a new batch file into the orders directory and verify Auto Loader picks it up automatically
2. Modify the schema of a new batch file (add a column) and verify schema evolution works
3. Query the `_input_file_name` column to see which records came from which files
4. Compare the record counts in Bronze vs the raw files to confirm nothing was lost

## Common Exam Gotchas

- Auto Loader uses `cloudFiles` as the format, NOT `autoloader`
- The `schemaLocation` is required for Auto Loader to track schema evolution
- Auto Loader creates a streaming DataFrame, so you must use `writeStream` (not `write`)
- COPY INTO is idempotent by default (won't re-process files)
- Auto Loader checkpoint location is different from schema location

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** A company receives 10 million small JSON files per day from IoT sensors into a cloud storage bucket. The data engineering team needs to ingest these files into a Bronze Delta table with exactly-once guarantees. Which approach should they use?

A) Write a `spark.read.json()` job that runs daily and reads all files  
B) Use COPY INTO with daily scheduling  
C) Use Auto Loader with `cloudFiles` format  
D) Use `dbutils.fs.ls()` to detect new files and process them manually  

<details><summary>Answer</summary>

**C** — Auto Loader is specifically designed for this use case. It uses file notification (or file listing) to efficiently handle millions of files, provides exactly-once processing via checkpoints, and avoids re-scanning previously processed files. COPY INTO doesn't scale well to millions of files. `spark.read.json()` would reprocess every file on each run.

</details>

---

**Scenario 2:** A data engineer is configuring Auto Loader to ingest JSON files. The source files occasionally include new fields that weren't in previous files. The team wants to automatically accommodate these new columns. Which Auto Loader option must be configured?

A) `cloudFiles.inferSchema = true`  
B) `cloudFiles.schemaLocation` (set to a cloud storage path)  
C) `cloudFiles.schemaEvolutionMode = addNewColumns`  
D) `mergeSchema = true`  

<details><summary>Answer</summary>

**B** — The `cloudFiles.schemaLocation` option is **required** for Auto Loader to track and evolve the schema over time. When new columns appear, Auto Loader detects the change, updates the stored schema, and can restart the stream to incorporate the new fields. Without `schemaLocation`, Auto Loader cannot persist or evolve the schema.

</details>

---

**Scenario 3:** A data engineer writes the following code:

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load("/data/orders/")
    .write
    .format("delta")
    .save("/bronze/orders")
```

The pipeline fails on execution. What is the issue?

A) The `cloudFiles.format` should be `"autoloader"`  
B) Auto Loader returns a streaming DataFrame, so `.writeStream` must be used instead of `.write`  
C) The `schemaLocation` path is invalid  
D) Delta format cannot be used with streaming writes  

<details><summary>Answer</summary>

**B** — Auto Loader (`cloudFiles`) creates a streaming DataFrame via `readStream`. You must use `.writeStream` (not `.write`) to write the output. The correct write call would be `.writeStream.format("delta").option("checkpointLocation", path).start(output_path)`.

</details>

---

**Scenario 4:** A team currently uses COPY INTO to load CSV files into a Bronze Delta table. Recently, their file volumes have grown from a few hundred files to over 5 million files per day, and COPY INTO is taking hours. What should they do?

A) Increase the cluster size for COPY INTO  
B) Switch to Auto Loader for better scalability  
C) Pre-partition the files before running COPY INTO  
D) Use `spark.read.csv()` instead  

<details><summary>Answer</summary>

**B** — Auto Loader scales to millions of files efficiently by using file notification (cloud event-based) or optimized file listing. COPY INTO tracks processed files via table metadata and becomes slow as file counts grow. This is a classic exam scenario testing when to choose Auto Loader over COPY INTO.

</details>

---

**Scenario 5:** A data engineer needs to add metadata to their Bronze table so they can trace each record back to its source file and ingestion time. Which columns should they add?

A) Use `input_file_name()` and `current_timestamp()` as additional columns  
B) Use `_metadata.file_path` from the source  
C) Store the filename in a separate lookup table  
D) Both A and B are valid approaches  

<details><summary>Answer</summary>

**D** — Both approaches are valid. `input_file_name()` is a Spark SQL function that returns the source file path, and `current_timestamp()` captures ingestion time. In newer Databricks runtimes, the `_metadata` column from Auto Loader also provides `file_path`, `file_name`, `file_modification_time`, etc. The exam may test either approach.

</details>

---

**Scenario 6:** A data engineer is choosing between `trigger(once=True)` and `trigger(availableNow=True)` for a batch-style ingestion job that runs nightly. There are typically 50,000 new files each night. Which trigger should they use and why?

A) `trigger(once=True)` — it processes all available data in one micro-batch  
B) `trigger(availableNow=True)` — it processes all available data across multiple micro-batches, then stops  
C) No trigger — just let it run continuously  
D) `trigger(processingTime="1 hour")` — it will run once per hour  

<details><summary>Answer</summary>

**B** — `trigger(availableNow=True)` processes all available data across multiple micro-batches (handling backpressure) and then stops. `trigger(once=True)` processes only a single micro-batch which may not include all 50,000 files. For nightly batch-style workloads, `availableNow=True` is the preferred option.

</details>
