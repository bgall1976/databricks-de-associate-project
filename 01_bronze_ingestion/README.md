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
