# Phase 0: Data Generation

## Purpose

Generate synthetic e-commerce data that simulates realistic data landing patterns. The generated data intentionally includes quality issues that you will detect and handle in later phases.

## Quick Start

```bash
# Install dependencies
pip install faker

# Generate all data
python generate_customers.py
python generate_products.py
python generate_orders.py
```

This creates a `generated_data/` directory with:

```
generated_data/
├── customers/
│   └── customers.csv              # 500 customers with PII
├── products/
│   └── products.csv               # 100 products with pricing
└── orders/
    ├── orders_batch_001_*.json    # 1,000 orders per batch
    ├── orders_batch_002_*.json    # (newline-delimited JSON)
    └── ... (10 batch files)
```

## Uploading to Databricks

### Community Edition

1. In Databricks, go to **Data** in the left sidebar
2. Click **Create Table** > **Upload File**
3. Upload each file to DBFS under `/FileStore/ecommerce_project/raw/`

Or use the Databricks CLI:

```bash
databricks fs cp -r generated_data/orders/ dbfs:/FileStore/ecommerce_project/raw/orders/
databricks fs cp -r generated_data/customers/ dbfs:/FileStore/ecommerce_project/raw/customers/
databricks fs cp -r generated_data/products/ dbfs:/FileStore/ecommerce_project/raw/products/
```

### Trial Workspace with Cloud Storage

Upload to your cloud storage (S3, ADLS, GCS) and configure an external location in Unity Catalog.

## Intentional Data Quality Issues

The order data includes deliberate issues for you to catch in Phase 2:

| Issue | Frequency | Purpose |
|---|---|---|
| Null `customer_id` | ~1% | Practice null handling and expectations |
| Null `order_date` | ~1% | Practice null handling |
| Negative quantities | ~2% | Practice constraint validation |
| Future order dates | ~2% | Practice date range validation |
| Duplicate order IDs | ~1% | Practice deduplication and MERGE |

## Exam Topics Covered

- Understanding raw data formats (JSON, CSV)
- Newline-delimited JSON (the format Auto Loader prefers)
- Data quality concepts that carry through the rest of the project

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam, mapped to this phase.

---

**Scenario 1:** A data engineering team receives e-commerce order data as newline-delimited JSON files. Each file lands in a cloud storage folder every hour. They need to ingest these files incrementally into a Bronze Delta table. What is the recommended approach?

A) Use `spark.read.json()` in a scheduled batch job  
B) Use Auto Loader with `cloudFiles` format  
C) Use COPY INTO with a cron schedule  
D) Use `dbutils.fs.ls()` to list new files and read them manually  

<details><summary>Answer</summary>

**B** — Auto Loader (`cloudFiles`) is the recommended approach for incremental ingestion of files landing continuously in cloud storage. It automatically tracks which files have been processed, scales to millions of files, and supports schema evolution. `spark.read.json()` would reprocess all files every run, and `COPY INTO` is better for infrequent, small-scale loads.

</details>

---

**Scenario 2:** A data engineer discovers that 2% of incoming order records contain negative quantities and 1% have null customer IDs. The team wants to preserve ALL raw records in the Bronze layer — including bad ones — and handle quality issues downstream. Which approach aligns with the medallion architecture?

A) Filter out bad records before writing to Bronze  
B) Write all records to Bronze as-is, then clean them in the Silver layer  
C) Fix bad records in-flight during ingestion  
D) Create separate Bronze tables for good and bad records  

<details><summary>Answer</summary>

**B** — The Bronze layer in the medallion architecture is meant to preserve raw data exactly as it arrived, including records with quality issues. Data cleaning, deduplication, and constraint enforcement are the responsibility of the Silver layer. This preserves a full audit trail and allows reprocessing if business rules change.

</details>

---

**Scenario 3:** A team is debating file formats for their raw data landing zone. Their downstream pipeline will use Auto Loader with schema inference. Which raw file format is best suited for Auto Loader schema inference?

A) Parquet — because it includes the schema in the file  
B) CSV — because it's human-readable  
C) JSON (newline-delimited) — because Auto Loader handles it natively  
D) Avro — because it's compact  

<details><summary>Answer</summary>

**C** — While Auto Loader supports multiple formats (JSON, CSV, Parquet, Avro, etc.), newline-delimited JSON is extremely common for streaming/landing data and Auto Loader handles it natively with the `cloudFiles.format` option set to `"json"`. The key exam point is that Auto Loader works with all these formats, but JSON is the most frequently tested scenario.

</details>

---

**Scenario 4:** An organization needs to detect data quality issues as early as possible in their pipeline. They have raw CSV files with no schema enforcement. At which medallion layer should they first discover schema mismatches and data type issues?

A) Bronze — use Auto Loader's `_rescued_data` column  
B) Silver — use explicit type casting and validation  
C) Gold — use aggregation checks  
D) Raw — validate before ingestion  

<details><summary>Answer</summary>

**A** — Auto Loader provides a `_rescued_data` column that captures any fields that don't match the inferred or expected schema. This allows the Bronze layer to capture schema mismatches without losing data. Full data quality enforcement happens in Silver, but schema awareness starts at Bronze with Auto Loader.

</details>
