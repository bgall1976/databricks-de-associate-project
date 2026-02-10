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
