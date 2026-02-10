# Data Lineage in Unity Catalog

## What is Data Lineage?

Data lineage tracks how data flows from source to destination. Unity Catalog automatically captures lineage for all operations that read from and write to Unity Catalog tables.

## How to View Lineage

1. Navigate to a table in the **Data Explorer** (left sidebar > Data)
2. Click on the table name (e.g., `ecommerce_project.gold.daily_revenue`)
3. Click the **Lineage** tab
4. You will see an interactive graph showing:
   - **Upstream tables:** Where this table's data comes from
   - **Downstream tables:** What tables depend on this one
   - **Notebooks/Jobs:** Which code produced this table

## What Lineage Shows for This Project

```
bronze.raw_orders ──┐
                    ├──> silver.cleaned_orders ──┐
bronze.raw_customers┘                           ├──> gold.daily_revenue
                    ┌──> silver.cleaned_customers┘
bronze.raw_customers┘
```

## Exam Topics

**EXAM TIP:** Know these facts about Unity Catalog lineage:

1. Lineage is captured **automatically** — no configuration required
2. It tracks table-to-table and column-to-column dependencies
3. Lineage works across notebooks, jobs, and DLT pipelines
4. It enables **impact analysis**: before changing a table, see what depends on it
5. Lineage is available in the Data Explorer UI and via REST API
6. Lineage helps with **regulatory compliance** (audit trail of data transformations)

## Exercise

After running the DLT pipeline (Phase 4), navigate to any Gold table in the Data Explorer and explore its lineage graph. Trace the data flow from Bronze through Silver to Gold.

## Limitations to Know

- Lineage is only captured for Unity Catalog tables (not legacy Hive metastore tables)
- External tools that read/write directly to storage bypass lineage tracking
- Lineage history has a retention period (check Databricks documentation for current limits)
