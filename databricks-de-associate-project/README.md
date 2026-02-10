# Databricks Data Engineer Associate - Hands-On Project

## Real-Time E-Commerce Order Processing Pipeline

A comprehensive, hands-on project designed to teach every domain tested on the Databricks Data Engineer Associate certification exam. Build an end-to-end data pipeline using Databricks-native tools and best practices.

---

## Exam Domains Covered

| Exam Domain | Weight | Project Phase |
|---|---|---|
| Databricks Lakehouse Platform | 24% | All phases |
| ELT with Spark SQL and Python | 29% | Phase 1, 2, 3 |
| Incremental Data Processing | 16% | Phase 1, 2 |
| Production Pipelines | 16% | Phase 5, 6 |
| Data Governance | 15% | Phase 4 |

---

## Project Architecture

```
Raw Data (JSON/CSV)
    │
    ▼
┌──────────────────┐
│   Auto Loader     │  ← Phase 1: Incremental Ingestion
│   (cloudFiles)    │
└──────┬───────────┘
       ▼
┌──────────────────┐
│   Bronze Layer    │  ← Raw data + metadata
│   (Delta Lake)    │
└──────┬───────────┘
       ▼
┌──────────────────┐
│   Silver Layer    │  ← Phase 2: Clean, Dedupe, Enrich
│   (DLT Pipeline)  │
└──────┬───────────┘
       ▼
┌──────────────────┐
│   Gold Layer      │  ← Phase 3: Business Aggregates
│   (DLT Pipeline)  │
└──────┬───────────┘
       ▼
┌──────────────────┐
│  Unity Catalog    │  ← Phase 4: Governance & Lineage
│  + Permissions    │
└──────────────────┘
```

---

## Prerequisites

- A Databricks workspace (Community Edition for basics, or a 14-day trial on AWS/Azure/GCP for Unity Catalog and DLT features)
- Basic familiarity with SQL and Python
- A GitHub account (to fork/clone this repo)

### What You Do NOT Need

- Prior Spark or Databricks experience
- A paid cloud account (Community Edition is free; trial workspaces are free for 14 days)

---

## Repository Structure

```
databricks-de-associate-project/
│
├── README.md                          # This file
├── STUDY_GUIDE.md                     # Exam topic checklist mapped to project phases
├── SETUP.md                           # Workspace setup instructions
│
├── 00_data_generation/
│   ├── README.md                      # Instructions for generating synthetic data
│   ├── generate_orders.py             # Generates order JSON files in batches
│   ├── generate_customers.py          # Generates customer CSV data
│   ├── generate_products.py           # Generates product catalog CSV
│   └── config.py                      # Data generation configuration
│
├── 01_bronze_ingestion/
│   ├── README.md                      # Phase 1 learning objectives and exam topics
│   ├── 01_explore_raw_data.py         # Notebook: explore raw files
│   ├── 02_autoloader_orders.py        # Notebook: Auto Loader ingestion for orders
│   ├── 03_autoloader_customers.py     # Notebook: Auto Loader ingestion for customers
│   ├── 04_autoloader_products.py      # Notebook: Auto Loader ingestion for products
│   └── 05_bronze_validation.sql       # Notebook: validate bronze tables
│
├── 02_silver_transformation/
│   ├── README.md                      # Phase 2 learning objectives and exam topics
│   ├── 01_clean_orders.py             # Notebook: dedup, clean, type cast orders
│   ├── 02_clean_customers.py          # Notebook: dedup, clean customers
│   ├── 03_merge_upserts.py            # Notebook: MERGE/upsert patterns
│   ├── 04_enrich_orders.py            # Notebook: join orders + customers + products
│   └── 05_silver_validation.sql       # Notebook: validate silver tables
│
├── 03_gold_aggregation/
│   ├── README.md                      # Phase 3 learning objectives and exam topics
│   ├── 01_daily_revenue.sql           # Notebook: daily revenue aggregation
│   ├── 02_top_products.sql            # Notebook: top products by revenue/quantity
│   ├── 03_customer_lifetime_value.sql # Notebook: CLV calculation
│   └── 04_regional_sales.sql          # Notebook: regional sales summaries
│
├── 04_delta_live_tables/
│   ├── README.md                      # DLT-specific learning objectives
│   ├── 01_dlt_bronze.py               # DLT pipeline: bronze layer
│   ├── 02_dlt_silver.py               # DLT pipeline: silver layer with expectations
│   ├── 03_dlt_gold.py                 # DLT pipeline: gold layer materialized views
│   └── dlt_pipeline_config.json       # DLT pipeline configuration
│
├── 05_data_governance/
│   ├── README.md                      # Phase 4 learning objectives and exam topics
│   ├── 01_unity_catalog_setup.sql     # Create catalog, schemas, tables
│   ├── 02_access_control.sql          # Grant/revoke permissions
│   ├── 03_dynamic_views_pii.sql       # Dynamic views for PII masking
│   └── 04_data_lineage.md             # Guide to exploring lineage in UI
│
├── 06_optimization/
│   ├── README.md                      # Phase 5 learning objectives and exam topics
│   ├── 01_optimize_zorder.sql         # OPTIMIZE and Z-ORDER examples
│   ├── 02_vacuum_history.sql          # VACUUM, DESCRIBE HISTORY, time travel
│   ├── 03_file_compaction.py          # File compaction strategies
│   └── 04_query_performance.sql       # Query plan analysis
│
├── 07_production_workflows/
│   ├── README.md                      # Phase 6 learning objectives and exam topics
│   ├── 01_parameterized_notebook.py   # Notebook with dbutils.widgets
│   ├── 02_workflow_config.json        # Databricks Workflow/Job JSON definition
│   ├── 03_error_handling.py           # Retry logic, error handling patterns
│   └── 04_git_integration.md          # Guide to Repos and CI/CD
│
└── exam_prep/
    ├── README.md                      # Final exam prep strategy
    ├── practice_questions.md          # Practice questions mapped to project phases
    ├── key_concepts_cheatsheet.md     # Quick reference for exam day
    └── common_mistakes.md             # Gotchas and frequently missed topics
```

---

## How to Use This Repo

### Step 1: Set Up Your Workspace
Follow [SETUP.md](SETUP.md) to configure your Databricks workspace.

### Step 2: Generate Synthetic Data
Run the scripts in `00_data_generation/` to create your raw data files.

### Step 3: Work Through Each Phase in Order
Each phase folder contains a README with:
- Learning objectives tied to specific exam topics
- Step-by-step instructions
- Exam tips and gotchas
- Exercises to complete before moving on

### Step 4: Build the DLT Pipeline
Phase 4 (`04_delta_live_tables/`) rebuilds Phases 1-3 using Delta Live Tables, the declarative framework Databricks tests heavily on the exam.

### Step 5: Apply Governance and Optimize
Phases 5 and 6 cover Unity Catalog, optimization, and production workflows.

### Step 6: Exam Prep
Use the `exam_prep/` folder to review, test yourself, and identify weak areas.

---

## Estimated Timeline (Full-Time Study)

| Week | Focus | Phases |
|---|---|---|
| 1 | Foundations + Ingestion | Setup, Phase 1, Phase 2 |
| 2 | DLT + Governance | Phase 3, Phase 4, Phase 5 |
| 3 | Production + Exam Prep | Phase 6, Phase 7, Exam Prep |
| 4 | Buffer / Retake Cushion | Review weak areas |

---

## Resources

- [Databricks Academy - Data Engineer Associate Learning Path](https://www.databricks.com/training/catalog/data-engineer-associate) (free)
- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Certified Data Engineer Associate Exam Guide](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Databricks Community Edition (Free)](https://community.cloud.databricks.com/)

---

## License

This project is for educational purposes. MIT License.
