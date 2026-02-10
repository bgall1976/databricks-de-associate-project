# Phase 5: Data Governance with Unity Catalog

## Learning Objectives

- Set up Unity Catalog's three-level namespace (catalog.schema.table)
- Grant and revoke permissions on data objects
- Create dynamic views for PII masking
- Explore data lineage in the Databricks UI

## Exam Topics Covered

- **Data Governance (15%):** Unity Catalog, permissions, dynamic views, lineage

## Prerequisites

Unity Catalog requires a trial or paid Databricks workspace. It is NOT available on Community Edition. If you are using Community Edition, read through these notebooks to understand the concepts and syntax, then apply them when you get access to a trial workspace.

## Key Concepts

### Three-Level Namespace

```
catalog.schema.table

Example: ecommerce_project.silver.cleaned_orders
```

**EXAM TIP:** This is the fundamental Unity Catalog structure. Every data object lives inside a catalog and schema.

### Permission Model

- `GRANT` gives access
- `REVOKE` removes access
- Permissions cascade: catalog grants apply to all schemas and tables within
- `OWNERSHIP` is the highest privilege

### Dynamic Views for PII

Dynamic views use functions like `current_user()` and `is_member()` to show different data to different users. This is how you implement column-level and row-level security.

## Notebooks

1. **01_unity_catalog_setup.sql** - Create catalog, schemas, and managed tables
2. **02_access_control.sql** - Grant and revoke permissions
3. **03_dynamic_views_pii.sql** - PII masking with dynamic views
4. **04_data_lineage.md** - Guide to exploring lineage in the Databricks UI
