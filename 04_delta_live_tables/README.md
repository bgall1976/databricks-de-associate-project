# Phase 4: Delta Live Tables (DLT)

## Learning Objectives

After completing this phase, you will be able to:

- Define DLT pipelines using the `@dlt.table` and `@dlt.view` decorators
- Use `dlt.read()` and `dlt.read_stream()` to reference upstream tables
- Implement data quality expectations with `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`
- Configure and deploy a DLT pipeline
- Understand streaming tables vs materialized views in DLT

## Exam Topics Covered

- **Incremental Data Processing (16%):** DLT syntax, expectations, streaming tables
- **Production Pipelines (16%):** DLT pipeline configuration and deployment

## Key Concepts

### DLT Table Types

| Type | Decorator | Use Case |
|---|---|---|
| Streaming table | `@dlt.table` with `dlt.read_stream()` | Incremental/append-only data |
| Materialized view | `@dlt.table` with `dlt.read()` | Aggregations, complete recomputation |
| View | `@dlt.view` | Intermediate transformations (not persisted) |

### Expectations (Data Quality)

```python
@dlt.expect("description", "constraint")              # Warn but keep record
@dlt.expect_or_drop("description", "constraint")      # Drop failing records
@dlt.expect_or_fail("description", "constraint")      # Fail the pipeline
```

**EXAM TIP:** Know which expectation to use:
- `expect` = log the violation, keep the row (monitoring)
- `expect_or_drop` = silently remove bad rows (quarantine)
- `expect_or_fail` = halt the pipeline (critical constraint)

## Important Notes

- DLT notebooks cannot be run interactively (they must be run as a DLT pipeline)
- DLT requires a trial workspace or paid workspace (not available on Community Edition)
- To test: create a DLT pipeline in the Workflows UI and attach these notebooks

## Notebooks

1. **01_dlt_bronze.py** - Bronze layer as DLT streaming tables
2. **02_dlt_silver.py** - Silver layer with expectations
3. **03_dlt_gold.py** - Gold layer as materialized views
4. **dlt_pipeline_config.json** - Pipeline configuration reference
