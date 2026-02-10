# Phase 6: Delta Lake Optimization

## Learning Objectives

- Use OPTIMIZE and Z-ORDER to improve query performance
- Understand and use VACUUM to clean up old files
- Query historical data with time travel
- Analyze query plans for performance tuning

## Exam Topics Covered

- **Databricks Lakehouse Platform (24%):** Delta Lake features (OPTIMIZE, VACUUM, time travel)
- **ELT with Spark SQL and Python (29%):** Query optimization

## Key Concepts

### OPTIMIZE
Compacts small files into larger ones. Delta Lake accumulates many small files from streaming and frequent writes. OPTIMIZE merges them for faster reads.

### Z-ORDER
Co-locates related data within files based on specified columns. Dramatically speeds up queries that filter on those columns.

### VACUUM
Removes files older than the retention period (default 7 days). Required for storage cost management but prevents time travel beyond the retention period.

### Time Travel
Query previous versions of a table by version number or timestamp. Essential for auditing and recovering from bad writes.

## Notebooks

1. **01_optimize_zorder.sql** - OPTIMIZE and Z-ORDER commands
2. **02_vacuum_history.sql** - VACUUM, DESCRIBE HISTORY, time travel
3. **03_file_compaction.py** - File compaction strategies
4. **04_query_performance.sql** - Query plan analysis
