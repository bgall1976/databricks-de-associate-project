# Databricks Data Engineer Associate - Study Guide

## Exam Topic Checklist

Use this checklist to track your progress. Each topic maps to a specific project phase where you will implement it hands-on.

---

## Section 1: Databricks Lakehouse Platform (24%)

- [ ] **Describe the relationship between the data lakehouse and the data warehouse**
  - Project Phase: All (you are building a lakehouse throughout)
  - Key concept: Lakehouse = data lake storage + data warehouse management features

- [ ] **Identify the improvement in data quality in the data lakehouse over the data lake**
  - Project Phase: 01, 02
  - Key concept: ACID transactions, schema enforcement, schema evolution

- [ ] **Compare and contrast silver and gold tables, and identify how they are used in the medallion architecture**
  - Project Phase: 01, 02, 03
  - Key concept: Bronze = raw, Silver = cleaned/conformed, Gold = business-level aggregates

- [ ] **Identify Delta Lake as the format of all tables in the lakehouse**
  - Project Phase: 01
  - Key concept: Delta Lake = Parquet files + transaction log (_delta_log)

- [ ] **Describe the benefits of Delta Lake (ACID transactions, time travel, schema enforcement, etc.)**
  - Project Phase: 01, 06
  - Key concept: Know all Delta Lake features and when to use each

- [ ] **Identify the cloud providers that support Databricks**
  - Key concept: AWS, Azure, GCP

- [ ] **Describe the purpose of a cluster and identify key configuration options**
  - Project Phase: Setup
  - Key concept: All-purpose vs Job clusters, autoscaling, runtime versions

- [ ] **Identify the purpose of Databricks SQL and describe its key features**
  - Key concept: SQL-native analytics interface, SQL warehouses, dashboards

- [ ] **Describe multi-hop architecture and its benefits**
  - Project Phase: 01, 02, 03, 04
  - Key concept: Medallion architecture = multi-hop (Bronze → Silver → Gold)

---

## Section 2: ELT with Spark SQL and Python (29%)

- [ ] **Extract data from a single file and a directory of files**
  - Project Phase: 01
  - Key concept: `spark.read.json()`, `spark.read.csv()`, directory reads

- [ ] **Identify key features of Spark DataFrames including schema definition, filtering, and column manipulation**
  - Project Phase: 01, 02
  - Key concept: StructType, StructField, `select()`, `filter()`, `withColumn()`

- [ ] **Create a view or temporary view from a query**
  - Project Phase: 02, 03
  - Key concept: `CREATE TEMP VIEW`, `CREATE VIEW`, `createOrReplaceTempView()`

- [ ] **Identify the benefits of using Spark SQL over DataFrames**
  - Key concept: Readability, interoperability, same execution engine under the hood

- [ ] **Create a Delta table from a query**
  - Project Phase: 01, 02
  - Key concept: `CREATE TABLE AS SELECT (CTAS)`, `df.write.format("delta")`

- [ ] **Write data into a Delta table using INSERT, MERGE, and COPY INTO**
  - Project Phase: 02
  - Key concept: MERGE for upserts, INSERT OVERWRITE for full refresh, COPY INTO for idempotent loads

- [ ] **Identify a query that reads data from Delta Lake for a specific version or timestamp**
  - Project Phase: 06
  - Key concept: `VERSION AS OF`, `TIMESTAMP AS OF`, `@v` syntax

- [ ] **Create a SQL UDF**
  - Project Phase: 02
  - Key concept: `CREATE FUNCTION`, deterministic vs non-deterministic

- [ ] **Extract child entities from a complex data type (struct, array, map)**
  - Project Phase: 01, 02
  - Key concept: dot notation for structs, `explode()` for arrays, bracket notation for maps

---

## Section 3: Incremental Data Processing (16%)

- [ ] **Describe the benefits of using Auto Loader over COPY INTO**
  - Project Phase: 01
  - Key concept: Auto Loader scales better, handles schema evolution, uses file notification/listing

- [ ] **Identify how Auto Loader ingests data**
  - Project Phase: 01
  - Key concept: `cloudFiles` format, `cloudFiles.format`, checkpoint location

- [ ] **Describe the benefits and use cases of structured streaming**
  - Project Phase: 01, 04
  - Key concept: Near-real-time, exactly-once processing, trigger modes

- [ ] **Identify which operations are supported and unsupported in streaming**
  - Key concept: No sorting, no full outer joins, limited aggregations on unwindowed streams

- [ ] **Identify a scenario in which structured streaming is an appropriate solution**
  - Project Phase: 01
  - Key concept: Continuous or micro-batch ingestion from growing data sources

- [ ] **Identify a scenario in which Delta Live Tables is an appropriate solution**
  - Project Phase: 04
  - Key concept: Declarative pipelines with built-in quality enforcement

- [ ] **Identify the key features and benefits of Delta Live Tables**
  - Project Phase: 04
  - Key concept: Expectations, auto-scaling, lineage, simplified development

- [ ] **Describe the syntax for creating DLT tables and views**
  - Project Phase: 04
  - Key concept: `@dlt.table`, `@dlt.view`, `dlt.read()`, `dlt.read_stream()`

- [ ] **Identify the purpose and functionality of DLT expectations**
  - Project Phase: 04
  - Key concept: `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`

---

## Section 4: Production Pipelines (16%)

- [ ] **Identify how Databricks Jobs/Workflows are used in production**
  - Project Phase: 07
  - Key concept: Orchestrate notebooks, JARs, Python scripts as tasks

- [ ] **Identify the benefits of using multiple tasks in a Databricks Workflow**
  - Project Phase: 07
  - Key concept: Dependency management, parallel execution, error isolation

- [ ] **Identify the benefits of using Databricks Repos for CI/CD**
  - Project Phase: 07
  - Key concept: Git integration, branch-based development, pull requests

- [ ] **Set up alerts for pipeline failures or data quality issues**
  - Project Phase: 07
  - Key concept: Email notifications, webhook alerts, retry policies

- [ ] **Identify the benefits of using Job clusters vs All-Purpose clusters**
  - Project Phase: 07
  - Key concept: Job clusters are cheaper, auto-terminate, purpose-built

- [ ] **Describe how to parameterize notebooks with widgets**
  - Project Phase: 07
  - Key concept: `dbutils.widgets.text()`, `dbutils.widgets.get()`, `dbutils.widgets.dropdown()`

---

## Section 5: Data Governance (15%)

- [ ] **Describe the three-level namespace of Unity Catalog**
  - Project Phase: 05
  - Key concept: `catalog.schema.table`

- [ ] **Identify the purpose and benefits of Unity Catalog**
  - Project Phase: 05
  - Key concept: Centralized governance, fine-grained access control, data lineage, auditing

- [ ] **Grant and revoke access to data objects**
  - Project Phase: 05
  - Key concept: `GRANT SELECT ON TABLE ... TO ...`, `REVOKE`, `DENY`

- [ ] **Create dynamic views for column-level and row-level security**
  - Project Phase: 05
  - Key concept: `current_user()`, `is_member()` inside view definitions

- [ ] **Identify the benefits of data lineage in Unity Catalog**
  - Project Phase: 05
  - Key concept: Automated lineage tracking, impact analysis, regulatory compliance

- [ ] **Describe how to manage table access and permissions**
  - Project Phase: 05
  - Key concept: Table ACLs, catalog/schema-level grants, ownership

---

## Progress Tracker

| Section | Topics | Completed | Notes |
|---|---|---|---|
| Lakehouse Platform | 9 | _/9 | |
| ELT with Spark SQL/Python | 9 | _/9 | |
| Incremental Data Processing | 9 | _/9 | |
| Production Pipelines | 6 | _/6 | |
| Data Governance | 6 | _/6 | |
| **Total** | **39** | **_/39** | |
