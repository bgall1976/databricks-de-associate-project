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

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** A data engineer grants `SELECT` on a table to a user, but the user still gets a "permission denied" error when querying the table. The three-level namespace is `ecommerce.silver.cleaned_orders`. What is the most likely cause?

A) The user doesn't have `SELECT` on the table  
B) The user is missing `USAGE` on the catalog (`ecommerce`) and/or schema (`silver`)  
C) The table doesn't exist  
D) The user's cluster doesn't have Unity Catalog enabled  

<details><summary>Answer</summary>

**B** — In Unity Catalog, `USAGE` on parent objects (catalog and schema) is required before any table-level privilege takes effect. Even with `SELECT` on the table, the user cannot access it without `USAGE` on both the catalog and the schema. This is one of the most commonly tested Unity Catalog concepts.

</details>

---

**Scenario 2:** A healthcare company needs to ensure that only members of the `compliance_team` group can see patient `ssn` values. All other users should see masked values (e.g., `***-**-****`). How should the data engineer implement this?

A) Create a separate table for the compliance team without the SSN column  
B) Use column-level encryption on the SSN column  
C) Create a dynamic view with `CASE WHEN is_member('compliance_team') THEN ssn ELSE '***-**-****' END`  
D) Grant `SELECT` on the SSN column only to the compliance team  

<details><summary>Answer</summary>

**C** — Dynamic views with `is_member()` are the primary mechanism for implementing column-level security in Unity Catalog. The view conditionally reveals or masks the sensitive column based on the querying user's group membership. This allows a single view to serve multiple user groups with different data visibility.

</details>

---

**Scenario 3:** A data engineer needs to set up a data governance structure for a new project. They need a catalog called `retail`, a schema called `bronze`, and a managed table called `raw_orders`. What is the correct order of SQL statements?

A) `CREATE TABLE retail.bronze.raw_orders` → `CREATE SCHEMA bronze` → `CREATE CATALOG retail`  
B) `CREATE CATALOG retail` → `CREATE SCHEMA retail.bronze` → `CREATE TABLE retail.bronze.raw_orders`  
C) `CREATE SCHEMA retail.bronze` → `CREATE CATALOG retail` → `CREATE TABLE retail.bronze.raw_orders`  
D) `CREATE CATALOG retail` → `CREATE TABLE retail.bronze.raw_orders` (schema is created automatically)  

<details><summary>Answer</summary>

**B** — Objects must be created top-down: catalog first, then schema within the catalog, then table within the schema. You cannot create a schema without its parent catalog, and you cannot create a table without its parent schema. This follows the three-level namespace hierarchy.

</details>

---

**Scenario 4:** A company has a `customers` table containing both PII and non-PII columns. The analytics team needs access to non-PII columns (e.g., `region`, `signup_date`), while the data governance team needs access to everything (including `email`, `phone`). What is the best approach?

A) Create two copies of the table with different columns  
B) Create a dynamic view that masks PII columns using `is_member()` and grant access to the view  
C) Grant `SELECT` on individual columns to different groups  
D) Use row-level security to filter by user  

<details><summary>Answer</summary>

**B** — A dynamic view is the recommended approach. It uses `CASE WHEN is_member('data_governance') THEN email ELSE '***' END` to conditionally show or mask PII. Both teams query the same view, but see different data based on their group membership. This avoids data duplication and ensures consistent access policies.

</details>

---

**Scenario 5:** A data engineer wants to understand how data flows from Bronze to Silver to Gold in their lakehouse. They want to see which tables depend on which upstream sources. Which Unity Catalog feature provides this?

A) DESCRIBE DETAIL  
B) DESCRIBE HISTORY  
C) Data Lineage (in the Databricks UI)  
D) SHOW TABLES  

<details><summary>Answer</summary>

**C** — Unity Catalog automatically tracks data lineage, showing upstream and downstream dependencies between tables, views, and notebooks. This is visible in the Databricks UI under the table's Lineage tab. `DESCRIBE DETAIL` shows table metadata, and `DESCRIBE HISTORY` shows operation history — neither shows cross-table dependencies.

</details>

---

**Scenario 6:** A data engineer creates a managed table in Unity Catalog and later runs `DROP TABLE`. What happens to the underlying data files?

A) Data files are preserved; only metadata is removed  
B) Both metadata and data files are deleted  
C) Data files are moved to a recycle bin  
D) Data files are preserved for 7 days then deleted  

<details><summary>Answer</summary>

**B** — For managed tables (tables created without a `LOCATION` clause), `DROP TABLE` deletes both the metadata and the underlying data files. For external tables (created with a `LOCATION` clause), `DROP TABLE` only removes the metadata — the data files remain. This distinction is heavily tested on the exam.

</details>

---

**Scenario 7:** A data engineer needs to restrict access so that users in the `marketing` group can only see rows where `region = 'US'` in the `customers` table. Which approach implements row-level security?

A) Create a dynamic view: `CREATE VIEW us_customers AS SELECT * FROM customers WHERE CASE WHEN is_member('marketing') THEN region = 'US' ELSE TRUE END`  
B) Use `GRANT SELECT ON TABLE customers WHERE region = 'US' TO marketing`  
C) Create a dynamic view: `CREATE VIEW customers_secure AS SELECT * FROM customers WHERE is_member('marketing') AND region = 'US' OR is_member('admin')`  
D) Create row-level policies with `ALTER TABLE`  

<details><summary>Answer</summary>

**C** — Dynamic views can implement row-level security by using `is_member()` in the `WHERE` clause to control which rows different groups can see. The view filters rows based on the querying user's group membership. Option B is not valid SQL syntax. Option A's CASE WHEN logic is incorrect in a WHERE clause for this purpose.

</details>
