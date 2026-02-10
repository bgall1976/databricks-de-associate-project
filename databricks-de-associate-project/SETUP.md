# Workspace Setup Guide

## Option 1: Databricks Community Edition (Free)

The Community Edition is sufficient for Phases 1-3 and Phase 6 (optimization). It does NOT support Delta Live Tables or Unity Catalog.

### Steps

1. Go to [https://community.cloud.databricks.com/](https://community.cloud.databricks.com/)
2. Sign up with your email
3. Create a cluster (Community Edition gives you a single-node cluster)
4. Import notebooks from this repo into your workspace

### Importing Notebooks

1. In the Databricks workspace, navigate to your user folder
2. Click "Import" and select "URL" or upload the `.py` / `.sql` files from this repo
3. Alternatively, connect this repo directly via Repos (Workspace > Repos > Add Repo)

### Uploading Data Files

1. Generate data locally using the scripts in `00_data_generation/`
2. In Databricks, go to "Data" in the sidebar
3. Upload files to DBFS: `/FileStore/ecommerce_project/raw/`
4. Update the file paths in the notebooks accordingly

---

## Option 2: Databricks Trial Workspace (14 days free)

A trial workspace on AWS, Azure, or GCP gives you access to ALL features including Delta Live Tables, Unity Catalog, and Workflows. This is recommended for the full project experience.

### AWS

1. Go to [https://www.databricks.com/try-databricks](https://www.databricks.com/try-databricks)
2. Select AWS and follow the setup wizard
3. The trial creates a workspace with a pre-configured AWS account

### Azure

1. Go to Azure Portal and search for "Azure Databricks"
2. Create a new workspace (Premium tier for Unity Catalog)
3. Or use the Databricks trial link above and select Azure

### GCP

1. Go to the Databricks trial link and select GCP
2. Follow the workspace creation wizard

### Post-Setup (Trial Workspace)

1. Enable Unity Catalog if not already enabled
2. Create a metastore and assign it to your workspace
3. Create an external storage location or use the default managed storage
4. Set up a cluster with the latest Databricks Runtime (13.3 LTS or newer)

---

## Storage Configuration

### For Community Edition

All data will be stored in DBFS (Databricks File System):

```
dbfs:/FileStore/ecommerce_project/
├── raw/
│   ├── orders/          # Raw JSON order files land here
│   ├── customers/       # Raw CSV customer files land here
│   └── products/        # Raw CSV product catalog lands here
├── checkpoints/         # Auto Loader checkpoint locations
├── bronze/              # Bronze Delta tables
├── silver/              # Silver Delta tables
└── gold/                # Gold Delta tables
```

### For Trial Workspace with Unity Catalog

Use a Unity Catalog managed location:

```
Catalog: ecommerce_project
├── Schema: bronze
│   ├── raw_orders
│   ├── raw_customers
│   └── raw_products
├── Schema: silver
│   ├── cleaned_orders
│   ├── cleaned_customers
│   └── enriched_orders
└── Schema: gold
    ├── daily_revenue
    ├── top_products
    ├── customer_lifetime_value
    └── regional_sales
```

---

## Cluster Configuration for the Exam

Understanding cluster configuration is an exam topic. Here is what to know:

### All-Purpose Clusters (Interactive)
- Used for development and exploration
- Stay running until manually terminated or idle timeout
- More expensive per hour
- Shared among multiple users/notebooks

### Job Clusters (Automated)
- Created when a job starts, terminated when it finishes
- Cheaper (no idle time charges)
- Dedicated to a single job run
- Best practice for production

### Recommended Cluster Settings for This Project
- **Runtime:** Databricks Runtime 13.3 LTS or newer
- **Node type:** Single node (Community Edition) or smallest available (trial)
- **Autoscaling:** Not available on Community Edition; enable on trial
- **Photon:** Enable if available (accelerates SQL workloads)

---

## Python Dependencies

The data generation scripts require the `faker` library. Install it locally:

```bash
pip install faker
```

No additional libraries are needed inside Databricks — PySpark and Delta Lake are pre-installed.

---

## Git Integration (Optional but Recommended)

### Connect This Repo to Databricks Repos

1. Fork this repo to your GitHub account
2. In Databricks: Workspace > Repos > Add Repo
3. Paste your fork URL
4. Databricks will sync the notebooks and files

This also prepares you for the CI/CD exam questions in Section 4.

---

## Verify Your Setup

Run this in a Databricks notebook to verify everything is working:

```python
# Verify Spark is running
print(f"Spark version: {spark.version}")

# Verify Delta Lake
spark.sql("CREATE TABLE IF NOT EXISTS default.setup_test (id INT) USING DELTA")
spark.sql("INSERT INTO default.setup_test VALUES (1)")
result = spark.sql("SELECT * FROM default.setup_test").collect()
print(f"Delta Lake test: {result}")
spark.sql("DROP TABLE default.setup_test")

# Verify DBFS access
dbutils.fs.ls("dbfs:/FileStore/")
print("DBFS access: OK")

print("\nSetup verified successfully!")
```
