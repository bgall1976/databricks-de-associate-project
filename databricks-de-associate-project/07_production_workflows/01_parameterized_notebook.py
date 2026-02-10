# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Parameterized Notebooks
# MAGIC
# MAGIC **Objective:** Use `dbutils.widgets` to make notebooks configurable.
# MAGIC
# MAGIC **EXAM TIP:** Know all widget types and how to get/set values.
# MAGIC Widgets allow the same notebook to be reused for different environments,
# MAGIC date ranges, or configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Types
# MAGIC
# MAGIC | Widget | Method | Use Case |
# MAGIC |---|---|---|
# MAGIC | Text | `dbutils.widgets.text()` | Free-form input |
# MAGIC | Dropdown | `dbutils.widgets.dropdown()` | Single selection from list |
# MAGIC | Combobox | `dbutils.widgets.combobox()` | Dropdown with free-form option |
# MAGIC | Multiselect | `dbutils.widgets.multiselect()` | Multiple selections |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Widgets

# COMMAND ----------

# EXAM TIP: Know this syntax for creating and retrieving widget values

# Text widget for environment
dbutils.widgets.text("environment", "dev", "Environment")

# Dropdown widget for processing mode
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full_refresh"], "Processing Mode")

# Text widget for date
dbutils.widgets.text("process_date", "2025-01-01", "Process Date")

# Dropdown for target layer
dbutils.widgets.dropdown("target_layer", "silver", ["bronze", "silver", "gold"], "Target Layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve Widget Values

# COMMAND ----------

# EXAM TIP: dbutils.widgets.get() retrieves the current value
environment = dbutils.widgets.get("environment")
mode = dbutils.widgets.get("mode")
process_date = dbutils.widgets.get("process_date")
target_layer = dbutils.widgets.get("target_layer")

print(f"Environment: {environment}")
print(f"Mode: {mode}")
print(f"Process Date: {process_date}")
print(f"Target Layer: {target_layer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Parameters in Logic

# COMMAND ----------

# Configure paths based on environment
base_paths = {
    "dev": "dbfs:/FileStore/ecommerce_project",
    "staging": "dbfs:/mnt/staging/ecommerce_project",
    "prod": "dbfs:/mnt/prod/ecommerce_project",
}

base_path = base_paths.get(environment, base_paths["dev"])
source_path = f"{base_path}/bronze/raw_orders"
target_path = f"{base_path}/{target_layer}/cleaned_orders"

print(f"Source: {source_path}")
print(f"Target: {target_path}")

# COMMAND ----------

# Choose processing mode
if mode == "full_refresh":
    print("Running full refresh: overwriting target table")
    write_mode = "overwrite"
elif mode == "incremental":
    print(f"Running incremental: processing data for {process_date}")
    write_mode = "append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up Widgets (Optional)

# COMMAND ----------

# Remove a specific widget
# dbutils.widgets.remove("environment")

# Remove all widgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passing Parameters from Workflows
# MAGIC
# MAGIC When this notebook is called from a Workflow (Job), parameters are passed
# MAGIC as key-value pairs in the task configuration. The widget values are
# MAGIC automatically set to the passed parameters.
# MAGIC
# MAGIC Example Workflow task configuration:
# MAGIC ```json
# MAGIC {
# MAGIC     "notebook_task": {
# MAGIC         "notebook_path": "/path/to/this/notebook",
# MAGIC         "base_parameters": {
# MAGIC             "environment": "prod",
# MAGIC             "mode": "incremental",
# MAGIC             "process_date": "{{job.trigger_time.iso_date}}"
# MAGIC         }
# MAGIC     }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Exam Takeaways
# MAGIC
# MAGIC 1. `dbutils.widgets.text(name, default, label)` creates a text widget
# MAGIC 2. `dbutils.widgets.dropdown(name, default, choices, label)` creates a dropdown
# MAGIC 3. `dbutils.widgets.get(name)` retrieves the current value
# MAGIC 4. `dbutils.widgets.remove(name)` removes a specific widget
# MAGIC 5. `dbutils.widgets.removeAll()` removes all widgets
# MAGIC 6. Workflow tasks pass parameters that override widget defaults
