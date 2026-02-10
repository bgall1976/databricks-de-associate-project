# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Error Handling Patterns
# MAGIC
# MAGIC **Objective:** Implement error handling for production pipelines.
# MAGIC
# MAGIC **Exam Topics:** dbutils.notebook.run(), exit values, retry logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils.notebook.run()
# MAGIC
# MAGIC **EXAM TIP:** `dbutils.notebook.run()` calls another notebook and returns its exit value.
# MAGIC This is how you orchestrate notebooks programmatically (alternative to Workflows UI).

# COMMAND ----------

# Run another notebook with a timeout of 300 seconds
# result = dbutils.notebook.run(
#     "/path/to/notebook",
#     timeout_seconds=300,
#     arguments={"environment": "prod", "mode": "incremental"}
# )
# print(f"Notebook returned: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils.notebook.exit()
# MAGIC
# MAGIC **EXAM TIP:** `exit()` returns a value to the calling notebook or Workflow.

# COMMAND ----------

# In a called notebook, return a status:
# dbutils.notebook.exit("SUCCESS")
# or
# dbutils.notebook.exit(json.dumps({"status": "success", "rows_processed": 1000}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling with Try/Except

# COMMAND ----------

import json

def run_pipeline_step(notebook_path, params, timeout=600):
    """Run a notebook with error handling."""
    try:
        result = dbutils.notebook.run(notebook_path, timeout, params)
        result_data = json.loads(result) if result else {}
        print(f"SUCCESS: {notebook_path} -> {result_data}")
        return result_data
    except Exception as e:
        print(f"FAILED: {notebook_path} -> {str(e)}")
        # In production: send alert, log to monitoring table, etc.
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate Multiple Notebooks

# COMMAND ----------

# Example: run the full pipeline with error handling
# pipeline_steps = [
#     ("/path/to/bronze_ingestion", {"mode": "incremental"}),
#     ("/path/to/silver_transform", {"mode": "incremental"}),
#     ("/path/to/gold_aggregate", {}),
# ]
#
# for notebook_path, params in pipeline_steps:
#     run_pipeline_step(notebook_path, params)
#
# print("Pipeline completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Exam Takeaways
# MAGIC
# MAGIC 1. `dbutils.notebook.run(path, timeout, arguments)` runs another notebook
# MAGIC 2. `dbutils.notebook.exit(value)` returns a string value to the caller
# MAGIC 3. The `arguments` parameter is a dict of key-value pairs (overrides widgets)
# MAGIC 4. If the called notebook fails, an exception is raised in the caller
# MAGIC 5. Timeout is in seconds; exceeding it raises a timeout exception
# MAGIC 6. In production, prefer Workflows UI over programmatic orchestration
