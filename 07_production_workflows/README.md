# Phase 7: Production Workflows

## Learning Objectives

- Parameterize notebooks with `dbutils.widgets`
- Create multi-task Databricks Workflows (Jobs)
- Configure retry policies and alerting
- Connect Git repos for CI/CD

## Exam Topics Covered

- **Production Pipelines (16%):** Workflows/Jobs, parameterized notebooks, Job clusters, CI/CD

## Key Concepts

### Job Clusters vs All-Purpose Clusters

| Feature | Job Cluster | All-Purpose Cluster |
|---|---|---|
| Lifecycle | Created per job, auto-terminated | Long-running |
| Cost | Cheaper (no idle time) | More expensive |
| Use case | Production pipelines | Development |
| Sharing | Single job only | Multiple users |

**EXAM TIP:** Job clusters are always the correct answer for production workloads.

### Workflow Tasks

A Workflow (Job) can have multiple tasks with dependencies:
- Task A: Ingest data (Bronze)
- Task B: Transform data (Silver) — depends on A
- Task C: Aggregate data (Gold) — depends on B
- Task D: Run quality checks — depends on B and C

## Notebooks

1. **01_parameterized_notebook.py** - Using dbutils.widgets for parameters
2. **02_workflow_config.json** - Workflow/Job definition
3. **03_error_handling.py** - Error handling and retry patterns
4. **04_git_integration.md** - Guide to Databricks Repos and CI/CD

---

## 📝 Exam Scenario Questions & Answers

These scenario-style questions reflect the types of questions likely to appear on the Databricks Data Engineer Associate certification exam.

---

**Scenario 1:** A data engineering team needs to run a nightly pipeline that executes three notebooks in sequence: Bronze ingestion → Silver transformation → Gold aggregation. If any step fails, the subsequent steps should not execute. Which Databricks feature should they use?

A) Schedule each notebook independently with separate cron jobs  
B) Create a Databricks Workflow (Job) with three tasks and linear dependencies  
C) Use `dbutils.notebook.run()` to chain the notebooks from a single orchestrator notebook  
D) Both B and C work; B is the recommended approach  

<details><summary>Answer</summary>

**D** — Both approaches work, but Databricks Workflows (Jobs) are the recommended approach for production. Workflows provide built-in dependency management, retry policies, alerting, and monitoring through the Jobs UI. `dbutils.notebook.run()` works for simple orchestration but lacks the production features of Workflows. For the exam, Workflows/Jobs is the expected answer.

</details>

---

**Scenario 2:** A production ETL pipeline runs on a Databricks cluster. The team wants to minimize costs while ensuring the pipeline completes reliably each night. Which cluster type should they use?

A) All-purpose cluster shared by the whole team  
B) A dedicated job cluster that auto-terminates after the job completes  
C) A SQL warehouse  
D) An interactive cluster with autoscaling  

<details><summary>Answer</summary>

**B** — Job clusters are created when a job starts and automatically terminated when it finishes. They are significantly cheaper than all-purpose clusters because there's no idle time. For the exam, job clusters are always the correct answer for production workloads. All-purpose clusters are for interactive development.

</details>

---

**Scenario 3:** A data engineer needs to create a notebook that can be reused for different environments (dev, staging, prod) by changing the target database name at runtime. How should they parameterize the notebook?

A) Hard-code the database name and change it before each run  
B) Use `dbutils.widgets.text("database", "dev_db")` and retrieve it with `dbutils.widgets.get("database")`  
C) Use environment variables with `os.environ`  
D) Use Spark configuration with `spark.conf.set()`  

<details><summary>Answer</summary>

**B** — `dbutils.widgets` is the Databricks-native way to parameterize notebooks. Widgets create input fields in the notebook UI and can receive values from Workflows or `dbutils.notebook.run()`. This is the standard and exam-expected approach for parameterized notebooks. The widget value is retrieved with `dbutils.widgets.get("name")`.

</details>

---

**Scenario 4:** A data engineer uses `dbutils.notebook.run("/path/to/child_notebook", 300, {"date": "2024-01-15"})` to call a child notebook. The child notebook processes data and wants to return the record count to the parent. How does the child notebook return this value?

A) `return record_count`  
B) `dbutils.notebook.exit(str(record_count))`  
C) `dbutils.widgets.set("result", str(record_count))`  
D) The child notebook cannot return values to the parent  

<details><summary>Answer</summary>

**B** — `dbutils.notebook.exit(value)` passes a string value back to the calling notebook. The value is returned as the result of `dbutils.notebook.run()` in the parent. Note that the value must be a string — if returning a number, convert it with `str()`. The parent receives it as: `result = dbutils.notebook.run(...)`.

</details>

---

**Scenario 5:** A data engineering team wants to implement version control for their Databricks notebooks. They need to collaborate using branches, pull requests, and code reviews — the same workflow they use for application code. Which Databricks feature enables this?

A) Databricks Notebooks export/import  
B) Databricks Repos (Git integration)  
C) Databricks CLI  
D) Databricks Connect  

<details><summary>Answer</summary>

**B** — Databricks Repos provides native Git integration within the Databricks workspace. Engineers can clone repositories, create branches, commit changes, and sync with remote repositories (GitHub, GitLab, Azure DevOps, etc.) directly from the Databricks UI. This enables standard CI/CD workflows for notebook development.

</details>

---

**Scenario 6:** A Databricks Workflow has three tasks: Task A (ingestion), Task B (transformation, depends on A), and Task C (quality checks, depends on A). Tasks B and C have no dependency on each other. How does Databricks execute these tasks?

A) A → B → C (all sequential)  
B) A runs first, then B and C run in parallel  
C) A, B, and C all run in parallel  
D) The execution order is random  

<details><summary>Answer</summary>

**B** — Databricks Workflows respect the declared dependencies. Since B and C both depend on A but not on each other, they run in parallel after A completes. This is a key benefit of multi-task Workflows — tasks without mutual dependencies execute concurrently, reducing total pipeline runtime.

</details>

---

**Scenario 7:** A production pipeline fails intermittently due to transient network errors when reading from an external API. The data engineer wants the pipeline to automatically retry failed tasks up to 3 times before marking the job as failed. How should they configure this?

A) Add try/except blocks in the notebook code with a retry loop  
B) Configure retry policies in the Databricks Workflow/Job task settings  
C) Use `dbutils.notebook.run()` in a loop  
D) Both A and B work; B is preferred for production  

<details><summary>Answer</summary>

**D** — Both approaches work. Workflow-level retry policies (configured in the Job/task settings) are the preferred approach because they're declarative, visible in the Jobs UI, and don't require code changes. Code-level retries (try/except) provide finer-grained control for specific operations within a notebook. For the exam, Workflow retry configuration is the expected answer.

</details>

---

**Scenario 8:** A data engineer needs to set up a Databricks Workflow that runs a DLT pipeline to refresh data, then triggers a SQL dashboard refresh. Which configuration is correct?

A) Create a Workflow with two tasks: Task 1 (DLT pipeline task) → Task 2 (SQL task)  
B) Put both operations in a single notebook  
C) Schedule two separate Workflows with the same cron schedule  
D) Use `dbutils.notebook.run()` to trigger the dashboard refresh after DLT  

<details><summary>Answer</summary>

**A** — Databricks Workflows support multiple task types including DLT pipeline tasks, notebook tasks, SQL tasks, and more. Creating a multi-task Workflow with explicit dependencies (Task 2 depends on Task 1) ensures the dashboard refreshes only after the DLT pipeline succeeds. This is the production-ready, exam-expected approach.

</details>
