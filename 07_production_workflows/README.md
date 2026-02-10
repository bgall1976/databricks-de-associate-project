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
