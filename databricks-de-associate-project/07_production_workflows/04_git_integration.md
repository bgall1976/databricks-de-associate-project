# Git Integration with Databricks Repos

## Overview

Databricks Repos provides Git integration directly in the workspace. You can clone repositories, create branches, commit changes, and push/pull — all from within Databricks.

## Why This Matters for the Exam

**EXAM TIP:** The exam tests your understanding of:

1. How Repos enables version control for notebooks
2. How Repos supports CI/CD workflows
3. The difference between Repos and the legacy workspace

## Setting Up Repos

### Step 1: Connect a Git Provider

1. Go to **User Settings** > **Git Integration**
2. Select your provider (GitHub, GitLab, Bitbucket, Azure DevOps)
3. Add a personal access token

### Step 2: Clone This Repo

1. In the sidebar, click **Repos**
2. Click **Add Repo**
3. Paste the URL of your forked repository
4. Click **Create Repo**

### Step 3: Work with Branches

1. Click the branch name at the top of any file
2. Create a new branch for your changes
3. Make edits to notebooks
4. Commit and push from the Databricks UI

## CI/CD Workflow

A typical CI/CD workflow for Databricks:

```
Developer Branch          Main Branch           Production
     │                        │                      │
     ├── Edit notebooks       │                      │
     ├── Test locally         │                      │
     ├── Commit + Push        │                      │
     │                        │                      │
     ├── Create Pull Request ─┤                      │
     │                        ├── Code review        │
     │                        ├── Automated tests    │
     │                        ├── Merge              │
     │                        │                      │
     │                        ├── Deploy to prod ────┤
     │                        │                      ├── Run Workflows
```

## Exam Topics

### What Repos Provides

- Git version control for Databricks notebooks
- Branch-based development
- Pull request workflows
- Integration with CI/CD systems (GitHub Actions, Azure DevOps, etc.)

### What Repos Does NOT Provide

- It does not replace Workflows/Jobs for orchestration
- It does not manage cluster configuration
- It does not handle secrets or credentials

### Key Facts

1. Repos syncs notebooks and files from a Git repository
2. Changes in Repos are NOT automatically deployed to production
3. You still need Workflows/Jobs to schedule and run pipelines
4. Repos supports .py, .sql, .scala, .r, and .ipynb files
5. Non-notebook files (like configs and README.md) are also synced

## Exercise

1. Fork this GitHub repository
2. Clone it into your Databricks workspace via Repos
3. Create a new branch
4. Make a small change to any notebook
5. Commit and push the change
6. View the commit history in both Databricks and GitHub
