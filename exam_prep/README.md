# Exam Preparation

## Exam Overview

- **Name:** Databricks Certified Data Engineer Associate
- **Duration:** 90 minutes
- **Questions:** 45 multiple-choice
- **Passing Score:** 70%
- **Cost:** $200 (includes one free retake)
- **Validity:** 2 years

## Preparation Strategy

### Week Before the Exam

1. Complete all project phases (or at minimum, read through all notebooks)
2. Review the [STUDY_GUIDE.md](../STUDY_GUIDE.md) checklist — any unchecked items need attention
3. Work through the **scenario questions** in each phase README — these simulate the actual exam format
4. Work through all 45 practice questions in `practice_questions.md`
5. Review the cheatsheet for quick recall
6. Study the "When in Doubt" rules in `common_mistakes.md` (Section 21)
7. Take the official Databricks practice exam

### Day of the Exam

1. Review the cheatsheet one more time
2. Remember: when in doubt, choose the **Databricks-native** solution
3. Read every question carefully — many are **scenario-based** with tricky wording
4. Most questions describe a situation and ask "what should the data engineer do?" — think about the Databricks-recommended approach
5. Flag difficult questions and come back to them
6. You have 2 minutes per question — don't rush but don't linger

## Exam Question Format

The actual exam heavily uses **scenario-based questions**. Instead of asking "What is X?", the exam typically asks:

> *"A data engineer needs to [do something]. The current situation is [context]. Which approach should they use?"*

This repo includes scenario questions in two places:
1. **Each phase README** — 4-8 scenario questions specific to that phase's topics
2. **practice_questions.md** — 45 questions covering all domains, including 15 scenario-based questions

**Total scenario questions across all READMEs: 50+**

## Files in This Folder

1. **practice_questions.md** - 45 practice questions covering all exam domains (including 15 scenario-based questions in Section 6)
2. **key_concepts_cheatsheet.md** - Quick reference for all major topics
3. **common_mistakes.md** - 21 frequently missed topics, tricky question patterns, and "when in doubt" exam rules

## Scenario Questions by Phase

Each phase README contains exam-style scenario questions relevant to its topic:

| Phase | README Location | Scenario Questions | Key Topics |
|---|---|---|---|
| Phase 0 | `00_data_generation/README.md` | 4 questions | Raw data formats, Bronze layer philosophy, Auto Loader basics |
| Phase 1 | `01_bronze_ingestion/README.md` | 6 questions | Auto Loader vs COPY INTO, triggers, schema evolution, metadata |
| Phase 2 | `02_silver_transformation/README.md` | 7 questions | MERGE, CTAS, struct/array/map access, UDFs, INSERT OVERWRITE |
| Phase 3 | `03_gold_aggregation/README.md` | 5 questions | Gold layer purpose, aggregations, window functions, DLT materialized views |
| Phase 4 | `04_delta_live_tables/README.md` | 7 questions | DLT expectations, streaming vs materialized views, DLT views, interactive execution |
| Phase 5 | `05_data_governance/README.md` | 7 questions | Unity Catalog namespace, USAGE, dynamic views, PII masking, lineage |
| Phase 6 | `06_optimization/README.md` | 8 questions | OPTIMIZE, Z-ORDER, VACUUM, time travel, RESTORE, auto compaction |
| Phase 7 | `07_production_workflows/README.md` | 8 questions | Workflows, job clusters, widgets, notebook orchestration, Repos |
