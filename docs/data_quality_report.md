# Data Quality Report

## 1. Overview

This document records data quality issues identified during source profiling and pipeline transformations. It documents their observations, investigation findings, potential impact, and handling decisions.

The report covers the Bronze, Silver, and Gold layers of the AMR data pipeline.

## 2. Data Quality Findings

### 2.1 DQ-001: Positive cultures with missing susceptibility information

**Dataset:** `microbiology_cultures_cohort`  
**Layer:** Bronze / Silver  
**Severity:** Informational  
**Status:** Investigated; source-level explanation unresolved

#### Scope

- Population: Positive culture records (`was_positive = true`).
- Total population: 1,610,252 rows.
- Affected population: 3,670 rows (0.228%).

#### Observation

Among 1,610,252 positive culture rows, 3,670 (0.228%) have missing susceptibility values.

The affected records fall into two patterns:

- 1,510 rows have missing organism, antibiotic, and susceptibility.
- 2,160 rows have organism and antibiotic values but missing susceptibility.

#### Distribution

- Respiratory cultures account for 1,096 of the 1,510 all-three-missing records.
- The missingness occurs across multiple culture years (2008–2024).
- Certain organism–antibiotic combinations account for recurring missing-susceptibility observations.

#### Treatment

- Preserve source records in Bronze.
- Normalize exact `"Null"` placeholders to SQL NULL in Silver.
- Retain affected records in Silver.
- Do not infer susceptibility or resistance from missing values.
- Exclude records without valid susceptibility observations from relevant susceptibility-based calculations, without removing them from the underlying datasets.

#### Limitation

The underlying reason for missing values has not been established.

Counts represent rows, not necessarily unique cultures or patients.

## 3. Data Quality Metrics

Summary of measurable data quality checks across datasets and pipeline layers.

Metrics may include:

- Missing and null-like values.
- Invalid or unparseable values.
- Duplicate records.
- Quarantined records.
- Referential integrity violations.
- Input/output reconciliation results.

## 4. Outstanding Issues

Issues that require further investigation or cannot yet be resolved.

## 5. Change History

| Date | Change |
|---|---|
| 2026-10-30 | Created report and documented DQ-001. |