
# Transformation and Data Quality Rules

## 1. Purpose

This document defines the proposed transformation rules, data-quality checks,
validation behavior, and data-integrity requirements for the AMR Data Pipeline.

The objective is to transform the selected ARMD source tables into standardized,
validated, traceable datasets that can support antimicrobial resistance (AMR)
analysis.

These rules are based on the initial source profiling and grain validation
documented in `source_profiling_report.md`.

The rules are a design specification. They will be implemented and tested
incrementally as the Bronze, Silver, and Gold pipeline components are built.

Where the meaning of a source field or repeated observation has not been
established, this document records the uncertainty rather than assuming a
clinical interpretation.

## 2. Scope

### 2.1 Clinical and event source tables

The pipeline currently includes the following eight source tables:

1. `microbiology_cultures_cohort.csv`
2. `microbiology_culture_prior_infecting_organism.csv`
3. `microbiology_cultures_prior_med.csv`
4. `microbiology_cultures_priorprocedures.csv`
5. `microbiology_cultures_antibiotic_class_exposure.csv`
6. `microbiology_cultures_antibiotic_subtype_exposure.csv`
7. `microbiology_cultures_microbial_resistance.csv`
8. `microbiology_cultures_implied_susceptibility.csv`

The supporting reference table is:

- `implied_susceptibility_rules.csv`

The reference table will be validated and standardized separately from the
clinical and event tables.

### 2.2 Processing layers

The pipeline follows a medallion architecture.

- **Bronze:** preserves source data and ingestion metadata.
- **Silver:** standardizes, validates, and prepares source records for analysis.
- **Gold:** contains analysis-ready datasets and aggregated metrics.

This document focuses on Bronze ingestion requirements and Silver
transformations and quality checks. Gold-specific metric definitions will be
documented separately.

## 3. General Transformation Principles

### TR-001: Preserve source data

Bronze must retain the ingested source records without applying business-level
deduplication, category remapping, or analytical filtering.

The pipeline should retain sufficient ingestion metadata to identify the source
file and pipeline run associated with each ingested dataset.

Recommended metadata includes:

- Source filename
- Ingestion timestamp
- Pipeline run identifier
- Source row count
- Source file checksum, where practical

Source column names and original values must remain recoverable from Bronze.

### TR-002: Define explicit schemas

Each source table must have a documented expected schema.

The schema should specify:

- Column name
- Expected data type
- Whether nulls are permitted
- Whether the field is required for a particular transformation
- Any applicable validation rule

Do not rely indefinitely on automatic type inference when reading CSV files.

Unexpected missing or additional columns must be detected and handled
explicitly. Schema changes should not silently alter downstream outputs.

### TR-003: Standardize column names in Silver

Silver will use consistent, documented column names.

Examples include:

- `pat_enc_csn_id_coded` -> `encounter_id`
- `order_proc_id_coded` -> `culture_order_source_id`
- `order_time_jittered_utc` -> `culture_time`
- `procedure_description` -> `procedure_name`
- `antibiotic_subtype` -> `subtype`
- `antibiotic_subtype_category` -> `subtype_category`

The source identifier is initially named `culture_order_source_id` here to
distinguish it from the proposed surrogate primary key `culture_order_sk`.

The final naming convention should be applied consistently across the modeled
datasets and their documentation.

Every rename must be recorded in the column mapping documentation.

If a source column contains a spelling error, preserve the original name in
Bronze and apply a documented standardized name in Silver.

For example, the profiled prior-infecting-organism table contains the source
column `prior_infecting_organism_days_to_culutre`. Confirm the exact source
spelling before implementing its standardized mapping.

### TR-004: Preserve source identifiers

Source identifiers must be retained as attributes even when surrogate keys are
introduced.

Do not assume that an identifier uniquely identifies every row in a table
simply because it identifies a culture order.

The source identifier `order_proc_id_coded` repeats across result and event
records. This is consistent with a one-to-many relationship between a culture
order and its associated observations.

### TR-005: Parse timestamps explicitly

Parse `order_time_jittered_utc` into the chosen Silver timestamp type using an
explicit format and timezone policy.

The standardized field will be named `culture_time`.

The transformation must:

- Handle the source's timezone-aware timestamp representation.
- Detect values that fail parsing.
- Avoid silently replacing invalid timestamps with arbitrary defaults.
- Preserve enough source information to investigate parsing failures.
- Document the timestamp type and timezone convention used in Silver.

The source timestamps are jittered for privacy preservation. Analyses must not
present them as exact real-world event times.

### TR-006: Standardize text values conservatively

Text fields may be normalized for whitespace and consistent representation
where appropriate.

Case normalization should be applied only where justified by the intended
analytical use.

Do not automatically merge values that appear similar but may represent
different source categories, organism names, antibiotics, or susceptibility
outcomes.

Any category mapping must be documented and reproducible.

### TR-007: Parse numeric fields explicitly

Fields representing intervals or numeric measurements must be converted to
their intended Silver data types using explicit parsing rules.

Examples include:

- `time_to_culturetime`
- `medication_time_to_culturetime`
- `procedure_time_to_culturetime`
- `prior_infecting_organism_days_to_culutre`
- `resistant_time_to_culturetime`

Standardized names may be used in Silver, including `days_before_culture`,
`days_since_prior_procedure`, and `days_since_prior_infection`, where their
meaning is supported by the source documentation.

The direction and unit of each interval must be confirmed before applying
clinical or temporal interpretation.

Do not assume that a field represents elapsed days before a culture merely
because its name suggests that interpretation.

### TR-008: Distinguish missing-value representations

The pipeline must distinguish among:

- Actual null values
- Empty or whitespace-only strings
- Literal strings such as `"Null"`
- Explicit categorical values such as `"No Interpretation"` or `"See Comment"`

A literal `"Null"` is not automatically equivalent to a missing value.

Where a source-specific mapping is approved, normalize the relevant values
into a documented Silver representation while preserving the original source
value in Bronze.

The mapping must be applied consistently across relevant tables.

### TR-009: Preserve observed and implied susceptibility separately

The `susceptibility` and `implied_susceptibility` fields must remain distinct.

Observed susceptibility must not be overwritten with an inferred value.

The pipeline must retain enough information to distinguish a measured or
reported susceptibility outcome from an implied susceptibility outcome.

Any mapping of source categories must be documented, including the treatment
of values such as:

- `Susceptible`
- `Resistant`
- `Intermediate`
- `Non Susceptible`
- `Susceptible - Dose Dependent`
- `No Interpretation`
- `See Comment`
- `See Levofloxacin Result`
- `Not done`
- `Negative`
- `Positive`
- `Not Detected`
- `Detected`
- Literal `"Null"`

These values must not be combined into broader categories without an explicit,
justified analytical rule.

### TR-010: Preserve transformation traceability

Every pipeline run should record:

- Run identifier
- Start and completion timestamps
- Source and output datasets
- Input and output row counts
- Number of records rejected or quarantined
- Number of exact duplicate rows removed, where applicable
- Data-quality rule results
- Task status
- Failure details, when applicable
- Transformation duration

Quality-check results must be available for inspection through task logs or
structured run metadata.

## 4. Culture Order Identity and Surrogate Keys

### 4.1 Rationale

The source identifier `order_proc_id_coded` identifies a culture order in the
source documentation, but it is not unique at the individual observation
level in the cohort or related event tables.

For example, one culture order may have multiple organisms, antibiotic
susceptibility results, prior medication records, exposure records, or
resistance-history observations.

The pipeline therefore needs to distinguish the identity of a culture order
from the identity of an observation associated with that order.

### 4.2 Proposed culture order entity

Create a modeled `culture_order` dataset or relational table containing one
record per validated distinct source culture order.

Proposed fields include:

- `culture_order_sk`: surrogate primary key
- `culture_order_source_id`: original source order identifier
- `anon_id`: de-identified patient identifier
- `encounter_id`: standardized encounter identifier
- `culture_time`: standardized jittered order timestamp

Additional order-level attributes may be added when their meaning and
consistency are established.

### 4.3 Surrogate-key requirements

The surrogate key `culture_order_sk` must:

- Be non-null.
- Be unique within the modeled culture-order entity.
- Identify one validated culture order.
- Remain stable across pipeline reruns when the same source order is
  encountered again, provided the identity mapping has not changed.
- Be mapped to the source identifier through a documented lookup or mapping
  strategy.

A simple sequential identifier regenerated independently on every run is not
sufficient if downstream foreign keys must remain stable.

For a local batch implementation, the project may maintain a deterministic
source-ID-to-surrogate-key mapping or a persistent key table. The final
approach must be chosen before implementing incremental processing.

### 4.4 Validate source order identity before assigning keys

Before treating `order_proc_id_coded` as a reliable source order identifier,
check that its mapping to expected order-level attributes is consistent.

At minimum, examine:

- Whether a source order identifier maps to multiple `anon_id` values.
- Whether it maps to multiple `pat_enc_csn_id_coded` values.
- Whether it maps to multiple `order_time_jittered_utc` values.
- Whether these mappings remain consistent across source tables.

A repeated order identifier across multiple rows is expected and is not itself
a failure.

Conflicting order-level attributes must be reported and investigated before
the affected source order is assigned an unambiguous modeled identity.

Do not silently choose an arbitrary patient, encounter, or timestamp when
conflicting mappings are found.

### 4.5 Foreign keys in related datasets

After the source order mapping is validated, related Silver datasets may use
`culture_order_sk` to reference the modeled culture order.

Examples include:

- Cohort result observations
- Prior medication observations
- Prior infecting organism observations
- Prior procedure observations
- Antibiotic class exposure observations
- Antibiotic subtype exposure observations
- Microbial resistance-history observations
- Implied susceptibility observations

The source identifier should remain available for lineage and reconciliation.

### 4.6 Separate observation identifiers

The culture-order surrogate key must not be used as a unique identifier for
individual cohort result rows or exposure observations.

Where a result or event table requires a primary key, introduce a separate
record identifier appropriate to that table.

For example, `cohort_result_sk` may identify individual cohort result records.

A surrogate record identifier provides a stable modeled identifier; it does
not prove that two otherwise identical source rows represent distinct
clinical events.

### 4.7 Candidate keys and grain

Candidate keys validated during profiling are not automatically primary keys.

The proposed grains remain:

| Dataset | Provisional grain |
|---|---|
| Cohort | Culture order x organism x antibiotic result |
| Prior medication | Culture order x prior medication record |
| Prior infecting organism | Culture order x prior-infection observation |
| Prior procedures | Culture order x prior procedure observation |
| Antibiotic class exposure | Culture order x antibiotic class exposure record |
| Antibiotic subtype exposure | Culture order x antibiotic subtype exposure record |
| Microbial resistance | Culture order x organism x antibiotic resistance-history observation |
| Implied susceptibility | Culture order x organism x antibiotic susceptibility observation |

These grains describe the intended record types and do not guarantee uniqueness
of the listed attributes.

The supporting implied-susceptibility rules table has a separate reference-data
grain that must be validated independently.

## 5. Deduplication Policy

### 5.1 General principle

Deduplication must be explicit, reproducible, and limited to a defined scope.

A repeated candidate key is not sufficient evidence that records are duplicates.

The pipeline must distinguish between:

- Exact duplicate rows across all relevant source columns.
- Records sharing a candidate key but differing in other attributes.
- Legitimate one-to-many relationships.
- Repeated patients or culture orders.

Bronze must retain the original source rows regardless of later Silver
deduplication decisions.

### 5.2 Prior medication

Initial profiling found:

- 9,823,458 source rows.
- 6,325,260 rows belonging to exact-duplicate groups.
- 3,961,688 additional exact copies.
- 5,861,770 distinct complete rows.

This table requires an explicit deduplication policy because exact repetitions
are substantial.

Proposed Silver policy:

1. Preserve all source rows in Bronze.
2. Identify exact duplicates across all source columns.
3. Retain one copy of each distinct complete row in the initial Silver
   analytical dataset.
4. Record the number of input rows, output rows, and exact copies removed.
5. Document that the source schema does not provide a validated medication
   event identifier capable of distinguishing otherwise identical events.
6. Revisit the policy if source documentation establishes that identical rows
   can intentionally represent separate events.

This policy is an analytical design decision, not proof that every repeated
source row is an accidental duplicate.

The policy must be tested to ensure that the deduplication process does not
remove records that differ in any source column.

### 5.3 Other clinical/event tables

Initial profiling found no exact duplicate rows in the other seven
clinical/event tables.

Do not introduce routine full-row deduplication for these tables without new
evidence or a documented requirement.

Repeated candidate-key combinations must be retained unless a separate,
well-defined rule establishes that the records are duplicates.

### 5.4 Deduplication audit

For any dataset subject to deduplication, record:

- Input row count
- Output row count
- Number of additional exact copies removed
- Columns used to define exact row equality
- Deduplication policy version or description

The count reconciliation must satisfy:

`input_rows = output_rows + removed_exact_duplicate_rows`

This reconciliation applies when no other filtering or rejection occurs in
the same step. If additional transformations remove records, account for
those records separately.

## 6. Data Quality Framework

### 6.1 Severity levels

Quality checks use the following severity levels.

**ERROR**

The output violates a requirement necessary for reliable processing or
publication.

An error may fail the current task, quarantine affected records, or block
publication depending on the rule.

**WARNING**

The check identifies a suspicious or unexpected condition that requires
visibility but does not automatically invalidate the entire dataset.

Warnings must be counted and logged. They may become errors if later evidence
establishes a strict requirement.

**INFO**

The check records useful statistics, such as row counts, category
distributions, and duplicate counts, without implying a data-quality failure.

### 6.2 Failure and quarantine behavior

The action taken must be appropriate to the rule.

- Structural failures, such as missing required columns, should fail the
  affected transformation.
- Invalid individual records may be quarantined if the remaining records can
  be processed safely.
- Referential-integrity violations should be reported and handled according
  to their documented severity.
- Warnings should be logged and included in run metadata.
- Gold outputs must not be published as successful when a required blocking
  check has failed.

A quarantined record must retain enough information to trace it back to its
source, including the source dataset and a record-level reference where
available.

Quarantine must not silently discard records.

## 7. Data Quality Rules

The following rules are proposed for implementation. A rule's severity may
differ by dataset when the field is optional for one transformation but
required for another.

### DQ-001: Required source columns

**Severity:** ERROR

**Check:** All columns required by the source-table specification exist.

**Action:** Fail the affected transformation when required columns are
missing. Unexpected additional columns must be reported and assessed rather
than silently ignored.

### DQ-002: Source schema and data types

**Severity:** ERROR for required fields that cannot be parsed; WARNING for
non-critical type or schema drift.

**Check:** Source fields can be read and converted according to the documented
schema.

**Action:** Fail or quarantine affected records according to the field's
importance. Log conversion failures.

### DQ-003: Required culture-order identifiers

**Severity:** ERROR for records that require a culture-order relationship.

**Check:** The source order identifier is present and parseable for each
record requiring that relationship.

**Action:** Quarantine or reject records with missing or invalid identifiers
from transformations that require a valid order reference. Preserve them in
Bronze and report their counts.

### DQ-004: Source order identity consistency

**Severity:** ERROR for unresolved conflicts in the modeled culture-order
identity; WARNING during initial discovery.

**Check:** Each source order identifier maps consistently to the expected
patient, encounter, and culture-time attributes across applicable records.

**Action:** Report conflicting mappings and prevent affected orders from
being treated as unambiguous modeled entities until resolved.

Repeated order identifiers across observation rows are not violations.

### DQ-005: Surrogate primary-key integrity

**Severity:** ERROR

**Check:** `culture_order_sk` is non-null and unique in the culture-order
entity.

**Action:** Fail the modeled output if the constraint is violated.

For record-level surrogate keys, apply the corresponding non-null and
uniqueness requirements within each relevant modeled dataset.

### DQ-006: Foreign-key integrity

**Severity:** ERROR for required relationships; WARNING for optional
relationships or documented exceptions.

**Check:** Every non-null `culture_order_sk` in a related dataset references
an existing culture-order record.

**Action:** Report unmatched records. Do not silently drop them during joins.
Quarantine or fail according to the relationship's documented requirement.

### DQ-007: Timestamp parsing

**Severity:** ERROR for records requiring a valid timestamp; WARNING when
the timestamp is optional for the intended transformation.

**Check:** `order_time_jittered_utc` can be parsed into the standardized
`culture_time` type.

**Action:** Record parsing failures and quarantine invalid records where the
timestamp is required.

Do not substitute arbitrary timestamps.

### DQ-008: Numeric interval parsing

**Severity:** ERROR for values that cannot be parsed when the field is
required; WARNING for suspicious but parseable values.

**Check:** Interval fields can be converted to their documented numeric type.

**Action:** Quarantine unparseable values where the interval is required.
Log unusual numeric values for review.

Do not enforce non-negative intervals or infer their temporal direction until
the source semantics have been confirmed.

### DQ-009: Missing and blank values

**Severity:** Depends on the field and transformation.

**Check:** Identify actual nulls, empty strings, and whitespace-only values
in fields required for the intended transformation.

**Action:** Normalize blank representations only under documented rules.
Quarantine records where required fields remain invalid after normalization.

A field must not be considered invalid merely because it is optional in the
source or unnecessary for a particular analytical output.

### DQ-010: Literal null-like strings

**Severity:** WARNING during profiling; governed by the approved
normalization policy during transformation.

**Check:** Identify literal strings such as `"Null"` and distinguish them
from actual null values.

**Action:** Apply a documented category-specific mapping. Preserve the source
value in Bronze and do not replace meaningful source categories with nulls
without an explicit rule.

### DQ-011: Exact duplicate detection

**Severity:** INFO for detection; ERROR only if a documented output constraint
requires the absence of exact duplicates.

**Check:** Count exact duplicate rows using the documented set of source
columns.

**Action:** Apply the deduplication policy in Section 5. Preserve Bronze,
record the number removed, and reconcile input and output counts.

Do not use candidate-key repetition alone as a deduplication rule.

### DQ-012: Candidate-key uniqueness

**Severity:** WARNING unless uniqueness has been explicitly established as a
required constraint.

**Check:** Measure repeated values for the candidate keys documented during
profiling.

**Action:** Log repeated candidate-key groups and counts. Do not delete
records or manufacture uniqueness by changing the source values.

Only enforce uniqueness for a key whose semantics and uniqueness requirement
have been validated.

### DQ-013: Susceptibility category handling

**Severity:** WARNING for unclassified values during initial implementation;
ERROR if an invalid mapping would corrupt a required Gold metric.

**Check:** Compare susceptibility values with the documented source-category
mapping.

**Action:** Preserve unmapped values, report their frequency, and prevent
unapproved categories from being silently assigned to susceptible or
resistant groups.

Observed and implied susceptibility must remain separate.

### DQ-014: Exposure and history record preservation

**Severity:** ERROR if a transformation unexpectedly drops records without
accounting for them.

**Check:** Reconcile input, output, filtered, quarantined, and deduplicated
record counts for exposure and history datasets.

**Action:** Report all differences. Do not collapse repeated exposure or
history observations solely because they share a culture order, medication,
organism, antibiotic, or procedure.

### DQ-015: Join cardinality and row multiplication

**Severity:** ERROR for unexpected multiplication in transformations expected
to preserve a defined grain; WARNING when the relationship is intentionally
one-to-many but not yet fully characterized.

**Check:** Compare row counts and key-level multiplicities before and after
joins.

**Action:** Investigate unexpected fanout before publishing the affected
output.

Do not join multiple one-to-many exposure and result tables together at raw
row level without accounting for the resulting multiplicative combinations.

### DQ-016: Output schema validation

**Severity:** ERROR

**Check:** The Silver or Gold output matches its documented schema, including
required columns and data types.

**Action:** Fail publication when required output fields are missing or have
incompatible types.

### DQ-017: Row-count reconciliation

**Severity:** ERROR when unexplained differences occur.

**Check:** Reconcile input rows against output rows, filtered rows,
quarantined rows, and deduplicated rows as applicable.

**Action:** Prevent successful publication if unexplained row loss or
duplication is detected.

The reconciliation formula must reflect the actual transformations applied
in the task.

### DQ-018: Pipeline run audit

**Severity:** ERROR for missing required run metadata; INFO for recorded
performance statistics.

**Check:** The pipeline records the run identifier, task status, input and
output counts, quality-check results, and failure information where
applicable.

**Action:** Mark the task as failed if required audit metadata cannot be
written reliably.

## 8. Data Quality Implementation Strategy

### 8.1 Begin with foundational checks

The initial implementation should prioritize:

1. Required columns and explicit schemas.
2. Identifier presence and source order consistency.
3. Timestamp and numeric parsing.
4. Missing-value normalization.
5. Exact duplicate handling for prior medication.
6. Surrogate-key and foreign-key integrity.
7. Output row-count reconciliation.
8. Structured recording of quality-check results.

This order establishes the controls needed before more complex analytical
transformations are implemented.

### 8.2 Reusable validation functions

Where practical, implement reusable PySpark validation functions for:

- Required-column validation
- Null and blank-value checks
- Data-type and parsing checks
- Exact duplicate counting
- Candidate-key repetition counts
- Referential-integrity checks
- Row-count reconciliation
- Output-schema validation

Each function should return or record a consistent result containing:

- Rule ID
- Dataset
- Severity
- Number of records checked
- Number of violations
- Pass/fail status
- Action taken

The implementation should avoid collecting entire large datasets into driver
memory solely to perform quality checks.

### 8.3 Testing

Use Pytest for unit tests of transformation and validation functions.

Tests should cover:

- Valid input passes the expected checks.
- Missing required columns cause a clear failure.
- Null and blank identifiers are detected.
- Invalid timestamps and numeric values are reported.
- Exact duplicate handling removes only the intended rows.
- Candidate-key repetition is reported without unintended deletion.
- Surrogate keys are unique and stable under the chosen mapping strategy.
- Foreign-key violations are detected.
- Observed and implied susceptibility remain separate.
- Row-count reconciliation detects unexplained losses or gains.
- Empty input and unexpected category values are handled explicitly.

Use small synthetic test fixtures for edge cases and representative source
samples for integration testing. Do not rely exclusively on the full source
datasets to validate individual rule functions.

## 9. Quality Monitoring and Operational Logging

For each pipeline run, record a quality summary for each processed dataset.

Recommended fields include:

| Field | Description |
|---|---|
| `run_id` | Unique pipeline execution identifier |
| `dataset_name` | Source or output dataset |
| `rule_id` | Identifier of the quality rule |
| `severity` | ERROR, WARNING, or INFO |
| `records_checked` | Number of records evaluated |
| `violations_count` | Number of records or conditions failing the rule |
| `status` | PASS, FAIL, or WARNING |
| `action_taken` | Fail, quarantine, log, or other documented response |
| `input_rows` | Number of input records |
| `output_rows` | Number of output records |
| `duration_seconds` | Processing or validation duration |
| `recorded_at` | Time the result was recorded |

The exact storage schema may be refined during pipeline implementation.

Airflow task logs and run metadata should make it possible to identify which
dataset and rule caused a failed task without rerunning the entire pipeline.

## 10. Unresolved Decisions

The following items require confirmation before their dependent rules can be
finalized.

1. **Source order identity:** Confirm that `order_proc_id_coded` consistently
   identifies one culture order across the selected tables.
2. **Surrogate-key persistence:** Choose a stable mapping strategy for
   `culture_order_sk` and any observation-level surrogate keys.
3. **Prior medication deduplication:** Adopt the proposed exact full-row
   deduplication policy for Silver, document its limitation, and retain all
   source rows in Bronze.
4. **Interval semantics:** Confirm the units and direction of each interval
   field before applying temporal or clinical interpretation.
5. **Susceptibility mapping:** Define the approved treatment of literal
   `"Null"` and non-standard or non-interpreted categories.
6. **Timestamp policy:** Confirm the intended Silver timestamp type and
   timezone convention.
7. **Quarantine policy:** Determine the storage location and retention policy
   for rejected records.
8. **Quality thresholds:** Establish any quantitative warning thresholds only
   where they have a defensible basis.

These decisions should be resolved as the relevant transformation is
implemented. They do not require reopening the full source-profiling exercise
unless new evidence materially changes the model.

## 11. Definition of Done

The transformation and data-quality design phase is complete when:

- [ ] Bronze preservation requirements are documented.
- [ ] Expected source and Silver schemas are defined.
- [ ] Column renaming and type-conversion rules are documented.
- [ ] Missing-value and susceptibility normalization policies are defined.
- [ ] The prior medication deduplication policy is documented.
- [ ] Culture-order identity and surrogate-key strategy are defined.
- [ ] Candidate-key and foreign-key constraints are documented.
- [ ] Data-quality rules have IDs, severity levels, and failure actions.
- [ ] Row-count reconciliation and run-audit requirements are documented.
- [ ] Initial PySpark validation functions and Pytest coverage are planned.

Implementation will follow this specification incrementally. A documented
rule should not be considered implemented until the corresponding validation
or transformation has been tested.
