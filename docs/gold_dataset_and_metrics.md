
# Gold Dataset and Metrics Specification

## 1. Purpose

This document defines the analytical datasets, transformations, metrics, and validation requirements for the Gold layer of the AMR Data Pipeline.

The Gold layer consumes standardized Silver datasets and produces analysis-ready datasets for querying, reporting, and visualization using PostgreSQL and Apache Superset.

The project focuses on four analytical areas:

1. Culture and susceptibility overview.
2. Microbial resistance history.
3. Prior antibiotic exposure.
4. Prior clinical history.

The Gold layer must preserve the meaning and grain of source observations, define metric denominators explicitly, prevent unintended row multiplication during joins, and document limitations that affect interpretation.

This document describes the intended analytical model. It does not claim that all proposed joins, metrics, or relationships have already been validated.

## 2. Architecture and conventions

### 2.1 Data flow

```text
Bronze CSV snapshots
        |
        v
Silver standardized Parquet datasets
        |
        v
Gold analytical transformations
        |
        +--------------------------+
        |                          |
        v                          v
Gold Parquet datasets       PostgreSQL Gold tables
                                   |
                                   v
                             Apache Superset
```

Gold Parquet datasets provide durable analytical outputs in the data lake. PostgreSQL provides a queryable serving layer for dashboards and interactive analysis.

Both outputs must represent the same approved Gold transformation logic.

### 2.2 Gold design principles

1. Each Gold dataset must have a documented grain.
2. Every metric must define its numerator, denominator, inclusion criteria, and exclusions.
3. Observed susceptibility and implied susceptibility must remain distinguishable.
4. Resistance-history observations must not automatically be interpreted as current culture resistance results.
5. Non-unique identifiers must not be treated as unique row-level keys.
6. One-to-many datasets must be aggregated to the intended join grain before being combined with other one-to-many datasets.
7. Unmatched and ambiguous records must be measured rather than silently discarded.
8. Missing, unavailable, uninterpretable, and literal `"Null"` values must be handled consistently.
9. Metrics must not imply causation where the data supports only descriptive or observational analysis.
10. All output row counts, metric counts, and quality checks must be traceable to a pipeline run.

### 2.3 Source datasets

Gold transformations consume the following Silver datasets:

| Silver dataset | Intended analytical use |
|---|---|
| `silver_culture_cohort` | Culture and observed susceptibility analysis |
| `silver_implied_susceptibility` | Analysis of source-provided implied susceptibility |
| `silver_susceptibility_rules` | Reference information about susceptibility inference rules |
| `silver_microbial_resistance` | Historical resistance observations |
| `silver_antibiotic_class_exposure` | Prior antibiotic class exposure |
| `silver_antibiotic_subtype_exposure` | Prior antibiotic subtype exposure |
| `silver_prior_medication` | Prior medication history |
| `silver_prior_infecting_organism` | Prior infection history |
| `silver_prior_procedure` | Prior procedure history |

The Silver data dictionary defines the expected source columns and standardized names. Gold transformations must use those names rather than reintroducing source CSV naming inconsistencies.

## 3. Gold dataset inventory

| Gold dataset | Analytical area | Intended grain |
|---|---|---|
| `gold_culture_susceptibility` | Culture and susceptibility | One retained source susceptibility observation |
| `gold_resistance_history` | Resistance history | One retained source resistance-history observation |
| `gold_antibiotic_class_exposure` | Prior antibiotic exposure | One retained antibiotic class exposure observation |
| `gold_antibiotic_subtype_exposure` | Prior antibiotic exposure | One retained antibiotic subtype exposure observation |
| `gold_culture_history_summary` | Prior clinical history | One row per validated culture order |

These names are proposed logical table names. The physical names and paths must be consistent across the Parquet and PostgreSQL outputs.

The first four datasets preserve detailed observations. The history summary is an aggregated dataset intended to prevent row multiplication when several history sources are combined.

Additional aggregate tables or views may be introduced if they support a clearly defined analytical need.

## 4. Gold dataset specifications

## 4.1 `gold_culture_susceptibility`

### Purpose

Provide a consistent dataset for analyzing culture observations, reported susceptibility interpretations, and source-provided implied susceptibility.

### Source datasets

- `silver_culture_cohort`
- `silver_implied_susceptibility`
- `silver_susceptibility_rules`, where required for reference or documented interpretation

### Grain

One retained source susceptibility observation from the cohort dataset.

The cohort's source observation must remain identifiable even when a corresponding implied susceptibility record cannot be matched.

### Proposed columns

| Column | Description |
|---|---|
| `cohort_result_id` | Stable identifier for the retained cohort result observation, if implemented |
| `anon_id` | De-identified patient identifier |
| `encounter_id` | Patient encounter identifier |
| `culture_order_id` | Source culture order identifier |
| `culture_time` | Jittered culture timestamp |
| `care_setting` | Source care-setting or ordering-mode value |
| `culture_type` | Culture type or description |
| `was_positive` | Source culture-positive indicator |
| `organism` | Organism reported for the culture observation |
| `antibiotic` | Antibiotic associated with the susceptibility observation |
| `susceptibility` | Observed susceptibility interpretation |
| `implied_susceptibility` | Matched source-provided implied susceptibility, when reliably matched |
| `implied_match_status` | Status describing whether a reliable implied-susceptibility match was established |
| `is_inpatient` | Inpatient indicator, if present and validated |
| `is_outpatient` | Outpatient indicator, if present and validated |
| `is_er` | Emergency-room indicator, if present and validated |
| `is_icu` | Intensive-care indicator, if present and validated |

The `cohort_result_id`, `implied_match_status`, and other derived fields are proposed Gold attributes, not existing Silver source columns.

The four ward indicators must only be included if they are present in the actual Silver schema.

### Transformation rules

1. Preserve the cohort observation as the base record.
2. Validate the intended matching relationship before joining implied susceptibility.
3. Do not assume that `culture_order_id` alone uniquely identifies a result.
4. Do not assume that `culture_order_id` + `organism` + `antibiotic` uniquely identifies an observation.
5. Determine whether additional fields or a validated composite relationship are needed to match observations.
6. If matching is ambiguous, retain the cohort observation and mark the match as ambiguous or unresolved.
7. Do not arbitrarily select one implied-susceptibility record when multiple candidates exist.
8. Preserve observed and implied values in separate columns.
9. Do not convert literal `"Null"` into a meaningful susceptibility category.
10. Normalize missing representations only under the approved missing-value policy.

### Proposed match-status values

| Value | Meaning |
|---|---|
| `matched` | A unique, validated implied-susceptibility match was established |
| `unmatched` | No corresponding implied-susceptibility observation was found |
| `ambiguous` | Multiple possible matches remain |
| `not_evaluated` | Matching was not attempted or could not be evaluated |

These values describe the pipeline's matching outcome, not a clinical interpretation.

### Metrics

#### A. Culture observation count

**Definition:** Number of retained cohort observations.

```text
culture_observation_count = COUNT(*)
```

This metric counts rows at the cohort observation grain. It must not be labelled as the number of unique patients or unique culture orders.

#### B. Distinct culture orders

**Definition:** Number of distinct culture orders under the validated order-identity rule.

```text
distinct_culture_orders = COUNT(DISTINCT validated_culture_order_key)
```

Do not assume that `culture_order_id` alone is sufficient until its order-level uniqueness and cross-dataset consistency have been validated.

#### C. Observed susceptibility distribution

For each retained observed susceptibility category:

```text
category_count =
    count of eligible observations in the category

category_percentage =
    category_count / eligible_observation_count * 100
```

The denominator must be explicitly defined for each output.

At minimum, report:

- Total cohort observations.
- Observations with interpretable observed susceptibility.
- Observations with unavailable or uninterpretable susceptibility.
- Observations with unresolved missing representations.

Do not silently remove non-standard categories from the source distribution.

#### D. Implied susceptibility distribution

Use the same approach as the observed susceptibility distribution, but calculate it from `implied_susceptibility`.

The denominator must include only observations eligible under the documented implied-susceptibility rules.

Observed and implied distributions must be reported separately.

#### E. Observed/implied agreement

Agreement may be calculated only for records with a validated, unambiguous match and two interpretable susceptibility values.

```text
agreement_percentage =
    matched observations with equal normalized interpretations
    / matched observations with two interpretable interpretations
    * 100
```

Report the eligible pair count alongside the percentage.

The normalization policy must define which labels are comparable. Do not automatically treat all non-missing labels as equivalent categories.

### Important interpretation limitation

Agreement measures consistency between two recorded or derived interpretations in the matched data. It does not independently establish clinical correctness.

## 4.2 `gold_resistance_history`

### Purpose

Expose historical microbial resistance observations for descriptive analysis by organism, antibiotic, and source interval.

### Source dataset

- `silver_microbial_resistance`

### Grain

One retained source resistance-history observation.

### Proposed columns

| Column | Description |
|---|---|
| `anon_id` | De-identified patient identifier |
| `encounter_id` | Patient encounter identifier |
| `culture_order_id` | Source culture order identifier |
| `culture_time` | Jittered culture timestamp |
| `organism` | Organism associated with the resistance observation |
| `antibiotic` | Antibiotic associated with the resistance observation |
| `resistant_time_to_culturetime` | Source interval associated with the resistance observation |

An additional stable row identifier may be introduced if required. Its generation strategy must be documented.

### Transformation rules

1. Preserve the source observation grain.
2. Validate identifier and timestamp fields.
3. Validate the numeric representation of `resistant_time_to_culturetime`.
4. Confirm the interval's units and direction before describing it as elapsed days or time before/after an event.
5. Do not deduplicate repeated order-organism-antibiotic combinations.
6. Preserve repeated observations with different interval values.
7. Do not combine these historical observations with cohort susceptibility observations without a separately validated matching rule.

### Metrics

#### A. Resistance-history observation count

```text
resistance_history_observation_count = COUNT(*)
```

This counts source resistance-history observations, not unique resistant patients.

#### B. Distinct orders with resistance history

```text
orders_with_resistance_history =
    count of distinct validated culture orders in the resistance dataset
```

The order identifier must be validated before being used as a distinct-order key.

#### C. Observation count by organism

Group resistance-history observations by `organism`.

```text
organism_observation_count = COUNT(*)
```

Report the count of observations and, where useful, the distinct-order count separately.

#### D. Observation count by antibiotic

Group resistance-history observations by `antibiotic`.

Report the observation count and distinct-order count separately.

#### E. Organism-antibiotic observation counts

Group by `organism` and `antibiotic`.

This metric describes the number of recorded resistance-history observations for each combination. It must not be labelled a resistance rate.

#### F. Interval distribution

Calculate interval statistics only after the interval's units and meaning are verified.

Potential statistics include:

- Minimum.
- Maximum.
- Median.
- 25th percentile.
- 75th percentile.
- Count of valid interval values.
- Count of invalid or unavailable interval values.

Until the semantics are verified, use a neutral label such as `resistant_time_to_culturetime` rather than describing the metric as elapsed days.

### Important interpretation limitation

The resistance-history table does not, by itself, provide a defensible denominator of all tested observations. Its row counts must not be used to calculate resistance prevalence or resistance rates.

## 4.3 `gold_antibiotic_class_exposure`

### Purpose

Support descriptive analysis of recorded antibiotic class exposure associated with culture orders.

### Source dataset

- `silver_antibiotic_class_exposure`

### Grain

One retained source antibiotic class exposure observation.

### Proposed columns

| Column | Description |
|---|---|
| `anon_id` | De-identified patient identifier |
| `encounter_id` | Patient encounter identifier |
| `culture_order_id` | Source culture order identifier |
| `culture_time` | Jittered culture timestamp |
| `med_category` | Medication category |
| `medication` | Medication name |
| `antibiotic_class` | Antibiotic class |
| `days_before_culture` | Source interval from `time_to_culturetime` |

The Silver naming decision for `antibiotic_class` must remain consistent with the Silver data dictionary.

### Transformation rules

1. Preserve exposure observations at their source grain.
2. Validate the type and meaning of `days_before_culture`.
3. Do not assume that a repeated order-medication-class combination is a duplicate.
4. Do not remove repeated observations based on the candidate key alone.
5. Do not combine this table with subtype exposure or prior medication records at row level without first resolving the join grain.
6. Retain invalid or unavailable interval outcomes in quality metrics, even if they are excluded from a particular interval statistic.

### Metrics

#### A. Class exposure observation count

```text
class_exposure_observation_count = COUNT(*)
```

#### B. Distinct orders with class exposure

Count distinct culture orders under the validated order-key rule.

#### C. Exposure observations by antibiotic class

Group by `antibiotic_class` and report:

- Observation count.
- Distinct orders with that class.
- Percentage of eligible exposure observations, if the denominator is clearly stated.

A percentage of exposure observations is not the percentage of patients exposed unless a patient-level denominator and deduplication rule have been defined.

#### D. Exposure observations by medication

Group by `medication` and report observation count and distinct-order count.

#### E. Exposure interval distribution

Calculate interval statistics only after confirming the meaning, units, and direction of `days_before_culture`.

Report the valid interval count and invalid or unavailable interval count alongside any statistics.

### Important interpretation limitation

Recorded prior exposure is an observed data attribute. These counts alone do not demonstrate that an exposure caused, prevented, or changed a resistance outcome.

## 4.4 `gold_antibiotic_subtype_exposure`

### Purpose

Support more detailed descriptive analysis of recorded antibiotic subtype exposure.

### Source dataset

- `silver_antibiotic_subtype_exposure`

### Grain

One retained source antibiotic subtype exposure observation.

### Proposed columns

| Column | Description |
|---|---|
| `anon_id` | De-identified patient identifier |
| `encounter_id` | Patient encounter identifier |
| `culture_order_id` | Source culture order identifier |
| `culture_time` | Jittered culture timestamp |
| `med_category` | Medication category |
| `medication` | Medication name |
| `subtype` | Standardized antibiotic subtype |
| `subtype_category` | Subtype category |
| `days_since_prior_medication` | Standardized source interval |

### Transformation rules

1. Preserve the source observation grain.
2. Use `subtype` and `subtype_category` as documented in Silver.
3. Validate the meaning and units of `days_since_prior_medication`.
4. Preserve repeated order-medication-subtype combinations.
5. Do not deduplicate observations merely because the order, medication, and subtype values match.
6. Keep class exposure and subtype exposure as separate datasets unless a validated relationship and specific analytical need justify a join.

### Metrics

#### A. Subtype exposure observation count

```text
subtype_exposure_observation_count = COUNT(*)
```

#### B. Distinct orders with subtype exposure

Count distinct culture orders using the validated order-key rule.

#### C. Exposure observations by subtype

Group by `subtype` and report observation count and distinct-order count.

#### D. Exposure observations by subtype category

Group by `subtype_category` and report observation count and distinct-order count.

#### E. Subtype interval distribution

Calculate interval statistics only after the meaning and units of `days_since_prior_medication` have been verified.

### Important interpretation limitation

The same medication or subtype may appear in multiple observations associated with a culture order. Observation counts and distinct-order counts must therefore remain separate metrics.

## 4.5 `gold_culture_history_summary`

### Purpose

Provide one row per validated culture order, summarizing the availability and counts of prior clinical-history observations.

This dataset supports analysis of prior infections, medications, procedures, and exposure records without multiplying culture orders through direct joins between multiple one-to-many datasets.

### Source datasets

- Validated culture order records derived from `silver_culture_cohort`.
- `silver_prior_infecting_organism`
- `silver_prior_medication`
- `silver_prior_procedure`
- `silver_antibiotic_class_exposure`
- `silver_antibiotic_subtype_exposure`

### Grain

One row per validated culture order.

The exact order-level key must be established before the dataset is implemented.

### Proposed columns

| Column | Description |
|---|---|
| `culture_order_id` | Validated culture order identifier |
| `anon_id` | De-identified patient identifier, if consistent at order grain |
| `encounter_id` | Encounter identifier, if consistent at order grain |
| `culture_time` | Jittered culture timestamp, if consistent at order grain |
| `prior_infection_observation_count` | Count of retained prior-infection observations associated with the order |
| `distinct_prior_organism_count` | Count of distinct prior organism values under the approved normalization policy |
| `prior_medication_observation_count` | Count of retained prior medication observations after the approved deduplication policy |
| `prior_procedure_observation_count` | Count of retained prior procedure observations |
| `antibiotic_class_exposure_count` | Count of retained antibiotic class exposure observations |
| `antibiotic_subtype_exposure_count` | Count of retained antibiotic subtype exposure observations |
| `has_prior_infection_record` | Indicator that one or more prior-infection observations are present |
| `has_prior_medication_record` | Indicator that one or more retained prior medication observations are present |
| `has_prior_procedure_record` | Indicator that one or more prior procedure observations are present |
| `has_antibiotic_class_exposure_record` | Indicator that one or more class exposure observations are present |
| `has_antibiotic_subtype_exposure_record` | Indicator that one or more subtype exposure observations are present |

These are proposed derived columns. They must not be confused with fields directly supplied by the source datasets.

### Transformation rules

1. Establish the validated culture-order population.
2. Validate the consistency of order-level identifiers and attributes.
3. Aggregate each child dataset independently to the validated order key.
4. Join the aggregated results to the order-level population.
5. Never join raw prior infection, medication, procedure, class exposure, and subtype exposure rows together before aggregation.
6. Distinguish a confirmed absence of linked records from a failed or ambiguous join.
7. Do not automatically convert a missing aggregate to zero when the join failed or relationship validation was incomplete.
8. Use the documented prior medication deduplication policy when counting retained medication observations.
9. Keep observation counts separate from distinct medication, organism, or procedure counts.
10. Document the population of culture orders included in the output.

### Metrics

#### A. Culture orders in history summary

```text
culture_orders_in_history_summary = COUNT(*)
```

This count is valid as a distinct order count only if the table has one row per validated order.

#### B. Orders with prior infection history

Count orders for which `has_prior_infection_record` is true.

```text
orders_with_prior_infection =
    count of eligible orders with a prior infection record
```

#### C. Orders with prior medication history

Count eligible orders for which `has_prior_medication_record` is true.

#### D. Orders with prior procedure history

Count eligible orders for which `has_prior_procedure_record` is true.

#### E. Orders with antibiotic class or subtype exposure

Count eligible orders with at least one retained class exposure record or subtype exposure record, reported separately.

#### F. History coverage percentage

For each history type:

```text
history_coverage_percentage =
    eligible culture orders with at least one linked history record
    / eligible culture orders in the defined population
    * 100
```

The denominator must be the same explicitly defined culture-order population for the comparison being made.

Do not use this metric to imply that an order with no linked record has no relevant clinical history. The source tables may not fully represent all historical information.

#### G. Observation-count distribution

Summarize the number of history observations per culture order for each history type.

Possible statistics include:

- Minimum.
- Maximum.
- Median.
- 25th percentile.
- 75th percentile.
- Number of orders with zero linked observations.
- Number of orders with one linked observation.
- Number of orders with multiple linked observations.

Calculate these statistics only after validating the order-level relationship and the definition of an observation.

### Important interpretation limitation

This summary describes records available in the selected source tables. It does not prove that unrecorded history is absent from a patient's clinical history.

## 5. Metric definitions and denominator policy

### 5.1 Required metric metadata

Every published metric must have a documented definition containing:

| Attribute | Requirement |
|---|---|
| Metric name | Stable, descriptive name |
| Business definition | What the metric measures |
| Source dataset | Dataset or datasets used |
| Grain | Unit represented by each input observation |
| Numerator | Count or value in the numerator, if applicable |
| Denominator | Eligible population for a percentage or rate |
| Inclusion rules | Records included in the metric |
| Exclusion rules | Records excluded and why |
| Missing-value handling | How null and non-interpretable values are handled |
| Deduplication policy | Any deduplication applied before calculation |
| Validation | Checks required before publishing |
| Interpretation limits | What the metric does not establish |

### 5.2 Observation counts versus entity counts

The following are different measures and must be labelled separately:

- Number of source observations.
- Number of distinct culture orders.
- Number of distinct encounters.
- Number of distinct de-identified patients.
- Number of distinct organisms.
- Number of distinct antibiotics.

A count of result rows must not be described as a patient count or culture-order count.

### 5.3 Percentages and rates

A percentage is only meaningful when the eligible denominator is explicitly defined.

For susceptibility distributions, report the number of interpretable observations and the number of unavailable or uninterpretable observations.

Do not exclude categories silently. If a metric excludes a category, publish the exclusion rule and the resulting denominator.

### 5.4 Resistance-rate restrictions

The `silver_microbial_resistance` dataset contains resistance-history observations. It does not, by itself, establish the total number of tested observations needed to calculate a resistance rate.

Therefore:

- Do not calculate a resistance rate using resistance-history row counts alone.
- Do not label resistance-history observation counts as resistance prevalence.
- Any future resistance-rate metric must be based on a validated set of tested observations, a defensible definition of a resistant result, and a clearly documented denominator.
- Such a metric must be added only after its analytical basis has been verified.

## 6. Join strategy and fanout prevention

### 6.1 General rule

Before joining datasets, establish the grain and uniqueness properties of both sides of the join.

The same `culture_order_id` can occur in multiple rows within the cohort and in multiple rows within exposure or history datasets.

A join on `culture_order_id` alone can therefore multiply observations.

### 6.2 Required aggregation pattern

For the culture history summary, the expected pattern is:

```text
Prior infection observations
        |
        v
Aggregate to validated culture-order grain
        |
        +-----------------------------------+
                                            |
Prior medication observations               |
        |                                   |
        v                                   |
Aggregate to validated culture-order grain  |
        |                                   |
        +-----------------------------------+
                                            |
Prior procedure observations                |
        |                                   |
        v                                   |
Aggregate to validated culture-order grain  |
        |                                   |
        +-----------------------------------+
                                            |
Class and subtype exposure observations     |
        |                                   |
        v                                   |
Aggregate each independently to order grain |
        |                                   |
        +-----------------------------------+
                                            |
                                            v
                         Join aggregates to validated
                         culture-order population
                                            |
                                            v
                              gold_culture_history_summary
```

Each child dataset must be aggregated independently before its results are joined to the parent population.

### 6.3 Join validation requirements

For every join:

1. Validate the proposed join keys.
2. Count duplicate keys on each side.
3. Determine whether the relationship is one-to-one, one-to-many, many-to-one, or many-to-many.
4. Compare row counts before and after the join.
5. Measure unmatched records on each side.
6. Investigate unexpected row multiplication.
7. Preserve unmatched source observations where required.
8. Record the join validation results.

A many-to-many join is not necessarily invalid, but it must not occur accidentally or be treated as a simple record match.

## 7. Data-quality checks for Gold outputs

### 7.1 Schema checks

- Output columns match the approved Gold schema.
- Required columns exist.
- Logical data types are correct.
- Derived fields have documented definitions.
- No unintended source columns or duplicate column names are introduced.

### 7.2 Grain checks

- `gold_culture_susceptibility` preserves its approved observation grain.
- `gold_resistance_history` preserves its approved source observation grain.
- `gold_antibiotic_class_exposure` preserves its approved exposure grain.
- `gold_antibiotic_subtype_exposure` preserves its approved exposure grain.
- `gold_culture_history_summary` has exactly one row per validated culture order.

### 7.3 Reconciliation checks

For observation-preserving Gold datasets:

- Compare source Silver row counts with Gold row counts.
- Explain any differences caused by approved filtering or deduplication.
- Verify that no rows are lost because of an unintended inner join.
- Verify that no rows are duplicated because of join fanout.

For aggregate Gold datasets:

- Compare the number of distinct validated orders in the parent population with the number of output rows.
- Confirm that each child aggregate contains no more than one row per validated order key.
- Verify that output counts reconcile with independently computed child counts.

### 7.4 Metric checks

- Numerators and denominators are non-negative.
- Numerators do not exceed their applicable denominators for ordinary subset-based percentages.
- Percentages are calculated using the documented eligible population.
- Missing and uninterpretable values are accounted for.
- Distinct-order counts use the validated order key.
- Resistance-history observation counts are not presented as resistance rates.
- Observed and implied susceptibility remain distinguishable.
- Agreement calculations include only eligible, uniquely matched pairs.

### 7.5 Run traceability

For every Gold transformation, record:

- Pipeline run identifier.
- Execution timestamp.
- Input dataset names.
- Input row counts.
- Output dataset name.
- Output row count.
- Transformation version or code revision.
- Data-quality results.
- Join validation results, where applicable.
- Failure details, if the transformation fails.

## 8. PostgreSQL and Parquet serving strategy

### 8.1 Parquet outputs

Write Gold outputs to documented Parquet paths.

Requirements:

- Use consistent naming conventions.
- Preserve the intended logical schema.
- Make writes reproducible for the same input snapshot and transformation version.
- Avoid exposing partially written outputs as successful runs.
- Record the output location and row count in run metadata.

### 8.2 PostgreSQL outputs

Load the approved Gold datasets into PostgreSQL tables for analytical queries and Superset dashboards.

Requirements:

- Use stable, documented table names.
- Define indexes based on actual query patterns.
- Apply primary-key constraints only where the row grain and key uniqueness have been validated.
- Avoid imposing false uniqueness constraints on source identifiers.
- Ensure that each published table corresponds to an approved Gold transformation.
- Validate row counts and schema after loading.

### 8.3 Consistency between serving outputs

Parquet and PostgreSQL outputs must derive from the same approved transformation logic.

After a successful load:

- Compare output row counts.
- Compare expected schemas.
- Validate key metrics on both outputs.
- Record the publication status of each output.

A successful Parquet write does not imply that the PostgreSQL load succeeded, and vice versa.

## 9. Dashboard-oriented analytical questions

The Gold layer should support the following descriptive questions.

### Culture and susceptibility

- How many culture result observations are present?
- How many distinct validated culture orders are represented?
- What is the distribution of observed susceptibility interpretations?
- How many observations have unavailable or uninterpretable susceptibility?
- What is the distribution of source-provided implied susceptibility?
- Among uniquely matched, interpretable pairs, how often do observed and implied interpretations agree?

### Resistance history

- How many resistance-history observations are recorded?
- Which organisms and antibiotics have the most recorded observations?
- How many distinct validated culture orders are associated with the observations?
- What is the distribution of the resistance interval field after its meaning and units are validated?

### Prior antibiotic exposure

- How many antibiotic class exposure observations are recorded?
- How many subtype exposure observations are recorded?
- Which classes and subtypes occur most frequently in the recorded exposure data?
- How many distinct validated culture orders have linked class or subtype exposure records?
- What are the interval distributions after interval semantics are verified?

### Prior clinical history

- How many validated culture orders have linked prior infection records?
- How many have linked prior medication records?
- How many have linked prior procedure records?
- How many have linked antibiotic class or subtype exposure records?
- What is the distribution of the number of recorded history observations per culture order?

These questions are descriptive. Any future analysis of associations between prior exposures and susceptibility outcomes requires an explicit analytical design, validated linkage, and careful interpretation of limitations.

## 10. Implementation sequence

Implement the Gold layer in the following order:

1. Confirm that all required Silver schemas match `silver_data_dictionary.md`.
2. Validate the culture-order identity and relationships across the selected datasets.
3. Define and test the missing-value and susceptibility normalization policies.
4. Implement `gold_culture_susceptibility` without an unvalidated many-to-many join.
5. Implement `gold_resistance_history` as an observation-preserving transformation.
6. Implement `gold_antibiotic_class_exposure`.
7. Implement `gold_antibiotic_subtype_exposure`.
8. Build independent order-level aggregates for prior infections, prior medication, prior procedures, and exposure observations.
9. Implement `gold_culture_history_summary` using the validated order population and aggregated child datasets.
10. Add automated schema, grain, reconciliation, and metric tests.
11. Write Gold outputs to Parquet.
12. Load the approved outputs into PostgreSQL.
13. Validate PostgreSQL tables against the Parquet outputs.
14. Connect Superset to the approved PostgreSQL tables.
15. Document the published metrics and dashboard filters.

Do not start dashboard metric interpretation until the corresponding Gold schema, grain, and join validation checks pass.

## 11. Outstanding decisions

The following decisions remain subject to validation:

1. The definitive culture-order business key and whether a stable surrogate key is required.
2. The matching strategy for cohort and implied-susceptibility observations.
3. The normalization policy for literal `"Null"` and other non-interpretable susceptibility values.
4. The exact eligibility rules for susceptibility distribution and agreement metrics.
5. The meaning, units, and direction of source interval fields.
6. The appropriate denominator for any future resistance-rate calculation.
7. The population of validated culture orders used in `gold_culture_history_summary`.
8. The handling of ambiguous and unmatched source relationships.
9. The physical Gold output paths, PostgreSQL schemas, and table names.
10. The final output schemas and metric definitions after implementation testing.

These decisions must be documented before the affected metrics are treated as final.

## 12. Completion criteria

The Gold layer is ready for analytical use when:

- Each Gold dataset has an approved schema and documented grain.
- Source relationships and join keys have been validated.
- Join fanout is prevented or explicitly justified.
- Metric definitions, numerators, denominators, and exclusions are documented.
- Missing and uninterpretable values are handled consistently.
- Observation counts and distinct entity counts are clearly distinguished.
- No resistance rate is published without a defensible denominator.
- Source and output row counts reconcile with documented transformations.
- Automated data-quality and metric tests pass.
- Parquet and PostgreSQL outputs are consistent.
- Pipeline runs and output versions are traceable.
- Analytical limitations are documented for downstream users.
