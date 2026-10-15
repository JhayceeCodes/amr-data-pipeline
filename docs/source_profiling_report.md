# Source Profiling Report



## 1. Overview

This report documents the initial profiling and candidate-key validation of all eight selected clinical/event source tables for the AMR Data Pipeline project.

The profiling covers row counts, column structures, inferred data types, missing values, exact duplicate rows, culture order identifier coverage, and candidate-key uniqueness.

The results represent initial observations from the source CSV files. Candidate grains remain provisional until the relevant relationships and business meanings are validated.



## 2. Cohort Results

Source file: `microbiology_cultures_cohort.csv`

### Dataset profile

- **Rows:** 2,241,050

- **Distinct order IDs:** 751,075

- **Missing order IDs:** 0

- **Exact duplicate rows:** 0

- **Additional exact copies:** 0



### Candidate-key validation

| Candidate Key                              | Result       |
|--------------------------------------------|--------------|
| `order_proc_id_coded`                      | Not unique   |
| `order_proc_id_coded` + `organism` + `antibiotic` | Not unique |


- 118,463 order IDs appear more than once.

- The maximum number of rows associated with one order ID is 75.

- 9,047 order–organism–antibiotic combinations are repeated.



### Observations and design implications

- No exact duplicate rows were detected across the complete set of source columns.

- The order identifier does not uniquely identify a cohort result row.

- Repeated order–organism–antibiotic combinations may differ in other attributes; their presence does not establish that the records are duplicates.

- A surrogate primary key (`cohort_result_id`) is planned for individual cohort result records when the data model is implemented.

- `order_proc_id_coded` will remain available as the source order identifier and linking field.



### Provisional grain
Culture order × organism × antibiotic result, subject to further validation.



## 3. Prior Medication

Source file: `microbiology_cultures_prior_med.csv`


### Dataset profile

- **Rows:** 9,823,458

- **Columns:** 7

- **Distinct order IDs:** 575,344

- **Missing values:** 0 across all seven columns

- **Rows belonging to exact-duplicate groups:** 6,325,260

- **Additional exact copies:** 3,961,688

- **Distinct complete rows:** 5,861,770



### Candidate-key validation

Candidate key tested: 

`anon_id` + `pat_enc_csn_id_coded` + `order_proc_id_coded` + `medication_name` + `medication_time_to_culturetime` + `medication_category`

- Repeated candidate-key groups were detected.

- No candidate-key group was associated with multiple `order_time_jittered_utc` values.


### Observations and design implications

- A substantial number of source rows are exact repetitions.

- The cause of these repetitions has not been established.

- Exact duplicate records will not be removed automatically during initial profiling.

- The source should remain intact in Bronze while the duplicate-handling policy is documented and implemented in a later stage.



### Provisional grain

Culture order × prior medication record, with the precise record-level meaning and duplicate-handling policy still to be established.



## 4. Prior Infecting Organism

Source file: `microbiology_culture_prior_infecting_organism.csv`

### Dataset profile

- **Rows:** 1,083,739

- **Columns:** 6

- **Distinct order IDs:** 209,723

- **Missing values:** 0 across all six columns

- **Rows belonging to exact-duplicate groups:** 0

- **Additional exact copies:** 0



### Candidate-key validation

Candidate key tested:

`order_proc_id_coded` + `prior_organism`

- Distinct candidate keys: 412,869

- Repeated candidate-key groups: 170,303

- The candidate key is not unique.



### Observations and design implications

- No exact duplicate rows were detected.

- The same order and prior organism can appear with different values of `prior_infecting_organism_days_to_culutre`.

- The meaning of repeated observations and the interval field has not been fully established.

- The source column contains the spelling `prior_infecting_organism_days_to_culutre`. Any standardized name should be applied in Silver while preserving the original name in Bronze.



### Provisional grain

Culture order × prior-infection observation, subject to validation of the observation's semantics.



## 5. Antibiotic Class Exposure

Source file: `microbiology_cultures_antibiotic_class_exposure.csv`

### Dataset profile

- **Rows:** 5,402,486

- **Columns:** 8

- **Distinct order IDs:** 484,370

- **Missing values:** 0 across all eight columns

- **Rows belonging to exact-duplicate groups:** 0

- **Additional exact copies:** 0



### Candidate-key validation
Candidate key tested:

`order_proc_id_coded` + `medication_name` + `antibiotic_class`


- Distinct candidate keys: 2,393,872

- Repeated candidate-key groups: 1,026,353

- The candidate key is not unique.


### Observations and design implications

- No exact duplicate rows were detected.

- The same order, medication, and antibiotic class can appear with different `time_to_culturetime` values.

- The repeated combinations may represent distinct exposure intervals, but the precise semantics have not been established.

- The candidate key will not be used as a primary key without further validation.



### Provisional grain

Culture order × antibiotic class exposure record, subject to validation of the exposure interval's meaning.



## 6. Prior Procedures

Source file: `microbiology_cultures_priorprocedures.csv`

### Dataset profile

- **Rows:** 1,664,615

- **Columns:** 6

- **Distinct order IDs:** 251,759

- **Missing values:** 0 across all six columns

- **Exact duplicate rows:** 0

- **Additional exact copies:** 0



### Candidate-key validation

Candidate key tested: `order_proc_id_coded` + `procedure_description`

- Distinct candidate keys: 425,961

- Repeated candidate-key groups: 193,465

- The candidate key is not unique.


### Observations and design implications

- No exact duplicate rows were detected.

- The same order and procedure description can occur with different `procedure_time_to_culturetime` values.

- The semantics of repeated procedure observations have not been fully established.

- The proposed candidate key will not be used as a primary key without further validation.



### Provisional grain

Culture order × prior procedure observation.



## 7. Implied Susceptibility

Source file: `microbiology_cultures_implied_susceptibility.csv`

### Dataset profile

- **Rows:** 1,978,731

- **Columns:** 7

- **Distinct order IDs:** 118,763

- **Missing values:** 0 across all seven columns

- **Exact duplicate rows:** 0

- **Additional exact copies:** 0



### Candidate-key validation

Candidate key tested: `order_proc_id_coded` + `organism` + `antibiotic`

- Distinct candidate keys: 1,970,992

- Repeated candidate-key groups: 7,561

- The candidate key is not unique.


### Observations and design implications

- No exact duplicate rows were detected.

- Some repeated order–organism–antibiotic combinations have different observed susceptibility values.

- The observed `susceptibility` and `implied_susceptibility` fields must be preserved separately.

- The literal string `Null` appears in both susceptibility distributions. It is distinct from a pandas missing value; all columns returned zero missing values in the initial null check.

- The reasons for conflicting observed susceptibility values have not been established.

- The proposed candidate key will not be used as a primary key without further validation.


### Provisional grain

Culture order × organism × antibiotic susceptibility observation.


## 8. Antibiotic Subtype Exposure

Source file: `microbiology_cultures_antibiotic_subtype_exposure.csv`

### Dataset profile

- Rows: 5,402,486
- Columns: 9
- Distinct order IDs: 484,370
- Missing values: 0 across all nine columns
- Rows belonging to exact-duplicate groups: 0
- Additional exact duplicate rows: 0

### Candidate-key validation

Candidate key tested: `order_proc_id_coded` + `medication_name` + `antibiotic_subtype`

- Distinct candidate keys: 2,393,872
- Repeated candidate-key groups: 1,026,353
- The candidate key is not unique.

### Observations and design implications

- No exact duplicate rows were detected.
- Repeated combinations can have different `antibiotic_subtype_category` values, such as `CEP13` and `CEP12`.
- The interval column is named `medication_time_to_cultureTime` in the source, with a capital `T` in `Time`.
- The repeated combinations do not by themselves establish duplicate records or explain the clinical meaning of the categories and interval values.
- Preserve the original source column names in Bronze; standardize names in Silver if appropriate.
- The proposed candidate key will not be used as a primary key without further validation.

### Provisional grain

Culture order × antibiotic subtype exposure record, subject to validation of the exposure record's semantics.



## 9. Microbial Resistance History

Source file: `microbiology_cultures_microbial_resistance.csv`

### Dataset profile

- Rows: 2,161,648
- Columns: 7
- Distinct order IDs: 166,537
- Missing values: 0 across all seven columns
- Rows belonging to exact-duplicate groups: 0
- Additional exact duplicate rows: 0

### Candidate-key validation

Candidate key tested: `order_proc_id_coded` + `organism` + `antibiotic`

- Distinct candidate keys: 867,757
- Repeated candidate-key groups: 348,999
- The candidate key is not unique.

### Resistance interval summary

For `resistant_time_to_culturetime`:

- Minimum: 1
- 25th percentile: 265
- Median: 758
- 75th percentile: 1,616
- Maximum: 5,773

These are numeric values as stored. The interval's unit and direction should be confirmed from dataset documentation before assigning a clinical interpretation.

### Observations and design implications

- No exact duplicate rows were detected.
- The same order–organism–antibiotic combination can have different interval values.
- The interval may distinguish multiple observations associated with the same combination, but its precise meaning has not been established by profiling alone.
- Do not assume that resistance was known before the culture until the interval's direction and definition are confirmed.
- The proposed candidate key will not be used as a primary key without further validation.

### Provisional grain

Culture order × organism × antibiotic resistance-history observation, subject to validation of the interval's meaning.


## 10. Cross-Table Findings

- `order_proc_id_coded` repeats in all eight clinical/event tables and should not be assumed to uniquely identify individual rows.
- Repeated order IDs are compatible with one-to-many relationships between culture orders and result, exposure, or history records.
- Exact duplicate rows and repeated candidate-key groups are different findings. The prior medication table contains exact repetitions; the other seven profiled tables do not.
- No pandas nulls were found in the profiled columns of the eight clinical/event tables. This does not mean literal strings such as `Null` are missing values.
- Candidate keys tested so far are provisional and must not be treated as validated primary keys where uniqueness checks failed.
- The cohort order identifier is not unique at the result-row level. Do not create foreign keys to cohort result rows using `order_proc_id_coded` unless uniqueness is established for the referenced relation.
- Surrogate identifiers will be considered for modeled records where a reliable natural key has not been established.
- Preserve raw source values in Bronze; apply documented naming, type, normalization, and duplicate-handling rules in Silver.



## 11. Outstanding Questions

1\. What is the precise record-level meaning of repeated prior-infection and antibiotic exposure observations?

2\. Which duplicate-handling rule, if any, is appropriate for the prior medication table?

3\. Which source fields reliably identify a culture order across the selected tables?

4\. What surrogate identifiers and relational constraints should be implemented in the Silver and Gold models?


These questions will be addressed as needed during data modeling and transformation-rule design. They do not prevent the remaining source tables from being profiled.



## 12. Profiling Status

All eight selected clinical/event source tables have completed initial structural and candidate-key profiling:

- [x] `microbiology_cultures_cohort.csv`
- [x] `microbiology_cultures_prior_med.csv`
- [x] `microbiology_culture_prior_infecting_organism.csv`
- [x] `microbiology_cultures_antibiotic_class_exposure.csv`
- [x] `microbiology_cultures_priorprocedures.csv`
- [x] `microbiology_cultures_implied_susceptibility.csv`
- [x] `microbiology_cultures_antibiotic_subtype_exposure.csv`
- [x] `microbiology_cultures_microbial_resistance.csv`

The supporting `implied_susceptibility_rules.csv` reference table should be documented separately from the clinical/event tables. Initial profiling is complete; unresolved semantics and duplicate policies will be addressed during data-quality rule and transformation design.
