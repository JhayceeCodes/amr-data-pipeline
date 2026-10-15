
import pandas as pd

path = "data/source/microbiology_cultures_cohort.csv"
df = pd.read_csv(path)

order_id = "order_proc_id_coded"

print(f"Rows: {len(df):,}")
print(f"Distinct order IDs: {df[order_id].nunique():,}")
print(f"Rows with missing order IDs: {df[order_id].isna().sum():,}")

# 1. Exact duplicate rows
exact_duplicate_mask = df.duplicated(keep=False)
print(
    "Rows belonging to exact-duplicate groups:",
    f"{exact_duplicate_mask.sum():,}",
)
print(
    "Additional exact copies:",
    f"{df.duplicated().sum():,}",
)

# 2. How many order IDs occur more than once?
rows_per_order = df.groupby(order_id, dropna=False).size()

print(
    "Order IDs appearing more than once:",
    f"{(rows_per_order > 1).sum():,}",
)
print(
    "Maximum rows per order:",
    rows_per_order.max(),
)

# 3. Test a plausible result-level candidate key
candidate_key = [
    order_id,
    "organism",
    "antibiotic",
]

key_counts = df.groupby(
    candidate_key,
    dropna=False,
).size()

print(
    "Repeated order-organism-antibiotic groups:",
    f"{(key_counts > 1).sum():,}",
)

print("\nMost repeated order IDs:")
print(rows_per_order.sort_values(ascending=False).head(10))
