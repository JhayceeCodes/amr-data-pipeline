import pandas as pd

path = "data/source/microbiology_cultures_microbial_resistance.csv"
df = pd.read_csv(path)

print("Rows:", len(df))
print("Columns:", len(df.columns))
print("\nColumn names:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)

print("\nMissing values:")
print(df.isna().sum())

# Exact duplicate checks
print("\nRows belonging to exact duplicate groups:", df.duplicated(keep=False).sum())
print("Additional exact duplicate rows:", df.duplicated().sum())

# Culture-order coverage
order_col = "order_proc_id_coded"
print("\nDistinct culture orders:", df[order_col].nunique())
print("Missing culture-order IDs:", df[order_col].isna().sum())

# Provisional candidate key
candidate_key = [
    "order_proc_id_coded",
    "organism",
    "antibiotic",
]

key_counts = df.groupby(candidate_key, dropna=False).size()

print("\nDistinct candidate keys:", len(key_counts))
print("Repeated candidate-key groups:", (key_counts > 1).sum())

print("\nSample repeated candidate-key records:")
repeated_keys = key_counts[key_counts > 1].head(5).index

if len(repeated_keys):
    sample = df.set_index(candidate_key).loc[repeated_keys].reset_index()
    print(sample.to_string(index=False))
else:
    print("No repeated candidate-key groups found.")

# Inspect the resistance-history interval
interval_col = "resistant_time_to_culturetime"

if interval_col in df.columns:
    print("\nResistance interval summary:")
    print(df[interval_col].describe())
    print("\nSample interval values:")
    print(df[interval_col].value_counts(dropna=False).head(10))
