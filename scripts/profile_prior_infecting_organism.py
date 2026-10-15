
import pandas as pd

path = "data/source/microbiology_culture_prior_infecting_organism.csv"
df = pd.read_csv(path)

print(f"Rows: {len(df):,}")
print(f"Columns: {len(df.columns)}")
print("\nColumn names:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)

print("\nNull counts:")
print(df.isna().sum())

print("\nExact duplicate rows:")
print(f"Rows in duplicate groups: {df.duplicated(keep=False).sum():,}")
print(f"Additional copies: {df.duplicated().sum():,}")

order_id = "order_proc_id_coded"

print("\nOrder identifier profile:")
print(f"Distinct order IDs: {df[order_id].nunique():,}")
print(f"Missing order IDs: {df[order_id].isna().sum():,}")

# Candidate key: order + prior organism
candidate_key = [
    order_id,
    "prior_organism",
]

key_counts = df.groupby(candidate_key, dropna=False).size()

print("\nCandidate-key profile:")
print(f"Distinct candidate keys: {len(key_counts):,}")
print(f"Repeated candidate-key groups: {(key_counts > 1).sum():,}")

# Check whether repeated candidate keys have different timestamps or intervals
if (key_counts > 1).any():
    repeated_keys = key_counts[key_counts > 1].index
    repeated_rows = df.set_index(candidate_key).loc[repeated_keys]

    print("\nSample repeated candidate-key records:")
    print(repeated_rows.reset_index().head(20).to_string(index=False))
