import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

path = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly\dashboard_zscores.parquet"

print("Opening corrupted parquet:", path)

# Low-level read (bypasses Arrow schema validation)
pf = pq.ParquetFile(path)
dfs = []

for i in range(pf.num_row_groups):
    print(f"Reading row group {i}/{pf.num_row_groups-1} ...")
    tbl = pf.read_row_group(i)          # read without schema 
    df = tbl.to_pandas()                # convert to pandas

    # Drop duplicate numeric columns ("0", "1", etc.)
    bad_numeric_cols = [c for c in df.columns if str(c).isdigit()]
    if bad_numeric_cols:
        print("Dropping numeric duplicate cols:", bad_numeric_cols)
        df = df.drop(columns=bad_numeric_cols, errors="ignore")

    # Remove unwanted metadata columns
    meta_cols = [
        "__fragment_index", "__batch_index",
        "__last_in_fragment", "__filename"
    ]
    df = df.drop(columns=[c for c in meta_cols if c in df.columns], errors="ignore")

    # Drop rows where instrument_token is NaN
    df = df[df["instrument_token"].notna()]

    dfs.append(df)

# Combine clean data
clean_df = pd.concat(dfs, ignore_index=True)

# Final sanity cleanup of duplicates
clean_df = clean_df.loc[:, ~clean_df.columns.duplicated()]

print("Saving cleaned parquet...")
clean_df.to_parquet(path, index=False)

print("\n✔ CLEANED FILE SAVED ✔")
print("Final columns:", clean_df.columns)
print("Rows:", len(clean_df))
