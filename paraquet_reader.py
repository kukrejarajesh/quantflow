import pyarrow.parquet as pq

# 1. Read the file
#parquet_file = pq.ParquetFile('historicaldata/ICIBAN.parquet')
#parquet_file = pq.ParquetFile('tick_data/2025-10-29/3329.parquet')
#parquet_file = pq.ParquetFile('metrics_data/2025-10-29/140033.parquet')
parquet_file = pq.ParquetFile('metrics_rollup/minute/2025-10-30/2170625.parquet')
#parquet_file = pq.ParquetFile('E:/working/historicaldata/1mincandles/ICIBAN.parquet')
#parquet_file = pq.ParquetFile('historicaldata/CADHEA_1day_20220101_20251017.parquet')
# 2. Inspect the Schema
schema = parquet_file.schema
# --- CORRECTION APPLIED HERE ---
# The schema object prints its string representation when passed to print()
print("Schema:\n", schema) 

# You can also use .__repr__() to explicitly get the string representation
# print("Schema:\n", schema.__repr__())
# ------------------------------

# 3. Inspect the Metadata (File-level and Row-Group-level statistics)
metadata = parquet_file.metadata
#print("\nMetadata:\n", metadata)

# 4. Read the data (e.g., the whole file or a subset)
table = parquet_file.read()
# Convert to a Pandas DataFrame for easy viewing
df = table.to_pandas()
#print("all columns:", df.columns.tolist())
#print("\nFirst 5 rows of data:\n", df.head())
print("first row only:\n", df.iloc[1])
print("totoal rows:", len(df))
# Use the .tail() method from Pandas to get the last 5 rows
last_five_rows = df.tail(5)
print("last row only:\n", df.iloc[-3])
#print("Index:", df.index)
#print("\nLast 5 rows of data:\n", last_five_rows)