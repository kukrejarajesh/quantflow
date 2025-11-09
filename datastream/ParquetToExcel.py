import os
import pandas as pd
import pyarrow.parquet as pq

def parquet_to_excel(date_str, instruments, base_dir='metrics_rollup/quarterly', output_file='output.xlsx'):
    """
    Reads multiple parquet files (one per instrument) and saves them into an Excel workbook
    where each sheet corresponds to one instrument token.

    Parameters
    ----------
    date_str : str
        Date folder to read from (e.g., '2025-11-06')
    instruments : list[int]
        List of instrument tokens (e.g., [3529217, 256265])
    base_dir : str, optional
        Base directory of parquet files
    output_file : str, optional
        Path to output Excel file
    """

    date_dir = os.path.join(base_dir, date_str)
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    for token in instruments:
        parquet_path = os.path.join(date_dir, f"{token}.parquet")

        if not os.path.exists(parquet_path):
            print(f"⚠️ File not found for token {token}: {parquet_path}")
            continue

        try:
            # Read parquet
            parquet_file = pq.ParquetFile(parquet_path)
            df = parquet_file.read().to_pandas()

            # Ensure timestamp parsing and sorting
            if 'exchange_timestamp' in df.columns:
                df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
                df = df.sort_values('exchange_timestamp')

            # Write to a separate sheet
            sheet_name = str(token)[:31]  # Excel limit on sheet name length
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"✅ Added token {token} to sheet '{sheet_name}'")

        except Exception as e:
            print(f"❌ Error reading {token}: {e}")

    writer.close()
    print(f"\n📘 Excel workbook created successfully: {output_file}")


if __name__ == "__main__":
    # ---- User Configuration ----
    date_str = "2025-11-06"  # manually change as needed
    instruments = [2714625,2170625,	951809,	5728513,	2977281,	424961,	5633,	175361,	1215745,	315393,	119553,	387073,	3520257,	4574465,	4632577,	1629185,	4454401,	6412545,	3930881,	3529217]  # list of tokens to include
    output_file = f"metrics_rollup/selected_stocks_metrics/metrics_summary_{date_str}.xlsx"

    # ---- Run ----
    parquet_to_excel(date_str, instruments, output_file=output_file)
