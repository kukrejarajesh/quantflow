import os
import pandas as pd
import pyarrow.parquet as pq

def parquet_to_excel( parquet_file_name, parquet_path, output_file='output.xlsx'):
    
    
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    parquet_path = os.path.join(parquet_path, f"{parquet_file_name}.parquet")

    print(parquet_path)
    print(output_file)
    try:
        # Read parquet
        parquet_file = pq.ParquetFile(parquet_path)
        df = parquet_file.read().to_pandas()

        df.to_excel(writer, index=False)
        

    except Exception as e:
                    print(f"❌ Error reading : {e}")

    writer.close()
    print(f"\n📘 Excel workbook created successfully: {output_file}")


if __name__ == "__main__":
    # ---- User Configuration ----
    
    
    parquet_path = f"two_stage_strategy_results"
    parquet_file_name = f"eod_crossovers"
    output_file = f"two_stage_strategy_results/{parquet_file_name}.xlsx"
    
    parquet_path = f"metrics_rollup/consolidated_eod"
    parquet_file_name = f"341249"
    output_file = f"{parquet_path}/{parquet_file_name}.xlsx"
    # ---- Run ----
    parquet_to_excel(parquet_file_name, parquet_path, output_file=output_file)
