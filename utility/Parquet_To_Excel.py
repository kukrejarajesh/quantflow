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
    
    
    # parquet_path = f"two_stage_strategy_results"
    # parquet_file_name = f"confirmed_entries"
    # output_file = f"two_stage_strategy_results/{parquet_file_name}.xlsx"
    instruments_to_process = [70401,	119553,	197633,	232961,	315393,	356865,	408065,	502785,	681985,	794369,	857857,	873217,	969473,	1102337,	1152769,	1215745,	1346049,	1510401,	1629185,	1850625,	2478849,	2714625,	2800641,	2953217,	2955009,	2995969,	3407361,	3465729,	3520257,	3725313,	3834113,	3920129,	4268801,	4843777,	5181953,	5197313,	5215745,	5573121,	6013185,	6191105,	6599681]
    
    for instrument_token in instruments_to_process:
        parquet_file_name = f"{instrument_token}"
        parquet_path = f"metrics_rollup/consolidated_eod"
        output_file = f"{parquet_path}/{parquet_file_name}_eod.xlsx"
   
        parquet_to_excel(parquet_file_name, parquet_path, output_file=output_file)
    # parquet_path = f"metrics_rollup/consolidated_hourly"
    # parquet_file_name = f"2911489"
    # output_file = f"{parquet_path}/{parquet_file_name}.xlsx"
    # # ---- Run ----
    # parquet_to_excel(parquet_file_name, parquet_path, output_file=output_file)
