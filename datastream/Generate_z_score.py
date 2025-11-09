import os
import pandas as pd
from datetime import time
from config import  instruments_to_process
instruments_to_process = set(instruments_to_process)
# === CONFIGURATION ===
ROLLUP_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly"
SUMMARY_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly\interval_volume_summary_2025-10-31_to_2025-11-07.parquet"
OUTPUT_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly\dashboard_zscores.parquet"

DATE_FOLDER = "2025-11-07"   # 👈 The folder to process (latest date)
INSTRUMENTS = instruments_to_process  # 👈 Set of instrument tokens to process
MARKET_CLOSE = time(15, 15)    # 3:15 PM cutoff

def compute_latest_metrics(date_folder, instruments, summary_df):
    results = []

    for token in instruments:
        file_path = os.path.join(ROLLUP_ROOT, date_folder, f"{token}.parquet")
        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            continue

        try:
            df = pd.read_parquet(file_path)
            df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
            df = df.sort_values("exchange_timestamp")

            df = df[df['exchange_timestamp'].dt.time <= MARKET_CLOSE]
            if len(df) < 2:
                continue

            # Get last two rows to compute 15-min volume
            last_row = df.iloc[-1].copy()
            prev_row = df.iloc[-2]

            # 15-min traded volume
            last_row["15min_Volume"] = max(last_row["volume_traded"] - prev_row["volume_traded"], 0)

            # Fetch Mean and StdDev from summary
            stats = summary_df[summary_df["instrument_token"] == token]
            if stats.empty:
                print(f"⚠️ No summary stats for instrument {token}")
                continue

            mean_val = stats["mean_interval_volume"].iloc[0]
            std_val = stats["std_interval_volume"].iloc[0]

            # Compute Z-score
            z = (last_row["15min_Volume"] - mean_val) / std_val if std_val != 0 else 0

            # Add columns
            last_row["Mean"] = mean_val
            last_row["S.D."] = std_val
            last_row["z_score"] = z

            # Select final display columns
            result = last_row[
                [
                    "instrument_token", "exchange_timestamp",
                    "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close",
                    "volume_traded", "vwap",
                    "Mean", "S.D.", "15min_Volume", "z_score"
                ]
            ]
            results.append(result)

        except Exception as e:
            print(f"⚠️ Error processing {file_path}: {e}")

    return pd.DataFrame(results)


if __name__ == "__main__":
    print(f"🔍 Generating Z-score metrics for {DATE_FOLDER}")

    summary_df = pd.read_parquet(SUMMARY_PATH)
    summary_df["instrument_token"] = summary_df["instrument_token"].astype(int)

    final_df = compute_latest_metrics(DATE_FOLDER, INSTRUMENTS, summary_df)

    if not final_df.empty:
        if os.path.exists(OUTPUT_PATH):
            try:
                # Read existing data
                existing_df = pd.read_parquet(OUTPUT_PATH)
                
                # Combine old + new
                combined_df = pd.concat([existing_df, final_df], ignore_index=True)
                
                # Drop duplicates based on timestamp & token if already exists
                combined_df.drop_duplicates(
                    subset=["instrument_token", "exchange_timestamp"],
                    keep="last",
                    inplace=True
                )
            except Exception as e:
                print(f"⚠️ Error reading existing parquet, recreating file: {e}")
                combined_df = final_df.copy()
        else:
            combined_df = final_df.copy()

        # Sort by time & token
        combined_df = combined_df.sort_values(["exchange_timestamp", "instrument_token"])

        # Save back (overwrite with appended data)
        combined_df.to_parquet(OUTPUT_PATH, index=False)

        print(f"\n✅ Z-score metrics appended to: {OUTPUT_PATH}")
        print(f"📈 Total rows: {len(combined_df)} | Added: {len(final_df)}")
    else:
        print("⚠️ No results generated. Check input files.")
