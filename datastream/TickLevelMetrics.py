import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from config import fast_window, slow_window
TICK_DATA_DIR = "tick_data"
METRICS_DATA_DIR = "metrics_data"

os.makedirs(METRICS_DATA_DIR, exist_ok=True)

def compute_metrics(df):
    """Compute volume delta, cumulative delta, and VWAP for a single instrument."""
    if df.empty:
        #print(f"⚠️ Empty dataframe for {instrument_token}, skipping...")
        return df

    # Sort properly
    df = df.sort_values(by=["exchange_timestamp", "volume_traded"]).reset_index(drop=True)

     # --- Dynamic OHLC Computation ---
    ohlc_open = []
    ohlc_high = []
    ohlc_low = []
    ohlc_close = []

    for i, price in enumerate(df['last_price']):
        if i == 0:
            ohlc_open.append(price)
            ohlc_high.append(price)
            ohlc_low.append(price)
            ohlc_close.append(price)
        else:
            prev_close = ohlc_close[-1]
            prev_high = ohlc_high[-1]
            prev_low = ohlc_low[-1]

            ohlc_open.append(prev_close)
            ohlc_high.append(max(prev_high, price))
            ohlc_low.append(min(prev_low, price))
            ohlc_close.append(price)

    df['ohlc_open'] = ohlc_open
    df['ohlc_high'] = ohlc_high
    df['ohlc_low'] = ohlc_low
    df['ohlc_close'] = ohlc_close
    
    # Volume Delta logic
    df['volume_delta'] = np.where(
        df['last_price'] >= df['depth_sell_0_price'],
        df["volume_traded"].diff().fillna(0),
        np.where(
            df['last_price'] <= df['depth_buy_0_price'],
            -df["volume_traded"].diff().fillna(0),
            0
        )
    )

    # Cumulative Volume Delta
    df['cumulative_volume_delta'] = df['volume_delta'].cumsum()

    # VWAP
    df['vwap'] = df['average_traded_price']

        # --- Moving Averages ---
    df["Fast_MA"] = df["last_price"].rolling(window=fast_window, min_periods=1).mean()
    df["Slow_MA"] = df["last_price"].rolling(window=slow_window, min_periods=1).mean()

    # --- Select only required columns ---
    selected_columns = [
        "instrument_token",
        "last_price",
        "average_traded_price",
        "volume_traded",
        "total_buy_quantity",
        "total_sell_quantity",
        "ohlc_open",
        "ohlc_high",
        "ohlc_low",
        "ohlc_close",
        "last_trade_time",
        "exchange_timestamp",
        "depth_buy_0_price",
        "depth_sell_0_price",
        "trading_date",
        "volume_delta",
        "cumulative_volume_delta",
        "vwap",
        "Fast_MA",
        "Slow_MA"
    ]

    df_out = df[selected_columns]
    return df_out


def process_instrument_file(file_path, output_path):
    """Read a tick parquet, compute metrics, and write to output parquet."""
    try:
        df = pd.read_parquet(file_path)
        df_metrics = compute_metrics(df)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_metrics.to_parquet(output_path, index=False)
        return True
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return False


def run_batch_metrics():
    """Iterate over all tick_data folders and process metrics for each file."""
    #for date_folder in sorted(os.listdir(TICK_DATA_DIR)):
    for date_folder in ['2025-10-30']:
        date_path = os.path.join(TICK_DATA_DIR, date_folder)
        if not os.path.isdir(date_path):
            continue

        print(f"\n📅 Processing date: {date_folder}")
        output_date_path = os.path.join(METRICS_DATA_DIR, date_folder)
        os.makedirs(output_date_path, exist_ok=True)

        parquet_files = [f for f in os.listdir(date_path) if f.endswith(".parquet")]

        for file_name in tqdm(parquet_files, desc=f"Processing {date_folder}"):
            instrument_token = os.path.splitext(file_name)[0]
            file_path = os.path.join(date_path, file_name)
            output_path = os.path.join(output_date_path, file_name)

            process_instrument_file(file_path, output_path)

    print("\n✅ All metrics computed successfully!")


if __name__ == "__main__":
    run_batch_metrics()
