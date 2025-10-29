import os
import pandas as pd

def rollup_metrics(base_dir: str, output_dir: str):
    """
    Rollup tick-level metrics to minute, hourly, and daily metrics for all instruments.

    Args:
        base_dir: Directory containing tick-level Parquet files (per instrument/date)
        output_dir: Root directory to save rolled-up metrics
    """
    for date_folder in sorted(os.listdir(base_dir)):
        date_path = os.path.join(base_dir, date_folder)
        if not os.path.isdir(date_path):
            continue

        for parquet_file in sorted(os.listdir(date_path)):
            if not parquet_file.endswith(".parquet"):
                continue

            file_path = os.path.join(date_path, parquet_file)
            df = pd.read_parquet(file_path)
            if df.empty:
                continue

            # Ensure proper timestamp
            df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])

            # Add minute, hour columns for grouping
            df['minute'] = df['exchange_timestamp'].dt.floor('T')  # round down to minute
            df['hour'] = df['exchange_timestamp'].dt.floor('H')    # round down to hour
            df['date'] = df['exchange_timestamp'].dt.date

            instrument_token = df['instrument_token'].iloc[0]

            # --- Rollup to minute ---
            minute_df = df.groupby('minute').agg({
                'last_price': 'last',
                'average_traded_price': 'last',
                'volume_traded': 'sum',
                'cumulative_volume_delta': 'last',
                'vwap': 'last',
                'fast_ma': 'last',
                'slow_ma': 'last',
                'ohlc_open': 'first',
                'ohlc_high': 'max',
                'ohlc_low': 'min',
                'ohlc_close': 'last'
            }).reset_index()
            
            minute_dir = os.path.join(output_dir, 'minute', str(date_folder))
            os.makedirs(minute_dir, exist_ok=True)
            minute_df.to_parquet(os.path.join(minute_dir, f"{instrument_token}.parquet"), index=False)

            # --- Rollup to hourly ---
            hourly_df = df.groupby('hour').agg({
                'last_price': 'last',
                'average_traded_price': 'last',
                'volume_traded': 'sum',
                'cumulative_volume_delta': 'last',
                'vwap': 'last',
                'fast_ma': 'last',
                'slow_ma': 'last',
                'ohlc_open': 'first',
                'ohlc_high': 'max',
                'ohlc_low': 'min',
                'ohlc_close': 'last'
            }).reset_index()

            hourly_dir = os.path.join(output_dir, 'hourly', str(date_folder))
            os.makedirs(hourly_dir, exist_ok=True)
            hourly_df.to_parquet(os.path.join(hourly_dir, f"{instrument_token}.parquet"), index=False)

            # --- Rollup to daily ---
            daily_df = df.groupby('date').agg({
                'last_price': 'last',
                'average_traded_price': 'last',
                'volume_traded': 'sum',
                'cumulative_volume_delta': 'last',
                'vwap': 'last',
                'fast_ma': 'last',
                'slow_ma': 'last',
                'ohlc_open': 'first',
                'ohlc_high': 'max',
                'ohlc_low': 'min',
                'ohlc_close': 'last'
            }).reset_index()

            daily_dir = os.path.join(output_dir, 'daily', str(date_folder))
            os.makedirs(daily_dir, exist_ok=True)
            daily_df.to_parquet(os.path.join(daily_dir, f"{instrument_token}.parquet"), index=False)

            print(f"✅ Rolled up metrics for instrument {instrument_token} on {date_folder}")



def main():
    # Directory where tick-level metrics are stored
    tick_metrics_dir = "metrics/tick"  # change this to your tick-level metrics folder

    # Directory where rolled-up data will be saved
    output_dir = "metrics/rollup"

    # Call the batch rollup function
    rollup_metrics(base_dir=tick_metrics_dir, output_dir=output_dir)

if __name__ == "__main__":
    main()
