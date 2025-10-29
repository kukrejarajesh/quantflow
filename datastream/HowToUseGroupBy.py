import pandas as pd
import numpy as np
import os

def rollup_metrics(df, freq='1Min'):
    """
    Roll up tick-level metrics to minute/hour/day level.
    freq can be '1Min', '1H', '1D', etc.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Ensure timestamps are datetime
    df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])

    grouped = df.groupby([
        'instrument_token',
        pd.Grouper(key='exchange_timestamp', freq=freq)
    ])

    rolled = grouped.agg({
        'last_price': ['first', 'max', 'min', 'last'],
        'volume_traded': 'last',
        'volume_delta': 'sum',
        'cumulative_volume_delta': 'last',
        'vwap': 'mean',
        'Fast_MA': 'mean',
        'Slow_MA': 'mean'
    })

    rolled.columns = [
        'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close',
        'volume_traded', 'volume_delta', 'cumulative_volume_delta',
        'vwap', 'Fast_MA', 'Slow_MA'
    ]

    rolled = rolled.reset_index()
    rolled['trading_date'] = rolled['exchange_timestamp'].dt.date

    return rolled

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


def save_rollup(df, instrument_token, freq, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    freq_label = freq.replace(" ", "").replace("Min", "min").replace("H", "hour").replace("D", "day")
    output_path = os.path.join(output_dir, f"{instrument_token}_{freq_label}.parquet")
    df.to_parquet(output_path, index=False)
    print(f"✅ {freq}-level rollup saved for {instrument_token} -> {output_path}")
