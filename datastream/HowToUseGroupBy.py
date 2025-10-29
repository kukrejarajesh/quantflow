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


def save_rollup(df, instrument_token, freq, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    freq_label = freq.replace(" ", "").replace("Min", "min").replace("H", "hour").replace("D", "day")
    output_path = os.path.join(output_dir, f"{instrument_token}_{freq_label}.parquet")
    df.to_parquet(output_path, index=False)
    print(f"✅ {freq}-level rollup saved for {instrument_token} -> {output_path}")
