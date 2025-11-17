import os
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')


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
        'vwap': 'last',
        'Fast_MA': 'last',
        'Slow_MA': 'last'
    })

    rolled.columns = [
        'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close',
        'volume_traded', 'volume_delta', 'cumulative_volume_delta',
        'vwap', 'Fast_MA', 'Slow_MA'
    ]

    rolled = rolled.reset_index()
    rolled['trading_date'] = rolled['exchange_timestamp'].dt.date
    
    return rolled




def rollup_all_instruments(metrics_base_dir="metrics_data", output_base_dir="metrics_rollup"):
    """
    Rolls up fast_ma and slow_ma from tick-level to minute, hourly, and daily for all instruments and dates.
    """
    intervals = ["minute", "hourly", "daily"]

    # Iterate over all date folders
    #for trade_date in os.listdir(metrics_base_dir):
    for trade_date in ['2025-11-17']:    
        date_path = os.path.join(metrics_base_dir, trade_date)
        print(f"Processing date: {trade_date}")
        print(f"Date path: {date_path}")
        if not os.path.isdir(date_path):
            continue

        # Iterate over all instruments in the date folder
        for file_name in os.listdir(date_path):
            if not file_name.endswith(".parquet"):
                continue

            instrument_token = file_name.replace(".parquet", "")
            file_path = os.path.join(date_path, file_name)
            
            # Load tick-level metrics
            df = pd.read_parquet(file_path)
            if df.empty:
                continue
            minute_df= rollup_metrics(df,"1min" )
            minute_dir = os.path.join(output_base_dir, 'minute', str(trade_date))
            os.makedirs(minute_dir, exist_ok=True)
            minute_df.to_parquet(os.path.join(minute_dir, f"{instrument_token}.parquet"), index=False)

            df_hourly = rollup_metrics(df,"1h")
            hourly_dir = os.path.join(output_base_dir, 'hourly', str(trade_date))
            os.makedirs(hourly_dir, exist_ok=True)
            df_hourly.to_parquet(os.path.join(hourly_dir, f"{instrument_token}.parquet"), index=True)

            df_daily = rollup_metrics(df,"1d")
            daily_dir = os.path.join(output_base_dir, "daily", trade_date)
            os.makedirs(daily_dir, exist_ok=True)
            df_daily.to_parquet(os.path.join(daily_dir, f"{instrument_token}.parquet"), index=True)
            
            df_quarterly = rollup_metrics(df,"15min")
            quarterly_dir = os.path.join(output_base_dir, "quarterly", trade_date)  
            os.makedirs(quarterly_dir, exist_ok=True)
            df_quarterly.to_parquet(os.path.join(quarterly_dir, f"{instrument_token}.parquet"), index=True)

            print(f"✅ Rolled up instrument {instrument_token} for {trade_date}")




def main():
    rollup_all_instruments(metrics_base_dir="metrics_data", output_base_dir="metrics_rollup")

if __name__ == "__main__":
    main()

