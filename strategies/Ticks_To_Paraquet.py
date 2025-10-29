import pandas as pd
import os
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa

# A helper to flatten the nested tick data
def flatten_tick(tick):
    flat = {
        "timestamp": datetime.now(),
        "instrument_token": tick.get("instrument_token"),
        "last_price": tick.get("last_price"),
        "last_traded_quantity": tick.get("last_traded_quantity"),
        "average_traded_price": tick.get("average_traded_price"),
        "volume_traded": tick.get("volume_traded"),
        "total_buy_quantity": tick.get("total_buy_quantity"),
        "total_sell_quantity": tick.get("total_sell_quantity"),
        "change": tick.get("change"),
        "last_trade_time": tick.get("last_trade_time"),
        "exchange_timestamp": tick.get("exchange_timestamp"),
        "oi": tick.get("oi"),
        "oi_day_high": tick.get("oi_day_high"),
        "oi_day_low": tick.get("oi_day_low"),
    }

    # OHLC fields
    if "ohlc" in tick:
        ohlc = tick["ohlc"]
        flat.update({
            "open": ohlc.get("open"),
            "high": ohlc.get("high"),
            "low": ohlc.get("low"),
            "close": ohlc.get("close")
        })

    # Flatten top 5 buy/sell orders
    if "depth" in tick:
        for i in range(5):
            buy_entry = tick["depth"]["buy"][i] if len(tick["depth"]["buy"]) > i else {}
            sell_entry = tick["depth"]["sell"][i] if len(tick["depth"]["sell"]) > i else {}
            flat.update({
                f"buy_{i+1}_price": buy_entry.get("price"),
                f"buy_{i+1}_qty": buy_entry.get("quantity"),
                f"sell_{i+1}_price": sell_entry.get("price"),
                f"sell_{i+1}_qty": sell_entry.get("quantity"),
            })
    return flat

# Function to save ticks incrementally
def save_ticks_to_parquet(ticks, instrument_name):
    # Flatten all ticks
    flat_ticks = [flatten_tick(t) for t in ticks]
    df = pd.DataFrame(flat_ticks)

    # Create folder for tick data
    os.makedirs("tick_data", exist_ok=True)

    # Use date-based filename
    today = datetime.now().strftime("%Y-%m-%d")
    file_path = f"tick_data/{instrument_name}_{today}.parquet"

    # Append if file exists, else create new
    table = pa.Table.from_pandas(df)
    if os.path.exists(file_path):
        existing = pq.read_table(file_path)
        combined = pa.concat_tables([existing, table])
        pq.write_table(combined, file_path)
    else:
        pq.write_table(table, file_path)

    print(f"✅ Saved {len(df)} ticks to {file_path}")
