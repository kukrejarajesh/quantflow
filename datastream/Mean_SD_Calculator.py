import os
import pandas as pd
from datetime import time, datetime
from config import example_tokens

example_tokens = set(example_tokens)

def calculate_mean_std_adjusted(
    root_folder,
    from_date=None,
    to_date=None,
    instruments=None
):
    """
    Calculates mean & std of 15-min interval volumes for each instrument
    across date folders, with:
    - optional from_date/to_date filtering (YYYY-MM-DD)
    - optional instrument list filtering
    - corrected first 9:15 interval and market hour limits
    """

    # Convert date strings to datetime objects if provided
    from_dt = datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None
    to_dt = datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None

    instrument_data = {}
    processed_dates = []

    for date_folder in sorted(os.listdir(root_folder)):
        date_path = os.path.join(root_folder, date_folder)
        if not os.path.isdir(date_path):
            continue

        # Ensure folder is a valid date
        try:
            folder_date = datetime.strptime(date_folder, "%Y-%m-%d").date()
        except ValueError:
            continue

        # Apply date range filtering
        if from_dt and folder_date < from_dt:
            continue
        if to_dt and folder_date > to_dt:
            continue

        processed_dates.append(folder_date)
        print(f"📅 Processing date folder: {date_folder}")

        for file_name in os.listdir(date_path):
            if not file_name.endswith(".parquet"):
                continue

            file_path = os.path.join(date_path, file_name)
            try:
                df = pd.read_parquet(file_path, columns=['instrument_token', 'exchange_timestamp', 'volume_traded'])
                if df.empty:
                    continue

                token = int(df['instrument_token'].iloc[0])

                # Apply instrument filtering
                if instruments and token not in instruments:
                    continue

                # Ensure datetime
                df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])

                # 1️⃣ Filter regular market hours
                df = df[
                    (df['exchange_timestamp'].dt.time >= time(9, 15)) &
                    (df['exchange_timestamp'].dt.time <= time(15, 15))
                ]
                if df.empty:
                    continue

                # 2️⃣ Sort and compute interval volume
                df = df.sort_values('exchange_timestamp')
                df['interval_volume'] = df['volume_traded'].diff().fillna(df['volume_traded'])
                df['interval_volume'] = df['interval_volume'].clip(lower=0)

                # 3️⃣ Replace first (09:15) interval with next (09:30) value
                if len(df) > 1:
                    df.loc[df.index[0], 'interval_volume'] = df.iloc[1]['interval_volume']

                # 4️⃣ Collect data per instrument
                if token not in instrument_data:
                    instrument_data[token] = []
                instrument_data[token].append(df['interval_volume'].values)

            except Exception as e:
                print(f"⚠️ Error reading {file_path}: {e}")

    # 5️⃣ Compute mean/std
    summary_records = []
    last_date = max(processed_dates) if processed_dates else None

    for token, arrays in instrument_data.items():
        flat = [v for arr in arrays for v in arr]
        s = pd.Series(flat)
        mean_val = s.mean()
        std_val = s.std(ddof=0)
        summary_records.append({
            'instrument_token': token,
            'mean_interval_volume': mean_val,
            'std_interval_volume': std_val,
            'from_date': from_date,
            'to_date': to_date,
            'last_processed_date': str(last_date)
        })

    summary_df = pd.DataFrame(summary_records).sort_values('instrument_token')
    return summary_df


if __name__ == "__main__":
    # === CONFIGURATION ===
    root_folder = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly"

    # 🔹 Choose date range (optional)
    from_date = "2025-10-31"
    to_date = "2025-11-14"

    # 🔹 Choose instruments (optional) — leave empty to include all
    instruments = example_tokens   # e.g., [189185, 5633, 1723649]
    # instruments = None            # Uncomment this to include all instruments

    summary_df = calculate_mean_std_adjusted(
        root_folder=root_folder,
        from_date=from_date,
        to_date=to_date,
        instruments=instruments
    )

    # 🔹 Save output
    output_path = os.path.join(root_folder, f"interval_volume_summary_{from_date}_to_{to_date}.parquet")
    summary_df.to_parquet(output_path, index=False)

    print(f"\n✅ Summary saved to: {output_path}")
    print(summary_df)
