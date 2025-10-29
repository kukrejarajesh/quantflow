import json
import pandas as pd
from pathlib import Path

def flatten_tick_data(json_obj):
    """Flatten one tick JSON into a flat dict."""
    flat = {}

    # Copy top-level keys
    for k, v in json_obj.items():
        if isinstance(v, (str, int, float, bool)):
            flat[k] = v
        elif k == "ohlc":
            for sub_k, sub_v in v.items():
                flat[f"ohlc_{sub_k}"] = sub_v
        elif k == "depth":
            # Flatten top 5 buy/sell levels
            for side in ["buy", "sell"]:
                levels = v.get(side, [])
                for i, level in enumerate(levels):
                    for sub_k, sub_v in level.items():
                        flat[f"{side}{i+1}_{sub_k}"] = sub_v
    return flat


def convert_json_to_excel(json_file_path, output_excel_path):
    json_file = Path(json_file_path)
    output_file = Path(output_excel_path)

    print(f"Reading file: {json_file}")
    data_list = []

    with open(json_file, "r", encoding="utf-8") as f:
        buffer = ""
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try loading single line JSON
                record = json.loads(line)
                data_list.append(flatten_tick_data(record))
            except json.JSONDecodeError:
                # Handle pretty-printed multi-line JSON objects
                buffer += line
                if line.endswith("}"):
                    try:
                        record = json.loads(buffer)
                        data_list.append(flatten_tick_data(record))
                        buffer = ""
                    except json.JSONDecodeError:
                        pass

    print(f"✅ Loaded {len(data_list)} records")

    if not data_list:
        print("❌ No valid JSON objects found — check file format.")
        return

    print("Converting to DataFrame...")
    df = pd.DataFrame(data_list)

    print("Saving to Excel (this may take some time)...")
    df.to_excel(output_file, index=False)
    print(f"✅ Excel file saved successfully: {output_file}")


if __name__ == "__main__":
    # Change these paths as needed
    input_path = r"F:\working\2024\Zerodha\breeze_data_service\tick_data_log_2.json"
    output_path = r"F:\working\2024\Zerodha\breeze_data_service\tick_data_log_2.xlsx"
    convert_json_to_excel(input_path, output_path)
