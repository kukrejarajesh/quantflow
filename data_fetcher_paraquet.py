import os
import sys
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

print("Starting data_fetcher_breeze.py...")

# -------------------------------
# 1. Import pandas and pyarrow
# -------------------------------
try:
    print("1. Importing pandas and pyarrow...")
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    print("✅ Pandas and PyArrow imported successfully")
except ImportError as e:
    print(f"❌ Required imports failed: {e}")
    print("Please install required packages: pip install pandas pyarrow")
    sys.exit(1)

# -------------------------------
# 2. Import BreezeConnect
# -------------------------------
try:
    print("2. Importing BreezeConnect...")
    from breeze_connect import BreezeConnect
    print("✅ BreezeConnect imported successfully")
except ImportError as e:
    print(f"❌ BreezeConnect import failed: {e}")
    print("Please install: pip install breeze-connect")
    sys.exit(1)

# -------------------------------
# 3. Import dotenv
# -------------------------------
try:
    print("3. Importing dotenv...")
    from dotenv import load_dotenv
    print("✅ dotenv imported successfully")
except ImportError as e:
    print(f"❌ dotenv import failed: {e}")
    print("Please install: pip install python-dotenv")
    sys.exit(1)

# -------------------------------
# 4. Load environment variables
# -------------------------------
try:
    print("4. Loading environment variables...")
    env_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
    print(f"Looking for .env at: {env_path}")
    
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print("✅ Environment variables loaded from config folder")
    else:
        load_dotenv()
        print("✅ Environment variables loaded from default location")
    
    api_key = os.getenv("BREEZE_API_KEY")
    api_secret = os.getenv("BREEZE_API_SECRET")
    access_token = os.getenv("BREEZE_ACCESS_TOKEN")
    
    if not api_key or not api_secret or not access_token:
        print("❌ Missing one of BREEZE_API_KEY, BREEZE_API_SECRET, BREEZE_ACCESS_TOKEN in .env")
        sys.exit(1)
        
    print(f"✅ API_KEY: {api_key[:10]}...")
    print(f"✅ ACCESS_TOKEN: {access_token[:10]}...")
except Exception as e:
    print(f"❌ Environment setup failed: {e}")
    sys.exit(1)

# -------------------------------
# 5. Initialize Breeze Connect
# -------------------------------
try:
    print("5. Initializing Breeze Connect...")
    breeze = BreezeConnect(api_key=api_key)
    breeze.generate_session(api_secret=api_secret, session_token=access_token)
    print("✅ Breeze Connect initialized")
except Exception as e:
    print(f"❌ Breeze Connect initialization failed: {e}")
    sys.exit(1)

# -------------------------------
# 6. Fetch Historical Data (since Jan 1, 2022)
# -------------------------------
try:
    print("6. Fetching historical data since Jan 1, 2022...")
    
    # Set date range
    to_date = datetime.now().date()
    from_date = datetime(2022, 1, 1).date()  # Start from Jan 1, 2022
    security_code="NIFTY"
    print(f"Fetching data from {from_date} to {to_date}")

    # Fetch data
    data = breeze.get_historical_data(
        interval="1day",
        from_date=from_date.strftime("%Y-%m-%d") + "T06:00:00.000Z",
        to_date=to_date.strftime("%Y-%m-%d") + "T06:00:00.000Z",
        stock_code=security_code,
        exchange_code="NSE",
        product_type="cash",
        expiry_date=None,
        right=None,
        strike_price=None
    )

    # Check if request was successful
    if "Success" not in str(data):
        print(f"❌ Data fetching failed: {data}")
        sys.exit(1)
    
    rows = data.get("Success", [])
    print(f"✅ Fetched {len(rows)} data points")
except Exception as e:
    print(f"❌ Data fetching failed: {e}")
    sys.exit(1)

# -------------------------------
# 7. Convert to DataFrame
# -------------------------------
try:
    print("7. Converting to DataFrame...")
    df = pd.DataFrame(rows)
    
    # Convert and set datetime as index
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
    
    # Ensure proper data types
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    print(f"✅ DataFrame created with shape: {df.shape}")
    print("DataFrame info:")
    print(df.info())
except Exception as e:
    print(f"❌ DataFrame creation failed: {e}")
    sys.exit(1)

# -------------------------------
# 8. Save to Parquet format
# -------------------------------
try:
    print("8. Saving to Parquet format...")
    
    # Create data directory if it doesn't exist
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate filename with date range
    from_str = from_date.strftime("%Y%m%d")
    to_str = to_date.strftime("%Y%m%d")
    parquet_filename = f"{data_dir}/{security_code}_1day_{from_str}_{to_str}.parquet"
    
    # Save as Parquet
    df.to_parquet(parquet_filename, engine="pyarrow", compression="snappy")
    
    print(f"✅ Data saved to {parquet_filename}")
    print(f"File size: {os.path.getsize(parquet_filename) / 1024:.2f} KB")
    
    # Verify the file can be read back
    print("Verifying Parquet file...")
    df_verify = pd.read_parquet(parquet_filename)
    print(f"Verified: Read back {len(df_verify)} rows")
    print("First few rows:")
    print(df_verify.head())
    
except Exception as e:
    print(f"❌ Parquet saving failed: {e}")
    sys.exit(1)

print("🎉 All steps completed successfully!")