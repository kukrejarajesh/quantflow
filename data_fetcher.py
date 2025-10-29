import os
import sys
from datetime import datetime, timedelta

print("Starting data_fetcher_breeze.py...")

# -------------------------------
# 1. Import pandas
# -------------------------------
try:
    print("1. Importing pandas...")
    import pandas as pd
    print("✅ Pandas imported successfully")
except ImportError as e:
    print(f"❌ Pandas import failed: {e}")
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
    access_token = os.getenv("BREEZE_ACCESS_TOKEN")  # generated via login/callback flow
    print(f"✅ API_KEY: {api_key[:10]}...")
    print(f"✅ API_SECRET: {api_secret[:10]}...")
    print(f"✅ ACCESS_TOKEN: {access_token[:10]}...")
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
    #breeze.set_access_token(api_secret=api_secret, access_token=access_token)
    breeze.generate_session(api_secret=api_secret, session_token=access_token)
    print("✅ Breeze Connect initialized")
except Exception as e:
    print(f"❌ Breeze Connect initialization failed: {e}")
    sys.exit(1)

# -------------------------------
# 6. Fetch Historical Data
# -------------------------------
try:
    print("6. Fetching historical data...")
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=30)

    print(f"Fetching data from {from_date} to {to_date}")

    data = breeze.get_historical_data(
    interval="1day",
    from_date=from_date.strftime("%Y-%m-%d") + "T06:00:00.000Z",
    to_date=to_date.strftime("%Y-%m-%d") + "T06:00:00.000Z",
    stock_code="RELIND",
    exchange_code="NSE",
    product_type="cash",
    expiry_date=None,
    right=None,
    strike_price=None
    )

    print("Raw response:", data)


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
    
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
    
    print(f"✅ DataFrame created with shape: {df.shape}")
except Exception as e:
    print(f"❌ DataFrame creation failed: {e}")
    sys.exit(1)

# -------------------------------
# 8. Save to CSV
# -------------------------------
try:
    print("8. Saving to CSV...")
    df.to_csv("icicibank_1day_data.csv")
    print("✅ Data saved to icicibank_1day_data.csv")
    print("First few rows:")
    print(df.head())
except Exception as e:
    print(f"❌ CSV saving failed: {e}")
    sys.exit(1)

print("🎉 All steps completed successfully!")
