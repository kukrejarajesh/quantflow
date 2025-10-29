import os
import sys
import time
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

print("Starting multi-stock data fetcher...")

# -------------------------------
# 1. Import required libraries
# -------------------------------
try:
    print("1. Importing required libraries...")
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
# 6. Define stock list and parameters
# -------------------------------
# Replace with your 20 stock symbols
#stock_symbols = ["APOHOS", 	"BAFINS", 	"BHAELE", 	"BOSLIM", 	"BRIIND", 	"CANBAN", 	"DABIND", 	"HERHON", 	"HINDAL", 	"HYUMOT", 	"ICIBAN", 	"JIOFIN", 	"KOTMAH", 	"MAHMAH", 	"PUNBAN", 	"RELIND", 	"SBILIF", 	"SIEMEN", 	"TVSMOT", 	"TITIND", 	"TRENT"]

stock_symbols =["ABB",	"ADATRA",	"ADAENT",	"ADAGRE",	"ADAPOR",	"ADAPOW",	"AMBCE",	"APOHOS",	"ASIPAI",	"AVESUP",	"AXIBAN",	"BAAUTO",
	"BAJFI",	"BAFINS",	"BAJHOL",	"BAJHOU",	"BANBAR",	"BHAELE",	"BHAPET",	"BHAAIR",	"BOSLIM",	"BRIIND",	"CROGRE",	"CANBAN",
	"CHOINV",	"CIPLA",	"COALIN",	"DLFLIM",	"DABIND",	"DIVLAB",	"DRREDD",	"EICMOT",	"ZOMLIM",	"GAIL",	"GODCON",	"GRASIM",	
	"HCLTEC",	"HDFBAN",	"HDFSTA",	"HAVIND",	"HERHON",	"HINDAL",	"HINAER",	"HINLEV",	"HYUMOT",	"ICIBAN",	"ICILOM",	"ICIPRU",	
	"ITC",	"INDHOT",	"INDOIL",	"INDR",	"INDBA",	"INFEDG",	"INFTEC",	"INTAVI",	"JSWENE",	"JSWSTE",	"JINSP",	"JIOFIN",	"KOTMAH",
	"LTINFO",	"LARTOU",	"LIC",	"MACDEV",	"MAHMAH",	"MARUTI",	"NTPC",	"NESIND",	"ONGC",	"PIDIND",	"POWFIN",	"POWGRI",	"PUNBAN",	
	"RURELE",	"RELIND",	"SBILIF",	"MOTSUM",	"SHRCEM",	"SHRTRA",	"SIEMEN",	"STABAN",	"SUNPHA",	"SWILIM",	"TVSMOT",	"TCS",	
	"TATGLO",	"TATMOT",	"TATPOW",	"TATSTE",	"TECMAH",	"TITIND",	"TORPHA",	"TRENT",	"ULTCEM",	"UNISPI",	"VARBEV",	"VEDLIM",	
	"WIPRO",	"CADHEA"]

# Set date range
to_date = datetime.now().date()
from_date = datetime(2022, 1, 1).date()  # Start from Jan 1, 2022

print(f"Fetching data for {len(stock_symbols)} stocks from {from_date} to {to_date}")

# Create data directory if it doesn't exist
data_dir = "historicaldata"
os.makedirs(data_dir, exist_ok=True)

# -------------------------------
# 7. Function to fetch data for a single stock
# -------------------------------
def fetch_stock_data(symbol):
    """Fetch historical data for a single stock"""
    try:
        print(f"Fetching data for {symbol}...")
        
        # Fetch data
        data = breeze.get_historical_data(
            interval="1day",
            from_date=from_date.strftime("%Y-%m-%d") + "T06:00:00.000Z",
            to_date=to_date.strftime("%Y-%m-%d") + "T06:00:00.000Z",
            stock_code=symbol,
            exchange_code="NSE",
            product_type="cash",
            expiry_date=None,
            right=None,
            strike_price=None
        )

        # Check if request was successful
        if "Success" not in data:
            print(f"❌ Data fetching failed for {symbol}: {data}")
            return None
        
        rows = data.get("Success", [])
        if not rows:
            print(f"⚠️ No data returned for {symbol}")
            return None
        
        print(f"✅ Fetched {len(rows)} data points for {symbol}")
        return rows
        
    except Exception as e:
        print(f"❌ Error fetching data for {symbol}: {e}")
        return None

# -------------------------------
# 8. Function to process and save data
# -------------------------------
def process_and_save_data(symbol, rows):
    """Process the API response and save as Parquet"""
    try:
        # Convert to DataFrame
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
        
        # Generate filename with date range
        from_str = from_date.strftime("%Y%m%d")
        to_str = to_date.strftime("%Y%m%d")
        parquet_filename = f"{data_dir}/{symbol}_1day_{from_str}_{to_str}.parquet"
        
        # Save as Parquet
        df.to_parquet(parquet_filename, engine="pyarrow", compression="snappy")
        
        print(f"✅ Data saved to {parquet_filename}")
        print(f"File size: {os.path.getsize(parquet_filename) / 1024:.2f} KB")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing data for {symbol}: {e}")
        return False

# -------------------------------
# 9. Main loop to process all stocks
# -------------------------------
success_count = 0
failed_symbols = []

for i, symbol in enumerate(stock_symbols):
    # Add delay between requests to respect API rate limits
    if i > 0:
        time.sleep(1)  # 1 second delay between requests
    
    # Fetch data for this stock
    rows = fetch_stock_data(symbol)
    
    if rows is not None:
        # Process and save the data
        if process_and_save_data(symbol, rows):
            success_count += 1
        else:
            failed_symbols.append(symbol)
    else:
        failed_symbols.append(symbol)

# -------------------------------
# 10. Summary report
# -------------------------------
print("\n" + "="*50)
print("DATA FETCHING SUMMARY")
print("="*50)
print(f"Total stocks processed: {len(stock_symbols)}")
print(f"Successfully fetched: {success_count}")
print(f"Failed: {len(failed_symbols)}")

if failed_symbols:
    print("Failed symbols:")
    for symbol in failed_symbols:
        print(f"  - {symbol}")
    
    # Save list of failed symbols for retry later
    with open(f"{data_dir}/failed_fetches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "w") as f:
        for symbol in failed_symbols:
            f.write(f"{symbol}\n")

print("🎉 Data fetching completed!")