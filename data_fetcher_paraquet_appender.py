import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
import glob
import time

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



def load_environment():
    """Load environment variables"""
    load_dotenv()
    
   # api_key = os.getenv("BREEZE_API_KEY")
    #api_secret = os.getenv("BREEZE_API_SECRET")
  #  access_token = os.getenv("BREEZE_ACCESS_TOKEN")
    
    if not all([api_key, api_secret, access_token]):
        raise ValueError("Missing API credentials in environment variables")
    
    return api_key, api_secret, access_token

def initialize_breeze(api_key, api_secret, access_token):
    """Initialize Breeze API connection"""
    breeze = BreezeConnect(api_key=api_key)
    breeze.generate_session(api_secret=api_secret, session_token=access_token)
    return breeze

# def get_latest_date_from_parquet(parquet_file):
#     """Get the latest date from a Parquet file"""
#     try:
#         # Read just the index to get the latest date
#         df = pd.read_parquet(parquet_file, columns=[])
#         if not df.empty:
#             return df.index.max()
#     except Exception as e:
#         print(f"Error reading {parquet_file}: {e}")
#     return None

def get_latest_date_from_parquet(parquet_file):
    """
    Get the latest date from a Parquet file without loading all data
    """
    try:
        # Method 1: Try to read just the index first
        try:
            # Read with engine='pyarrow' for better performance
            df = pd.read_parquet(parquet_file, columns=[], engine='pyarrow')
            if not df.empty and hasattr(df.index, 'max'):
                latest_date = df.index.max()
                if pd.notna(latest_date):
                    return latest_date
        except Exception as e:
            print(f"Method 1 failed: {e}")
        
        # Method 2: If index reading fails, try reading a single column
        try:
            # Read just one column to minimize memory usage
            df = pd.read_parquet(parquet_file, columns=['close'], engine='pyarrow')
            if not df.empty:
                return df.index.max()
        except Exception as e:
            print(f"Method 2 failed: {e}")
        
        # Method 3: As a last resort, read the entire file but with minimal processing
        try:
            df = pd.read_parquet(parquet_file, engine='pyarrow')
            if not df.empty:
                return df.index.max()
        except Exception as e:
            print(f"Method 3 failed: {e}")
            
        return None
        
    except Exception as e:
        print(f"Error reading {parquet_file}: {e}")
        return None
def fetch_daily_data(breeze, symbol,  from_date, to_date):
    """Fetch daily data from Breeze API"""
    
    print(f"Just before API call for {symbol} from {from_date} to {to_date}")
    try:
        data = breeze.get_historical_data(
            interval="1day",
            from_date=from_date.strftime("%Y-%m-%d") + "T03:30:00.000Z",
            to_date=to_date.strftime("%Y-%m-%d") + "T10:00:00.000Z",
            stock_code=symbol,
            exchange_code="NSE",
            product_type="cash",
            expiry_date=None,
            right=None,
            strike_price=None
        )
        print(f"API response for {symbol}: {data}") 
        if "Success" in data:
            return data["Success"]
        else:
            print(f"No data returned for {symbol}: {data}")
            return []
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return []

def append_to_parquet(new_data, parquet_file):
    """Append new data to existing Parquet file"""
    if not new_data:
        return False
    
    try:
        # Convert new data to DataFrame
        new_df = pd.DataFrame(new_data)
        
        # Convert datetime column
        if "datetime" in new_df.columns:
            new_df["datetime"] = pd.to_datetime(new_df["datetime"])
            new_df.set_index("datetime", inplace=True)
        
        # Ensure proper data types
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            if col in new_df.columns:
                new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
        
        # Read existing data if file exists
        if os.path.exists(parquet_file):
            existing_df = pd.read_parquet(parquet_file)
            
            # Ensure the index is datetime
            if not isinstance(existing_df.index, pd.DatetimeIndex):
                if "datetime" in existing_df.columns:
                    existing_df["datetime"] = pd.to_datetime(existing_df["datetime"])
                    existing_df.set_index("datetime", inplace=True)
                else:
                    existing_df.index = pd.to_datetime(existing_df.index)
            
            # Remove any duplicates that might already exist
            new_dates = new_df.index
            existing_df = existing_df[~existing_df.index.isin(new_dates)]
            
            # Combine old and new data
            combined_df = pd.concat([existing_df, new_df])
        else:
            combined_df = new_df
        
        # Sort by date
        combined_df.sort_index(inplace=True)
        
        # Save back to Parquet
        combined_df.to_parquet(parquet_file, engine="pyarrow", compression="snappy")
        
        print(f"Successfully updated {parquet_file} with {len(new_data)} new records")
        return True
        
    except Exception as e:
        print(f"Error updating {parquet_file}: {e}")
        return False

def main():
    """Main function to update Parquet files with latest data"""
    print("Starting daily data update...")
    
    try:
        # Load environment variables
       # api_key, api_secret, access_token = load_environment()
        
        # Initialize Breeze API
        #breeze = initialize_breeze(api_key, api_secret, access_token)
        
        # Define your stocks (symbol, exchange_code)
        #stocks = ["APOHOS", 	"BAFINS", 	"BHAELE", 	"BOSLIM", 	"BRIIND", 	"CANBAN", 	"DABIND", 	"HERHON", 	"HINDAL", 	"HYUMOT", 	"ICIBAN", 	"JIOFIN", 	"KOTMAH", 	"MAHMAH", 	"PUNBAN", 	"RELIND", 	"SBILIF", 	"SIEMEN", 	"TVSMOT", 	"TITIND", 	"TRENT"]
        #stocks = ["RELIND", "DABIND", 	"HERHON", "APOHOS", "TVSMOT"]
        #stocks =["TVSMOT","HERHON","RELIND","MAHMAH","PUNBAN","APOHOS","HYUMOT","CANBAN","ICIBAN","HDFBAN"]
        stocks = ["IIFWEA",	"ABB",	"ACC",	"APLAPO",	"AUSMA",	"ADATRA",	"ADAENT",	"ADAGRE",	"ADAPOR",	"ADAGAS",	"ADICAP",	"ALKLAB",	
            "AMBCE",	"APOHOS",	"ASHLEY",	"ASIPAI",	"ASTPOL",	"AURPHA",	"AVESUP",	"AXIBAN",	"BSE",	"BAAUTO",	"BAJFI",	"BAFINS",
            "BAJHOL",	"BAJHOU",	"BANBAR",	"BANIND",	"BHADYN",	"BHAELE",	"BHAFOR",	"BHEL",	"BHAPET",	"BHAAIR",	"BHAHEX",	"BIOCON",	
            "BLUSTA",	"BOSLIM",	"BRIIND",	"CROGRE",	"CANBAN",	"CHOINV",	"CIPLA",	"COALIN",	"COCSHI",	"NIITEC",	"COLPAL",	"CONCOR",	
            "CORINT",	"CUMIND",	"DLFLIM",	"DABIND",	"DIVLAB",	"DIXTEC",	"DRREDD",	"EICMOT",	"ZOMLIM",	"EXIIND",	"FSNECO",	"FEDBAN",	
            "FORHEA",	"GAIL",	"GMRINF",	"GLEPHA",	"GODPHI",	"GODCON",	"GODPRO",	"GRASIM",	"HCLTEC",	"HDFAMC",	"HDFBAN",	"HDFSTA",	"HAVIND",	
            "HERHON",	"HINDAL",	"HINAER",	"HINPET",	"HINLEV",	"HINZIN",	"ABBPOW",	"HUDCO",	"HYUMOT",	"ICIBAN",	"ICILOM",	"IDFBAN",	"IRBINF",
            "ITCHOT",	"ITC",	"INDIBA",	"INDHOT",	"INDOIL",	"INDRAI",	"INDR",	"INDREN",	"INDGAS",	"BHAINF",	"INDBA",	"INFEDG",	"INFTEC",
            "INTAVI",	"JSWENE",	"JSWSTE",	"JINSP",	"JIOFIN",	"JUBFOO",	"KEIIND",	"KPITE",	"KALJEW",	"KOTMAH",	"LTFINA",	"LICHF",	"LTINFO",
            "LARTOU",	"LIC",	"MACDEV",	"LUPIN",	"MRFTYR",	"MAHFIN",	"MAHMAH",	"MAPHA",	"MARLIM",	"MARUTI",	"MAXFIN",	"MAXHEA",	"MAZDOC",
            "MOTOSW",	"MPHLIM",	"MUTFIN",	"NHPC",	"NATMIN",	"NTPGRE",	"NTPC",	"NATALU",	"NESIND",	"OBEREA",	"ONGC",	"OILIND",	"ONE97",	"ORAFIN",
            "PBFINT",	"PIIND",	"PAGIND",	"RUCSOY",	"PERSYS",	"PHOMIL",	"PIDIND",	"POLI",	"POWFIN",	"POWGRI",	"PREENR",	"PREEST",	"PUNBAN",
            "RURELE",	"RAIVIK",	"RELIND",	"SBICAR",	"SBILIF",	"SRF",	"MOTSUM",	"SHRCEM",	"SHRTRA",	"SIEENE",	"SIEMEN",	"SOLIN",	"SONBLW",	
            "STABAN",	"SAIL",	"SUNPHA",	"SUPIND",	"SUZENE",	"SWILIM",	"TVSMOT",	"TATCOM",	"TCS",	"TATGLO",	"TATELX",	"TATMOT",	"TATPOW",	
            "TATSTE",	"TATTEC",	"TECMAH",	"TITIND",	"TORPHA",	"TORPOW",	"TRENT",	"TUBIN",	"UNIP",	"ULTCEM",	"UNIBAN",	"VARBEV",	"VEDLIM",
            "VISMEG",	"IDECEL",	"VOLTAS",	"WAAENE",	"WIPRO",	"YESBAN",	"CADHEA"]

        #stocks = ["RELIND"]
        # Get today's date
        # Get today's date as a pandas Timestamp
        today = pd.Timestamp(datetime.now().date())
        print(f"Today's date: {today}")
        # Process each stock
        for symbol in stocks:
            print(f"Processing {symbol}...")
            
            # Define the Parquet file path
            parquet_file = f"historicaldata/{symbol}.parquet"
            print(f"Parquet file: {parquet_file}")
            # Get the latest date in the existing Parquet file
            latest_date = get_latest_date_from_parquet(parquet_file)
            print(latest_date)
            # Determine the start date for fetching new data
            if latest_date:
                if not isinstance(latest_date, pd.Timestamp):
                    latest_date = pd.Timestamp(latest_date)
                start_date = latest_date + timedelta(days=1)
                print(f"Latest date in {parquet_file}: {latest_date.date()}")
                print(f"Start date : {start_date.date()}")
            else:
                # If no existing data, fetch from a default start date
                start_date = pd.Timestamp(datetime(2025, 8, 1).date())
                print(f"No existing data found for {symbol}, fetching from {start_date}")
            
            # Ensure start_date is a pandas Timestamp for comparison
            if not isinstance(start_date, pd.Timestamp):
                start_date = pd.Timestamp(start_date)
            # Don't fetch data if we're already up to date
            # if start_date > today:
            #     print(f"{symbol} is already up to date")
            #     print(f"Today date : {today}")
            #     continue
            
            # Fetch new data - convert to date objects for the API call
            new_data = fetch_daily_data(breeze, symbol, start_date.date(), today.date())
            
            if new_data:
                # Append to Parquet file
                print("Appending new data to Parquet...has file name changed", parquet_file)
                success = append_to_parquet(new_data, parquet_file)
                if success:
                    print(f"Successfully updated {symbol} with {len(new_data)} new records")
                else:
                    print(f"Failed to update {symbol}")
            else:
                print(f"No new data available for {symbol}")
            
            # Add a delay to respect API rate limits
            time.sleep(1)
        
        print("Daily data update completed!")
        
    except Exception as e:
        print(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()