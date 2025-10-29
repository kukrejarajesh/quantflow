import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
import glob
import time
# Add these imports
import json
from datetime import datetime, timedelta
import calendar

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

def get_latest_date_from_parquet(parquet_file):
    """Get the latest date from a Parquet file"""
    try:
        # Read just the index to get the latest date
        df = pd.read_parquet(parquet_file, columns=[])
        if not df.empty:
            return df.index.max()
    except Exception as e:
        print(f"Error reading {parquet_file}: {e}")
    return None
# Add this function to calculate trading days
def get_trading_days(start_date, end_date):
    """Calculate number of trading days between two dates (excludes weekends)"""
    days = (end_date - start_date).days + 1
    trading_days = 0
    for i in range(days):
        current_date = start_date + timedelta(days=i)
        if current_date.weekday() < 5:  # Monday to Friday
            trading_days += 1
    return trading_days

# Modify your main function
def main():
    """Main function to update Parquet files with latest data"""
    print("Starting data update...")
    
    try:
        # Load API usage state
        state_file = "api_usage_state.json"
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                api_state = json.load(f)
        else:
            api_state = {
                "last_reset_date": datetime.now().strftime("%Y-%m-%d"),
                "calls_today": 0,
                "stock_progress": {}
            }
        
        # Reset counter if it's a new day
        current_date = datetime.now().strftime("%Y-%m-%d")
        if api_state["last_reset_date"] != current_date:
            api_state["last_reset_date"] = current_date
            api_state["calls_today"] = 0
        
        # Define your stocks
        # stocks = ["APOHOS", "BAFINS", "BHAELE", "BOSLIM", "BRIIND", "CANBAN", "DABIND", 
        #         "HERHON", "HINDAL", "HYUMOT", "ICIBAN", "JIOFIN", "KOTMAH", "MAHMAH", 
         #        "PUNBAN", "RELIND", "SBILIF", "SIEMEN", "TVSMOT", "TITIND", "TRENT"]
        
        stocks = ["TVSMOT", "HERHON", "HYUMOT", "APOHOS", "DABIND", "RELIND"]
        
        # Get today's date
        today = datetime.now().date()
        
        # Process each stock
        for symbol in stocks:
            if api_state["calls_today"] >= 5000:
                print("Daily API limit reached. Stopping for today.")
                break
                
            print(f"Processing {symbol}...")
            
            # Initialize progress tracking for this stock
            if symbol not in api_state["stock_progress"]:
                api_state["stock_progress"][symbol] = {
                    "last_successful_date": None,
                    "historical_complete": False
                }
            
            # Define the Parquet file path
            parquet_file = f"historicaldata/{symbol}.parquet"
            
            # Get the latest date in the existing Parquet file
            latest_date = get_latest_date_from_parquet(parquet_file)
            
            # Determine the start date for fetching new data
            if latest_date:
                start_date = latest_date + timedelta(days=1)
                print(f"Latest date in {parquet_file}: {latest_date.date()}")
            else:
                # If no existing data, fetch from a default start date
                start_date = datetime(2022, 1, 1).date()
                print(f"No existing data found for {symbol}, fetching from {start_date}")
            
            # Don't fetch data if we're already up to date
            if start_date > today:
                print(f"{symbol} is already up to date")
                continue
            
            # Calculate how many API calls needed for this stock
            trading_days_needed = get_trading_days(start_date, today)
            api_calls_needed = (trading_days_needed + 1) // 2  # 2 days per API call
            
            # Limit API calls for this stock based on remaining daily quota
            remaining_calls = 5000 - api_state["calls_today"]
            api_calls_to_make = min(api_calls_needed, remaining_calls)
            
            if api_calls_to_make == 0:
                print(f"No API calls remaining for {symbol}")
                continue
            
            print(f"Making {api_calls_to_make} API calls for {symbol}")
            
            # Fetch data in chunks
            current_start = start_date
            calls_made = 0
            
            while calls_made < api_calls_to_make and current_start <= today:
                # Calculate end date (2 trading days from start)
                end_date = current_start + timedelta(days=2)
                
                # Ensure we don't go beyond today
                if end_date > today:
                    end_date = today
                
                # Fetch data for this chunk
                new_data = fetch_daily_data(breeze, symbol, current_start, end_date)
                
                if new_data:
                    # Append to Parquet file
                    success = append_to_parquet(new_data, parquet_file)
                    if success:
                        print(f"Successfully updated {symbol} with data from {current_start} to {end_date}")
                        # Update progress
                        api_state["stock_progress"][symbol]["last_successful_date"] = end_date.strftime("%Y-%m-%d")
                    else:
                        print(f"Failed to update {symbol} with data from {current_start} to {end_date}")
                
                # Update counters
                api_state["calls_today"] += 1
                calls_made += 1
                
                # Move to next chunk
                current_start = end_date + timedelta(days=1)
                
                # Add a delay to respect API rate limits
                time.sleep(0.1)
            
            # Check if historical data is complete for this stock
            if current_start > today:
                api_state["stock_progress"][symbol]["historical_complete"] = True
                print(f"Historical data complete for {symbol}")
        
        # Save API usage state
        with open(state_file, 'w') as f:
            json.dump(api_state, f, indent=4)
        
        print(f"Data update completed! API calls made today: {api_state['calls_today']}")
        
    except Exception as e:
        print(f"Error in main process: {e}")
        # Still save state if possible
        try:
            with open(state_file, 'w') as f:
                json.dump(api_state, f, indent=4)
        except:
            pass
        sys.exit(1)