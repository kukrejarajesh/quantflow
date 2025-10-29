#!/usr/bin/env python3
"""
Breeze API Data Fetcher with Efficient Scheduling
Fetches 1-minute OHLCV data while respecting API limits (2 days per call, 5000 calls/day)
"""

from logging.handlers import RotatingFileHandler
import os
import json
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
import schedule
from typing import Dict, List, Optional

# Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("breeze_data_fetcher.log"),
#         logging.StreamHandler()
#     ]
# )
#
# Define the new logs directory
log_dir = Path("E:/working/historicaldata/1mincandles/logs")
log_dir.mkdir(exist_ok=True)


# Define the log file path
log_file = log_dir / "breeze_data_fetcher.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # Rotate at 10 MB
            backupCount=5               # Keep 5 backup files
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("BreezeDataFetcher")

class BreezeDataFetcher:
    def __init__(self):
        print(f"Current working directory: {os.getcwd()}")
        env_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
        print(f"Looking for .env at: {env_path}")
        # Load environment variables
        load_dotenv(dotenv_path=env_path)
        #load_dotenv()
        
        self.api_key = os.getenv("BREEZE_API_KEY")
        self.api_secret = os.getenv("BREEZE_API_SECRET")
        self.access_token = os.getenv("BREEZE_ACCESS_TOKEN")
        
        # Validate credentials
        if not all([self.api_key, self.api_secret, self.access_token]):
            raise ValueError("Missing API credentials in environment variables")
        
        # Initialize Breeze API
        self.breeze = BreezeConnect(api_key=self.api_key)
        self.breeze.generate_session(api_secret=self.api_secret, session_token=self.access_token)
        
        # Configuration
        # self.stocks = ["APOHOS", "BAFINS", "BHAELE", "BOSLIM", "BRIIND", "CANBAN", "DABIND", 
        #               "HERHON", "HINDAL", "HYUMOT", "ICIBAN", "JIOFIN", "KOTMAH", "MAHMAH", 
        #               "PUNBAN", "RELIND", "SBILIF", "SIEMEN", "TVSMOT", "TITIND", "TRENT"]
        #self.stocks = ["TVSMOT", "HERHON", "HYUMOT", "APOHOS", "DABIND", "RELIND"]
        # self.stocks = ["TVSMOT","HERHON","RELIND","MAHMAH","PUNBAN","APOHOS","HYUMOT","CANBAN","ICIBAN","HDFBAN",
        #                "CIPLA",	"DIVLAB",	"DRREDD",	"SUNPHA",	"TORPHA",	"CADHEA",
        #                 "BAAUTO",	"BOSLIM",	"EICMOT",	"MARUTI",	"MOTSUM",	"TATMOT", "AMBCE",	"GRASIM",	"SHRCEM",	"ULTCEM"]
        #self.stocks = ["ADATRA",	"ADAGRE",	"ADAPOW",	"BHAAIR",	"JSWENE",	"NTPC",	"POWGRI",	"TATPOW"]
        self.stocks = ["APOHOS", "BAFINS", "BHAELE", "BOSLIM", "BRIIND", "CANBAN", "DABIND", 
                       "HERHON", "HINDAL", "HYUMOT", "ICIBAN", "JIOFIN", "KOTMAH", "MAHMAH", 
                       "PUNBAN", "RELIND", "SBILIF", "SIEMEN", "TVSMOT", "TITIND", "TRENT",
                       "HDFBAN","CIPLA","DIVLAB",	"DRREDD",	"SUNPHA",	"TORPHA",	"CADHEA",
                       "BAAUTO",	"",	"EICMOT",	"MARUTI",	"MOTSUM",	"TATMOT", "AMBCE",	"GRASIM",	
		               "SHRCEM",	"ULTCEM", "ADATRA",	"ADAGRE",	"ADAPOW",	"BHAAIR",	"JSWENE",
                       "NTPC",	"POWGRI",	"TATPOW"]

        self.data_dir = Path("E:/working/historicaldata/1mincandles")
        self.data_dir.mkdir(exist_ok=True)
        
        # State management
        self.state_file = self.data_dir / "fetch_state.json"
        self.load_state()
        
        # Ensure state has all stocks
        for stock in self.stocks:
            if stock not in self.state:
                self.state[stock] = {
                    "last_fetched_date": None,
                    "fetch_complete": False,
                    "api_calls_made": 0,
                    "total_records": 0
                }
    
    def load_state(self):
        """Load the fetch state from JSON file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
                logger.info("Loaded existing state file")
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
                self.state = {}
        else:
            self.state = {}
            logger.info("Created new state")
    
    def save_state(self):
        """Save the fetch state to JSON file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=4)
            logger.info("State saved successfully")
        except Exception as e:
            logger.error(f"Error saving state file: {e}")
    
    def get_latest_parquet_date(self, symbol: str) -> Optional[datetime]:
        """Get the latest date from a Parquet file for a symbol"""
        file_path = self.data_dir / f"{symbol}.parquet"
        if not file_path.exists():
            return None
        
        try:
            # Read just the last row to get the latest date
            df = pd.read_parquet(file_path)
            if not df.empty:
                return pd.to_datetime(df.index.max())
        except Exception as e:
            logger.error(f"Error reading Parquet file for {symbol}: {e}")
        
        return None
    
    def fetch_data_chunk(self, symbol: str, from_date: datetime, to_date: datetime) -> Optional[pd.DataFrame]:
        """
        Fetch a chunk of data (max 2 days) from Breeze API
        Returns: DataFrame with data or None if failed
        """
        try:
            # Format dates for API
            from_str = from_date.strftime("%Y-%m-%dT06:00:00.000Z")
            to_str = to_date.strftime("%Y-%m-%dT06:00:00.000Z")
            
            logger.info(f"Fetching {symbol} data from {from_date.date()} to {to_date.date()}")
            
            # Make API call
            data = self.breeze.get_historical_data(
                interval="1minute",
                from_date=from_str,
                to_date=to_str,
                stock_code=symbol,
                exchange_code="NSE",
                product_type="cash"
            )
            
            if "Success" in data and data["Success"]:
                # Convert to DataFrame
                df = pd.DataFrame(data["Success"])
                
                # Convert and set datetime index
                df["datetime"] = pd.to_datetime(df["datetime"])
                df.set_index("datetime", inplace=True)
                
                # Convert numeric columns
                numeric_cols = ["open", "high", "low", "close", "volume"]
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                
                logger.info(f"Fetched {len(df)} records for {symbol}")
                return df
            else:
                logger.warning(f"No data returned for {symbol} from {from_date.date()} to {to_date.date()}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def append_to_parquet(self, symbol: str, new_data: pd.DataFrame):
        """Append new data to existing Parquet file or create new one"""
        file_path = self.data_dir / f"{symbol}.parquet"
        
        try:
            if file_path.exists():
                # Read existing data
                existing_data = pd.read_parquet(file_path)
                
                # Remove any duplicates
                combined_data = pd.concat([existing_data, new_data])
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
            else:
                combined_data = new_data
            
            # Sort by datetime
            combined_data.sort_index(inplace=True)
            
            # Save to Parquet
            combined_data.to_parquet(file_path, engine="pyarrow", compression="snappy")
            
            # Update state
            self.state[symbol]["last_fetched_date"] = combined_data.index.max().strftime("%Y-%m-%d")
            self.state[symbol]["total_records"] = len(combined_data)
            
            logger.info(f"Saved {len(new_data)} records for {symbol}, total: {len(combined_data)}")
            
        except Exception as e:
            logger.error(f"Error saving data for {symbol}: {e}")
    
    def is_market_open(self, dt: datetime) -> bool:
        """Check if the market was open on a given date"""
        # Simple implementation - assume market is open on weekdays
        # You might want to enhance this with a proper holiday calendar
        return dt.weekday() < 5  # Monday to Friday
    
    def calculate_days_needed(self, symbol: str) -> int:
        """Calculate how many days of data we need to fetch for a symbol"""
        # If we've never fetched data for this symbol, start from 3 years ago
        if not self.state[symbol]["last_fetched_date"]:
            start_date = datetime.now() - timedelta(days=3*365)
        else:
            start_date = datetime.strptime(self.state[symbol]["last_fetched_date"], "%Y-%m-%d") + timedelta(days=1)
        
        end_date = datetime.now()
        
        # Count trading days between start_date and end_date
        days_needed = 0
        current_date = start_date
        while current_date <= end_date:
            if self.is_market_open(current_date):
                days_needed += 1
            current_date += timedelta(days=1)
        
        return days_needed
    
    def fetch_stock_data(self, symbol: str, max_api_calls: int = 10) -> int:
        """
        Fetch data for a single stock
        Returns: number of API calls made
        """
        api_calls_made = 0
        
        # Determine start date
        if not self.state[symbol]["last_fetched_date"]:
            # Start from 3 years ago if no data exists
            start_date = datetime.now() - timedelta(days=3*365)
            logger.info(f"Starting fresh fetch for {symbol} from {start_date.date()}")
        else:
            # Start from day after last fetched date
            start_date = datetime.strptime(self.state[symbol]["last_fetched_date"], "%Y-%m-%d") + timedelta(days=1)
            logger.info(f"Resuming fetch for {symbol} from {start_date.date()}")
        
        end_date = datetime.now()
        current_date = start_date
        
        # Fetch data in chunks of 2 days
        while current_date <= end_date and api_calls_made < max_api_calls:
            # Calculate chunk end date (max 2 days from current date)
            chunk_end_date = min(current_date + timedelta(days=1), end_date)
            
            # Skip non-trading days
            if not self.is_market_open(current_date):
                current_date += timedelta(days=1)
                continue
            
            # Fetch data chunk
            data_chunk = self.fetch_data_chunk(symbol, current_date, chunk_end_date)
            api_calls_made += 1
            self.state[symbol]["api_calls_made"] += 1
            
            if data_chunk is not None and not data_chunk.empty:
                # Append to Parquet file
                self.append_to_parquet(symbol, data_chunk)
            
            # Move to next chunk
            current_date = chunk_end_date + timedelta(days=1)
            
            # Respect API rate limits
            time.sleep(2)
        
        # Check if we've fetched all available data
        if current_date > end_date:
            self.state[symbol]["fetch_complete"] = True
            logger.info(f"Completed data fetch for {symbol}")
        
        return api_calls_made
    
    def run_daily_fetch(self, max_daily_calls: int = 5000):
        """Run the daily fetch process respecting API limits"""
        logger.info("Starting daily data fetch")
        
        total_calls_made = 0
        stocks_processed = 0
        
        # Process each stock until we hit the daily limit
        for symbol in self.stocks:
            if total_calls_made >= max_daily_calls:
                logger.info(f"Reached daily API limit of {max_daily_calls} calls")
                break
            
            # Skip stocks that are already up to date
            if self.state[symbol]["fetch_complete"]:
                # Check if we need to update with today's data
                last_date = datetime.strptime(self.state[symbol]["last_fetched_date"], "%Y-%m-%d")
                if last_date.date() < datetime.now().date():
                    self.state[symbol]["fetch_complete"] = False
                else:
                    logger.info(f"Skipping {symbol} - already up to date")
                    continue
            
            # Calculate available calls for this stock
            available_calls = min(400, max_daily_calls - total_calls_made)
            
            # Fetch data for this stock
            calls_made = self.fetch_stock_data(symbol, available_calls)
            total_calls_made += calls_made
            stocks_processed += 1
            
            # Save state after each stock to preserve progress
            self.save_state()
            
            logger.info(f"Processed {symbol}, calls made: {calls_made}, total: {total_calls_made}")
        
        logger.info(f"Daily fetch completed. Processed {stocks_processed} stocks with {total_calls_made} API calls")
    
    def schedule_daily_fetch(self):
        """Schedule the daily fetch to run at a specific time"""
        # Schedule to run at 6 PM every day
        schedule.every().day.at("18:00").do(self.run_daily_fetch)
        
        logger.info("Scheduled daily fetch at 6:00 PM")
        
        # Run immediately if it's the first time
        self.run_daily_fetch()
        
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

def main():
    """Main function"""
    try:
        fetcher = BreezeDataFetcher()
        
        # Run once immediately
        fetcher.run_daily_fetch()
        
        # Then schedule for daily runs
        fetcher.schedule_daily_fetch()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        # Ensure state is saved even on error
        if 'fetcher' in locals():
            fetcher.save_state()

if __name__ == "__main__":
    main()