

import json
import logging
import time
import os

import pathlib
from typing import Any, Dict, Union, Optional
from datetime import datetime, timedelta

#from pyparsing import Dict # Import the pathlib module for robust path handling
from RealTimeOrderFlowStrategy import RealTimeOrderFlowStrategy 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# logging configuration for logging to both console and file
# Create a logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

file_logger = logging.getLogger("orderflow_file")
file_logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.FileHandler) for h in file_logger.handlers):
    file_path = os.path.join("logs", f"orderflow_backtest_logs_{datetime.now().strftime('%Y%m%d')}.log")
    fh = logging.FileHandler(file_path, mode='a', encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    file_logger.addHandler(fh)

file_logger.propagate = False  # prevents console output
# --- Configuration ---
# Set the filename or path relative to the script location. 
# We will construct the absolute path below.
# Setting to '../../' because the script is in 'src/strategies/' and 
# the data file is likely in the project root.
TICK_DATA_FILE = '../../tick_data_log_2.json'
DELAY_SECONDS = 0.1  # The required delay between processing each tick

# Define a dictionary to store instantiated strategies
STRATEGIES: Dict[str, RealTimeOrderFlowStrategy] = {} 
current_bars: Dict[int, Dict[str, Any]] = {}
BAR_INTERVAL_SECONDS = 60  # 5 minutes
bar_start_cumulative_volume: Dict[int, int] = {}
token_map = {
            878593: "TataConsumer", #
            738561: "RELIND",     #   
            197633: "Dabur",
            356865: "HindustanUnilever",
}
# Ensure this utility function is accessible (e.g., as a method in ZerodhaKiteDataDisplay)
@staticmethod
def _get_bar_time(timestamp_input: Union[str, datetime], interval_sec: int) -> datetime:
    """Rounds down a tick timestamp to the start of the defined bar interval."""
    
    if isinstance(timestamp_input, str):
        # If it's a string, parse it using the expected format
        dt = datetime.strptime(timestamp_input, '%Y-%m-%d %H:%M:%S')
    elif isinstance(timestamp_input, datetime):
        # If it's already a datetime object (the cause of the error), use it directly
        dt = timestamp_input
    else:
        # Handle unexpected input type
        raise TypeError(f"Timestamp must be str or datetime, not {type(timestamp_input)}")
        
    # Calculate seconds since midnight
    seconds_since_midnight = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    
    # Calculate the start time of the bar interval
    bar_start_seconds = (seconds_since_midnight // interval_sec) * interval_sec
    
    # Reconstruct the datetime object for the bar's open
    bar_start_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=bar_start_seconds)
    
    return bar_start_dt
@staticmethod
def aggregate_to_bar( tick_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Aggregates a raw tick into the current OHLCV bar and triggers the OHLC 
    callback if the bar closes.
    
    Returns:
        The closed OHLCV bar dictionary, or None if the bar is still open.
    """
    token = tick_data.get('instrument_token')
    print("***********************Aggregate to Bar Function Called********************")
    # --- Data Extraction ---
    current_time_str = tick_data.get('exchange_timestamp')
    print(f"Current Time String: {current_time_str}")
    last_price = tick_data.get('last_price')
    volume_traded_cumulative = tick_data.get('volume_traded')
    
    if not current_time_str or not last_price:
        return None # Skip invalid ticks

    # 1. Determine the current bar's OPEN time
    bar_open_time = _get_bar_time(current_time_str, BAR_INTERVAL_SECONDS)
    
    # 2. Check for initialization or bar change
    if token not in current_bars:
        # --- INITIALIZE FIRST BAR ---
        current_bars[token] = {
            'Open': last_price,
            'High': last_price,
            'Low': last_price,
            'Volume': 0, # Will be calculated at close
            'OpenTime': bar_open_time
        }
        bar_start_cumulative_volume[token] = volume_traded_cumulative
        return None

    current_bar = current_bars[token]
    
    # 3. Bar Close Check: Has the current tick crossed into a new bar interval?
    if bar_open_time > current_bar['OpenTime']:
        # --- BAR IS CLOSED - FINALIZING ---
        print("Bar is closed, finalizing...") 
        # Calculate Bar Volume: Current cumulative volume minus volume at bar start
        bar_volume = max(0, volume_traded_cumulative - bar_start_cumulative_volume[token])
        print(f"Bar Volume: {bar_volume}")
        # The Close price of the just-completed bar is the last price of the previous tick
        closed_bar = {
            'timestamp': current_bar['OpenTime'] + timedelta(seconds=BAR_INTERVAL_SECONDS),
            'symbol': token_map.get(token, 'UNKNOWN'),
            'instrument_token': token,
            'open': current_bar['Open'],
            'high': current_bar['High'],
            'low': current_bar['Low'],
            'close': current_bar['Close'], # The last price stored from the previous tick
            'volume': bar_volume
        }
        print("Closed Bar Data:", closed_bar)
        # --- RESET FOR NEW BAR ---
        current_bars[token] = {
            'Open': last_price,
            'High': last_price,
            'Low': last_price,
            'Close': last_price,
            'Volume': 0,
            'OpenTime': bar_open_time
        }
        bar_start_cumulative_volume[token] = volume_traded_cumulative
        
        return closed_bar # Return the completed bar
        
    else:
        # --- BAR IS OPEN - AGGREGATING ---
        print("Bar is still open, aggregating data...")
        # Update High, Low, and Current Close Price
        current_bar['High'] = max(current_bar['High'], last_price)
        current_bar['Low'] = min(current_bar['Low'], last_price)
        current_bar['Close'] = last_price # The Close is always the last price seen
        
        return None # Bar is still open

def process_single_tick(tick_data):
    """
    Simulates the function that would process a single tick in a live system.
    This function now extracts more key details from the 'full' mode tick data.
    """
    
    # Extract core key information from the tick
    token = tick_data.get('instrument_token', 'N/A')
    
    # Step 3: Dynamically instantiate strategies and store them
    print("Instantiating strategies and preparing for data stream...")
    
    if token not in STRATEGIES:
        strategy_instance = RealTimeOrderFlowStrategy(symbol=token)
        STRATEGIES[token] = strategy_instance
        print(f"✅ Strategy created for {token}")

    strategy = STRATEGIES[token]
    strategy.handle_tick_update(tick_data)
    print(f"Processed Tick for Token: {token}, Last Price: {tick_data.get('last_price', 'N/A')} | Full Tick: {tick_data}")

    # 2. Check for completed OHLCV bar
    closed_bar = aggregate_to_bar(tick_data)
    print("Closed Bar:", closed_bar)
    if closed_bar:   
        print(f"📊 {closed_bar['symbol']} 5m Bar Closed: {closed_bar['close']:.2f}, Volume: {closed_bar['volume']:,}")
        strategy.handle_ohlc_update(closed_bar)
    
            
    # Display the processed information
    # print("-" * 60)
    # print(json.dumps(tick_data, indent=2, sort_keys=True))


def simulate_tick_feed(file_path_relative, delay):
    """
    Reads concatenated JSON objects separated by '}\n{' from file
    and simulates a real-time tick feed with a delay between ticks.
    Works without modifying the JSON file.
    """
    print("Starting tick feed simulation...:::STEP 1")
    script_dir = pathlib.Path(__file__).resolve().parent
    data_file_path = (script_dir / file_path_relative).resolve()

    if not data_file_path.exists():
        print(f"Error: The file '{data_file_path}' was not found.")
        return

    print(f"\n--- Starting Tick Feed Simulation ---")
    print(f"Delay set to {delay} second(s) between ticks.")

    try:
        # Read full file once
        with open(data_file_path, "r") as f:
            raw_data = f.read().strip()

        # Split safely where one JSON ends and another begins
        json_chunks = raw_data.split("}\n{")

        # Add back the missing braces
        json_chunks = [
            chunk + "}" if i == 0 else "{" + chunk + "}" if i != len(json_chunks) - 1 else "{" + chunk
            for i, chunk in enumerate(json_chunks)
        ]

        print(f"Total JSON chunks found: {len(json_chunks)}")
        tick_count = 0
        start_time = time.time()

        for chunk in json_chunks:
            try:
                tick = json.loads(chunk)
                process_single_tick(tick)
                tick_count += 1
                time.sleep(delay)
            except json.JSONDecodeError as e:
                print(f"--- SKIPPING INVALID JSON BLOCK #{tick_count + 1} ---")
                print(f"Error: {e}")
                continue

        print("@@@@@@@@@@@Total ticks processed: {tick_count}")
        duration = time.time() - start_time
        print("\n" + "=" * 60)
        print(f"Simulation Complete. Processed {tick_count} ticks in {duration:.2f}s.")
        print("=" * 60)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Ensure the script is running directly before starting the simulation
    simulate_tick_feed(TICK_DATA_FILE, DELAY_SECONDS)
