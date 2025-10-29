from datetime import datetime
import logging
import os
import time
import json
from kiteconnect import KiteTicker
from typing import Dict, Any

# ==============================================================================
# 1. CORE DEPENDENCY IMPORT
# Assuming ZerodhaKiteDataDisplay.py is accessible for import.
# In a real environment, you might need to adjust the path:
# from your_project.data_service import initialize_zerodha, ZerodhaKiteDataDisplay
# For this example, we assume it's in the same directory or accessible.
# ==============================================================================
from ZerodhaKiteDataDisplay import initialize_zerodha, ZerodhaKiteDataDisplay #
# Add the import for your strategy here
from RealTimeOrderFlowStrategy import RealTimeOrderFlowStrategy 


# token_map = {
#             738561: "RELIANCE", #
#             2953217: "TCS"     #
#         }
token_map = {
            8912129: "ADANI GREEN ENERGY",
            3861249:  "ADANI PORT & SEZ",
            81153: "BAJAJ FINANCE",
            2426881: "LIFE INSURA CORP OF INDIA",
            3785729: "SUN PHARMA ADV.RES.CO.LTD",
            2674433: "UNITED SPIRITS",
            341249: "HDFC Bank", #
            424961: "ITC",
            738561: "RELIANCE", #
            2953217: "TCS" 
        }

# token_map = {
#             341249: "HDFC Bank", #
#             424961: "ITC"     #
#         }

example_tokens = [8912129, 81153 ,	2426881, 3785729, 2674433, 341249,	424961, 738561, 2953217] #
# Define a dictionary to store instantiated strategies
STRATEGIES: Dict[str, RealTimeOrderFlowStrategy] = {} 
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# logging configuration for logging to both console and file
# Create a logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

file_logger = logging.getLogger("orderflow_file")
file_logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.FileHandler) for h in file_logger.handlers):
    file_path = os.path.join("logs", f"orderflow_logs_{datetime.now().strftime('%Y%m%d')}.log")
    fh = logging.FileHandler(file_path, mode='a', encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    file_logger.addHandler(fh)

file_logger.propagate = False  # prevents console output
# ==============================================================================
# 2. SIMPLE DATA READING CALLBACK FUNCTIONS
# These functions simply read the data dictionary and print key information.
# ==============================================================================

def read_ohlc_data(ohlc_data: Dict[str, Any]):
    """Callback for OHLC data updates."""
    symbol = ohlc_data.get('symbol', 'Unknown')
    close_price = ohlc_data.get('ohlc', {}).get('close', 0)
    volume = ohlc_data.get('volume', 0)
    logger.info(f"📈 [OHLC] {symbol}: Close=₹{close_price:.2f}, Volume={volume:,}")

def read_tick_data(tick_data: Dict[str, Any]):
    """Callback for Tick/Last Trade data updates."""
    print("\n" + "="*50)
    print(" *****************TICK DATA inside ORDERFLOW_CLIENT***************")
    symbol = tick_data.get('symbol', 'Unknown')
    last_price = tick_data.get('last_price', 0)
    logger.info(f"🔔 [TICK] {symbol}: Price=₹{last_price:.2f}, Volume={tick_data.get('volume', 0)}")
    
def read_depth_data(depth_data: Dict[str, Any]):
    """Callback for Market Depth data updates."""
    symbol = depth_data.get('symbol', 'Unknown')
    best_bid = depth_data.get('best_bid', 0)
    best_ask = depth_data.get('best_ask', 0)
    spread = depth_data.get('spread', 0)
    logger.info(f"🏛️  [DEPTH] {symbol}: Bid=₹{best_bid:.2f}, Ask=₹{best_ask:.2f}, Spread=₹{spread:.2f}")

def read_error_data(error_data: Dict[str, Any]):
    """Callback for error events."""
    logger.error(f"🚨 [ERROR] Type: {error_data.get('type')}, Reason: {error_data.get('error')}")

def read_connect_data(connect_data: Dict[str, Any]):
    """Callback for connection events."""
    logger.info(f"🔗 [CONNECT] Status: {connect_data.get('status')}")

# ==============================================================================
# 2. DISPATCHER FUNCTION
# This single function handles ALL incoming tick data and routes it.
# ==============================================================================
def ohlc_dispatcher(ohlcv_data: Dict[str, Any]):
    """
    Routes incoming tick data to the correct RealTimeOrderFlowStrategy instance.
    """
    #print("\n" + "?"*50)
    print(" *****************TInside OHLC Dispatcher***************")
    try:
        # Ticks from Zerodha provide 'instrument_token'
        token = ohlcv_data.get('instrument_token')
        
        if not token:
            logger.warning("Tick received with no instrument_token.")
            return

        # Look up the symbol using the token map
        # NOTE: Assumes TOKEN_MAP is accessible (e.g., as a global or passed)
        symbol = token_map.get(token) 
        
        if symbol and symbol in STRATEGIES:
            # 🌟 Found the strategy, now dispatch the data!
            strategy = STRATEGIES[symbol]
            strategy.handle_ohlc_update(ohlcv_data)
            
        else:
            # logger.debug(f"Tick received for unsubscribed token/symbol: {token} ({symbol})")
            pass

    except Exception as e:
        logger.error(f"❌ Error in tick_dispatcher: {e}")

# ==============================================================================
# 2. DISPATCHER FUNCTION
# This single function handles ALL incoming tick data and routes it.
# ==============================================================================
def tick_dispatcher(tick_data: Dict[str, Any]):
    """
    Routes incoming tick data to the correct RealTimeOrderFlowStrategy instance.
    """
    #print("\n" + "?"*50)
    #print(" *****************TInside Tick Dispatcher***************")
    try:
        # Ticks from Zerodha provide 'instrument_token'
        token = tick_data.get('instrument_token')
        
        if not token:
            logger.warning("Tick received with no instrument_token.")
            return

        # Look up the symbol using the token map
        # NOTE: Assumes TOKEN_MAP is accessible (e.g., as a global or passed)
        symbol = token_map.get(token) 
        
        if symbol and symbol in STRATEGIES:
            # 🌟 Found the strategy, now dispatch the data!
            strategy = STRATEGIES[symbol]
            strategy.handle_tick_update(tick_data)
            
        else:
            # logger.debug(f"Tick received for unsubscribed token/symbol: {token} ({symbol})")
            pass

    except Exception as e:
        logger.error(f"❌ Error in tick_dispatcher: {e}")

# ==============================================================================
# 3. INTEGRATION EXECUTION
# ==============================================================================

def run_datastream_integration():
    """Main function to initialize and start the data stream."""
    try:
        # Step 1: Initialize Zerodha KiteTicker connection
        kite_ticker = initialize_zerodha() #
        print("✅ Zerodha KiteTicker initialized successfully. Inside ORDERFLOW_CLIENT")
        # Step 2: Initialize the enhanced data display service
        data_service = ZerodhaKiteDataDisplay(kite_ticker) #
        
        # Step 2: Set token mapping on the data service
        data_service.set_token_symbol_map(token_map)

        # Step 3: Dynamically instantiate strategies and store them
        print("Instantiating strategies and preparing for data stream...")
        for token, symbol in token_map.items():
            # Create strategy instance for the symbol, I am passing token as symbol so that strategy works on tokenonly
            strategy_instance = RealTimeOrderFlowStrategy(symbol=token)
            # Store the instance in the global dictionary keyed by symbol
            STRATEGIES[symbol] = strategy_instance
            print(f"✅ Strategy created for {symbol}")

        # Step 4: Register the single dispatcher function
        print("\nRegistering data reading callbacks...")
        # Register the single dispatcher for ALL tick data
        data_service.register_tick_callback(tick_dispatcher)
        data_service.register_ohlc_callback(ohlc_dispatcher) #
        
        # Step 3: Register data reading callbacks with the service
        logger.info("Registering data reading callbacks...INSIDE ORDERFLOW_CLIENT")
        # data_service.register_ohlc_callback(read_ohlc_data) #
        # data_service.register_tick_callback(read_tick_data) #
        # data_service.register_depth_callback(read_depth_data) #
        # data_service.register_error_callback(read_error_data) #
        # data_service.register_connect_callback(read_connect_data) #
        # data_service.register_tick_callback(read_tick_data)
        
        print("✅ Callbacks registered successfully. Inside ORDERFLOW_CLIENT")
        # Step 4: Define tokens and mapping
        # NOTE: You must replace these with actual valid instrument tokens 
        # for your Zerodha account.
          # Example: RELIANCE, TCS
        
        data_service.set_token_symbol_map(token_map) #
        logger.info(f"Tokens set: {token_map}")
        
        # Step 5: Start streaming with FULL mode (provides OHLC, depth, etc.)
        # The streaming logic is handled inside data_service.start_streaming()
        data_service.start_streaming(
            tokens=example_tokens, 
            mode=KiteTicker.MODE_FULL
        )
        
    except ValueError as e:
        logger.critical(f"❌ Configuration error: {e}. Ensure ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN are set.")
    except Exception as e:
        logger.critical(f"❌ Main execution error: {e}")

if __name__ == "__main__":
    run_datastream_integration()