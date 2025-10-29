import pandas as pd
from typing import Dict, List, Callable, Any, Optional
import vectorbt as vbt
from kiteconnect import KiteTicker
import json
import logging
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
from collections import deque
from typing import Dict, List, Callable, Any, Union
import threading
from ParquetTickWriter import TickBufferManager
from config import example_tokens, token_map
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
#logger = logging.getLogger("orderflow")
file_logger = logging.getLogger("orderflow_file")
count=0

# Load environment variables from .env file
try:
    print("4. Loading environment variables...")
    env_path = os.path.join(os.path.dirname(__file__), '..','..','config', '.env')
    print(f"Looking for .env at: {env_path}")
    
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print("✅ Environment variables loaded from config folder")
    else:
        load_dotenv()
        print("✅ Environment variables loaded from default location")
except Exception as e:
    print(f"❌ Error loading environment variables: {e}")

def initialize_zerodha(api_key=None, access_token=None):
    """
    Initialize Zerodha Kite connection
    """
    try:
        # Use provided credentials or fall back to environment variables
        api_key = api_key or os.getenv('ZERODHA_API_KEY')
        access_token = access_token or os.getenv('ZERODHA_ACCESS_TOKEN')
        
        if not all([api_key, access_token]):
            raise ValueError(
                "Missing required credentials. Please provide them as arguments "
                "or set environment variables: ZERODHA_API_KEY, ZERODHA_ACCESS_TOKEN"
            )
        
        logger.info("Initializing Zerodha KiteTicker...")
        
        # Create KiteTicker instance
        kite_ticker = KiteTicker(api_key, access_token)
        
        logger.info("✅ Zerodha KiteTicker initialized successfully!")
        return kite_ticker
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Zerodha connection: {e}")
        raise

class KiteLiveDataStream:
    """
    Enhanced DataLiveStream with event callbacks for real-time strategy integration
    """
    def __init__(self, kite_ticker=None):
        """
        Initialize data display with Zerodha KiteTicker connection and event system
        """
        self.log_dir="tick_logs"
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        self.parquet_writer = TickBufferManager()
        
        # Use provided connection or create new one
        if kite_ticker:
            self.kite_ticker = kite_ticker
            logger.info("Using provided Zerodha KiteTicker connection")
        else:
            self.kite_ticker = initialize_zerodha()
        
        # Data storage with buffers
        self.ohlcv_data = pd.DataFrame()
        self.trade_data = []
        self.depth_data = {}
        

        # Data buffers for strategies (rolling windows)
        self.setup_data_buffers()
        
        # Event callback system
        self.setup_event_system()
        
        # Set up WebSocket connection and callbacks
        self.setup_websocket()
        
        # Token to symbol mapping
        self.token_symbol_map = {}

        # Dictionary to hold the currently aggregating bar data for each token
        self.current_bars: Dict[int, Dict[str, Any]] = {}
        # Dictionary to store the cumulative volume at the start of the current bar
        self.bar_start_cumulative_volume: Dict[int, int] = {}
        # Define the bar interval (in seconds)
        self.BAR_INTERVAL_SECONDS = 60 # 5 minutes = 300 seconds
        
    def setup_data_buffers(self):
        """Setup rolling buffers for real-time strategy processing"""
        # OHLCV buffer - last 100 bars
        self.ohlcv_buffer = deque(maxlen=100)
        
        # Tick buffer - last 5000 ticks
        self.tick_buffer = deque(maxlen=5000)
        
        # Depth buffer - last 100 snapshots
        self.depth_buffer = deque(maxlen=100)
        
        # Current instrument being tracked
        self.current_instrument = None
        
        logger.info("✅ Data buffers initialized")
    
    def setup_event_system(self):
        """Initialize event callback system"""
        # Callback registry
        self.callbacks = {
            'ohlc': [],      # OHLC data callbacks
            'tick': [],      # Tick trade data callbacks  
            'depth': [],     # Market Depth callbacks
            'error': [],     # Error handling callbacks
            'connect': [],   # Connection event callbacks
        }
        
        # Thread lock for thread-safe callback operations
        self.callback_lock = threading.RLock()
        
        # Statistics
        self.callback_stats = {
            'ohlc_calls': 0,
            'tick_calls': 0,
            'depth_calls': 0,
            'errors': 0
        }
        
        logger.info("✅ Event callback system initialized")
    
    def setup_websocket(self):
        """Set up WebSocket connection and callbacks as per Zerodha Kite documentation"""
        try:
            # Assign callbacks
            self.kite_ticker.on_ticks = self.on_ticks
            self.kite_ticker.on_connect = self.on_connect
            self.kite_ticker.on_close = self.on_close
            self.kite_ticker.on_error = self.on_error
            
            logger.info("✅ WebSocket callbacks assigned")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup WebSocket callbacks: {e}")
            self._trigger_callbacks('error', {
                'type': 'websocket_setup', 
                'error': str(e),
                'timestamp': datetime.now()
            })
            raise
    
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
    
    def aggregate_to_bar(self, tick_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
        bar_open_time = self._get_bar_time(current_time_str, self.BAR_INTERVAL_SECONDS)
        
        # 2. Check for initialization or bar change
        if token not in self.current_bars:
            # --- INITIALIZE FIRST BAR ---
            self.current_bars[token] = {
                'Open': last_price,
                'High': last_price,
                'Low': last_price,
                'Volume': 0, # Will be calculated at close
                'OpenTime': bar_open_time
            }
            self.bar_start_cumulative_volume[token] = volume_traded_cumulative
            return None

        current_bar = self.current_bars[token]
        
        # 3. Bar Close Check: Has the current tick crossed into a new bar interval?
        if bar_open_time > current_bar['OpenTime']:
            # --- BAR IS CLOSED - FINALIZING ---
            print("Bar is closed, finalizing...") 
            # Calculate Bar Volume: Current cumulative volume minus volume at bar start
            bar_volume = max(0, volume_traded_cumulative - self.bar_start_cumulative_volume[token])
            print(f"Bar Volume: {bar_volume}")
            # The Close price of the just-completed bar is the last price of the previous tick
            closed_bar = {
                'timestamp': current_bar['OpenTime'] + timedelta(seconds=self.BAR_INTERVAL_SECONDS),
                'symbol': self.token_symbol_map.get(token, 'UNKNOWN'),
                'instrument_token': token,
                'open': current_bar['Open'],
                'high': current_bar['High'],
                'low': current_bar['Low'],
                'close': current_bar['Close'], # The last price stored from the previous tick
                'volume': bar_volume
            }
            print("Closed Bar Data:", closed_bar)
            # --- RESET FOR NEW BAR ---
            self.current_bars[token] = {
                'Open': last_price,
                'High': last_price,
                'Low': last_price,
                'Close': last_price,
                'Volume': 0,
                'OpenTime': bar_open_time
            }
            self.bar_start_cumulative_volume[token] = volume_traded_cumulative
            
            return closed_bar # Return the completed bar
            
        else:
            # --- BAR IS OPEN - AGGREGATING ---
            print("Bar is still open, aggregating data...")
            # Update High, Low, and Current Close Price
            current_bar['High'] = max(current_bar['High'], last_price)
            current_bar['Low'] = min(current_bar['Low'], last_price)
            current_bar['Close'] = last_price # The Close is always the last price seen
            
            return None # Bar is still open
    
    def on_connect(self, ws, response):
        """Callback when connection is established"""
        logger.info("✅ WebSocket connection established")
        self._trigger_callbacks('connect', {
            'status': 'connected', 
            'response': response,
            'timestamp': datetime.now()
        })
        # ✅ Subscribe *after* connection is ready
        print("Subscribing to tokens after connection..." )
        if self.tokens_to_subscribe:
            ws.subscribe(self.tokens_to_subscribe)
            ws.set_mode(self.kite_ticker.MODE_FULL, self.tokens_to_subscribe)
            logger.info(f"✅ Subscribed to {self.tokens_to_subscribe} with mode FULL after connection.")
        else:
            logger.warning("⚠️ No tokens to subscribe after connection.")
    
    def on_close(self, ws, code, reason):
        """Callback when connection is closed"""
        logger.info(f"🔴 WebSocket connection closed: {code} - {reason}")
        self._trigger_callbacks('connect', {
            'status': 'disconnected', 
            'code': code,
            'reason': reason,
            'timestamp': datetime.now()
        })
    
    def on_error(self, ws, code, reason):
        """Callback when error occurs"""
        logger.error(f"❌ WebSocket error: {code} - {reason}")
        self._trigger_callbacks('error', {
            'type': 'websocket_error',
            'code': code,
            'reason': reason,
            'timestamp': datetime.now()
        })
    
    def on_ticks(self, ws, ticks):
        """
        Enhanced callback for tick data with event triggering
        """
        # #count+=1
        # print("Inside on_ticks callback...data streaming... count:",count)
        #print(json.dumps(ticks, indent=2, default=str))
        try:
            # Handle single tick or multiple ticks
            if isinstance(ticks, list):
                for tick in ticks:
                    self.process_tick_data(tick)
            else:
                self.process_tick_data(ticks)
                
        except Exception as e:
            logger.error(f"Error processing ticks data: {e}")
            self._trigger_callbacks('error', {
                'type': 'tick_processing',
                'error': str(e),
                'timestamp': datetime.now()
            })
    
    def _get_filepath(self, instrument_token: int, timestamp: datetime) -> str:
        """Constructs the dynamic file path for the Excel log."""
        date_str = timestamp.strftime('%Y%m%d')
        filename = f"{instrument_token}_{date_str}.csv"
        return os.path.join(self.log_dir, filename)
    
    def _flatten_tick_data(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flattens nested fields (ohlc, depth_buy/sell) into flat key-value pairs."""
        flat = {}

        for key, value in tick_data.items():
            # Case 1: ohlc or other nested dicts
            if isinstance(value, dict) and key != 'depth':
                for sub_key, sub_value in value.items():
                    flat[f"{key}_{sub_key}"] = sub_value

            # Case 2: depth_buy / depth_sell (lists of levels)
            elif isinstance(value, list) and key in ['depth_buy', 'depth_sell']:
                side = key.split('_')[1]  # 'buy' or 'sell'
                for i, level in enumerate(value):
                    flat[f"depth_{side}_{i}_price"] = level.get('price')
                    flat[f"depth_{side}_{i}_quantity"] = level.get('quantity')
                    flat[f"depth_{side}_{i}_orders"] = level.get('orders')

            # Case 3: depth with nested buy/sell dicts
            elif key == 'depth' and isinstance(value, dict):
                for side in ['buy', 'sell']:
                    depth_list = value.get(side, [])
                    for i, level in enumerate(depth_list):
                        flat[f"depth_{side}_{i}_price"] = level.get('price')
                        flat[f"depth_{side}_{i}_quantity"] = level.get('quantity')
                        flat[f"depth_{side}_{i}_orders"] = level.get('orders')

            # Case 4: normal scalar values
            else:
                flat[key] = value

        return flat

    def store_tick_data_excel(self, tick_data: Dict[str, Any]):
        try:
            # 1. Extract Key Information
            token = tick_data.get('instrument_token')
            timestamp = tick_data.get('exchange_timestamp')
            
            # Use 'timestamp' if 'exchange_timestamp' is missing for robustness
            if timestamp is None:
                 timestamp = tick_data.get('timestamp')
            
            # Ensure we have the critical data points
            if token is None or timestamp is None:
                print(f"ERROR: Tick data missing token or timestamp: {tick_data}")
                return

            # Convert timestamp to a datetime object if it's a string (common when loading from file/json)
            if isinstance(timestamp, str):
                # Try a common format for Kite ticks. Add more formats if needed.
                try:
                    timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            # 2. Determine Filepath
            filepath = self._get_filepath(token, timestamp)
            
            # 3. Flatten the Tick Data for DataFrame compatibility
            # Nested fields like 'ohlc' and 'depth' need to be flattened to single columns
            flat_data = self._flatten_tick_data(tick_data)
            
            # 4. Create single-row DataFrame
            df_tick = pd.DataFrame([flat_data])
            df_tick = df_tick.set_index('exchange_timestamp') # Use timestamp as index
            
            # 5. Save/Append to CSV (The efficient part!)
            
            # Check if the file exists to decide whether to write the header
            write_header = not os.path.exists(filepath)
            
            # Use 'a' (append) mode for true, fast appending
            df_tick.to_csv(
                filepath, 
                mode='a', 
                index=True, 
                header=write_header
            )

        except Exception as e:
            print(f"An unexpected error occurred during tick logging: {e}")
            # print(f"Problematic Tick Data: {tick_data}")
    
    def process_tick_data(self, tick_data):
        """Process individual tick data and trigger events"""
        try:
            
            #commenting tick callback to test the ohlc callback, uncomment next line to enable tick callback
            #self._trigger_callbacks('tick', tick_data)
            #self.read_tick_data(tick_data)
            flat_data = self._flatten_tick_data(tick_data)
            #print(f"Flattened Tick Data: {flat_data}")
            
            self.parquet_writer.add_tick(flat_data)
            
        except Exception as e:
            logger.error(f"Error processing individual tick: {e}")
            self._trigger_callbacks('error', {
                'type': 'tick_processing_individual',
                'error': str(e),
                'tick_data': tick_data,
                'timestamp': datetime.now()
            })
    
    def process_trade_data(self, trade_data, symbol):
        """Process trade data and trigger tick callbacks"""
        print("\n" + "="*50)
        print("📊 TICK TRADE DATA")
        print("="*50)
        
        # Store trade data
        self.trade_data.append(trade_data)
        
        # Update current instrument
        self.current_instrument = symbol
        
        # Display trade information
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbol: {symbol}")
        print(f"Last Price: ₹{trade_data.get('last_price', 0):.2f}")
        print(f"Volume: {trade_data.get('volume', 0)}")
        
        # Bid/Ask information
        if 'depth' in trade_data and trade_data['depth']['buy']:
            best_bid = trade_data['depth']['buy'][0]
            print(f"Best Bid: ₹{best_bid.get('price', 0):.2f} (Qty: {best_bid.get('quantity', 0)})")
        
        if 'depth' in trade_data and trade_data['depth']['sell']:
            best_ask = trade_data['depth']['sell'][0]
            print(f"Best Ask: ₹{best_ask.get('price', 0):.2f} (Qty: {best_ask.get('quantity', 0)})")
        
        # Change information (calculate if not provided)
        if 'ohlc' in trade_data:
            ohlc = trade_data['ohlc']
            if ohlc['close'] > 0:
                change = trade_data.get('last_price', 0) - ohlc['close']
                change_pct = (change / ohlc['close']) * 100
                print(f"Change: {change:.2f}")
                print(f"Change %: {change_pct:.2f}%")
        
        print("-" * 30)
        
        # Enhanced data for callbacks
        enhanced_trade_data = {
            **trade_data,
            'event_type': 'tick',
            'symbol': symbol,
            'processing_timestamp': datetime.now(),
            'buffer_size': len(self.tick_buffer)
        }
        
        # Trigger tick callbacks
        self._trigger_callbacks('tick', enhanced_trade_data)
        
        # Also process depth data if available
        if 'depth' in trade_data:
            self.process_depth_data(trade_data, symbol)
    
    def process_ohlc_data(self, ohlc_data, symbol):
        """Process OHLC data and trigger OHLC callbacks"""
        print("\n" + "="*50)
        print("📈 OHLC DATA")      
        print("="*50)
        
        ohlc = ohlc_data.get('ohlc', {})
        
        # Create DataFrame entry
        new_data = pd.DataFrame([{
            'timestamp': datetime.now(),
            'open': ohlc.get('open', 0),
            'high': ohlc.get('high', 0),
            'low': ohlc.get('low', 0),
            'close': ohlc.get('close', 0),
            'volume': ohlc_data.get('volume', 0),
            'symbol': symbol
        }])
        
        # Update OHLCV data
        if self.ohlcv_data.empty:
            self.ohlcv_data = new_data
        else:
            self.ohlcv_data = pd.concat([self.ohlcv_data, new_data], ignore_index=True)
        
        # Store in buffer
        buffer_entry = new_data.iloc[0].to_dict()
        buffer_entry['instrument_token'] = ohlc_data.get('instrument_token')
        self.ohlcv_buffer.append(buffer_entry)
        
        # Update current instrument
        self.current_instrument = symbol
        
        # Display OHLC information
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbol: {symbol}")
        print(f"Open: ₹{ohlc.get('open', 0):.2f}")
        print(f"High: ₹{ohlc.get('high', 0):.2f}")
        print(f"Low: ₹{ohlc.get('low', 0):.2f}")
        print(f"Close: ₹{ohlc.get('close', 0):.2f}")
        print(f"Volume: {ohlc_data.get('volume', 0):,}")
        
        print("-" * 30)
        
        # Enhanced data for callbacks
        enhanced_ohlc_data = {
            **ohlc_data,
            'event_type': 'ohlc',
            'symbol': symbol,
            'processing_timestamp': datetime.now(),
            'buffer_size': len(self.ohlcv_buffer),
            'dataframe_row': new_data.iloc[0].to_dict()
        }
        
        # Trigger OHLC callbacks
        self._trigger_callbacks('ohlc', enhanced_ohlc_data)
    
    def process_depth_data(self, depth_data, symbol):
        """Process depth data and trigger depth callbacks"""
        print("\n" + "="*50)
        print("🏛️  MARKET DEPTH")
        print("="*50)
        
        # Store depth data
        self.depth_data = depth_data
        self.depth_buffer.append(depth_data)
        
        # Update current instrument
        self.current_instrument = symbol
        
        # Display depth information
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbol: {symbol}")
        
        # Display bids
        print("\n🟢 BIDS (Buy Orders):")
        bids = depth_data.get('depth', {}).get('buy', [])
        if bids:
            for i, bid in enumerate(bids[:5]):  # Top 5 bids
                price = bid.get('price', 0)
                quantity = bid.get('quantity', 0)
                orders = bid.get('orders', 0)
                print(f"  {i+1}. Price: ₹{price:.2f} | Qty: {quantity:,} | Orders: {orders}")
        else:
            print("  No bids available")
        
        # Display asks
        print("\n🔴 ASKS (Sell Orders):")
        asks = depth_data.get('depth', {}).get('sell', [])
        if asks:
            for i, ask in enumerate(asks[:5]):  # Top 5 asks
                price = ask.get('price', 0)
                quantity = ask.get('quantity', 0)
                orders = ask.get('orders', 0)
                print(f"  {i+1}. Price: ₹{price:.2f} | Qty: {quantity:,} | Orders: {orders}")
        else:
            print("  No asks available")
        
        # Calculate spread
        if bids and asks:
            best_bid = bids[0].get('price', 0) if bids else 0
            best_ask = asks[0].get('price', 0) if asks else 0
            if best_bid > 0 and best_ask > 0:
                spread = best_ask - best_bid
                spread_pct = (spread / best_bid) * 100
                print(f"\n💰 Spread: ₹{spread:.2f} ({spread_pct:.2f}%)")
        
        print("-" * 30)
        
        # Enhanced data for callbacks
        enhanced_depth_data = {
            **depth_data,
            'event_type': 'depth',
            'symbol': symbol,
            'processing_timestamp': datetime.now(),
            'buffer_size': len(self.depth_buffer),
            'best_bid': best_bid if bids else 0,
            'best_ask': best_ask if asks else 0,
            'spread': spread if bids and asks else 0
        }
        
        # Trigger depth callbacks
        self._trigger_callbacks('depth', enhanced_depth_data)
    
    # ==========================================================================
    # EVENT CALLBACK REGISTRATION METHODS
    # ==========================================================================
    
    def register_ohlc_callback(self, callback: Callable[[Dict], None]):
        """Register a callback for OHLC data updates"""
        with self.callback_lock:
            self.callbacks['ohlc'].append(callback)
        logger.info(f"✅ Registered OHLC callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_tick_callback(self, callback: Callable[[Dict], None]):
        """Register a callback for tick trade data updates"""
        with self.callback_lock:
            self.callbacks['tick'].append(callback)
        logger.info(f"✅ Registered Tick callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_depth_callback(self, callback: Callable[[Dict], None]):
        """Register a callback for Market Depth updates"""
        with self.callback_lock:
            self.callbacks['depth'].append(callback)
        logger.info(f"✅ Registered Depth callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_error_callback(self, callback: Callable[[Dict], None]):
        """Register a callback for error events"""
        with self.callback_lock:
            self.callbacks['error'].append(callback)
        logger.info(f"✅ Registered Error callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_connect_callback(self, callback: Callable[[Dict], None]):
        """Register a callback for connection events"""
        with self.callback_lock:
            self.callbacks['connect'].append(callback)
        logger.info(f"✅ Registered Connect callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def unregister_callback(self, callback: Callable, callback_type: str = None):
        """Unregister a specific callback"""
        with self.callback_lock:
            if callback_type:
                if callback_type in self.callbacks and callback in self.callbacks[callback_type]:
                    self.callbacks[callback_type].remove(callback)
                    logger.info(f"✅ Unregistered callback from {callback_type}")
            else:
                # Remove from all types
                for cb_type in self.callbacks:
                    if callback in self.callbacks[cb_type]:
                        self.callbacks[cb_type].remove(callback)
                logger.info("✅ Unregistered callback from all types")
    
    def _trigger_callbacks(self, callback_type: str, data: Dict):
        """Internal method to trigger callbacks of specific type"""
        if callback_type not in self.callbacks:
            logger.warning(f"Unknown callback type: {callback_type}")
            return
        
        # Update statistics
        if callback_type in ['ohlc', 'tick', 'depth']:
            self.callback_stats[f'{callback_type}_calls'] += 1
        
        callbacks_to_call = []
        with self.callback_lock:
            callbacks_to_call = self.callbacks[callback_type].copy()
        
        # Call each callback
        for callback in callbacks_to_call:
            try:
                # print(f"📞 Triggering {callback_type} callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
                # print("Data passed to callback:", data)
                callback(data)
            except Exception as e:
                logger.error(f"❌ Error in {callback_type} callback {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}: {e}")
                self.callback_stats['errors'] += 1
                
                # Trigger error callbacks
                error_data = {
                    'type': f'callback_error_{callback_type}',
                    'error': str(e),
                    'callback': callback.__name__ if hasattr(callback, '__name__') else 'anonymous',
                    'timestamp': datetime.now()
                }
                self._trigger_callbacks('error', error_data)
    
    # ==========================================================================
    # UTILITY METHODS FOR STRATEGIES
    # ==========================================================================
    
    def get_recent_ohlcv(self, count: int = 10) -> pd.DataFrame:
        """Get recent OHLCV data for strategies"""
        if len(self.ohlcv_buffer) == 0:
            return pd.DataFrame()
        
        recent_data = list(self.ohlcv_buffer)[-count:]
        return pd.DataFrame(recent_data)
    
    def get_recent_ticks(self, count: int = 100) -> List[Dict]:
        """Get recent tick data for strategies"""
        return list(self.tick_buffer)[-count:]
    
    def get_recent_depth(self, count: int = 5) -> List[Dict]:
        """Get recent depth snapshots for strategies"""
        return list(self.depth_buffer)[-count:]
    
    def get_callback_stats(self) -> Dict:
        """Get callback execution statistics"""
        return self.callback_stats.copy()
    
    def get_data_summary(self) -> Dict:
        """Get summary of current data state"""
        return {
            'ohlcv_records': len(self.ohlcv_buffer),
            'tick_records': len(self.tick_buffer),
            'depth_records': len(self.depth_buffer),
            'current_instrument': self.current_instrument,
            'callback_stats': self.get_callback_stats()
        }
    
    # ==========================================================================
    # ZERODHA SPECIFIC METHODS
    # ==========================================================================
    
    def get_symbol_from_token(self, token):
        """Get symbol name from instrument token"""
        # This would typically involve mapping tokens to symbols
        # For now, return token if mapping not found
        return self.token_symbol_map.get(token, f"TOKEN_{token}")
    
    def set_token_symbol_map(self, token_map: Dict):
        """Set mapping between instrument tokens and symbols"""
        self.token_symbol_map = token_map
        logger.info(f"✅ Set token-symbol mapping for {len(token_map)} instruments")
    
    def subscribe_instruments(self, tokens: List):
        """Subscribe to instruments by token"""
        try:
            if not tokens:
                logger.warning("No tokens provided for subscription")
                return
            
            # Subscribe to tokens
            self.kite_ticker.subscribe(tokens)
            
            logger.info(f"✅ Subscribed to {len(tokens)} instruments")
            
        except Exception as e:
            logger.error(f"❌ Error subscribing to instruments: {e}")
            self._trigger_callbacks('error', {
                'type': 'subscription_error',
                'error': str(e),
                'tokens': tokens,
                'timestamp': datetime.now()
            })
    
    def set_subscription_mode(self, tokens: List, mode):
    #"""Set mode for subscribed instruments"""
        try:
            if not tokens:
                logger.warning("No tokens provided for mode setting")
                return
            
            self.kite_ticker.set_mode(mode, tokens)
            logger.info(f"✅ Set mode {mode} for {len(tokens)} instruments")
            
        except Exception as e:
            logger.error(f"❌ Error setting subscription mode: {e}")
            self._trigger_callbacks('error', {
                'type': 'mode_setting_error',
                'error': str(e),
                'tokens': tokens,
                'mode': mode,
                'timestamp': datetime.now()
            })
    def subscribe_mode(self, tokens: List, mode):
        """Subscribe with specific mode (quote, full)"""
        print("inside subscribe mode function, mode:", mode)
        try:
            if not tokens:
                logger.warning("No tokens provided for subscription")
                return
            
            # Subscribe to tokens with specific mode
            print("before subscribing to tokens:", tokens)
            self.kite_ticker.set_mode(self.kite_ticker.MODE_FULL, tokens)
            self.kite_ticker.subscribe(tokens)
            print("subscribed to tokens:", tokens)
                # If you want to set mode, use set_mode() method separately
            
            #mode=KiteTicker.MODE_FULL
            print("set mode value to kiteticker.mode_full:", mode)
            if mode:
                # Note: set_mode expects mode constant and tokens list
                # mode should be a single value like self.kite_ticker.MODE_QUOTE, not a list
                if isinstance(mode, list):
                    # Take first mode from list if mode is passed as list
                    #actual_mode = mode[0]
                    print("...mode is a list:", mode)
                    actual_mode = mode
                    self.kite_ticker.set_mode(actual_mode, tokens)
                else:
                    # If mode is not a list, use it directly
                    print("...mode is not a list:", mode)
                    self.kite_ticker.set_mode(mode, tokens)
                logger.info(f"✅ Subscribed to {len(tokens)} instruments in mode {mode}")
            
        except Exception as e:
            logger.error(f"❌ Error subscribing to instruments with specific mode: {e}")
            self._trigger_callbacks('error', {
                'type': 'subscription_error',
                'error': str(e),
                'tokens': tokens,
                'mode': mode,
                'timestamp': datetime.now()
            })
    
    def unsubscribe_instruments(self, tokens: List):
        """Unsubscribe from instruments"""
        try:
            if not tokens:
                logger.warning("No tokens provided for unsubscription")
                return
            
            # Unsubscribe from tokens
            self.kite_ticker.unsubscribe(tokens)
            
            logger.info(f"✅ Unsubscribed from {len(tokens)} instruments")
            
        except Exception as e:
            logger.error(f"❌ Error unsubscribing from instruments: {e}")
            self._trigger_callbacks('error', {
                'type': 'unsubscription_error',
                'error': str(e),
                'tokens': tokens,
                'timestamp': datetime.now()
            })
    
    def start_streaming(self, tokens: List = None, mode: str = None):
        """Start WebSocket streaming"""
        try:
            print("🚀 Starting Zerodha Kite Real-time Data Stream...")
            print("Press Ctrl+C to stop\n")
            # Save tokens & mode for later use
            self.tokens_to_subscribe = tokens
            self.mode_to_use = mode
            # Connect to WebSocket (non-blocking)
            self.kite_ticker.connect(threaded=True)
            print("📡 WebSocket connection started in threaded mode...")
            print("Data will appear below as it arrives:\n")
             # Keep the main thread alive
            count=0
            
            # Subscribe if tokens provided
            # if tokens:
            #     if mode:
            #         print("Subscribing with specific mode...")
            #         self.subscribe_mode(tokens,mode)
            #     else:
            #         print("Subscribing to instruments...")
            #         self.subscribe_instruments(tokens)

            try:
                while True:
                    time.sleep(0.1)  # Shorter sleep for better responsiveness
                   
            except KeyboardInterrupt:
                print("\n\n🛑 Stopping data stream...")
                self.kite_ticker.close()
                self.display_summary()

            
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping data stream...")
            self.kite_ticker.close()
            self.display_summary()
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
            self._trigger_callbacks('error', {
                'type': 'streaming_error',
                'error': str(e),
                'timestamp': datetime.now()
            })
    
    def display_summary(self):
        """Display summary of collected data"""
        print("\n" + "="*60)
        print("📋 DATA SUMMARY")
        print("="*60)
        print(f"Trade Ticks: {len(self.trade_data)}")
        print(f"Depth Updates: {1 if self.depth_data else 0}")
        print(f"OHLCV Records: {len(self.ohlcv_data)}")
        
        # Callback statistics
        stats = self.get_callback_stats()
        print(f"\n📊 CALLBACK STATISTICS:")
        print(f"  OHLC Callbacks: {stats['ohlc_calls']}")
        print(f"  Tick Callbacks: {stats['tick_calls']}")
        print(f"  Depth Callbacks: {stats['depth_calls']}")
        print(f"  Callback Errors: {stats['errors']}")
        
        if not self.ohlcv_data.empty:
            latest_ohlcv = self.ohlcv_data.iloc[-1]
            print(f"\nLatest OHLCV:")
            print(f"  O: ₹{latest_ohlcv['open']:.2f} | H: ₹{latest_ohlcv['high']:.2f}")
            print(f"  L: ₹{latest_ohlcv['low']:.2f} | C: ₹{latest_ohlcv['close']:.2f}")
            print(f"  Volume: {latest_ohlcv['volume']:,}")
            print(f"  Symbol: {latest_ohlcv.get('symbol', 'N/A')}")
        
        print("="*60)
    def read_tick_data(self, tick_data: Dict[str, Any]):
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
            print(f"Tick data recived and data is: {tick_data}")
            flat_data = self._flatten_tick_data(tick_data)
            print(f"Flattened Tick Data: {flat_data}")
            
            self.parquet_writer.add_tick(flat_data)
            # # Look up the symbol using the token map
            # # NOTE: Assumes TOKEN_MAP is accessible (e.g., as a global or passed)
            # symbol = token_map.get(token) 
            
            # if symbol and symbol in STRATEGIES:
            #     # 🌟 Found the strategy, now dispatch the data!
            #     strategy = STRATEGIES[symbol]
            #     strategy.handle_tick_update(tick_data)
                
            # else:
            #     # logger.debug(f"Tick received for unsubscribed token/symbol: {token} ({symbol})")
            #     pass

        except Exception as e:
            logger.error(f"❌ Error in tick_dispatcher: {e}")

# ==============================================================================
# DEMONSTRATION: How to use the enhanced Zerodha DataLiveStream with callbacks
# ==============================================================================

def demo_ohlc_callback(ohlc_data):
    """Example OHLC callback for demonstration"""
    print(f"🔔 OHLC CALLBACK: {ohlc_data.get('symbol', 'Unknown')} "
          f"Close: ₹{ohlc_data.get('ohlc', {}).get('close', 0):.2f} "
          f"Volume: {ohlc_data.get('volume', 0):,}")

def demo_tick_callback(tick_data):
    """Example Tick callback for demonstration"""
    print(f"🔔 TICK CALLBACK: {tick_data.get('symbol', 'Unknown')} "
          f"Price: ₹{tick_data.get('last_price', 0):.2f} "
          f"Volume: {tick_data.get('volume', 0)}")

def demo_depth_callback(depth_data):
    """Example Depth callback for demonstration"""
    print(f"🔔 DEPTH CALLBACK: {depth_data.get('symbol', 'Unknown')} "
          f"Best Bid: ₹{depth_data.get('best_bid', 0):.2f} "
          f"Best Ask: ₹{depth_data.get('best_ask', 0):.2f}")

def demo_error_callback(error_data):
    """Example Error callback for demonstration"""
    print(f"🚨 ERROR CALLBACK: {error_data.get('type', 'Unknown')} - {error_data.get('error', 'No details')}")

def demo_connect_callback(connect_data):
    """Example Connect callback for demonstration"""
    print(f"🔗 CONNECT CALLBACK: {connect_data.get('status', 'Unknown')}")


def main():
    """Main function with callback demonstration"""
    try:
        # Initialize Zerodha connection
        kite_ticker = initialize_zerodha()
        
        # Initialize enhanced data display
        data_display = KiteLiveDataStream(kite_ticker)
        
        # Register on_tick callbacks
        #data_display.register_tick_callback(read_tick_data)
        
        
        
        # Set token to symbol mapping
        # token_map = {
        #     8912129: "ADANI GREEN ENERGY",
        #     3861249:  "ADANI PORT & SEZ",
        #     81153: "BAJAJ FINANCE",
        #     2426881: "LIFE INSURA CORP OF INDIA",
        #     3785729: "SUN PHARMA ADV.RES.CO.LTD",
        #     2674433: "UNITED SPIRITS"
        # }

        data_display.set_token_symbol_map(token_map)
        print("✅ Token to symbol mapping set")
        print(f"Subscribed Tokens: {example_tokens}")
        
        
        
        # Start streaming (use 'quote' for OHLC + depth, 'full' for all data)
        data_display.start_streaming(tokens=example_tokens, mode=KiteTicker.MODE_FULL)
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print("\n❌ Please check your .env file with proper credentials")
    except Exception as e:
        logger.error(f"Main execution error: {e}")

if __name__ == "__main__":
    main()