import pandas as pd
import vectorbt as vbt
from breeze_connect import BreezeConnect
import json
import logging
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from collections import deque
from typing import Dict, List, Callable, Any
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
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
except Exception as e:
    print(f"❌ Error loading environment variables: {e}")

def initialize_breeze(api_key=None, api_secret=None, session_token=None):
    """
    Initialize Breeze API connection
    """
    try:
        # Use provided credentials or fall back to environment variables
        api_key = api_key or os.getenv('BREEZE_API_KEY')
        api_secret = api_secret or os.getenv('BREEZE_API_SECRET')
        session_token = session_token or os.getenv('BREEZE_ACCESS_TOKEN')
        
        if not all([api_key, api_secret, session_token]):
            raise ValueError(
                "Missing required credentials. Please provide them as arguments "
                "or set environment variables: BREEZE_API_KEY, BREEZE_API_SECRET, BREEZE_ACCESS_TOKEN"
            )
        
        logger.info("Initializing Breeze connection...")
        breeze = BreezeConnect(api_key=api_key)
        
        logger.info("Generating session...")
        breeze.generate_session(api_secret=api_secret, session_token=session_token)
        
        logger.info("✅ Breeze connection established successfully!")
        return breeze
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Breeze connection: {e}")
        raise

class ICICIBreezeDataDisplay:
    """
    Enhanced DataLiveStream with event callbacks for real-time strategy integration
    """
    def __init__(self, breeze_connection=None):
        """
        Initialize data display with Breeze connection and event system
        """
        # Use provided connection or create new one
        if breeze_connection:
            self.breeze = breeze_connection
            logger.info("Using provided Breeze connection")
        else:
            self.breeze = initialize_breeze()
        
        # Data storage with buffers
        self.ohlcv_data = pd.DataFrame()
        self.trade_data = []
        self.dom_data = {}
        
        # Data buffers for strategies (rolling windows)
        self.setup_data_buffers()
        
        # Event callback system
        self.setup_event_system()
        
        # Set up WebSocket connection and callbacks
        self.setup_websocket()
        
    def setup_data_buffers(self):
        """Setup rolling buffers for real-time strategy processing"""
        # OHLCV buffer - last 100 bars
        self.ohlcv_buffer = deque(maxlen=100)
        
        # Tick buffer - last 5000 ticks
        self.tick_buffer = deque(maxlen=5000)
        
        # DOM buffer - last 100 snapshots
        self.dom_buffer = deque(maxlen=100)
        
        # Current symbol being tracked
        self.current_symbol = None
        
        logger.info("✅ Data buffers initialized")
    
    def setup_event_system(self):
        """Initialize event callback system"""
        # Callback registry
        self.callbacks = {
            'ohlc': [],      # OHLC data callbacks
            'tick': [],      # Tick trade data callbacks  
            'dom': [],       # Depth of Market callbacks
            'error': [],     # Error handling callbacks
            'connect': [],   # Connection event callbacks
        }
        
        # Thread lock for thread-safe callback operations
        self.callback_lock = threading.RLock()
        
        # Statistics
        self.callback_stats = {
            'ohlc_calls': 0,
            'tick_calls': 0,
            'dom_calls': 0,
            'errors': 0
        }
        
        logger.info("✅ Event callback system initialized")
    
    def setup_websocket(self):
        """Set up WebSocket connection and callbacks as per Breeze documentation"""
        try:
            # Connect to WebSocket
            self.breeze.ws_connect()
            
            # Assign callbacks as per documentation
            self.breeze.on_ticks = self.on_ticks
            self.breeze.on_depth = self.on_depth
            
            # Trigger connection callbacks
            self._trigger_callbacks('connect', {'status': 'connected', 'timestamp': datetime.now()})
            
            logger.info("✅ WebSocket connection established with callbacks")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup WebSocket: {e}")
            self._trigger_callbacks('error', {
                'type': 'websocket_connection', 
                'error': str(e),
                'timestamp': datetime.now()
            })
            raise
    
    # ==========================================================================
    # EVENT CALLBACK REGISTRATION METHODS
    # ==========================================================================
    
    def register_ohlc_callback(self, callback: Callable[[Dict], None]):
        """
        Register a callback for OHLC data updates
        
        Args:
            callback: Function that receives OHLC data dict
        """
        with self.callback_lock:
            self.callbacks['ohlc'].append(callback)
        logger.info(f"✅ Registered OHLC callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_tick_callback(self, callback: Callable[[Dict], None]):
        """
        Register a callback for tick trade data updates
        
        Args:
            callback: Function that receives tick data dict
        """
        with self.callback_lock:
            self.callbacks['tick'].append(callback)
        logger.info(f"✅ Registered Tick callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_dom_callback(self, callback: Callable[[Dict], None]):
        """
        Register a callback for Depth of Market updates
        
        Args:
            callback: Function that receives DOM data dict
        """
        with self.callback_lock:
            self.callbacks['dom'].append(callback)
        logger.info(f"✅ Registered DOM callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_error_callback(self, callback: Callable[[Dict], None]):
        """
        Register a callback for error events
        
        Args:
            callback: Function that receives error information
        """
        with self.callback_lock:
            self.callbacks['error'].append(callback)
        logger.info(f"✅ Registered Error callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def register_connect_callback(self, callback: Callable[[Dict], None]):
        """
        Register a callback for connection events
        
        Args:
            callback: Function that receives connection status
        """
        with self.callback_lock:
            self.callbacks['connect'].append(callback)
        logger.info(f"✅ Registered Connect callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
    
    def unregister_callback(self, callback: Callable, callback_type: str = None):
        """
        Unregister a specific callback
        
        Args:
            callback: The callback function to remove
            callback_type: Specific type to remove from, or None for all types
        """
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
        """
        Internal method to trigger callbacks of specific type
        
        Args:
            callback_type: Type of callback to trigger ('ohlc', 'tick', 'dom', 'error', 'connect')
            data: Data to pass to callbacks
        """
        if callback_type not in self.callbacks:
            logger.warning(f"Unknown callback type: {callback_type}")
            return
        
        # Update statistics
        if callback_type in ['ohlc', 'tick', 'dom']:
            self.callback_stats[f'{callback_type}_calls'] += 1
        
        callbacks_to_call = []
        with self.callback_lock:
            callbacks_to_call = self.callbacks[callback_type].copy()
        
        # Call each callback
        for callback in callbacks_to_call:
            try:
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
    # ENHANCED WEBSOCKET HANDLERS WITH EVENT TRIGGERS
    # ==========================================================================
    
    def on_ticks(self, ticks_data):
        """
        Enhanced callback for tick data with event triggering
        """
        try:
            # Handle single tick or multiple ticks
            if isinstance(ticks_data, list):
                for tick in ticks_data:
                    self.process_tick_data(tick)
            else:
                self.process_tick_data(ticks_data)
                
        except Exception as e:
            logger.error(f"Error processing ticks data: {e}")
            self._trigger_callbacks('error', {
                'type': 'tick_processing',
                'error': str(e),
                'timestamp': datetime.now()
            })
    
    def process_tick_data(self, tick_data):
        """Process individual tick data and trigger events"""
        try:
            # Store in buffer
            self.tick_buffer.append(tick_data)
            
            # Check if this is OHLC data or trade data
            if 'interval' in tick_data or 'open' in tick_data:
                self.process_ohlc_data(tick_data)
            else:
                self.process_trade_data(tick_data)
                
        except Exception as e:
            logger.error(f"Error processing individual tick: {e}")
            self._trigger_callbacks('error', {
                'type': 'tick_processing_individual',
                'error': str(e),
                'tick_data': tick_data,
                'timestamp': datetime.now()
            })
    
    def process_trade_data(self, trade_data):
        """Process trade data and trigger tick callbacks"""
        print("\n" + "="*50)
        print("📊 TICK TRADE DATA")
        print("="*50)
        
        # Store trade data
        self.trade_data.append(trade_data)
        
        # Update current symbol
        if 'stock_code' in trade_data:
            self.current_symbol = trade_data['stock_code']
        
        # Display trade information
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbol: {trade_data.get('stock_code', 'N/A')}")
        print(f"Last Price: ₹{trade_data.get('last', 0):.2f}")
        print(f"Volume: {trade_data.get('volume', 0)}")
        
        # Bid/Ask information
        if 'bid_price' in trade_data:
            print(f"Bid: ₹{trade_data.get('bid_price', 0):.2f} (Qty: {trade_data.get('bid_quantity', 0)})")
        if 'ask_price' in trade_data:
            print(f"Ask: ₹{trade_data.get('ask_price', 0):.2f} (Qty: {trade_data.get('ask_quantity', 0)})")
        
        # Change information
        if 'change_absolute' in trade_data:
            print(f"Change: {trade_data.get('change_absolute', 0):.2f}")
        if 'change_percentage' in trade_data:
            print(f"Change %: {trade_data.get('change_percentage', 0):.2f}%")
        
        print("-" * 30)
        
        # Enhanced data for callbacks
        enhanced_trade_data = {
            **trade_data,
            'event_type': 'tick',
            'processing_timestamp': datetime.now(),
            'buffer_size': len(self.tick_buffer),
            'symbol': self.current_symbol
        }
        
        # Trigger tick callbacks
        self._trigger_callbacks('tick', enhanced_trade_data)
    
    def process_ohlc_data(self, ohlc_data):
        """Process OHLC data and trigger OHLC callbacks"""
        print("\n" + "="*50)
        print("📈 OHLC DATA")
        print("="*50)
        
        # Create DataFrame entry
        new_data = pd.DataFrame([{
            'timestamp': datetime.now(),
            'open': ohlc_data.get('open', 0),
            'high': ohlc_data.get('high', 0),
            'low': ohlc_data.get('low', 0),
            'close': ohlc_data.get('close', 0),
            'volume': ohlc_data.get('volume', 0),
            'interval': ohlc_data.get('interval', '')
        }])
        
        # Update OHLCV data
        if self.ohlcv_data.empty:
            self.ohlcv_data = new_data
        else:
            self.ohlcv_data = pd.concat([self.ohlcv_data, new_data], ignore_index=True)
        
        # Store in buffer
        self.ohlcv_buffer.append(new_data.iloc[0].to_dict())
        
        # Update current symbol
        if 'stock_code' in ohlc_data:
            self.current_symbol = ohlc_data['stock_code']
        
        # Display OHLC information
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbol: {ohlc_data.get('stock_code', 'N/A')}")
        print(f"Interval: {ohlc_data.get('interval', 'N/A')}")
        print(f"Open: ₹{ohlc_data.get('open', 0):.2f}")
        print(f"High: ₹{ohlc_data.get('high', 0):.2f}")
        print(f"Low: ₹{ohlc_data.get('low', 0):.2f}")
        print(f"Close: ₹{ohlc_data.get('close', 0):.2f}")
        print(f"Volume: {ohlc_data.get('volume', 0):,}")
        
        print("-" * 30)
        
        # Enhanced data for callbacks
        enhanced_ohlc_data = {
            **ohlc_data,
            'event_type': 'ohlc',
            'processing_timestamp': datetime.now(),
            'buffer_size': len(self.ohlcv_buffer),
            'symbol': self.current_symbol,
            'dataframe_row': new_data.iloc[0].to_dict()
        }
        
        # Trigger OHLC callbacks
        self._trigger_callbacks('ohlc', enhanced_ohlc_data)
    
    def on_depth(self, depth_data):
        """
        Enhanced callback for depth data with event triggering
        """
        print("\n" + "="*50)
        print("🏛️  DEPTH OF MARKET")
        print("="*50)
        
        # Store DOM data
        self.dom_data = depth_data
        self.dom_buffer.append(depth_data)
        
        # Update current symbol
        if 'stock_code' in depth_data:
            self.current_symbol = depth_data['stock_code']
        
        # Display DOM information
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbol: {depth_data.get('stock_code', 'N/A')}")
        
        # Display bids
        print("\n🟢 BIDS (Buy Orders):")
        bids = depth_data.get('bids', [])
        if bids:
            for i, bid in enumerate(bids[:5]):  # Top 5 bids
                price = bid.get('price', 0)
                quantity = bid.get('quantity', 0)
                print(f"  {i+1}. Price: ₹{price:.2f} | Qty: {quantity:,}")
        else:
            print("  No bids available")
        
        # Display asks
        print("\n🔴 ASKS (Sell Orders):")
        asks = depth_data.get('asks', [])
        if asks:
            for i, ask in enumerate(asks[:5]):  # Top 5 asks
                price = ask.get('price', 0)
                quantity = ask.get('quantity', 0)
                print(f"  {i+1}. Price: ₹{price:.2f} | Qty: {quantity:,}")
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
        enhanced_dom_data = {
            **depth_data,
            'event_type': 'dom',
            'processing_timestamp': datetime.now(),
            'buffer_size': len(self.dom_buffer),
            'symbol': self.current_symbol,
            'best_bid': best_bid if bids else 0,
            'best_ask': best_ask if asks else 0,
            'spread': spread if bids and asks else 0
        }
        
        # Trigger DOM callbacks
        self._trigger_callbacks('dom', enhanced_dom_data)
    
    # ==========================================================================
    # UTILITY METHODS FOR STRATEGIES
    # ==========================================================================
    
    def get_recent_ohlcv(self, count: int = 10) -> pd.DataFrame:
        """
        Get recent OHLCV data for strategies
        
        Args:
            count: Number of recent bars to return
            
        Returns:
            DataFrame with recent OHLCV data
        """
        if len(self.ohlcv_buffer) == 0:
            return pd.DataFrame()
        
        recent_data = list(self.ohlcv_buffer)[-count:]
        return pd.DataFrame(recent_data)
    
    def get_recent_ticks(self, count: int = 100) -> List[Dict]:
        """
        Get recent tick data for strategies
        
        Args:
            count: Number of recent ticks to return
            
        Returns:
            List of recent tick data
        """
        return list(self.tick_buffer)[-count:]
    
    def get_recent_dom(self, count: int = 5) -> List[Dict]:
        """
        Get recent DOM snapshots for strategies
        
        Args:
            count: Number of recent DOM snapshots to return
            
        Returns:
            List of recent DOM data
        """
        return list(self.dom_buffer)[-count:]
    
    def get_callback_stats(self) -> Dict:
        """
        Get callback execution statistics
        
        Returns:
            Dictionary with callback statistics
        """
        return self.callback_stats.copy()
    
    def get_data_summary(self) -> Dict:
        """
        Get summary of current data state
        
        Returns:
            Dictionary with data summary
        """
        return {
            'ohlcv_records': len(self.ohlcv_buffer),
            'tick_records': len(self.tick_buffer),
            'dom_records': len(self.dom_buffer),
            'current_symbol': self.current_symbol,
            'callback_stats': self.get_callback_stats()
        }
    
    # ==========================================================================
    # EXISTING METHODS (minimal changes)
    # ==========================================================================
    
    def subscribe_stock(self, stock_code, exchange_code="NSE", product_type="cash", interval=""):
        """Subscribe to stock data as per Breeze documentation"""
        try:
            # Convert stock code to token if needed (Breeze uses tokens)
            stock_token = self.get_stock_token(stock_code, exchange_code)
            
            if interval:
                # Subscribe to OHLC data
                self.breeze.subscribe_feeds(
                    stock_token=stock_token,
                    exchange_code=exchange_code,
                    product_type=product_type,
                    interval=interval
                )
                logger.info(f"✅ Subscribed to OHLC data for {stock_code} (Interval: {interval})")
            else:
                # Subscribe to tick data (includes trades)
                self.breeze.subscribe_feeds(
                    stock_token=stock_token,
                    exchange_code=exchange_code,
                    product_type=product_type
                )
                logger.info(f"✅ Subscribed to tick data for {stock_code}")
                
        except Exception as e:
            logger.error(f"❌ Error subscribing to {stock_code}: {e}")
            self._trigger_callbacks('error', {
                'type': 'subscription_error',
                'error': str(e),
                'stock_code': stock_code,
                'timestamp': datetime.now()
            })
    
    def subscribe_stock_gpt(self, stock_symbol, exchange_code="NSE", product_type="cash", intervals=("1second", "1minute")):
        # """
        # Subscribe to live tick and OHLC feeds for a given stock using dynamic token lookup.

        # Args:
        #     stock_symbol (str): Stock symbol (e.g., "RELIANCE")
        #     exchange_code (str): Exchange code ("NSE", "BSE")
        #     product_type (str): Product type ("cash", "futures", etc.)
        #     intervals (tuple): OHLC intervals to subscribe to
        # """
        try:
            # ✅ Fetch instrument details dynamically
            instruments = self.breeze.get_instruments(exchange_code=exchange_code)

            instrument_info = next(
                (inst for inst in instruments if inst.get('stock_code') == stock_symbol),
                None
            )

            if not instrument_info:
                logger.error(f"❌ Could not find token for {stock_symbol} on {exchange_code}")
                return

            stock_token = instrument_info.get("stock_token")
            stock_code = instrument_info.get("stock_code")
            print(f"stock_token: {stock_token}, stock_code: {stock_code}")
            logger.info(f"✅ Found {stock_symbol} - Token: {stock_token}")

            # ✅ Subscribe to tick data
            self.breeze.subscribe_feeds(
                stock_code=stock_code,
                stock_token=stock_token,
                exchange_code=exchange_code,
                product_type=product_type
            )
            logger.info(f"✅ Subscribed to tick data for {stock_symbol}")

            # ✅ Subscribe to OHLC data for multiple intervals
            for interval in intervals:
                self.breeze.subscribe_feeds(
                    stock_code=stock_code,
                    stock_token=stock_token,
                    exchange_code=exchange_code,
                    product_type=product_type,
                    interval=interval
                )
                logger.info(f"✅ Subscribed to OHLC data for {stock_symbol} (Interval: {interval})")

            logger.info(f"✅ Completed subscriptions for {stock_symbol}")

        except Exception as e:
            logger.error(f"❌ Error subscribing {stock_symbol}: {e}")
            self._trigger_callbacks('error', {
                'type': 'subscription_error',
                'error': str(e),
                'symbol': stock_symbol,
                'timestamp': datetime.now()
            })

    
    def get_stock_token(self, stock_code, exchange_code="NSE"):
        """Get stock token from stock code"""
        token_map = {
            "NSE": {
                "RELIANCE": "1",
                "TCS": "2", 
                "INFY": "3",
                "RELIIND": "4",
                "HDFC": "5"
            }
        }
        
        token = token_map.get(exchange_code, {}).get(stock_code, stock_code)
        logger.debug(f"Token for {stock_code} on {exchange_code}: {token}")
        return token
    
    def subscribe_to_all_data_types(self, symbols, exchange_code="NSE", intervals=["1second", "1minute"]):
        """Subscribe to all data types for given symbols"""
        try:
            print("🚀 Subscribing to real-time data...")
            
            for symbol in symbols:
                # Subscribe to tick data (trade-by-trade)
                self.subscribe_stock_gpt(symbol, exchange_code, "cash", "")
                
                # Subscribe to OHLC data for different intervals
                for interval in intervals:
                    self.subscribe_stock_gpt(symbol, exchange_code, "cash", interval)
                
                logger.info(f"✅ Completed subscriptions for {symbol}")
                time.sleep(0.5)  # Small delay between symbols
            
            print("✅ All subscriptions completed!")
            
        except Exception as e:
            logger.error(f"Error in subscription process: {e}")
            self._trigger_callbacks('error', {
                'type': 'bulk_subscription_error',
                'error': str(e),
                'symbols': symbols,
                'timestamp': datetime.now()
            })
    
    def display_summary(self):
        """Display summary of collected data"""
        print("\n" + "="*60)
        print("📋 DATA SUMMARY")
        print("="*60)
        print(f"Trade Ticks: {len(self.trade_data)}")
        print(f"DOM Updates: {1 if self.dom_data else 0}")
        print(f"OHLCV Records: {len(self.ohlcv_data)}")
        
        # Callback statistics
        stats = self.get_callback_stats()
        print(f"\n📊 CALLBACK STATISTICS:")
        print(f"  OHLC Callbacks: {stats['ohlc_calls']}")
        print(f"  Tick Callbacks: {stats['tick_calls']}")
        print(f"  DOM Callbacks: {stats['dom_calls']}")
        print(f"  Callback Errors: {stats['errors']}")
        
        if not self.ohlcv_data.empty:
            latest_ohlcv = self.ohlcv_data.iloc[-1]
            print(f"\nLatest OHLCV:")
            print(f"  O: ₹{latest_ohlcv['open']:.2f} | H: ₹{latest_ohlcv['high']:.2f}")
            print(f"  L: ₹{latest_ohlcv['low']:.2f} | C: ₹{latest_ohlcv['close']:.2f}")
            print(f"  Volume: {latest_ohlcv['volume']:,}")
            print(f"  Interval: {latest_ohlcv.get('interval', 'N/A')}")
        
        print("="*60)
    
    def start_streaming(self, symbols, exchange_code="NSE"):
        """Start streaming data for given symbols"""
        try:
            print("🚀 Starting ICICI Direct Breeze Real-time Data Stream...")
            print("Press Ctrl+C to stop\n")
            
            # Subscribe to all data types
            self.subscribe_to_all_data_types(symbols, exchange_code)
            
            print("\n📡 Waiting for real-time data...")
            print("Data will appear below as it arrives:\n")
            
            # Keep the connection alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping data stream...")
            self.display_summary()
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
            self._trigger_callbacks('error', {
                'type': 'streaming_error',
                'error': str(e),
                'timestamp': datetime.now()
            })

# ==============================================================================
# DEMONSTRATION: How to use the enhanced DataLiveStream with callbacks
# ==============================================================================

def demo_ohlc_callback(ohlc_data):
    """Example OHLC callback for demonstration"""
    print(f"🔔 OHLC CALLBACK: {ohlc_data.get('stock_code', 'Unknown')} "
          f"Close: ₹{ohlc_data.get('close', 0):.2f} "
          f"Volume: {ohlc_data.get('volume', 0):,}")

def demo_tick_callback(tick_data):
    """Example Tick callback for demonstration"""
    print(f"🔔 TICK CALLBACK: {tick_data.get('stock_code', 'Unknown')} "
          f"Price: ₹{tick_data.get('last', 0):.2f} "
          f"Volume: {tick_data.get('volume', 0)}")

def demo_dom_callback(dom_data):
    """Example DOM callback for demonstration"""
    print(f"🔔 DOM CALLBACK: {dom_data.get('stock_code', 'Unknown')} "
          f"Bid: ₹{dom_data.get('best_bid', 0):.2f} "
          f"Ask: ₹{dom_data.get('best_ask', 0):.2f}")

def demo_error_callback(error_data):
    """Example Error callback for demonstration"""
    print(f"🚨 ERROR CALLBACK: {error_data.get('type', 'Unknown')} - {error_data.get('error', 'No details')}")

def main():
    """Main function with callback demonstration"""
    try:
        # Initialize Breeze connection
        breeze = initialize_breeze()
        
        # Initialize enhanced data display
        data_display = ICICIBreezeDataDisplay(breeze)
        
        # Register demonstration callbacks
        data_display.register_ohlc_callback(demo_ohlc_callback)
        data_display.register_tick_callback(demo_tick_callback)
        data_display.register_dom_callback(demo_dom_callback)
        data_display.register_error_callback(demo_error_callback)
        
        # Symbols to monitor
        symbols = ["RELIND", "TVSMOT"]
        
        # Start streaming
        data_display.start_streaming(symbols)
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print("\n❌ Please check your .env file with proper credentials")
    except Exception as e:
        logger.error(f"Main execution error: {e}")

if __name__ == "__main__":
    main()