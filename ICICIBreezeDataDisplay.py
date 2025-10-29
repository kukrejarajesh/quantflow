import pandas as pd
import vectorbt as vbt
from breeze_connect import BreezeConnect
import json
import logging
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv()

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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)




def initialize_breeze(api_key=None, api_secret=None, access_token=None):
    """
    Initialize Breeze API connection
    
    Args:
        api_key: ICICI API Key (optional, will use env vars if not provided)
        api_secret: ICICI Secret Key (optional, will use env vars if not provided) 
        access_token: ICICI Session Token (optional, will use env vars if not provided)
    
    Returns:
        BreezeConnect: Initialized Breeze connection object
    
    Raises:
        ValueError: If credentials are missing
        Exception: If connection fails
    """
    try:
        # Use provided credentials or fall back to environment variables
        api_key = api_key or os.getenv('BREEZE_API_KEY')
        api_secret = api_secret or os.getenv('BREEZE_API_SECRET')
        access_token = access_token or os.getenv('BREEZE_ACCESS_TOKEN')
        
        print("api_key:", api_key)
        print("token:", access_token)
        
        if not all([api_key, api_secret, access_token]):
            raise ValueError(
                "Missing required credentials. Please provide them as arguments "
                "or set environment variables: ICICI_API_KEY, ICICI_SECRET_KEY, ICICI_SESSION_TOKEN"
            )
        
        logger.info("Initializing Breeze connection...")
        breeze = BreezeConnect(api_key=api_key)
        
        logger.info("Generating session...")
        breeze.generate_session(api_secret=api_secret, session_token=access_token)
        
        logger.info("✅ Breeze connection established successfully!")
        return breeze
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Breeze connection: {e}")
        raise

class ICICIBreezeDataDisplay:
    def __init__(self, breeze_connection=None):
        """
        Initialize data display with Breeze connection
        
        Args:
            breeze_connection: Pre-initialized Breeze connection (optional)
        """
        # Use provided connection or create new one
        if breeze_connection:
            self.breeze = breeze_connection
            logger.info("Using provided Breeze connection")
        else:
            self.breeze = initialize_breeze()
        
        # Data storage
        self.ohlcv_data = pd.DataFrame()
        self.trade_data = []
        self.dom_data = {}
        
        # Set up callbacks
        self.setup_callbacks()
        
    def setup_callbacks(self):
        """Set up WebSocket callbacks"""
        self.breeze.on_ticks = self.on_ticks
        self.breeze.on_depth = self.on_depth
        self.breeze.on_ohlc = self.on_ohlc
        
    def on_ticks(self, tick_data):
        """Handle tick trade data with aggressor information"""
        try:
            print("\n" + "="*50)
            print("📊 TICK TRADE DATA UPDATE")
            print("="*50)
            
            # Store tick data
            self.trade_data.append(tick_data)
            
            # Display tick information
            print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Symbol: {tick_data.get('stock_code', 'N/A')}")
            print(f"Price: ₹{tick_data.get('last', 0):.2f}")
            print(f"Volume: {tick_data.get('volume', 0)}")
            print(f"Change: {tick_data.get('change_absolute', 0):.2f}")
            print(f"Change %: {tick_data.get('change_percentage', 0):.2f}%")
            
            # Aggressor information (buy/sell)
            if 'trade_volume' in tick_data:
                print(f"Trade Volume: {tick_data['trade_volume']}")
            
            print("-" * 30)
            
        except Exception as e:
            logger.error(f"Error processing tick data: {e}")

    def on_ohlc(self, ohlc_data):
        """Handle OHLCV data"""
        try:
            print("\n" + "="*50)
            print("📈 OHLCV DATA UPDATE")
            print("="*50)
            
            # Create DataFrame from OHLC data
            new_data = pd.DataFrame([{
                'timestamp': datetime.now(),
                'open': ohlc_data.get('open', 0),
                'high': ohlc_data.get('high', 0),
                'low': ohlc_data.get('low', 0),
                'close': ohlc_data.get('close', 0),
                'volume': ohlc_data.get('volume', 0)
            }])
            
            # Update OHLCV data
            if self.ohlcv_data.empty:
                self.ohlcv_data = new_data
            else:
                self.ohlcv_data = pd.concat([self.ohlcv_data, new_data], ignore_index=True)
            
            # Display OHLCV information
            print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Symbol: {ohlc_data.get('stock_code', 'N/A')}")
            print(f"Open: ₹{ohlc_data.get('open', 0):.2f}")
            print(f"High: ₹{ohlc_data.get('high', 0):.2f}")
            print(f"Low: ₹{ohlc_data.get('low', 0):.2f}")
            print(f"Close: ₹{ohlc_data.get('close', 0):.2f}")
            print(f"Volume: {ohlc_data.get('volume', 0):,}")
            
            # Calculate basic statistics if we have enough data
            if len(self.ohlcv_data) > 1:
                current_close = ohlc_data.get('close', 0)
                prev_close = self.ohlcv_data.iloc[-2]['close'] if len(self.ohlcv_data) > 1 else current_close
                change = current_close - prev_close
                change_pct = (change / prev_close) * 100 if prev_close != 0 else 0
                
                print(f"Change: {change:+.2f} ({change_pct:+.2f}%)")
            
            print("-" * 30)
            
        except Exception as e:
            logger.error(f"Error processing OHLC data: {e}")

    def on_depth(self, depth_data):
        """Handle Depth of Market (DOM) data"""
        try:
            print("\n" + "="*50)
            print("🏛️  DEPTH OF MARKET UPDATE")
            print("="*50)
            
            # Store DOM data
            self.dom_data = depth_data
            
            # Display DOM information
            print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Symbol: {depth_data.get('stock_code', 'N/A')}")
            
            # Display bids
            print("\n🟢 BIDS (Buy Orders):")
            bids = depth_data.get('bids', [])
            if bids:
                for i, bid in enumerate(bids[:5]):
                    print(f"  {i+1}. Price: ₹{bid.get('price', 0):.2f} | Qty: {bid.get('quantity', 0):,}")
            else:
                print("  No bids available")
            
            # Display asks
            print("\n🔴 ASKS (Sell Orders):")
            asks = depth_data.get('asks', [])
            if asks:
                for i, ask in enumerate(asks[:5]):
                    print(f"  {i+1}. Price: ₹{ask.get('price', 0):.2f} | Qty: {ask.get('quantity', 0):,}")
            else:
                print("  No asks available")
            
            # Calculate spread
            if bids and asks:
                best_bid = bids[0].get('price', 0)
                best_ask = asks[0].get('price', 0)
                spread = best_ask - best_bid
                spread_pct = (spread / best_bid) * 100 if best_bid != 0 else 0
                
                print(f"\n💰 Spread: ₹{spread:.2f} ({spread_pct:.2f}%)")
            
            print("-" * 30)
            
        except Exception as e:
            logger.error(f"Error processing DOM data: {e}")

    def subscribe_to_symbol(self, symbol, exchange_code="NSE"):
        """Subscribe to a symbol for real-time data"""
        try:
            self.breeze.subscribe_feeds(
                stock_token=symbol,
                exchange_code=exchange_code,
                product_type="cash",
                feed_type=["tick", "depth", "ohlc"]
            )
            logger.info(f"✅ Subscribed to {symbol} on {exchange_code}")
            
        except Exception as e:
            logger.error(f"❌ Error subscribing to {symbol}: {e}")

    def display_summary(self):
        """Display summary of collected data"""
        print("\n" + "="*60)
        print("📋 DATA SUMMARY")
        print("="*60)
        print(f"OHLCV Records: {len(self.ohlcv_data)}")
        print(f"Trade Ticks: {len(self.trade_data)}")
        print(f"DOM Updates: {len([k for k in self.dom_data.keys() if k])}")
        
        if not self.ohlcv_data.empty:
            latest_ohlcv = self.ohlcv_data.iloc[-1]
            print(f"\nLatest OHLCV:")
            print(f"  O: ₹{latest_ohlcv['open']:.2f} | H: ₹{latest_ohlcv['high']:.2f}")
            print(f"  L: ₹{latest_ohlcv['low']:.2f} | C: ₹{latest_ohlcv['close']:.2f}")
            print(f"  Volume: {latest_ohlcv['volume']:,}")
        
        print("="*60)

    def start_streaming(self, symbols):
        """Start streaming data for given symbols"""
        try:
            print("🚀 Starting ICICI Direct Breeze Data Stream...")
            print("Press Ctrl+C to stop\n")
            
            for symbol in symbols:
                self.subscribe_to_symbol(symbol)
                time.sleep(1)
            
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping data stream...")
            self.display_summary()
        except Exception as e:
            logger.error(f"Error in streaming: {e}")

# Example usage patterns
def main():
    """Main function with different usage examples"""
    
    # Example 1: Initialize with environment variables (simplest)
    print("=== Example 1: Using Environment Variables ===")
    try:
        breeze = initialize_breeze()  # Uses env vars automatically
        data_display = ICICIBreezeDataDisplay(breeze)
        data_display.start_streaming(["RELIND", "TVSMOT"])
    except Exception as e:
        logger.error(f"Example 1 failed: {e}")
    
    # Example 2: Initialize with explicit credentials
    print("\n=== Example 2: Using Explicit Credentials ===")
    try:
        breeze = initialize_breeze(
            api_key="your_api_key",
            api_secret="your_secret", 
            access_token="your_token"
        )
        data_display = ICICIBreezeDataDisplay(breeze)
        data_display.start_streaming(["INFY"])
    except Exception as e:
        logger.error(f"Example 2 failed: {e}")
    
    # Example 3: Let the class handle initialization
    print("\n=== Example 3: Class Handles Initialization ===")
    try:
        data_display = ICICIBreezeDataDisplay()  # No connection provided
        data_display.start_streaming(["HDFC"])
    except Exception as e:
        logger.error(f"Example 3 failed: {e}")

# Utility function to test connection
def test_breeze_connection():
    """Test if Breeze connection works"""
    try:
        breeze = initialize_breeze()
        print("✅ Connection test successful!")
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

if __name__ == "__main__":
    # Test connection first
    if test_breeze_connection():
        main()
    else:
        print("\nPlease check your credentials in the .env file:")
        print("""
ICICI_API_KEY=your_api_key_here
ICICI_SECRET_KEY=your_secret_key_here  
ICICI_SESSION_TOKEN=your_session_token_here
        """)