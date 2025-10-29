import pandas as pd
import vectorbt as vbt
from breeze_connect import BreezeConnect
import json
import logging
from datetime import datetime
import time
import os
from dotenv import load_dotenv


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ICICI_STOCK_TOKENS = {
    "NSE": {
        "TVSMOT":"11648",
        "RELIANCE": "2885",
        "TCS": "11536",
        "INFY": "1594", 
        "HDFCBANK": "1333",
        "HDFC": "1333",  # Sometimes same as HDFCBANK
        "ICICIBANK": "4963",
        "KOTAKBANK": "1922",
        "SBIN": "3045",
        "BHARTIARTL": "10604",
        "ITC": "1660",
        "LT": "18595",
        "AXISBANK": "5900",
        "ASIANPAINT": "1363",
        "MARUTI": "10999",
        "SUNPHARMA": "857",
        "TITAN": "3506",
        "WIPRO": "3787",
        "NESTLEIND": "17963",
        "ULTRACEMCO": "11532",
        "ONGC": "174",
        "POWERGRID": "383",
        "NTPC": "11630",
        "HINDUNILVR": "1394",
        "BAJFINANCE": "8110",
        "HCLTECH": "7229",
        "TECHM": "10825",
        "TATAMOTORS": "3456",
        "SBILIFE": "11039",
        "DRREDDY": "881",
        "CIPLA": "5990",
        "TATASTEEL": "8954",
        "BRITANNIA": "547",
        "ADANIPORTS": "15083",
        "INDUSINDBK": "5258",
        "BAJAJFINSV": "8100",
        "EICHERMOT": "910",
        "GRASIM": "1232",
        "HDFCLIFE": "11915",
        "DIVISLAB": "10990",
        "UPL": "11287",
        "HEROMOTOCO": "685",
        "SHREECEM": "3103",
        "APOLLOHOSP": "157",
        "BAJAJ-AUTO": "207",
        "COALINDIA": "5210",
        "JSWSTEEL": "11723",
        "TATACONSUM": "11483",
        "BPCL": "6393",
        "HINDALCO": "1360",
        "VEDL": "958",
        "GODREJCP": "17818"
    }
}
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




def initialize_breeze(api_key=None, api_secret=None, session_token=None):
    """
    Initialize Breeze API connection
    
    Args:
        api_key: ICICI API Key (optional, will use env vars if not provided)
        api_secret: ICICI Secret Key (optional, will use env vars if not provided) 
        session_token: ICICI Session Token (optional, will use env vars if not provided)
    
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
        session_token = session_token or os.getenv('BREEZE_ACCESS_TOKEN')
        
        if not all([api_key, api_secret, session_token]):
            raise ValueError(
                "Missing required credentials. Please provide them as arguments "
                "or set environment variables: ICICI_API_KEY, ICICI_SECRET_KEY, ICICI_SESSION_TOKEN"
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
        
        # Set up WebSocket connection and callbacks
        self.setup_websocket()
        
    def setup_websocket(self):
        """Set up WebSocket connection and callbacks as per Breeze documentation"""
        try:
            
            # Assign callbacks as per documentation
            self.breeze.on_ticks = self.on_ticks
            self.breeze.on_depth = self.on_depth
            # Connect to WebSocket
            self.breeze.ws_connect()
            
            
            
            # Note: OHLC data typically comes through on_ticks callback in Breeze
            
            logger.info("✅ WebSocket connection established with callbacks")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup WebSocket: {e}")
            raise
    
    
    def subscribe_stock_static(self, stock_symbol, exchange_code="NSE", product_type="cash", intervals=("1second", "1minute")):
        """
        Subscribe using reliable static token mapping.
        Args:
            stock_symbol (str or list): Stock symbol(s) to subscribe.
            exchange_code (str): Exchange code ("NSE", "BSE", etc.).
            product_type (str): Product type ("cash", "futures", etc.).
            intervals (tuple): OHLC intervals to subscribe to.
        Returns:
            dict: Success and failed subscriptions.
        """
        try:
            if isinstance(stock_symbol, str):
                stock_symbols = [stock_symbol]
            else:
                stock_symbols = stock_symbol

            successful_subscriptions = []
            failed_subscriptions = []

            # Ensure intervals is a list/tuple
            if isinstance(intervals, str):
                intervals = [intervals]

            for symbol in stock_symbols:
                # Clean the symbol
                clean_symbol = symbol.upper().strip()

                # Get token from mapping
                token_map = ICICI_STOCK_TOKENS.get(exchange_code, {})
                stock_token = token_map.get(clean_symbol)

                segment_code = "4.1" if exchange_code == "NSE" else "4.2"
                stock_token = f"{segment_code}!{stock_token}" if stock_token else None

                if not stock_token:
                    logger.error(f"❌ Token not found for {clean_symbol} on {exchange_code}")
                    failed_subscriptions.append(clean_symbol)
                    continue

                try:
                    #self.breeze.subscribe_feeds(stock_token="4.1!2885")
                    print(f"Subscribed to 4.1!2885")
                    # Subscribe to tick data (no interval)
                    self.breeze.subscribe_feeds(
                        stock_code=clean_symbol,
                        stock_token=stock_token,
                        exchange_code=exchange_code,
                        product_type=product_type,
                        interval=""
                        # get_exchange_quotes=True, 
                        # get_market_depth=False
                    )
                    logger.info(f"✅ Subscribed to tick data for {clean_symbol}")
                    
                     # ✅ Subscribe to market depth data
                    self.breeze.subscribe_feeds(
                        stock_code=clean_symbol,
                        stock_token=stock_token,
                        exchange_code=exchange_code,
                        product_type=product_type,
                        interval="",  # Market depth doesn't use interval
                        get_market_depth=True  # ✅ THIS IS THE KEY PARAMETER
                    )
                    logger.info(f"✅ Subscribed to market depth for {clean_symbol}")

                    # Subscribe to OHLC intervals
                    for interval in intervals:
                        self.breeze.subscribe_feeds(
                            stock_code=clean_symbol,
                            stock_token=stock_token,
                            exchange_code=exchange_code,
                            product_type=product_type,
                            interval=interval
                        )
                        logger.info(f"✅ Subscribed to {interval} data for {clean_symbol}")

                    successful_subscriptions.append(clean_symbol)
                    #successful_subscriptions.append("4.1!2885")
                    

                except Exception as e:
                    logger.error(f"❌ Subscription failed for {clean_symbol}: {e}")
                    failed_subscriptions.append(clean_symbol)

            return {
                'success': successful_subscriptions,
                'failed': failed_subscriptions
            }

        except Exception as e:
            logger.error(f"❌ Error in subscription: {e}")
            return {'success': [], 'failed': stock_symbols}
    
    def get_stock_token(self, stock_code, exchange_code="NSE"):
        """
        Get stock token from stock code.
        In production, you might want to maintain a mapping of stock codes to tokens.
        """
        # This is a simplified mapping - you should maintain a proper token mapping
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

    def on_ticks(self, ticks_data):
        """
        Callback for tick data as per Breeze documentation
        This receives both trade data and OHLC data
        """
        print(f"✅ TICK DATA RECEIVED: {datetime.now()}") 
        try:
            
            print(f"Raw ticks data: {ticks_data}")
            # Handle single tick or multiple ticks
            if isinstance(ticks_data, list):
                for tick in ticks_data:
                    self.process_tick_data(tick)
            else:
                self.process_tick_data(ticks_data)
                
        except Exception as e:
            logger.error(f"Error processing ticks data: {e}")

    def process_tick_data(self, tick_data):
        """Process individual tick data"""
        try:
            # Check if this is OHLC data or trade data
            if 'interval' in tick_data or 'open' in tick_data:
                self.process_ohlc_data(tick_data)
            else:
                self.process_trade_data(tick_data)
                
        except Exception as e:
            logger.error(f"Error processing individual tick: {e}")

    def process_trade_data(self, trade_data):
        """Process trade data (tick-by-tick)"""
        print("\n" + "="*50)
        print("📊 TICK TRADE DATA")
        print("="*50)
        
        # Store trade data
        self.trade_data.append(trade_data)
        
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

    def process_ohlc_data(self, ohlc_data):
        """Process OHLC data"""
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

    def on_depth(self, depth_data):
        """
        Callback for depth data as per Breeze documentation
        """
        print("\n" + "="*50)
        print("🏛️  DEPTH OF MARKET")
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

    def subscribe_to_all_data_types(self, symbols, exchange_code="NSE", intervals=["1second"]):
        """
        Subscribe to all data types for given symbols
        
        Args:
            symbols: List of stock symbols
            exchange_code: Exchange code
            intervals: List of intervals for OHLC data
        """
        try:
            print("🚀 Subscribing to real-time data...")
            
            for symbol in symbols:
                # Subscribe to tick data (trade-by-trade)
                self.subscribe_stock_static(symbol, exchange_code, "cash", "")
                
                # Subscribe to OHLC data for different intervals
                for interval in intervals:
                    self.subscribe_stock_static(symbol, exchange_code, "cash", interval)
                
                logger.info(f"✅ Completed subscriptions for {symbol}")
                time.sleep(0.5)  # Small delay between symbols
            
            print("✅ All subscriptions completed!")
            
        except Exception as e:
            logger.error(f"Error in subscription process: {e}")

    def display_summary(self):
        """Display summary of collected data"""
        print("\n" + "="*60)
        print("📋 DATA SUMMARY")
        print("="*60)
        print(f"Trade Ticks: {len(self.trade_data)}")
        print(f"DOM Updates: {1 if self.dom_data else 0}")
        print(f"OHLCV Records: {len(self.ohlcv_data)}")
        
        if not self.ohlcv_data.empty:
            latest_ohlcv = self.ohlcv_data.iloc[-1]
            print(f"\nLatest OHLCV:")
            print(f"  O: ₹{latest_ohlcv['open']:.2f} | H: ₹{latest_ohlcv['high']:.2f}")
            print(f"  L: ₹{latest_ohlcv['low']:.2f} | C: ₹{latest_ohlcv['close']:.2f}")
            print(f"  Volume: {latest_ohlcv['volume']:,}")
            print(f"  Interval: {latest_ohlcv.get('interval', 'N/A')}")
        
        print("="*60)

    def start_streaming(self, symbols, exchange_code="NSE"):
        """
        Start streaming data for given symbols
        
        Args:
            symbols: List of stock symbols to monitor
            exchange_code: Exchange code (NSE, BSE, etc.)
        """
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

def main():
    """Main function"""
    try:
        # Initialize Breeze connection
        breeze = initialize_breeze()
        
        # Initialize data display
        data_display = ICICIBreezeDataDisplay(breeze)
        
        # Symbols to monitor (update with actual symbols you want to track)
        symbols = ["TCS","TVSMOT"]
        
        # Start streaming
        data_display.start_streaming(symbols)
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print("\n❌ Please create a .env file with your ICICI Direct credentials:")
        print("""
ICICI_API_KEY=your_api_key_here
ICICI_SECRET_KEY=your_secret_key_here  
ICICI_SESSION_TOKEN=your_session_token_here
        """)
    except Exception as e:
        logger.error(f"Main execution error: {e}")

if __name__ == "__main__":
    main()