# basic_connection.py
import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker
import logging
from nse_token_utils import get_nse_index_tokens
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    print("4. Loading environment variables...")
    env_path = os.path.join(os.path.dirname(__file__), '..', '..','config', '.env')
    print(f"Looking for .env at: {env_path}")
    
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print("✅ Environment variables loaded from config folder")
    else:
        load_dotenv()
        print("✅ Environment variables loaded from default location")
except Exception as e:
    print(f"❌ Error loading environment variables: {e}")

def setup_zerodha():
    """Step 3.1: Load credentials and initialize KiteConnect"""
    load_dotenv()
    
    api_key = os.getenv('ZERODHA_API_KEY')
    access_token = os.getenv('ZERODHA_ACCESS_TOKEN')
    
    if not api_key or not access_token:
        raise ValueError("Missing API credentials in .env file")
    
    # Initialize KiteConnect for REST API
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    # Initialize KiteTicker for WebSocket
    kite_ticker = KiteTicker(api_key, access_token)
    
    logger.info("✅ Zerodha connection initialized successfully!")
    return kite, kite_ticker

def get_instrument_token(kite, symbol, exchange="NSE"):
    """Step 3.2: Get instrument token for a symbol"""
    instruments = kite.instruments(exchange)
    
    for instrument in instruments:
        if instrument['tradingsymbol'] == symbol:
            logger.info(f"✅ Found {symbol}: Token {instrument['instrument_token']}")
            return instrument['instrument_token']
    
    raise ValueError(f"Instrument {symbol} not found on {exchange}")

# Test the connection
if __name__ == "__main__":
    try:
        kite, kite_ticker = setup_zerodha()
        
        # Get profile to verify connection
        profile = kite.profile()
        print(f"✅ Connected as: {profile['user_name']}")
        print(f"📧 Email: {profile['email']}")
        
        # Get instrument token for RELIANCE
        reliance_token = get_instrument_token(kite, "HDFCBANK")
        print(f"🔢 RELIANCE Token: {reliance_token}")
        
        exchange="NSE"
        print(f"Fetching full instrument list for {exchange}...")
        
        # 2. Get the full list of instruments as a CSV dump (in bytes)
        # The instruments() method returns a large CSV file.
        instruments_list = kite.instruments(exchange=exchange)
        df_full = pd.DataFrame(instruments_list)
        output_filename = f"{exchange}_CashEquities_Tokens.csv"
        df_full.to_csv(output_filename, index=False)
        print(f"✅ Full instrument list saved to {output_filename}")
        tokens_list = get_nse_index_tokens(kite, index_name="NIFTY 100")
        nsetoken_filename = f"NSE_NIFTY100_Tokens.csv"
        pd.DataFrame(tokens_list, columns=['instrument_token']).to_csv(nsetoken_filename, index=False)
        print(tokens_list[:5])
    except Exception as e:
        print(f"❌ Connection failed: {e}")