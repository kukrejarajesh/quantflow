# generate_access_token.py
from kiteconnect import KiteConnect
import webbrowser
from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), "../../config\.env")
print(f"Loading .env from: {env_path}")
# Load .env file from that path
load_dotenv(dotenv_path=env_path)

api_key=os.getenv("ZERODHA_API_KEY")
api_secret=os.getenv("ZERODHA_API_SECRET")
access_token=os.getenv("ZERODHA_ACCESS_TOKEN")
print(f"Loaded API Key: {api_key}")
# Step 2.1: Initialize with API Key

kite = KiteConnect(api_key=api_key)

# Step 2.2: Generate login URL
print("Opening browser for login...")
login_url = kite.login_url()
webbrowser.open(login_url)

# Step 2.3: Get request token from redirect URL
# After login, you'll be redirected to your callback URL
# Extract request_token from that URL
request_token = input("Enter the request token from URL: ")

# Step 2.4: Generate session and access token


data = kite.generate_session(request_token, api_secret=api_secret)

access_token = data["access_token"]

print(f"Access Token: {access_token}")
print("Save this access token in your .env file")