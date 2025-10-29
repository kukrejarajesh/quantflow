# generate_access_token.py
from kiteconnect import KiteConnect
import webbrowser

# Step 2.1: Initialize with API Key
api_key = "your_api_key_here"
kite = KiteConnect(api_key="aetakpl518dwhg5l")

# Step 2.2: Generate login URL
print("Opening browser for login...")
login_url = kite.login_url()
webbrowser.open(login_url)

# Step 2.3: Get request token from redirect URL
# After login, you'll be redirected to your callback URL
# Extract request_token from that URL
request_token = input("Enter the request token from URL: ")

# Step 2.4: Generate session and access token

#data = kite.generate_session(request_token, api_secret="haeb4dodlolylu9ignme4fbqa9l80o3e")
data = kite.generate_session(request_token, api_secret="hsctl0kpfcqqhf7jka82wk30itxl2q1p")

access_token = data["access_token"]

print(f"Access Token: {access_token}")
print("Save this access token in your .env file")