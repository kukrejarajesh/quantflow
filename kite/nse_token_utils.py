import requests
import pandas as pd

def get_nse_index_tokens(kite, index_name="NIFTY 200"):
    """
    Fetch list of stock tokens for a given NSE index (e.g. NIFTY 50, NIFTY 100, NIFTY 200).
    """
    url = f"https://www.nseindia.com/api/equity-stockIndices?index={index_name.replace(' ', '%20')}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }

    try:
        session = requests.Session()
        # Make initial request to set cookies
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()  # <– this is where the previous error happened

        # Extract list of symbols from response
        stocks = [item["symbol"] for item in data["data"] if "symbol" in item]
        instruments = kite.instruments("NSE")
        df = pd.DataFrame(instruments)

        df_filtered = df[df["tradingsymbol"].isin(stocks)]
        tokens = df_filtered["instrument_token"].tolist()

        print(f"✅ Found {len(tokens)} tokens for {index_name}")
        return tokens

    except requests.exceptions.JSONDecodeError:
        print("❌ Error: NSE returned non-JSON response (likely blocked). Try again after a few seconds.")
        return []
    except Exception as e:
        print(f"❌ Error fetching {index_name} list from NSE: {e}")
        return []
