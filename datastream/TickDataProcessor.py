import json
import pandas as pd
from typing import Dict, Any, List

class TickDataProcessor:
    """
    A processor class for real-time tick data streams.

    It calculates Volume Delta and Cumulative Volume Delta (CVD) based on the
    order book imbalance (Total Buy Quantity - Total Sell Quantity) and flattens
    the complex nested tick structure into a flat dictionary suitable for
    DataFrame conversion or database storage.
    """

    def __init__(self, initial_cvd: float = 0.0):
        """
        Initializes the processor with a starting CVD value.

        Args:
            initial_cvd (float): The starting value for Cumulative Volume Delta.
        """
        # State variable to track the running CVD
        self._cvd = initial_cvd
        print(f"Processor initialized. Initial CVD: {self._cvd}")

    def _flatten_depth(self, depth: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Flattens the nested 'depth' structure, extracting key metrics.

        We extract the top 5 levels of Bid (Buy) and Ask (Sell) data.

        Args:
            depth (dict): The 'depth' part of the tick data.

        Returns:
            dict: A flattened dictionary containing top 5 bid/ask metrics.
        """
        flat_data = {}
        
        # Process Bid (Buy) Side
        for i, entry in enumerate(depth.get('buy', [])[:5]):
            prefix = f'bid_{i+1}'
            flat_data[f'{prefix}_qty'] = entry.get('quantity', 0)
            flat_data[f'{prefix}_price'] = entry.get('price', 0.0)
            flat_data[f'{prefix}_orders'] = entry.get('orders', 0)

        # Process Ask (Sell) Side
        for i, entry in enumerate(depth.get('sell', [])[:5]):
            prefix = f'ask_{i+1}'
            flat_data[f'{prefix}_qty'] = entry.get('quantity', 0)
            flat_data[f'{prefix}_price'] = entry.get('price', 0.0)
            flat_data[f'{prefix}_orders'] = entry.get('orders', 0)
            
        return flat_data

    def _calculate_metrics(self, tick: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculates Volume Delta and updates the Cumulative Volume Delta (CVD).

        Volume Delta is calculated as the order book imbalance:
        (Total Buy Quantity - Total Sell Quantity).

        Args:
            tick (dict): The raw, incoming tick data.

        Returns:
            dict: Dictionary containing the calculated 'volume_delta' and 'cvd'.
        """
        total_buy = tick.get('total_buy_quantity', 0)
        total_sell = tick.get('total_sell_quantity', 0)
        
        # 1. Volume Delta (Order Book Imbalance)
        volume_delta = total_buy - total_sell
        
        # 2. Cumulative Volume Delta (CVD)
        # Note: In a real-world streaming scenario, volume_delta would typically
        # be based on the instantaneous *trade* volume difference, but using the 
        # total book imbalance is a common proxy for market pressure in book-snapshot data.
        self._cvd += volume_delta
        
        return {
            'volume_delta': volume_delta,
            'cvd': self._cvd
        }

    def process_tick(self, tick: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a single raw tick dictionary to produce a flat, enriched record.

        Args:
            tick (dict): The incoming raw tick data dictionary.

        Returns:
            dict: A single, flat dictionary containing all data points, delta, and CVD.
        """
        # 1. Calculate Delta and CVD
        metrics = self._calculate_metrics(tick)

        # 2. Flatten the nested 'depth' field
        depth_data = self._flatten_depth(tick.get('depth', {}))

        # 3. Flatten the 'ohlc' field
        ohlc_data = tick.get('ohlc', {})
        
        # 4. Construct the final flat tick record
        flat_tick = {
            'instrument_token': tick.get('instrument_token'),
            'exchange_timestamp': tick.get('exchange_timestamp'),
            'last_price': tick.get('last_price'),
            'last_traded_quantity': tick.get('last_traded_quantity'),
            'volume_traded': tick.get('volume_traded'),
            'total_buy_quantity': tick.get('total_buy_quantity'),
            'total_sell_quantity': tick.get('total_sell_quantity'),
            'ohlc_open': ohlc_data.get('open'),
            'ohlc_high': ohlc_data.get('high'),
            'ohlc_low': ohlc_data.get('low'),
            'ohlc_close': ohlc_data.get('close'),
            **metrics,        # Add calculated delta and CVD
            **depth_data       # Add flattened depth data
        }

        # Remove the nested fields if they exist to ensure data is flat
        for key in ['depth', 'ohlc', 'last_trade_time', 'tradable', 'mode', 'change', 'oi', 'oi_day_high', 'oi_day_low']:
            if key in flat_tick:
                del flat_tick[key]

        return flat_tick

# --- Demonstration and Example Usage ---

# 1. Load the sample tick data from the file content
# NOTE: The provided content is a snippet, so we'll wrap it into a list for
# sequential processing demonstration. In a real stream, this would be an event.

# We will simulate three consecutive ticks:
sample_tick_1 = {
  "instrument_token": 738561,
  "last_price": 1382.9,
  "last_traded_quantity": 25,
  "volume_traded": 2803229,
  "total_buy_quantity": 1279736,
  "total_sell_quantity": 601864,
  "ohlc": {"open": 1376.0, "high": 1390.7, "low": 1375.9, "close": 1375.0},
  "exchange_timestamp": "2025-10-07 11:10:07",
  "depth": {
    "buy": [
      {"quantity": 902, "price": 1382.9, "orders": 18},
      {"quantity": 102, "price": 1382.8, "orders": 4}
    ],
    "sell": [
      {"quantity": 72, "price": 1383.0, "orders": 6},
      {"quantity": 102, "price": 1383.1, "orders": 4}
    ]
  }
}

sample_tick_2 = {
  "instrument_token": 738561,
  "last_price": 1383.0, # Price moved up
  "last_traded_quantity": 50,
  "volume_traded": 2803279,
  "total_buy_quantity": 1300000, # Buy pressure increased
  "total_sell_quantity": 550000, # Sell pressure decreased
  "ohlc": {"open": 1376.0, "high": 1390.7, "low": 1375.9, "close": 1375.0},
  "exchange_timestamp": "2025-10-07 11:10:08",
  "depth": {
    "buy": [
      {"quantity": 500, "price": 1383.0, "orders": 10},
      {"quantity": 300, "price": 1382.9, "orders": 5}
    ],
    "sell": [
      {"quantity": 100, "price": 1383.1, "orders": 2},
      {"quantity": 50, "price": 1383.2, "orders": 1}
    ]
  }
}

sample_tick_3 = {
  "instrument_token": 738561,
  "last_price": 1382.5, # Price moved down
  "last_traded_quantity": 10,
  "volume_traded": 2803289,
  "total_buy_quantity": 1250000, # Buy pressure decreased
  "total_sell_quantity": 650000, # Sell pressure increased
  "ohlc": {"open": 1376.0, "high": 1390.7, "low": 1375.9, "close": 1375.0},
  "exchange_timestamp": "2025-10-07 11:10:09",
  "depth": {
    "buy": [
      {"quantity": 400, "price": 1382.5, "orders": 8},
    ],
    "sell": [
      {"quantity": 200, "price": 1382.6, "orders": 4},
    ]
  }
}

tick_stream = [sample_tick_1, sample_tick_2, sample_tick_3]
processed_data = []

# Initialize the processor
processor = TickDataProcessor(initial_cvd=0)

# Process the stream of ticks
for i, raw_tick in enumerate(tick_stream):
    print(f"\n--- Processing Tick {i+1} ---")
    flat_tick = processor.process_tick(raw_tick)
    processed_data.append(flat_tick)
    
    # Print key metrics for demonstration
    print(f"Timestamp: {flat_tick['exchange_timestamp']}")
    print(f"Last Price: {flat_tick['last_price']}")
    print(f"Delta: {flat_tick['volume_delta']}")
    print(f"CVD: {flat_tick['cvd']}")
    print(f"Top 1 Bid Qty: {flat_tick.get('bid_1_qty')}, Price: {flat_tick.get('bid_1_price')}")
    print(f"Top 1 Ask Qty: {flat_tick.get('ask_1_qty')}, Price: {flat_tick.get('ask_1_price')}")
    
# Convert the final list of processed ticks into a Pandas DataFrame
# This DataFrame is ready for storage (e.g., Parquet) or further roll-up aggregation.
df_processed = pd.DataFrame(processed_data)

print("\n" + "="*50)
print("Final Processed Data (Ready for Storage/Roll-up)")
print("="*50)
# Display the DataFrame with all calculated and flattened columns
print(df_processed[['exchange_timestamp', 'last_price', 'volume_delta', 'cvd', 'total_buy_quantity', 'total_sell_quantity', 'bid_1_price', 'ask_1_price']])

# Optional: Demonstrate saving to a Parquet file (as per your intended use case)
# df_processed.to_parquet('processed_ticks.parquet', index=False)
# print("\nData successfully saved to 'processed_ticks.parquet'")
