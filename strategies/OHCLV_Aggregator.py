from datetime import datetime, timedelta
from typing import Dict, Any

from pyparsing import Optional

# Ensure this utility function is accessible (e.g., as a method in ZerodhaKiteDataDisplay)
def _get_bar_time(timestamp_str: str, interval_sec: int) -> datetime:
    """Rounds down a tick timestamp to the start of the defined bar interval."""
    # Convert '2025-10-07 11:10:07' to a datetime object
    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    
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
    
    # --- Data Extraction ---
    current_time_str = tick_data.get('exchange_timestamp')
    last_price = tick_data.get('last_price')
    volume_traded_cumulative = tick_data.get('volume_traded')
    
    if not current_time_str or not last_price:
        return None # Skip invalid ticks

    # 1. Determine the current bar's OPEN time
    bar_open_time = _get_bar_time(current_time_str, self.BAR_INTERVAL_SECONDS)
    
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
        
        # Calculate Bar Volume: Current cumulative volume minus volume at bar start
        bar_volume = max(0, volume_traded_cumulative - self.bar_start_cumulative_volume[token])

        # The Close price of the just-completed bar is the last price of the previous tick
        closed_bar = {
            'timestamp': current_bar['OpenTime'] + timedelta(seconds=self.BAR_INTERVAL_SECONDS),
            'symbol': self.token_symbol_map.get(token, 'UNKNOWN'),
            'open': current_bar['Open'],
            'high': current_bar['High'],
            'low': current_bar['Low'],
            'close': current_bar['Close'], # The last price stored from the previous tick
            'volume': bar_volume
        }
        
        # --- RESET FOR NEW BAR ---
        self.current_bars[token] = {
            'Open': last_price,
            'High': last_price,
            'Low': last_price,
            'Volume': 0,
            'OpenTime': bar_open_time
        }
        self.bar_start_cumulative_volume[token] = volume_traded_cumulative
        
        return closed_bar # Return the completed bar
        
    else:
        # --- BAR IS OPEN - AGGREGATING ---
        
        # Update High, Low, and Current Close Price
        current_bar['High'] = max(current_bar['High'], last_price)
        current_bar['Low'] = min(current_bar['Low'], last_price)
        current_bar['Close'] = last_price # The Close is always the last price seen
        
        return None # Bar is still open