import vectorbt as vbt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from collections import deque, defaultdict
import warnings
warnings.filterwarnings('ignore')

class RealTimeOrderBookStrategy:
    def __init__(self, 
                 volume_delta_window=50,
                 cvd_lookback=100,
                 imbalance_threshold=0.7,
                 absorption_threshold=0.8,
                 min_trade_volume=10):
        
        self.volume_delta_window = volume_delta_window
        self.cvd_lookback = cvd_lookback
        self.imbalance_threshold = imbalance_threshold
        self.absorption_threshold = absorption_threshold
        self.min_trade_volume = min_trade_volume
        
        # Data storage for each instrument
        self.instrument_data = defaultdict(self._create_instrument_data)
        
        # Strategy parameters
        self.signals = {}
        
    def _create_instrument_data(self):
        """Initialize data structure for each instrument"""
        return {
            'timestamps': deque(maxlen=1000),
            'prices': deque(maxlen=1000),
            'volumes': deque(maxlen=1000),
            'buy_volumes': deque(maxlen=1000),
            'sell_volumes': deque(maxlen=1000),
            'volume_delta': deque(maxlen=1000),
            'cumulative_delta': deque(maxlen=1000),
            'order_book_imbalance': deque(maxlen=1000),
            'price_imbalance': deque(maxlen=1000),
            'best_bid': deque(maxlen=1000),
            'best_ask': deque(maxlen=1000),
            'mid_price': deque(maxlen=1000),
            'trade_flow': deque(maxlen=1000),
            'last_tick': None
        }
    
    def on_tick(self, tick_data):
        """Main tick processing function"""
        instrument_token = tick_data['instrument_token']
        data = self.instrument_data[instrument_token]
        
        # Store current tick
        data['last_tick'] = tick_data
        
        # Extract basic data
        timestamp = datetime.strptime(tick_data['exchange_timestamp'], '%Y-%m-%d %H:%M:%S')
        last_price = tick_data['last_price']
        last_quantity = tick_data['last_traded_quantity']
        total_buy = tick_data['total_buy_quantity']
        total_sell = tick_data['total_sell_quantity']
        
        # Calculate volume delta
        volume_delta = self._calculate_volume_delta(tick_data, data)
        
        # Calculate cumulative volume delta
        cumulative_delta = self._calculate_cumulative_delta(volume_delta, data)
        
        # Calculate order book imbalance
        ob_imbalance = self._calculate_orderbook_imbalance(tick_data)
        
        # Calculate price-volume imbalance
        price_imbalance = self._calculate_price_imbalance(tick_data)
        
        # Get best bid/ask
        best_bid, best_ask = self._get_best_bid_ask(tick_data)
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else last_price
        
        # Store all calculated values
        data['timestamps'].append(timestamp)
        data['prices'].append(last_price)
        data['volumes'].append(last_quantity)
        data['buy_volumes'].append(total_buy)
        data['sell_volumes'].append(total_sell)
        data['volume_delta'].append(volume_delta)
        data['cumulative_delta'].append(cumulative_delta)
        data['order_book_imbalance'].append(ob_imbalance)
        data['price_imbalance'].append(price_imbalance)
        data['best_bid'].append(best_bid)
        data['best_ask'].append(best_ask)
        data['mid_price'].append(mid_price)
        data['trade_flow'].append(1 if volume_delta > 0 else (-1 if volume_delta < 0 else 0))
        
        # Generate trading signals
        signal = self._generate_signals(instrument_token, data)
        
        return signal
    
    def _calculate_volume_delta(self, tick_data, historical_data):
        """Calculate volume delta (buy volume - sell volume)"""
        if not historical_data['volumes']:
            return 0
            
        # Use order book pressure and trade analysis
        depth = tick_data.get('depth', {})
        buy_side = depth.get('buy', [])
        sell_side = depth.get('sell', [])
        
        # Calculate immediate pressure
        buy_pressure = sum(level['quantity'] for level in buy_side[:2])  # Top 2 levels
        sell_pressure = sum(level['quantity'] for level in sell_side[:2])  # Top 2 levels
        
        # Analyze last trade direction
        last_trade_qty = tick_data['last_traded_quantity']
        best_bid = buy_side[0]['price'] if buy_side else 0
        best_ask = sell_side[0]['price'] if sell_side else 0
        
        if best_bid and best_ask:
            if last_trade_qty >= self.min_trade_volume:
                if tick_data['last_price'] >= best_ask:
                    # Trade at ask - aggressive buying
                    trade_delta = last_trade_qty
                elif tick_data['last_price'] <= best_bid:
                    # Trade at bid - aggressive selling
                    trade_delta = -last_trade_qty
                else:
                    # Trade between spread - neutral
                    trade_delta = 0
            else:
                trade_delta = 0
        else:
            trade_delta = 0
        
        # Combine order book pressure and trade delta
        ob_delta = (buy_pressure - sell_pressure) / (buy_pressure + sell_pressure) if (buy_pressure + sell_pressure) > 0 else 0
        volume_delta = trade_delta + (ob_delta * last_trade_qty * 0.1)  # Weighted combination
        
        return volume_delta
    
    def _calculate_cumulative_delta(self, current_delta, historical_data):
        """Calculate cumulative volume delta"""
        if not historical_data['cumulative_delta']:
            return current_delta
        
        prev_cvd = historical_data['cumulative_delta'][-1]
        new_cvd = prev_cvd + current_delta
        
        # Apply mean reversion for very large values
        if abs(new_cvd) > self.cvd_lookback * 10:
            new_cvd = new_cvd * 0.9  # Dampening
        
        return new_cvd
    
    def _calculate_orderbook_imbalance(self, tick_data):
        """Calculate order book imbalance (OBI)"""
        depth = tick_data.get('depth', {})
        buy_side = depth.get('buy', [])
        sell_side = depth.get('sell', [])
        
        if not buy_side or not sell_side:
            return 0
        
        # Calculate total quantities at different levels
        buy_qty_level1 = buy_side[0]['quantity'] if len(buy_side) > 0 else 0
        buy_qty_level2 = buy_side[1]['quantity'] if len(buy_side) > 1 else 0
        sell_qty_level1 = sell_side[0]['quantity'] if len(sell_side) > 0 else 0
        sell_qty_level2 = sell_side[1]['quantity'] if len(sell_side) > 1 else 0
        
        total_buy_pressure = buy_qty_level1 + (buy_qty_level2 * 0.5)
        total_sell_pressure = sell_qty_level1 + (sell_qty_level2 * 0.5)
        
        if total_buy_pressure + total_sell_pressure == 0:
            return 0
        
        imbalance = (total_buy_pressure - total_sell_pressure) / (total_buy_pressure + total_sell_pressure)
        return imbalance
    
    def _calculate_price_imbalance(self, tick_data):
        """Calculate price-volume imbalance"""
        depth = tick_data.get('depth', {})
        buy_side = depth.get('buy', [])
        sell_side = depth.get('sell', [])
        
        if not buy_side or not sell_side:
            return 0
        
        best_bid = buy_side[0]['price']
        best_ask = sell_side[0]['price']
        spread = best_ask - best_bid
        
        if spread == 0:
            return 0
        
        # Calculate weighted average prices
        bid_volume_weighted = sum(level['quantity'] * level['price'] for level in buy_side[:3])
        total_bid_volume = sum(level['quantity'] for level in buy_side[:3])
        
        ask_volume_weighted = sum(level['quantity'] * level['price'] for level in sell_side[:3])
        total_ask_volume = sum(level['quantity'] for level in sell_side[:3])
        
        if total_bid_volume == 0 or total_ask_volume == 0:
            return 0
        
        avg_bid = bid_volume_weighted / total_bid_volume
        avg_ask = ask_volume_weighted / total_ask_volume
        
        mid_price = (best_bid + best_ask) / 2
        price_imbalance = (avg_ask - avg_bid) / (avg_ask + avg_bid) if (avg_ask + avg_bid) > 0 else 0
        
        return price_imbalance
    
    def _get_best_bid_ask(self, tick_data):
        """Extract best bid and ask from depth data"""
        depth = tick_data.get('depth', {})
        buy_side = depth.get('buy', [])
        sell_side = depth.get('sell', [])
        
        best_bid = buy_side[0]['price'] if buy_side else None
        best_ask = sell_side[0]['price'] if sell_side else None
        
        return best_bid, best_ask
    
    def _detect_absorption(self, instrument_token, data):
        """Detect absorption patterns"""
        if len(data['volume_delta']) < 10:
            return False, 0
        
        recent_deltas = list(data['volume_delta'])[-5:]
        recent_prices = list(data['prices'])[-5:]
        recent_obi = list(data['order_book_imbalance'])[-5:]
        
        # Absorption: High volume but little price movement + opposing OB pressure
        volume_spike = any(abs(delta) > np.mean([abs(d) for d in recent_deltas]) * 2 for delta in recent_deltas)
        price_stable = np.std(recent_prices) < np.mean(recent_prices) * 0.001
        opposing_pressure = (recent_obi[-1] * recent_deltas[-1]) < 0  # OBI and volume delta in opposite directions
        
        absorption_strength = 0
        if volume_spike and price_stable and opposing_pressure:
            absorption_strength = abs(recent_deltas[-1]) / max(1, np.mean([abs(d) for d in recent_deltas]))
        
        return (volume_spike and price_stable and opposing_pressure), absorption_strength
    
    def _detect_exhaustion(self, instrument_token, data):
        """Detect exhaustion moves"""
        if len(data['cumulative_delta']) < 20:
            return False, 0
        
        recent_cvd = list(data['cumulative_delta'])[-10:]
        recent_prices = list(data['prices'])[-10:]
        recent_volume = list(data['volumes'])[-10:]
        
        # Exhaustion: Extreme CVD with price reversal + volume climax
        cvd_extreme = abs(recent_cvd[-1]) > np.mean([abs(c) for c in recent_cvd]) * 2
        price_reversal = (
            (recent_prices[-1] < recent_prices[-3] and recent_prices[-1] < recent_prices[-5]) or
            (recent_prices[-1] > recent_prices[-3] and recent_prices[-1] > recent_prices[-5])
        )
        volume_climax = recent_volume[-1] > np.mean(recent_volume) * 1.5
        
        exhaustion_strength = 0
        if cvd_extreme and price_reversal and volume_climax:
            exhaustion_strength = abs(recent_cvd[-1]) / max(1, np.mean([abs(c) for c in recent_cvd]))
        
        return (cvd_extreme and price_reversal and volume_climax), exhaustion_strength
    
    def _detect_liquidity_grab(self, instrument_token, data):
        """Detect liquidity grab/run patterns"""
        if len(data['order_book_imbalance']) < 10:
            return False, 0
        
        recent_obi = list(data['order_book_imbalance'])[-5:]
        recent_prices = list(data['prices'])[-5:]
        recent_volume = list(data['volumes'])[-5:]
        
        # Liquidity grab: Sharp OBI move followed by quick reversal
        obi_spike = abs(recent_obi[-1]) > self.imbalance_threshold
        quick_reversal = len(recent_obi) >= 3 and (recent_obi[-1] * recent_obi[-3] < 0)  # Sign change
        price_momentum = np.sign(np.diff(recent_prices)[-1]) != np.sign(recent_obi[-1])
        
        grab_strength = 0
        if obi_spike and quick_reversal and price_momentum:
            grab_strength = abs(recent_obi[-1])
        
        return (obi_spike and quick_reversal and price_momentum), grab_strength
    
    def _generate_signals(self, instrument_token, data):
        """Generate trading signals based on all indicators"""
        if len(data['prices']) < 20:
            return 0  # Not enough data
        
        # Get current values
        current_price = data['prices'][-1]
        volume_delta = data['volume_delta'][-1]
        cumulative_delta = data['cumulative_delta'][-1]
        ob_imbalance = data['order_book_imbalance'][-1]
        price_imbalance = data['price_imbalance'][-1]
        
        # Detect patterns
        absorption_detected, absorption_strength = self._detect_absorption(instrument_token, data)
        exhaustion_detected, exhaustion_strength = self._detect_exhaustion(instrument_token, data)
        liquidity_grab_detected, grab_strength = self._detect_liquidity_grab(instrument_token, data)
        
        # Initialize signal strength
        signal_strength = 0
        
        # Bullish signals
        bullish_signals = 0
        if absorption_detected and volume_delta > 0 and ob_imbalance > 0:
            bullish_signals += 1
            signal_strength += absorption_strength
        
        if exhaustion_detected and cumulative_delta < -self.cvd_lookback:  # Oversold exhaustion
            bullish_signals += 1
            signal_strength += exhaustion_strength
        
        if liquidity_grab_detected and ob_imbalance > 0 and price_imbalance > 0:
            bullish_signals += 1
            signal_strength += grab_strength
        
        # Bearish signals
        bearish_signals = 0
        if absorption_detected and volume_delta < 0 and ob_imbalance < 0:
            bearish_signals += 1
            signal_strength -= absorption_strength
        
        if exhaustion_detected and cumulative_delta > self.cvd_lookback:  # Overbought exhaustion
            bearish_signals += 1
            signal_strength -= exhaustion_strength
        
        if liquidity_grab_detected and ob_imbalance < 0 and price_imbalance < 0:
            bearish_signals += 1
            signal_strength -= grab_strength
        
        # Generate final signal
        if bullish_signals > bearish_signals and signal_strength > self.absorption_threshold:
            return 1  # Buy signal
        elif bearish_signals > bullish_signals and signal_strength < -self.absorption_threshold:
            return -1  # Sell signal
        else:
            return 0  # No signal
    
    def get_current_metrics(self, instrument_token):
        """Get current metrics for monitoring"""
        data = self.instrument_data[instrument_token]
        if not data['prices']:
            return None
        
        return {
            'price': data['prices'][-1],
            'volume_delta': data['volume_delta'][-1],
            'cumulative_delta': data['cumulative_delta'][-1],
            'order_book_imbalance': data['order_book_imbalance'][-1],
            'price_imbalance': data['price_imbalance'][-1],
            'best_bid': data['best_bid'][-1],
            'best_ask': data['best_ask'][-1],
            'signal': self.signals.get(instrument_token, 0)
        }

# Example usage with your data stream
class DataStream:
    def __init__(self, strategy):
        self.strategy = strategy
        self.callbacks = []
    
    def register_callback(self, callback):
        self.callbacks.append(callback)
    
    def on_tick_update(self, tick_data):
        """Process new tick data"""
        # Convert your JSON tick to dictionary if needed
        if isinstance(tick_data, str):
            tick_data = json.loads(tick_data)
        
        # Process through strategy
        signal = self.strategy.on_tick(tick_data)
        
        # Store signal
        instrument_token = tick_data['instrument_token']
        self.strategy.signals[instrument_token] = signal
        
        # Execute callbacks
        for callback in self.callbacks:
            callback(tick_data, signal)
        
        return signal

# VectorBT Portfolio Integration
def create_vectorbt_portfolio(signals, prices):
    """Create VectorBT portfolio from generated signals"""
    # Convert signals to pandas Series
    signals_series = pd.Series(signals, index=prices.index)
    
    # Create portfolio
    portfolio = vbt.Portfolio.from_signals(
        prices,
        entries=signals_series == 1,
        exits=signals_series == -1,
        freq='1s',  # High frequency
        direction='both'  # Both long and short
    )
    
    return portfolio

# Real-time monitoring function
def real_time_monitor(tick_data, signal):
    """Callback function for real-time monitoring"""
    instrument_token = tick_data['instrument_token']
    price = tick_data['last_price']
    
    if signal != 0:
        action = "BUY" if signal == 1 else "SELL"
        print(f"[{tick_data['exchange_timestamp']}] {action} Signal for {instrument_token} at {price}")
        
        # Get current metrics
        metrics = strategy.get_current_metrics(instrument_token)
        if metrics:
            print(f"  Volume Delta: {metrics['volume_delta']:.2f}")
            print(f"  Cumulative Delta: {metrics['cumulative_delta']:.2f}")
            print(f"  OB Imbalance: {metrics['order_book_imbalance']:.3f}")

# Initialize strategy and data stream
strategy = RealTimeOrderBookStrategy(
    volume_delta_window=50,
    cvd_lookback=100,
    imbalance_threshold=0.7,
    absorption_threshold=0.6,
    min_trade_volume=5
)

data_stream = DataStream(strategy)
data_stream.register_callback(real_time_monitor)

# Example of processing your tick data
def process_tick_data_log(json_file_path):
    """Process your JSON tick data log"""
    with open(json_file_path, 'r') as f:
        for line in f:
            if line.strip():
                tick_data = json.loads(line)
                signal = data_stream.on_tick_update(tick_data)
    
    return strategy

# Usage with your actual data
# strategy = process_tick_data_log('tick_data_log_1.json')