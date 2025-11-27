import pandas as pd
import numpy as np
import vectorbt as vbt
from scipy.signal import argrelextrema
import talib as ta

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def calculate_ema_filter(close, period=20, min_days=3):
    """
    Step 1: EMA 20 Filter
    Returns: 1 (bullish), -1 (bearish), 0 (neutral)
    """
    ema = ta.EMA(close, timeperiod=period)
    
    # Check if price above/below EMA for at least min_days
    above_ema = close > ema
    below_ema = close < ema
    
    # Count consecutive days
    above_count = above_ema.rolling(window=min_days).sum()
    below_count = below_ema.rolling(window=min_days).sum()
    
    bias = pd.Series(0, index=close.index)
    bias[above_count >= min_days] = 1   # Bullish
    bias[below_count >= min_days] = -1  # Bearish
    
    return bias, ema


def is_retracing_to_ema(close, ema, lookback=5, threshold_pct=0.03):
    """
    Step 2: Price Retracement Check
    Check if price is retracing toward EMA (within threshold%)
    """
    distance_to_ema = abs(close - ema) / ema
    is_near = distance_to_ema < threshold_pct
    return is_near


def find_swing_points(high, low, order=5):
    """
    Step 3: Find swing highs and lows for trendline drawing
    """
    # Find local maxima (swing highs)
    swing_highs_idx = argrelextrema(high.values, np.greater, order=order)[0]
    # Find local minima (swing lows)
    swing_lows_idx = argrelextrema(low.values, np.less, order=order)[0]
    
    return swing_highs_idx, swing_lows_idx


def calculate_trendline_breakout(close, high, low, bias, lookback=20):
    """
    Step 3 & 4: Identify trendline breakout
    Simplified: Using recent high/low as support/resistance
    For production: Implement actual trendline calculation with slope
    """
    breakout_long = pd.Series(False, index=close.index)
    breakout_short = pd.Series(False, index=close.index)
    
    # For longs: breakout above recent resistance
    recent_high = high.rolling(window=lookback).max()
    breakout_long = (close > recent_high.shift(1)) & (bias == 1)
    
    # For shorts: breakout below recent support
    recent_low = low.rolling(window=lookback).min()
    breakout_short = (close < recent_low.shift(1)) & (bias == -1)
    
    return breakout_long, breakout_short, recent_high, recent_low


def calculate_relative_volume(volume, period=50):
    """
    Step 5a: Relative Volume > 2
    """
    avg_volume = volume.rolling(window=period).mean()
    rel_volume = volume / avg_volume
    return rel_volume


def calculate_relative_strength(close, market_index_close):
    """
    Step 5b: Relative Strength Line
    Returns: RS ratio (stock_performance / market_performance)
    > 1 means outperforming (blue), < 1 means underperforming (pink)
    """
    stock_returns = close.pct_change()
    market_returns = market_index_close.pct_change()
    
    # Cumulative performance ratio
    rs_line = (1 + stock_returns).cumprod() / (1 + market_returns).cumprod()
    
    return rs_line


def calculate_atr_stop_loss(high, low, close, atr_period=14, atr_multiplier=1.0):
    """
    Step 6: ATR-based Stop Loss
    """
    atr = ta.ATR(high, low, close, timeperiod=atr_period)
    return atr * atr_multiplier


# ==============================================================================
# MAIN STRATEGY CLASS
# ==============================================================================

class SwingBreakoutStrategy:
    def __init__(self, data, market_data=None, params=None):
        """
        data: DataFrame with columns ['open', 'high', 'low', 'close', 'volume']
        market_data: DataFrame with market index close prices (e.g., NIFTY 50)
        params: Dictionary of strategy parameters
        """
        self.data = data.copy()
        self.market_data = market_data
        
        # Default parameters
        self.params = {
            'ema_period': 20,
            'ema_min_days': 3,
            'retracement_threshold': 0.03,  # 3%
            'trendline_lookback': 20,
            'rel_volume_threshold': 2.0,
            'rel_volume_period': 50,
            'atr_period': 14,
            'atr_multiplier': 1.0,
            'risk_reward_ratio': 2.0,
            'entry_time_start': '10:15',
            'entry_time_end': '10:30'
        }
        
        if params:
            self.params.update(params)
        
        self.signals = None
        self.portfolio = None
    
    def generate_signals(self):
        """Generate trading signals based on the 6-step method"""
        df = self.data.copy()
        
        # Step 1: EMA Filter
        bias, ema = calculate_ema_filter(
            df['close'], 
            self.params['ema_period'], 
            self.params['ema_min_days']
        )
        df['ema20'] = ema
        df['bias'] = bias
        
        # Step 2: Retracement Check
        df['near_ema'] = is_retracing_to_ema(
            df['close'], 
            df['ema20'], 
            threshold_pct=self.params['retracement_threshold']
        )
        
        # Step 3 & 4: Trendline Breakout
        breakout_long, breakout_short, resistance, support = calculate_trendline_breakout(
            df['close'], 
            df['high'], 
            df['low'], 
            df['bias'],
            self.params['trendline_lookback']
        )
        df['breakout_long'] = breakout_long
        df['breakout_short'] = breakout_short
        df['resistance'] = resistance
        df['support'] = support
        
        # Step 5a: Relative Volume
        df['rel_volume'] = calculate_relative_volume(
            df['volume'], 
            self.params['rel_volume_period']
        )
        df['volume_confirmed'] = df['rel_volume'] > self.params['rel_volume_threshold']
        
        # Step 5b: Relative Strength
        if self.market_data is not None:
            df['rs_line'] = calculate_relative_strength(df['close'], self.market_data)
            df['rs_strong'] = df['rs_line'] > 1.0  # Outperforming market
            df['rs_weak'] = df['rs_line'] < 1.0    # Underperforming market
        else:
            # If no market data, assume RS is always valid
            df['rs_strong'] = True
            df['rs_weak'] = True
        
        # Step 6: ATR for Stop Loss
        df['atr'] = calculate_atr_stop_loss(
            df['high'], 
            df['low'], 
            df['close'],
            self.params['atr_period'],
            self.params['atr_multiplier']
        )
        
        # FINAL ENTRY SIGNALS
        # Long entries: All conditions must be met
        df['entry_long'] = (
            (df['bias'] == 1) &
            df['near_ema'] &
            df['breakout_long'] &
            df['volume_confirmed'] &
            df['rs_strong']
        )
        
        # Short entries: All conditions must be met
        df['entry_short'] = (
            (df['bias'] == -1) &
            df['near_ema'] &
            df['breakout_short'] &
            df['volume_confirmed'] &
            df['rs_weak']
        )
        
        # Calculate Stop Loss and Take Profit levels
        # For longs: SL = support - ATR, TP = entry + 2*(entry - SL)
        df['sl_long'] = df['support'].shift(1) - df['atr']
        df['risk_long'] = df['close'] - df['sl_long']
        df['tp_long'] = df['close'] + (df['risk_long'] * self.params['risk_reward_ratio'])
        
        # For shorts: SL = resistance + ATR, TP = entry - 2*(SL - entry)
        df['sl_short'] = df['resistance'].shift(1) + df['atr']
        df['risk_short'] = df['sl_short'] - df['close']
        df['tp_short'] = df['close'] - (df['risk_short'] * self.params['risk_reward_ratio'])
        
        # Filter by entry time window (if timestamp is available)
        if pd.api.types.is_datetime64_any_dtype(df.index):
            entry_time_mask = (
                (df.index.time >= pd.to_datetime(self.params['entry_time_start']).time()) &
                (df.index.time <= pd.to_datetime(self.params['entry_time_end']).time())
            )
            df['entry_long'] = df['entry_long'] & entry_time_mask
            df['entry_short'] = df['entry_short'] & entry_time_mask
        
        self.signals = df
        return df
    
    def backtest(self, init_cash=100000, fees=0.001):
        """Run backtest using vectorBT"""
        if self.signals is None:
            self.generate_signals()
        
        df = self.signals
        
        # Create combined entry/exit signals for vectorBT
        entries = df['entry_long'] | df['entry_short']
        
        # Exit signals: Hit stop loss or take profit
        # Simplified: Exit after N bars or when bias changes
        exits = (
            (df['bias'] != df['bias'].shift(1)) |  # Bias flip
            (df['close'] <= df['sl_long'].shift(1)) |  # Long SL hit
            (df['close'] >= df['tp_long'].shift(1)) |  # Long TP hit
            (df['close'] >= df['sl_short'].shift(1)) |  # Short SL hit
            (df['close'] <= df['tp_short'].shift(1))    # Short TP hit
        )
        
        # Determine direction (1 for long, -1 for short)
        direction = pd.Series(0, index=df.index)
        direction[df['entry_long']] = 1
        direction[df['entry_short']] = -1
        
        # Create portfolio with stop loss and take profit
        self.portfolio = vbt.Portfolio.from_signals(
            close=df['close'],
            entries=entries,
            exits=exits,
            direction=direction,
            init_cash=init_cash,
            fees=fees,
            sl_stop=df['sl_long'],  # Stop loss (simplified)
            tp_stop=df['tp_long']   # Take profit (simplified)
        )
        
        return self.portfolio
    
    def get_stats(self):
        """Get portfolio statistics"""
        if self.portfolio is None:
            raise ValueError("Run backtest() first")
        
        return self.portfolio.stats()
    
    def plot(self):
        """Plot strategy performance"""
        if self.portfolio is None:
            raise ValueError("Run backtest() first")
        
        return self.portfolio.plot()


# ==============================================================================
# EXAMPLE USAGE
# ==============================================================================

if __name__ == "__main__":
    # Load your data (example structure)
    # Replace with your actual data loading
    
    # Example: Load from your existing parquet
    df_zscore = pd.read_parquet("dashboard_zscores.parquet")
    
    # Convert to OHLCV format for a single instrument
    token = df_zscore['instrument_token'].unique()[0]
    data = df_zscore[df_zscore['instrument_token'] == token].copy()
    data = data.set_index('exchange_timestamp').sort_index()
    
    # Create OHLCV DataFrame
    ohlcv = pd.DataFrame({
        'open': data['ohlc_open'],
        'high': data['ohlc_high'],
        'low': data['ohlc_low'],
        'close': data['ohlc_close'],
        'volume': data['volume_traded']
    })
    
    # Load market index data (e.g., NIFTY 50)
    # For demonstration, using same data - replace with actual NIFTY data
    market_data = ohlcv['close'].copy()
    
    # Initialize strategy
    strategy = SwingBreakoutStrategy(
        data=ohlcv,
        market_data=market_data,
        params={
            'ema_period': 20,
            'ema_min_days': 3,
            'rel_volume_threshold': 2.0,
            'risk_reward_ratio': 2.0
        }
    )
    
    # Generate signals
    signals = strategy.generate_signals()
    print("\n" + "="*80)
    print("SIGNAL SUMMARY")
    print("="*80)
    print(f"Total Long Entries: {signals['entry_long'].sum()}")
    print(f"Total Short Entries: {signals['entry_short'].sum()}")
    print(f"\nSample Signals:")
    print(signals[signals['entry_long'] | signals['entry_short']].head(10))
    
    # Run backtest
    portfolio = strategy.backtest(init_cash=100000, fees=0.001)
    
    # Display statistics
    print("\n" + "="*80)
    print("BACKTEST RESULTS")
    print("="*80)
    print(strategy.get_stats())
    
    # Plot results
    strategy.plot().show()
    
    # ==============================================================================
    # MULTI-INSTRUMENT SCANNER
    # ==============================================================================
    
    def scan_all_instruments(df_zscore, market_data, params=None):
        """
        Scan all instruments and return those with valid entry signals
        """
        instruments = df_zscore['instrument_token'].unique()
        results = []
        
        for token in instruments:
            try:
                # Prepare data
                data = df_zscore[df_zscore['instrument_token'] == token].copy()
                data = data.set_index('exchange_timestamp').sort_index()
                
                ohlcv = pd.DataFrame({
                    'open': data['ohlc_open'],
                    'high': data['ohlc_high'],
                    'low': data['ohlc_low'],
                    'close': data['ohlc_close'],
                    'volume': data['volume_traded']
                })
                
                # Run strategy
                strategy = SwingBreakoutStrategy(ohlcv, market_data, params)
                signals = strategy.generate_signals()
                
                # Check if there's a current entry signal
                latest = signals.iloc[-1]
                if latest['entry_long'] or latest['entry_short']:
                    results.append({
                        'instrument_token': token,
                        'timestamp': signals.index[-1],
                        'signal': 'LONG' if latest['entry_long'] else 'SHORT',
                        'entry_price': latest['close'],
                        'stop_loss': latest['sl_long'] if latest['entry_long'] else latest['sl_short'],
                        'take_profit': latest['tp_long'] if latest['entry_long'] else latest['tp_short'],
                        'rel_volume': latest['rel_volume'],
                        'rs_line': latest.get('rs_line', None)
                    })
            except Exception as e:
                print(f"Error processing {token}: {e}")
                continue
        
        return pd.DataFrame(results)
    
    # Scan for opportunities
    print("\n" + "="*80)
    print("SCANNING FOR BREAKOUT OPPORTUNITIES")
    print("="*80)
    opportunities = scan_all_instruments(df_zscore, market_data)
    print(opportunities)