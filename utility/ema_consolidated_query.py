"""
Query Utility for Consolidated EOD Data
Provides easy access to consolidated data with EMA calculations
"""

import pandas as pd
import os
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURATION
# ==============================================================================

CONSOLIDATED_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_eod"


# ==============================================================================
# QUERY FUNCTIONS
# ==============================================================================

class EODDataQuery:
    """Easy interface to query consolidated EOD data"""
    
    def __init__(self, data_root=CONSOLIDATED_DATA_ROOT):
        self.data_root = data_root
        self.cache = {}  # Cache loaded data
    
    def load_instrument(self, instrument_token, use_cache=True):
        """
        Load consolidated data for a single instrument
        """
        if use_cache and instrument_token in self.cache:
            return self.cache[instrument_token]
        
        file_path = os.path.join(self.data_root, f"{instrument_token}.parquet")
        
        if not os.path.exists(file_path):
            print(f"⚠️  No data found for instrument: {instrument_token}")
            return None
        
        df = pd.read_parquet(file_path)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        df = df.sort_values('trading_date')
        
        if use_cache:
            self.cache[instrument_token] = df
        
        return df
    
    def get_latest_data(self, instrument_token, days=1):
        """
        Get latest N days of data for an instrument
        """
        df = self.load_instrument(instrument_token)
        
        if df is None:
            return None
        
        return df.tail(days)
    
    def get_date_range(self, instrument_token, start_date, end_date):
        """
        Get data for a specific date range
        """
        df = self.load_instrument(instrument_token)
        
        if df is None:
            return None
        
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        mask = (df['trading_date'] >= start_date) & (df['trading_date'] <= end_date)
        return df[mask]
    
    def get_ema_crossover_signals(self, instrument_token, fast_ema=9, slow_ema=21, days=None):
        """
        Detect EMA crossover signals
        Returns: DataFrame with crossover points marked
        """
        df = self.load_instrument(instrument_token)
        
        if df is None:
            return None
        
        fast_col = f'EMA_{fast_ema}'
        slow_col = f'EMA_{slow_ema}'
        
        if fast_col not in df.columns or slow_col not in df.columns:
            print(f"⚠️  EMA columns not found. Available: {df.columns.tolist()}")
            return None
        
        # Detect crossovers
        df['ema_fast'] = df[fast_col]
        df['ema_slow'] = df[slow_col]
        
        # Bullish crossover: fast crosses above slow
        df['bullish_crossover'] = (
            (df['ema_fast'] > df['ema_slow']) & 
            (df['ema_fast'].shift(1) <= df['ema_slow'].shift(1))
        )
        
        # Bearish crossover: fast crosses below slow
        df['bearish_crossover'] = (
            (df['ema_fast'] < df['ema_slow']) & 
            (df['ema_fast'].shift(1) >= df['ema_slow'].shift(1))
        )
        
        if days:
            df = df.tail(days)
        
        return df
    
    def scan_for_signals(self, instruments, fast_ema=9, slow_ema=21, signal_type='bullish'):
        """
        Scan multiple instruments for crossover signals in latest data
        
        Args:
            instruments: List of instrument tokens
            fast_ema: Fast EMA period
            slow_ema: Slow EMA period
            signal_type: 'bullish', 'bearish', or 'both'
        
        Returns: List of instruments with signals
        """
        signals = []
        
        for instrument_token in instruments:
            df = self.get_ema_crossover_signals(instrument_token, fast_ema, slow_ema, days=5)
            
            if df is None:
                continue
            
            # Check for recent signals (last 2 days)
            recent_bullish = df['bullish_crossover'].tail(2).any()
            recent_bearish = df['bearish_crossover'].tail(2).any()
            
            if signal_type == 'bullish' and recent_bullish:
                signals.append({
                    'instrument_token': instrument_token,
                    'signal': 'BULLISH',
                    'date': df[df['bullish_crossover']].iloc[-1]['trading_date'],
                    'price': df[df['bullish_crossover']].iloc[-1]['ohlc_close'],
                    'fast_ema': df.iloc[-1]['ema_fast'],
                    'slow_ema': df.iloc[-1]['ema_slow']
                })
            
            elif signal_type == 'bearish' and recent_bearish:
                signals.append({
                    'instrument_token': instrument_token,
                    'signal': 'BEARISH',
                    'date': df[df['bearish_crossover']].iloc[-1]['trading_date'],
                    'price': df[df['bearish_crossover']].iloc[-1]['ohlc_close'],
                    'fast_ema': df.iloc[-1]['ema_fast'],
                    'slow_ema': df.iloc[-1]['ema_slow']
                })
            
            elif signal_type == 'both':
                if recent_bullish:
                    signals.append({
                        'instrument_token': instrument_token,
                        'signal': 'BULLISH',
                        'date': df[df['bullish_crossover']].iloc[-1]['trading_date'],
                        'price': df[df['bullish_crossover']].iloc[-1]['ohlc_close']
                    })
                if recent_bearish:
                    signals.append({
                        'instrument_token': instrument_token,
                        'signal': 'BEARISH',
                        'date': df[df['bearish_crossover']].iloc[-1]['trading_date'],
                        'price': df[df['bearish_crossover']].iloc[-1]['ohlc_close']
                    })
        
        return pd.DataFrame(signals) if signals else None
    
    def get_multiple_instruments(self, instrument_tokens):
        """
        Load data for multiple instruments at once
        Returns: Dictionary of {token: DataFrame}
        """
        data_dict = {}
        
        for token in instrument_tokens:
            df = self.load_instrument(token)
            if df is not None:
                data_dict[token] = df
        
        return data_dict
    
    def get_instruments_above_ema(self, instruments, ema_period=50):
        """
        Find instruments where price is above specified EMA
        """
        above_ema = []
        
        ema_col = f'EMA_{ema_period}'
        
        for instrument_token in instruments:
            df = self.load_instrument(instrument_token)
            
            if df is None or ema_col not in df.columns:
                continue
            
            latest = df.iloc[-1]
            
            if latest['ohlc_close'] > latest[ema_col]:
                above_ema.append({
                    'instrument_token': instrument_token,
                    'close': latest['ohlc_close'],
                    'ema': latest[ema_col],
                    'distance_pct': ((latest['ohlc_close'] - latest[ema_col]) / latest[ema_col] * 100)
                })
        
        return pd.DataFrame(above_ema) if above_ema else None
    
    def clear_cache(self):
        """Clear the data cache"""
        self.cache = {}


# ==============================================================================
# USAGE EXAMPLES
# ==============================================================================

def example_usage():
    """Demonstrate how to use the query utility"""
    
    # Initialize query interface
    query = EODDataQuery()
    
    # Example 1: Load single instrument
    print("="*80)
    print("Example 1: Load single instrument")
    print("="*80)
    df = query.load_instrument(6401)
    if df is not None:
        print(f"Loaded {len(df)} days of data")
        print("\nLatest 5 days:")
        print(df.tail(5)[['trading_date', 'ohlc_close', 'EMA_9', 'EMA_21']])
    
    # Example 2: Get latest data
    print("\n" + "="*80)
    print("Example 2: Get latest 3 days")
    print("="*80)
    latest = query.get_latest_data(6401, days=3)
    if latest is not None:
        print(latest[['trading_date', 'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close']])
    
    # Example 3: Get date range
    print("\n" + "="*80)
    print("Example 3: Get specific date range")
    print("="*80)
    range_data = query.get_date_range(6401, "2025-11-20", "2025-11-25")
    if range_data is not None:
        print(range_data[['trading_date', 'ohlc_close', 'volume_traded']])
    
    # Example 4: Detect EMA crossovers
    print("\n" + "="*80)
    print("Example 4: Detect EMA crossovers")
    print("="*80)
    crossovers = query.get_ema_crossover_signals(6401, fast_ema=9, slow_ema=21, days=30)
    if crossovers is not None:
        bullish = crossovers[crossovers['bullish_crossover']]
        bearish = crossovers[crossovers['bearish_crossover']]
        print(f"Bullish crossovers: {len(bullish)}")
        print(f"Bearish crossovers: {len(bearish)}")
        if len(bullish) > 0:
            print("\nLatest bullish crossover:")
            print(bullish.tail(1)[['trading_date', 'ohlc_close', 'ema_fast', 'ema_slow']])
    
    # Example 5: Scan multiple instruments
    print("\n" + "="*80)
    print("Example 5: Scan for signals across instruments")
    print("="*80)
    instruments = [6401, 3329, 424961]  # Add your instruments
    signals = query.scan_for_signals(instruments, fast_ema=9, slow_ema=21, signal_type='bullish')
    if signals is not None and len(signals) > 0:
        print(f"Found {len(signals)} instruments with bullish signals:")
        print(signals)
    else:
        print("No signals found")
    
    # Example 6: Find instruments above EMA
    print("\n" + "="*80)
    print("Example 6: Find instruments above EMA 50")
    print("="*80)
    above_ema = query.get_instruments_above_ema(instruments, ema_period=50)
    if above_ema is not None and len(above_ema) > 0:
        print(above_ema)
    else:
        print("No instruments found above EMA 50")


# ==============================================================================
# BATCH QUERY FOR BACKTESTING
# ==============================================================================

def prepare_backtest_data(instruments, start_date, end_date, ema_periods=[9, 21]):
    """
    Prepare data for backtesting across multiple instruments
    """
    query = EODDataQuery()
    
    backtest_data = {}
    
    print(f"📊 Preparing backtest data for {len(instruments)} instruments...")
    
    for instrument_token in instruments:
        df = query.get_date_range(instrument_token, start_date, end_date)
        
        if df is None or len(df) == 0:
            continue
        
        # Ensure required EMA columns exist
        required_cols = [f'EMA_{p}' for p in ema_periods]
        if all(col in df.columns for col in required_cols):
            backtest_data[instrument_token] = df
    
    print(f"✅ Loaded data for {len(backtest_data)} instruments")
    
    return backtest_data


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    
    print("="*80)
    print("🔍 CONSOLIDATED EOD DATA QUERY UTILITY")
    print("="*80)
    
    # Run examples
    example_usage()
    
    print("\n" + "="*80)
    print("✅ Examples Complete")
    print("="*80)
    
    # Additional: Prepare data for backtesting
    print("\n" + "="*80)
    print("📊 Preparing Backtest Data Example")
    print("="*80)
    
    instruments = [6401, 3329]  # Add your instruments
    backtest_data = prepare_backtest_data(
        instruments,
        start_date="2025-11-01",
        end_date="2025-11-25",
        ema_periods=[9, 21, 50]
    )
    
    if backtest_data:
        print(f"\n✅ Backtest data ready for {len(backtest_data)} instruments")
        for token, df in backtest_data.items():
            print(f"   {token}: {len(df)} days")