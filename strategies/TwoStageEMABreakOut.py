"""
Two-Stage EMA Breakout Strategy:
Stage 1: EOD close crosses above EMA 9 (daily) - Creates "watchlist"
Stage 2: Next day, hourly candles stay above EMA 9 (hourly) - Generates actual entry
"""

import os
import pandas as pd
import numpy as np
import vectorbt as vbt
from datetime import datetime, timedelta
from config import example_tokens, instruments_to_process

INSTRUMENTS = example_tokens
#INSTRUMENTS =[3329]
# ==============================================================================
# CONFIGURATION
# ==============================================================================

class TwoStageConfig:
    # Data paths
    EOD_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_eod"
    HOURLY_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_hourly"
    
    # Strategy parameters - Stage 1 (EOD)
    EOD_EMA_PERIOD = 9
    
    # Strategy parameters - Stage 2 (Hourly)
    HOURLY_EMA_PERIOD = 9
    CONSECUTIVE_HOURS_ABOVE_EMA = 3  # Need N consecutive hourly candles above EMA
    LOOKBACK_RSI_PERIOD=10
    # Entry timing
    ENTRY_WAIT_CANDLES = 1  # Wait N hourly candles before entering
    MAX_HOURS_TO_WAIT = 6   # If no confirmation in 3 hours, skip
    
    # Exit parameters
    STOP_LOSS_PCT = 2.5
    TRAILING_STOP_PCT = .75


# ==============================================================================
# DATA LOADING
# ==============================================================================

def load_eod_data(instrument_token, eod_root):
    """Load consolidated EOD data"""
    file_path = os.path.join(eod_root, f"{instrument_token}.parquet")
    #print("Loading EOD data from:", file_path)
    if not os.path.exists(file_path):
        return None
    
    df = pd.read_parquet(file_path)
    df['trading_date'] = pd.to_datetime(df['trading_date'])
    df = df.sort_values('trading_date')
    return df


def load_hourly_data(instrument_token, date, hourly_root):
    """
    Load hourly data for specific date
    Assumes structure: hourly_root/YYYY-MM-DD/instrument_token.parquet
    """
    #date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
    #file_path = os.path.join(hourly_root, date_str, f"{instrument_token}.parquet")
    file_path = os.path.join(hourly_root,  f"{instrument_token}.parquet")
    #print("Loading Hourly data from:", file_path)
    if not os.path.exists(file_path):
        return None
    
    df = pd.read_parquet(file_path)
    # Convert the trading_date column to date only
    df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

    # Convert your filter date
    filter_date = pd.to_datetime(date).date()

    # Filter the rows
    df_filtered = df[df["trading_date"] == filter_date].copy()

    df_filtered['exchange_timestamp'] = pd.to_datetime(df_filtered['exchange_timestamp'])
    df_filtered = df_filtered.sort_values('exchange_timestamp')
    return df_filtered

def load_all_stats_hourly_data(instrument_token, date, hourly_root):
    """
    Load hourly data for specific date
    Assumes structure: hourly_root/YYYY-MM-DD/instrument_token.parquet
    """
    #date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
    #file_path = os.path.join(hourly_root, date_str, f"{instrument_token}.parquet")
    file_path = os.path.join(hourly_root,  f"{instrument_token}.parquet")
    #print("Loading Hourly data from:", file_path)
    if not os.path.exists(file_path):
        return None
    
    df = pd.read_parquet(file_path)
    # Convert the trading_date column to date only
    df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

    # Convert your filter date
    #filter_date = pd.to_datetime(date).date()

    # Filter the rows
    #df_filtered = df[df["trading_date"] == filter_date]

    df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
    df = df.sort_values('exchange_timestamp')
    return df

# ==============================================================================
# STAGE 1: EOD FILTER
# ==============================================================================

def detect_eod_crossover(eod_df, ema_period=9):
    """
    Stage 1: Detect when daily close crosses above EMA
    Returns: DataFrame with crossover dates
    """
    if len(eod_df) < ema_period:
        return None
    
    close = eod_df['ohlc_close']
    #print("Close Series:\n", close)
    # Check if EMA column exists, if not calculate
    ema_col = f'EMA_{ema_period}'
    if ema_col in eod_df.columns:
        ema = eod_df[ema_col]
    else:
        ema = close.ewm(span=ema_period, adjust=False).mean()
    
    # Detect crossover: close crosses above EMA
    prev_close = close.shift(1)
    prev_ema = ema.shift(1)
    
    crossover = (close > ema) & (prev_close <= prev_ema)
    #print("Crossover Series:\n", crossover)
    # Create result dataframe
    result = eod_df[crossover].copy()
    result['crossover_price'] = result['ohlc_close']
    result['crossover_ema'] = ema[crossover]
    
    return result


# ==============================================================================
# STAGE 2: HOURLY CONFIRMATION
# ==============================================================================
def check_hourly_confirmation_on_rsi(hourly_df, hourly_stats_df, lookback_rsi_period=10):
    """
    Hourly confirmation using RSI logic.

    Input:
    - hourly_df: ONLY today's hourly candles (no stats needed here)
    - hourly_stats_df: MULTI-DAY hourly data (must contain 'RSI' and 'exchange_timestamp')
    
    Logic:
    1. RSI crosses above 50 (on today's candles only)
    2. RSI was oversold (<30) in last N bars (based on multi-day stats)
    3. Oversold condition must occur BEFORE the cross (shifted)
    """

    # Validate
    if hourly_df is None or hourly_df.empty:
        return (False, None, None, None)
    if hourly_stats_df is None or hourly_stats_df.empty:
        return (False, None, None, None)

    # --- Ensure datetime format ---
    hourly_df = hourly_df.copy()
    hourly_stats_df = hourly_stats_df.copy()

    hourly_df["exchange_timestamp"] = pd.to_datetime(hourly_df["exchange_timestamp"])
    hourly_stats_df["exchange_timestamp"] = pd.to_datetime(hourly_stats_df["exchange_timestamp"])

    # --- Sort both dataframes ---
    hourly_df.sort_values("exchange_timestamp", inplace=True)
    hourly_stats_df.sort_values("exchange_timestamp", inplace=True)

    # --------------------------------------------
    # 1️⃣ Compute TODAY'S RSI CROSS ABOVE 50
    # --------------------------------------------
    today_rsi = hourly_stats_df[hourly_stats_df["exchange_timestamp"].isin(hourly_df["exchange_timestamp"])]["RSI_6"] 

    if today_rsi.empty:
        return (False, None, None, None)

    prev_rsi = today_rsi.shift(1)
    rsi_cross_today = (prev_rsi <= 50) & (today_rsi > 50)
    rsi_cross_today = rsi_cross_today.fillna(False)

    # --------------------------------------------
    # 2️⃣ Detect OVERSOLD (<30) using MULTI-DAY RSI
    # --------------------------------------------
    full_rsi = hourly_stats_df["RSI_6"]

    oversold_multi_day = full_rsi.rolling(lookback_rsi_period).min() < 30

    # Prevent lookahead: oversold must occur BEFORE the cross
    oversold_shifted = oversold_multi_day.shift(1)

    # Map oversold back to today's candles (align index)
    oversold_today = oversold_shifted.loc[hourly_df.index]

    # --------------------------------------------
    # 3️⃣ Final signal today
    # --------------------------------------------
    final_signal = rsi_cross_today.values & oversold_today.values

    # --------------------------------------------
    # 4️⃣ Return first entry signal (if exists)
    # --------------------------------------------
    if final_signal.any():
        idx = np.argmax(final_signal)  # first True
        entry_ts = hourly_df.iloc[idx]["exchange_timestamp"]
        entry_close = hourly_df.iloc[idx]["ohlc_close"]
        return (True, entry_ts, entry_close, "RSI Confirmation")

    return (False, None, None, None)
def check_hourly_confirmation_mean_reversion(hourly_df, hourly_stats_df, EMA_period=9):
    """
    Strategy to detect entry signals for mean reversion based on:
    1. Price pulled back to EMA over last 5 hours (within current day)
    2. Current close crosses above EMA
    3. Close is above the daily breakout level (from previous day stats)
    
    Args:
        hourly_df: DataFrame with OHLC data for current trading day only
        hourly_stats_df: DataFrame with stats across all days (for bias level)
        EMA_period: Period for EMA calculation (default 9)
    
    Returns:
        Tuple: (signal_found, entry_timestamp, entry_price, quantity)
               or (False, None, None, None) if no signal
    """
    
    if len(hourly_df) == 0:
        return (False, None, None, None)
    
    # Sort by timestamp to ensure chronological order
    hourly_df = hourly_df.sort_values('exchange_timestamp').reset_index(drop=True)
    
    # Use pre-calculated EMA from parquet
    hourly_close = hourly_df['ohlc_close']
    hourly_ema = hourly_df[f'EMA_{EMA_period}']
    hourly_low = hourly_df['ohlc_low']
    
    # Rule 1: Look back at last 5 hours to see if price pulled back to EMA
    # Find the minimum low over the last 5 candles within current day
    min_low_5h = hourly_low.rolling(window=5, min_periods=1).min()
    pulled_back = min_low_5h <= hourly_ema
    
    # Rule 2: Current close crosses UP above EMA
    # Detect crossover by checking if previous close was below EMA and current is above
    close_below_ema = hourly_close < hourly_ema
    close_above_ema = hourly_close > hourly_ema
    
    # Shift to get previous bar's relationship
    prev_close_below_ema = close_below_ema.shift(1)
    
    crossed_above = prev_close_below_ema & close_above_ema
    
    # Rule 3: Ensure pullback occurred in previous bars
    pullback_occurred = pulled_back.shift(1).fillna(False)
    
    # Get daily bias level from previous day stats
    # Assume hourly_stats_df has a column like 'daily_breakout_high' or similar
    current_date = pd.to_datetime(hourly_df['trading_date'].iloc[0]).date()
    previous_date = pd.to_datetime(current_date) - pd.Timedelta(days=1)
    
    prev_day_stats = hourly_stats_df[
        pd.to_datetime(hourly_stats_df['trading_date']).dt.date == previous_date
    ]
    
    if len(prev_day_stats) == 0:
        # If no previous day data, use current day's low as reference
        crossover_price = hourly_low.min()
    else:
        # Use previous day's high as the breakout level (bias for entry above this)
        crossover_price = prev_day_stats['ohlc_high'].iloc[-1]
    
    # Combine all conditions
    entry_signal = (
        crossed_above &              # Price just crossed above EMA
        pullback_occurred &          # Pullback occurred recently
        (hourly_close > crossover_price)  # Above previous day's high (daily bias)
    )
    
    # Find the first True entry signal
    signal_indices = entry_signal[entry_signal == True].index
    
    if len(signal_indices) > 0:
        entry_idx = signal_indices[0]
        entry_ts = hourly_df['exchange_timestamp'].iloc[entry_idx]
        entry_price = hourly_close.iloc[entry_idx]
        
        return (True, entry_ts, entry_price, 1)
    
    return (False, None, None, None)
    
def check_hourly_confirmation(hourly_df, crossover_price, consecutive_hours=3):
    """
    Checks for a minimum of 'min_consecutive' hourly candles 
    confirming momentum following a daily EMA crossover.

    Confirmation rules:
    1. The first hourly close must be greater than the daily crossover price.
    2. Subsequent hourly closes must be greater than the previous hour's open (indicating up-close/momentum).

    Args:
        hourly_df (pd.DataFrame): DataFrame containing hourly OHLC data, 
                                  with a 'crossover_price' column.
        crossover_price_col (str): Name of the column containing the daily filter price.
        min_consecutive (int): The required number of consecutive bars confirming momentum.

    Returns:
        tuple: (True, timestamp, close_price, count) on first successful confirmation, 
               or (False, None, None, 0) otherwise.
    """
    #print(crossover_price)
    if hourly_df is None:
        return False, None, None, 0
    
    # 1. Ensure the DataFrame is properly indexed for shifting
    hourly_df = hourly_df.sort_values(by='exchange_timestamp').reset_index(drop=True)
    #print("Hourly DataFrame for confirmation:\n", hourly_df)
    # Calculate the previous hour's open for the condition
    hourly_df['prev_open'] = hourly_df['ohlc_open'].shift(1)
    #print("Hourly DataFrame with prev_open:\n", hourly_df)
    # Initialize a counter for consecutive confirmations
    hourly_df['consecutive_above'] = 0
    
    # Iterate through the rows to check for the sequential pattern
    for i in range(len(hourly_df)):
        current_row = hourly_df.loc[i]
        
        # --- RULE 1: Initial Crossover Check (Only for the very first bar in sequence) ---
        
        # For the first bar (i=0): Close must be above the daily crossover price
        if i == 0:
            if current_row['ohlc_close'] >= crossover_price:
                # Start the sequence count
                hourly_df.loc[i, 'consecutive_above'] = 1
            else:
                # If the first bar fails, the sequence cannot start
                hourly_df.loc[i, 'consecutive_above'] = 0
        
        # --- RULE 2: Subsequent Momentum Check ---
        
        # For subsequent bars (i > 0): 
        # Close must be greater than the previous hour's Open AND 
        # a sequence must have already been started (i.e., previous count > 0)
        elif i > 0:
            prev_count = hourly_df.loc[i-1, 'consecutive_above']
            
            # Condition: Current Close > Previous Open
            momentum_check = current_row['ohlc_close'] > current_row['prev_open']
            
            if momentum_check and prev_count > 0:
                # Continue the sequence
                hourly_df.loc[i, 'consecutive_above'] = prev_count + 1
            else:
                # Sequence broken
                hourly_df.loc[i, 'consecutive_above'] = 0

        # --- CHECK FOR ENTRY CONDITION ---
        if hourly_df.loc[i, 'consecutive_above'] >= consecutive_hours:
            first_confirmation = hourly_df.loc[i]
            
            # Return the details of the candle that completed the sequence
            return (
                True,
                first_confirmation['exchange_timestamp'],
                first_confirmation['ohlc_close'],
                int(first_confirmation['consecutive_above'])
            )

    # If the loop finishes without meeting the condition
    return (False, None, None, 0)
    
    


# ==============================================================================
# COMBINED STRATEGY
# ==============================================================================

class TwoStageEMAStrategy:
    """
    Two-stage entry strategy:
    1. Daily close crosses above EMA (creates watchlist)
    2. Next day, hourly candles confirm by staying above hourly EMA
    """
    
    def __init__(self, config):
        self.config = config
        self.eod_signals = {}  # {date: [instruments with EOD crossover]}
        self.confirmed_entries = []  # Final entry signals
    
    def scan_eod_crossovers(self, instruments, start_date, end_date):
        """
        Stage 1: Scan all instruments for EOD crossovers
        """
        print("="*80)
        print("🔍 STAGE 1: Scanning EOD Crossovers")
        print("="*80)
        
        all_crossovers = []
        print("Instruments to scan:", len(instruments))
        for instrument_token in instruments:
            # Load EOD data
            eod_df = load_eod_data(instrument_token, self.config.EOD_DATA_ROOT)
            
            if eod_df is None:
                continue
            
            # Filter to date range
            mask = (eod_df['trading_date'] >= pd.to_datetime(start_date)) & \
                   (eod_df['trading_date'] <= pd.to_datetime(end_date))
            eod_df = eod_df[mask]
            
            # Detect crossovers
            crossovers = detect_eod_crossover(eod_df, self.config.EOD_EMA_PERIOD)
            
            if crossovers is not None and len(crossovers) > 0:
                for _, row in crossovers.iterrows():
                    crossover_date = row['trading_date']
                    next_day = crossover_date + timedelta(days=1)
                    
                    all_crossovers.append({
                        'instrument_token': instrument_token,
                        'crossover_date': crossover_date,
                        'next_trading_day': next_day,
                        'crossover_price': row['crossover_price'],
                        'crossover_ema': row['crossover_ema']
                    })
                    
                    # Store in watchlist for next day
                    if next_day not in self.eod_signals:
                        self.eod_signals[next_day] = []
                    self.eod_signals[next_day].append(instrument_token)
        
        crossovers_df = pd.DataFrame(all_crossovers) if all_crossovers else None
        
        if crossovers_df is not None:
            print(f"✅ Found {len(crossovers_df)} EOD crossovers")
            print(f"📅 Dates to watch: {sorted(self.eod_signals.keys())}")
            print(f"\nSample crossovers:")
            print(crossovers_df.head(10))
        else:
            print("❌ No EOD crossovers found")
        
        return crossovers_df
    
    def check_hourly_confirmations(self, crossovers_df):
        """
        Stage 2: For each EOD crossover, check hourly confirmation next day
        """
        print("\n" + "="*80)
        print("🔍 STAGE 2: Checking Hourly Confirmations")
        print("="*80)
        #print(("crossovers_df:", crossovers_df ))
        if crossovers_df is None or len(crossovers_df) == 0:
            print("❌ No EOD signals to confirm")
            return None
        
        confirmations = []
        
        for _, crossover in crossovers_df.iterrows():
            instrument_token = crossover['instrument_token']
            next_day = crossover['next_trading_day']
            crossover_price = crossover['crossover_price']
            # Load hourly data for next day
            hourly_df = load_hourly_data(
                instrument_token,
                next_day,
                self.config.HOURLY_DATA_ROOT
            )
            hourly_stats_df = load_all_stats_hourly_data(
                instrument_token,
                next_day,
                self.config.HOURLY_DATA_ROOT
            )
            if hourly_df is None:
                continue
            
            #hourly_df["crossover_price"] = crossover_price
            # Limit to first few hours (don't wait all day)
            if self.config.MAX_HOURS_TO_WAIT:
                hourly_df = hourly_df.head(self.config.MAX_HOURS_TO_WAIT)
            
            # Check confirmation
            # confirmed, entry_time, entry_price, num_candles = check_hourly_confirmation(
            #     hourly_df,
            #     self.config.HOURLY_EMA_PERIOD,
            #     self.config.CONSECUTIVE_HOURS_ABOVE_EMA
            # )
            # confirmed, entry_time, entry_price, num_candles = check_hourly_confirmation(
            #     hourly_df,
            #     crossover_price,
            #     self.config.CONSECUTIVE_HOURS_ABOVE_EMA
            # )
            confirmed, entry_time, entry_price, num_candles = check_hourly_confirmation_on_rsi(
                hourly_df,hourly_stats_df,
                self.config.LOOKBACK_RSI_PERIOD,
            )
            
            # confirmed, entry_time, entry_price, num_candles = check_hourly_confirmation_mean_reversion(
            #     hourly_df,hourly_stats_df,
            #     self.config.HOURLY_EMA_PERIOD,
            # )
            if confirmed:
                confirmations.append({
                    'instrument_token': instrument_token,
                    'eod_crossover_date': crossover['crossover_date'],
                    'eod_crossover_price': crossover['crossover_price'],
                    'entry_date': next_day,
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'hours_above_ema': num_candles,
                    'eod_ema': crossover['crossover_ema']
                })
                
                print(f"✅ {instrument_token}: Confirmed after {num_candles} hours above EMA")
        
        confirmations_df = pd.DataFrame(confirmations) if confirmations else None
        
        if confirmations_df is not None:
            print(f"\n🎯 Total Confirmed Entries: {len(confirmations_df)}")
            self.confirmed_entries = confirmations
        else:
            print("\n❌ No hourly confirmations found")
        
        return confirmations_df
    
    def generate_entry_signals(self, instruments, start_date, end_date):
        """
        Run complete two-stage analysis
        """
        # Stage 1: EOD crossovers
        crossovers_df = self.scan_eod_crossovers(instruments, start_date, end_date)
        
        # Stage 2: Hourly confirmations
        confirmations_df = self.check_hourly_confirmations(crossovers_df)
        
        return crossovers_df, confirmations_df


# ==============================================================================
# BACKTESTING WITH TWO-STAGE SIGNALS
# ==============================================================================

def backtest_two_stage_strategy(confirmations_df, config, instruments):
    """
    Backtest using confirmed entry signals
    """
    if confirmations_df is None or len(confirmations_df) == 0:
        print("⚠️  No confirmed signals to backtest")
        return None
    
    print("\n" + "="*80)
    print("📊 BACKTESTING CONFIRMED SIGNALS")
    print("="*80)
    
    trades = []
    
    for _, signal in confirmations_df.iterrows():
        instrument_token = signal['instrument_token']
        entry_time = signal['entry_time']
        entry_price = signal['entry_price']
        
        # Load subsequent hourly data to track position
        entry_date = pd.to_datetime(signal['entry_date'])
        
        # Load multiple days of hourly data after entry (for position tracking)
        position_data = []
        for days_ahead in range(5):  # Track for 5 days
            future_date = entry_date + timedelta(days=days_ahead)
            hourly_df = load_hourly_data(
                instrument_token,
                future_date,
                config.HOURLY_DATA_ROOT
            )
            if hourly_df is not None:
                position_data.append(hourly_df)
        
        if not position_data:
            continue
        
        # Combine all position tracking data
        position_df = pd.concat(position_data, ignore_index=True)
        position_df = position_df.sort_values('exchange_timestamp')
        
        # Filter to after entry time
        position_df = position_df[position_df['exchange_timestamp'] >= entry_time]
        
        if len(position_df) == 0:
            continue
        
        # Calculate stop loss
        stop_loss = entry_price * (1 - config.STOP_LOSS_PCT / 100)
        trailing_stop = entry_price * (1 - config.TRAILING_STOP_PCT / 100)
        highest_price = entry_price
        
        # Track position
        exit_price = None
        exit_time = None
        exit_reason = None
        
        for _, candle in position_df.iterrows():
            current_price = candle['ohlc_close']
            current_low = candle['ohlc_low']
            
            # Update trailing stop
            if current_price > highest_price:
                highest_price = current_price
                trailing_stop = highest_price * (1 - config.TRAILING_STOP_PCT / 100)
            
            # Check stop loss
            if current_low <= stop_loss:
                exit_price = stop_loss
                exit_time = candle['exchange_timestamp']
                exit_reason = 'STOP_LOSS'
                break
            
            # Check trailing stop
            if current_low <= trailing_stop:
                exit_price = trailing_stop
                exit_time = candle['exchange_timestamp']
                exit_reason = 'TRAILING_STOP'
                break
        
        # If no exit, mark as open position
        if exit_price is None:
            exit_price = position_df.iloc[-1]['ohlc_close']
            exit_time = position_df.iloc[-1]['exchange_timestamp']
            exit_reason = 'OPEN'
        
        # Calculate P&L
        pnl = exit_price - entry_price
        pnl_pct = (pnl / entry_price) * 100
        holding_hours = (exit_time - entry_time).total_seconds() / 3600
        
        trades.append({
            'instrument_token': instrument_token,
            'eod_crossover_date': signal['eod_crossover_date'],
            'entry_time': entry_time,
            'entry_price': entry_price,
            'exit_time': exit_time,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'holding_hours': holding_hours,
            'highest_price': highest_price
        })
    
    trades_df = pd.DataFrame(trades)
    
    # Print results
    if len(trades_df) > 0:
        completed_trades = trades_df[trades_df['exit_reason'] != 'OPEN']
        
        if len(completed_trades) > 0:
            print(f"\n📈 BACKTEST RESULTS:")
            print(f"   Total Trades: {len(completed_trades)}")
            print(f"   Win Rate: {(completed_trades['pnl'] > 0).mean() * 100:.2f}%")
            print(f"   Total P&L: ₹{completed_trades['pnl'].sum():+,.2f}")
            print(f"   Average P&L: ₹{completed_trades['pnl'].mean():+,.2f}")
            print(f"   Average Holding: {completed_trades['holding_hours'].mean():.1f} hours")
            
            print(f"\n🎯 EXIT REASONS:")
            print(completed_trades['exit_reason'].value_counts())
    
    return trades_df


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    
    # Configuration
    config = TwoStageConfig()
    
    # Instruments to analyze
   # INSTRUMENTS = [6401, 3329, 424961]  # Add your instruments
    
    # Date range
    START_DATE = "2025-11-01"
    END_DATE = "2025-12-02"
    
    print("="*80)
    print("🚀 TWO-STAGE EMA BREAKOUT STRATEGY")
    print("="*80)
    print(f"Stage 1: Daily close crosses above EMA {config.EOD_EMA_PERIOD}")
    print(f"Stage 2: {config.CONSECUTIVE_HOURS_ABOVE_EMA} consecutive hourly candles above EMA {config.HOURLY_EMA_PERIOD}")
    print(f"\nDate Range: {START_DATE} to {END_DATE}")
    print(f"Instruments: {len(INSTRUMENTS)}")
    
    # Initialize strategy
    strategy = TwoStageEMAStrategy(config)
    
    # Generate signals
    crossovers_df, confirmations_df = strategy.generate_entry_signals(
        INSTRUMENTS,
        START_DATE,
        END_DATE
    )
    
    # Backtest
    if confirmations_df is not None:
        trades_df = backtest_two_stage_strategy(confirmations_df, config, INSTRUMENTS)
        
        # Save results
        if trades_df is not None:
            output_dir = "two_stage_strategy_results"
            os.makedirs(output_dir, exist_ok=True)
            
            crossovers_df.to_parquet(f"{output_dir}/eod_crossovers.parquet", index=False)
            confirmations_df.to_parquet(f"{output_dir}/confirmed_entries.parquet", index=False)
            trades_df.to_parquet(f"{output_dir}/trades.parquet", index=False)
            
            print(f"\n💾 Results saved to: {output_dir}/")
    
    print("\n" + "="*80)
    print("✅ ANALYSIS COMPLETE")
    print("="*80)