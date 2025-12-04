"""
Fixed VectorBT-Based RSI Mean Reversion Strategy Backtester
Correct logic:
T=0: EOD breakout detection
T=1: RSI entry signal on next day
T=2: RSI exit signal on exit day (can be same day or next day)
Multiple entries allowed on different days, max 1 entry per day
"""

import os
import pandas as pd
import numpy as np
import vectorbt as vbt
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

from config import example_tokens, instruments_to_process
# ==============================================================================
# CONFIGURATION
# ==============================================================================

class BacktestConfig:
    EOD_DATA_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_eod"
    HOURLY_DATA_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_hourly"
    
    INSTRUMENTS_TO_PROCESS = example_tokens
    #INSTRUMENTS_TO_PROCESS = [1270529]

    
    RSI_ENTRY_THRESHOLD = 10
    RSI_EXIT_THRESHOLD = 90
    RSI_COLUMN = 'RSI_6'
    MAX_HOLD_DAYS = 3  # Max days to hold if no exit signal

    START_DATE = "2025-11-01"
    END_DATE = "2025-12-04"
    
    INIT_CASH = 100000
    FEES = 0.001


# ==============================================================================
# DATA LOADING
# ==============================================================================

def load_eod_data(token, eod_path):
    """Load EOD data for a token"""
    file_path = os.path.join(eod_path, f"{token}.parquet")
    
    if not os.path.exists(file_path):
        return None
    
    try:
        df = pd.read_parquet(file_path)
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        return df.sort_values('trading_date').reset_index(drop=True)
    except Exception as e:
        print(f"  ❌ Error loading EOD data for {token}: {e}")
        return None


def load_hourly_data(token, hourly_path):
    """Load hourly data for a token"""
    file_path = os.path.join(hourly_path, f"{token}.parquet")
    
    if not os.path.exists(file_path):
        return None
    
    try:
        df = pd.read_parquet(file_path)
        df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
        df['trading_date'] = pd.to_datetime(df['trading_date'])
        return df.sort_values('exchange_timestamp').reset_index(drop=True)
    except Exception as e:
        print(f"  ❌ Error loading hourly data for {token}: {e}")
        return None


# ==============================================================================
# SIGNAL GENERATION
# ==============================================================================

def get_breakout_dates(eod_df):
    """
    T=0: Identify dates where close > open AND close > previous close
    Returns list of dates that have breakout signal
    """
    eod_df = eod_df.sort_values('trading_date').reset_index(drop=True)
    breakout_dates = []
    
    for i in range(1, len(eod_df)):
        current_row = eod_df.iloc[i]
        previous_row = eod_df.iloc[i-1]
        
        curr_close = current_row['ohlc_close']
        curr_open = current_row['ohlc_open']
        prev_close = previous_row['ohlc_close']
        prev_open = previous_row['ohlc_open']
        
        # Breakout condition, redundant "and curr_close > prev_open"
        if curr_close > curr_open   and  prev_close > prev_open and curr_close > prev_close:
            print(current_row['trading_date'].date())
            breakout_dates.append(current_row['trading_date'].date())
    
    return breakout_dates


def get_entry_signals_for_day(hourly_day_df, rsi_column='RSI_6', rsi_threshold=25):
    """
    T=1: For a given trading day, find RSI entry point
    - RSI goes below threshold
    - Then reverses and increases
    Returns index of entry candle (only first one per day)
    """
    
    if rsi_column not in hourly_day_df.columns or len(hourly_day_df) < 2:
        return None
    
    rsi = hourly_day_df[rsi_column].values
    
    # Find where RSI goes below threshold
    below_threshold_indices = np.where(rsi < rsi_threshold)[0]
    
    if len(below_threshold_indices) == 0:
        return None
    
    # Find reversal: RSI increasing after being below threshold
    for i in range(1, len(rsi)):
        # Check if any point before current was below threshold
        if np.any(rsi[:i] < rsi_threshold):
            # Check if RSI is increasing (current > previous)
            if rsi[i] > rsi[i-1]:
                return i  # Return first reversal point
    
    return None


def get_exit_signals_for_day(hourly_day_df, entry_idx, rsi_column='RSI_6', rsi_threshold=80):
    """
    T=2: For a given trading day, find RSI exit point from entry index
    - RSI goes above threshold
    - Then reverses and decreases
    Returns index of exit candle (only first one after entry)
    """
    
    if rsi_column not in hourly_day_df.columns or entry_idx >= len(hourly_day_df):
        return None
    
    rsi = hourly_day_df[rsi_column].values
    
    # Search from entry index onwards
    rsi_subset = rsi[entry_idx:]
    
    # Find where RSI goes above threshold
    above_threshold_indices = np.where(rsi_subset > rsi_threshold)[0]
    
    if len(above_threshold_indices) == 0:
        return None
    
    # Find reversal: RSI decreasing after being above threshold
    for i in range(1, len(rsi_subset)):
        if np.any(rsi_subset[:i] > rsi_threshold):
            if rsi_subset[i] < rsi_subset[i-1]:
                return entry_idx + i  # Return first reversal point
    
    return None


# ==============================================================================
# TRADE GENERATION
# ==============================================================================

def generate_trades_for_instrument(hourly_df, eod_df, config):
    """
    Generate entry/exit signals for an instrument
    Multiple trades allowed on different days, max 1 per day
    """
    
    # Get breakout dates
    breakout_dates = get_breakout_dates(eod_df)
    
    if not breakout_dates:
        return []
    
    breakout_dates_file = "breakout_dates.csv"
    df = pd.DataFrame(breakout_dates)
    df["token"]= hourly_df['instrument_token'].iloc[0]
    df.to_csv(breakout_dates_file, mode='a', header=False, index=False)
    #df.to_csv(breakout_dates_file, index=False)
    print(f"\n💾 Trade details saved to: {breakout_dates_file}")

    hourly_df = hourly_df.sort_values('exchange_timestamp').reset_index(drop=True)
    trades = []
    
    # For each breakout date, look for entry on next trading day
    unique_hourly_dates = sorted(hourly_df['trading_date'].dt.date.unique())
    
    for breakout_date in breakout_dates:
        # Find next trading day after breakout
        breakout_idx = unique_hourly_dates.index(breakout_date) if breakout_date in unique_hourly_dates else -1
        
        if breakout_idx == -1 or breakout_idx + 1 >= len(unique_hourly_dates):
            continue
        
        print("Unique hourly date at breakout_idx:", unique_hourly_dates[breakout_idx+1])
        entry_date = unique_hourly_dates[breakout_idx + 1]
        
        # Get hourly data for entry day
        entry_day_hourly = hourly_df[hourly_df['trading_date'].dt.date == entry_date].reset_index(drop=True)
        
        if len(entry_day_hourly) == 0:
            continue
        
        # T=1: Find entry signal on entry_date
        entry_idx = get_entry_signals_for_day(entry_day_hourly, config.RSI_COLUMN, config.RSI_ENTRY_THRESHOLD)
        
        if entry_idx is None:
            continue
        
        entry_row = entry_day_hourly.iloc[entry_idx]
        entry_price = entry_row['ohlc_close']
        entry_timestamp = entry_row['exchange_timestamp']
        entry_rsi = entry_row[config.RSI_COLUMN]
        
        # T=2: Look for exit signal
        # First try to find exit on same day
        # exit_idx = get_exit_signals_for_day(entry_day_hourly, entry_idx, config.RSI_COLUMN, config.RSI_EXIT_THRESHOLD)
        
        # if exit_idx is not None:
        #     # Exit found on same day
        #     exit_row = entry_day_hourly.iloc[exit_idx]
        #     exit_price = exit_row['ohlc_close']
        #     exit_timestamp = exit_row['exchange_timestamp']
        #     exit_rsi = exit_row[config.RSI_COLUMN]
        #     exit_date = entry_date
        # else:
        #     # No exit found on entry day, use close of same day
        #     exit_row = entry_day_hourly.iloc[-1]  # Last candle of the day
        #     exit_price = exit_row['ohlc_close']
        #     exit_timestamp = exit_row['exchange_timestamp']
        #     exit_rsi = exit_row[config.RSI_COLUMN]
        #     exit_date = entry_date
        # T=2: Look for exit signal across multiple days
        exit_found = False
        exit_row = None

        # Search from entry_date onwards through following days
        entry_date_idx = unique_hourly_dates.index(entry_date)

        #for day_offset in range(len(unique_hourly_dates) - entry_date_idx):
        for day_offset in range(min(config.MAX_HOLD_DAYS + 1, len(unique_hourly_dates) - entry_date_idx)):
            print("Searching exit in date range:", len(unique_hourly_dates) - entry_date_idx)
            search_date = unique_hourly_dates[entry_date_idx + day_offset]
            search_day_hourly = hourly_df[hourly_df['trading_date'].dt.date == search_date].reset_index(drop=True)
            
            if len(search_day_hourly) == 0:
                continue
            
            # For first day, start from entry_idx; for subsequent days, start from 0
            start_idx = entry_idx if day_offset == 0 else 0
            
            exit_idx = get_exit_signals_for_day(search_day_hourly, start_idx, config.RSI_COLUMN, config.RSI_EXIT_THRESHOLD)
            
            if exit_idx is not None:
                exit_row = search_day_hourly.iloc[exit_idx]
                exit_price = exit_row['ohlc_close']
                exit_timestamp = exit_row['exchange_timestamp']
                exit_rsi = exit_row[config.RSI_COLUMN]
                exit_date = search_date
                exit_found = True
                break

        # If no exit signal found after N days, exit at last candle of last day checked
        if not exit_found:
            last_search_date = unique_hourly_dates[min(entry_date_idx + config.MAX_HOLD_DAYS, len(unique_hourly_dates) - 1)]  # Look max 5 days ahead
            last_day_hourly = hourly_df[hourly_df['trading_date'].dt.date == last_search_date].reset_index(drop=True)
            exit_row = last_day_hourly.iloc[-1]
            exit_price = exit_row['ohlc_close']
            exit_timestamp = exit_row['exchange_timestamp']
            exit_rsi = exit_row[config.RSI_COLUMN]
            exit_date = last_search_date
        # Calculate PnL
        pnl = exit_price - entry_price
        pnl_pct = (pnl / entry_price) * 100
        
        trades.append({
            'breakout_date': breakout_date,
            'entry_date': entry_date,
            'entry_timestamp': entry_timestamp,
            'entry_price': entry_price,
            'entry_rsi': entry_rsi,
            'exit_date': exit_date,
            'exit_timestamp': exit_timestamp,
            'exit_price': exit_price,
            'exit_rsi': exit_rsi,
            'pnl': pnl,
            'pnl_pct': pnl_pct
        })
    
    return trades


# ==============================================================================
# BACKTEST ENGINE
# ==============================================================================

def backtest_instrument(token, config):
    """
    Run backtest for a single instrument
    """
    print(f"\n🔄 Backtesting Token: {token}")
    
    # Load data
    eod_df = load_eod_data(token, config.EOD_DATA_PATH)
    hourly_df = load_hourly_data(token, config.HOURLY_DATA_PATH)
    
    if eod_df is None or hourly_df is None:
        print(f"  ⚠️  Skipping {token} - missing data")
        return None
    
    # Filter by date range
    start_date = pd.to_datetime(config.START_DATE)
    end_date = pd.to_datetime(config.END_DATE)
    
    eod_df = eod_df[(eod_df['trading_date'] >= start_date) & (eod_df['trading_date'] <= end_date)]
    hourly_df = hourly_df[(hourly_df['exchange_timestamp'] >= start_date) & (hourly_df['exchange_timestamp'] <= end_date)]
    
    if len(eod_df) == 0 or len(hourly_df) == 0:
        print(f"  ⚠️  No data in date range for {token}")
        return None
    
    # Generate trades
    trades = generate_trades_for_instrument(hourly_df, eod_df, config)
    
    if not trades:
        print(f"  ℹ️  No trades generated for {token}")
        return pd.DataFrame()
    
    trades_df = pd.DataFrame(trades)
    
    print(f"  ✓ Generated {len(trades_df)} trades")
    print(f"  ✓ Winning trades: {(trades_df['pnl'] > 0).sum()}")
    print(f"  ✓ Losing trades: {(trades_df['pnl'] < 0).sum()}")
    print(f"  ✓ Total PnL: {trades_df['pnl'].sum():.2f}")
    print(f"  ✓ Win Rate: {(trades_df['pnl'] > 0).sum() / len(trades_df) * 100:.2f}%")
    
    return trades_df


# ==============================================================================
# RESULTS AGGREGATION
# ==============================================================================

def run_backtest(config):
    """
    Run backtest across all instruments
    """
    
    print("=" * 80)
    print("🚀 RSI MEAN REVERSION STRATEGY BACKTESTER (FIXED LOGIC)")
    print("=" * 80)
    
    print(f"\n⚙️  Configuration:")
    print(f"  Instruments: {config.INSTRUMENTS_TO_PROCESS}")
    print(f"  Date Range: {config.START_DATE} to {config.END_DATE}")
    print(f"  Entry RSI Threshold: {config.RSI_ENTRY_THRESHOLD}")
    print(f"  Exit RSI Threshold: {config.RSI_EXIT_THRESHOLD}")
    
    all_trades = []
    
    # Backtest each instrument
    for token in config.INSTRUMENTS_TO_PROCESS:
        try:
            trades_df = backtest_instrument(token, config)
            
            if trades_df is not None and len(trades_df) > 0:
                trades_df['token'] = token
                all_trades.append(trades_df)
        
        except Exception as e:
            print(f"  ❌ Error backtesting {token}: {e}")
            continue
    
    # Combine all trades
    if not all_trades:
        print("\n❌ No trades generated")
        return None
    
    combined_trades = pd.concat(all_trades, ignore_index=True)
    
    # Calculate statistics
    print("\n" + "=" * 80)
    print("📊 BACKTEST RESULTS")
    print("=" * 80)
    
    print(f"\n✓ Total Trades: {len(combined_trades)}")
    print(f"✓ Winning Trades: {(combined_trades['pnl'] > 0).sum()}")
    print(f"✓ Losing Trades: {(combined_trades['pnl'] < 0).sum()}")
    print(f"✓ Win Rate: {(combined_trades['pnl'] > 0).sum() / len(combined_trades) * 100:.2f}%")
    
    print(f"\n💰 PnL Statistics:")
    print(f"  Total PnL: {combined_trades['pnl'].sum():.2f}")
    print(f"  Average PnL: {combined_trades['pnl'].mean():.2f}")
    print(f"  Average PnL %: {combined_trades['pnl_pct'].mean():.2f}%")
    print(f"  Max PnL: {combined_trades['pnl'].max():.2f}")
    print(f"  Min PnL: {combined_trades['pnl'].min():.2f}")
    print(f"  Std Dev: {combined_trades['pnl'].std():.2f}")
    
    print(f"\n📈 Trades by Instrument:")
    for token in combined_trades['token'].unique():
        token_trades = combined_trades[combined_trades['token'] == token]
        print(f"  Token {token}: {len(token_trades)} trades, PnL: {token_trades['pnl'].sum():.2f}, Win Rate: {(token_trades['pnl'] > 0).sum() / len(token_trades) * 100:.2f}%")
    
    # Save results
    trades_file = "backtest_trades_detailed.csv"
    combined_trades.to_csv(trades_file, index=False)
    print(f"\n💾 Trade details saved to: {trades_file}")
    
    # Save summary
    summary_data = []
    for token in combined_trades['token'].unique():
        token_trades = combined_trades[combined_trades['token'] == token]
        summary_data.append({
            'token': token,
            'num_trades': len(token_trades),
            'winning_trades': (token_trades['pnl'] > 0).sum(),
            'losing_trades': (token_trades['pnl'] < 0).sum(),
            'win_rate': (token_trades['pnl'] > 0).sum() / len(token_trades) * 100,
            'total_pnl': token_trades['pnl'].sum(),
            'avg_pnl': token_trades['pnl'].mean(),
            'avg_pnl_pct': token_trades['pnl_pct'].mean()
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_file = "backtest_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"💾 Summary saved to: {summary_file}")
    
    return combined_trades


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    config = BacktestConfig()
    
    results = run_backtest(config)
    
    if results is not None:
        print("\n" + "=" * 80)
        print("✅ BACKTEST COMPLETE")
        print("=" * 80)