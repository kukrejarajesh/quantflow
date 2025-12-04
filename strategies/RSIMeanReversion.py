"""
VectorBT-Based RSI Mean Reversion Strategy Backtester
Leverages vectorbt for fast, vectorized backtesting across instruments
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
    # Data paths
    EOD_DATA_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_eod"
    HOURLY_DATA_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_hourly"
    
    # Instruments to backtest
    INSTRUMENTS_TO_PROCESS = example_tokens
    
    # Strategy parameters
    RSI_ENTRY_THRESHOLD = 30
    RSI_EXIT_THRESHOLD = 70
    RSI_COLUMN = 'RSI_6'
    
    # Backtest period
    START_DATE = "2025-11-01"
    END_DATE = "2025-12-02"
    
    # VectorBT settings
    INIT_CASH = 100000  # Initial capital
    FEES = 0.001  # 0.1% commission


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
# SIGNAL GENERATION (VECTORIZED)
# ==============================================================================

def generate_entry_signals(hourly_df, rsi_column='RSI_6', rsi_threshold=25):
    """
    Generate entry signals where RSI crosses below threshold then reverses up
    Returns boolean series
    """
    
    if rsi_column not in hourly_df.columns:
        return pd.Series(False, index=hourly_df.index)
    
    rsi = hourly_df[rsi_column].values
    
    # Detect RSI below threshold
    rsi_below = rsi < rsi_threshold
    print("@@@@@@@@RSI BELOW ARRAY@@@@@@@@")
    print(f"rsi_below sum: {rsi_below.sum()}")
    # Detect RSI increasing (positive change)
    rsi_change = np.diff(rsi, prepend=np.nan)
    rsi_increasing = rsi_change > 0
    
    # Entry signal: was below threshold and now increasing
    entry_signals = np.zeros(len(rsi), dtype=bool)
    
    for i in range(1, len(rsi)):
        # Check if any point before current had RSI below threshold
        if rsi_below[:i].any() and rsi_increasing[i]:
            
            entry_signals[i] = True
            break  # Only one entry per signal
    # print("!!!!!!!!!!!!!!!!!!!!!@@@@@@@@ENTRY SIGNALS ARRAY@@@@@@@@")
    # print("entry_signals sum:", entry_signals.sum())
    return pd.Series(entry_signals, index=hourly_df.index)


def generate_exit_signals(hourly_df, entry_index, rsi_column='RSI_6', rsi_threshold=80):
    """
    Generate exit signals from entry point where RSI crosses above threshold then reverses down
    Returns boolean series
    """
    
    if rsi_column not in hourly_df.columns:
        return pd.Series(False, index=hourly_df.index)
    
    rsi = hourly_df[rsi_column].values
    
    if entry_index >= len(rsi):
        return pd.Series(False, index=hourly_df.index)
    
    # Detect RSI above threshold (from entry onwards)
    rsi_above = rsi[entry_index:] > rsi_threshold
    
    # Detect RSI decreasing
    rsi_change = np.diff(rsi[entry_index:], prepend=np.nan)
    rsi_decreasing = rsi_change < 0
    
    # Exit signal: was above threshold and now decreasing
    exit_signals = np.zeros(len(rsi), dtype=bool)
    
    for i in range(1, len(rsi_above)):
        if rsi_above[:i].any() and rsi_decreasing[i]:
            exit_signals[entry_index + i] = True
            break
    
    return pd.Series(exit_signals, index=hourly_df.index)


def generate_breakout_filter(eod_df, hourly_df):
    """
    Create a filter based on EOD breakout conditions
    Returns array indicating which hourly candles are on breakout days
    """
    
    hourly_df = hourly_df.copy()
    trading_dates = hourly_df['trading_date'].dt.date.unique()
    
    breakout_dates = []
    
    for i in range(1, len(trading_dates)):
        current_date = trading_dates[i]
        prev_date = trading_dates[i-1]
        
        current_eod = eod_df[eod_df['trading_date'].dt.date == current_date]
        prev_eod = eod_df[eod_df['trading_date'].dt.date == prev_date]
        
        if len(current_eod) == 0 or len(prev_eod) == 0:
            continue
        
        curr_close = current_eod['ohlc_close'].iloc[0]
        curr_open = current_eod['ohlc_open'].iloc[0]
        prev_close = prev_eod['ohlc_close'].iloc[-1]
        prev_open = prev_eod['ohlc_open'].iloc[-1]
        
        # Breakout condition
        if curr_close > curr_open and curr_close > prev_close and curr_close > prev_open:
            breakout_dates.append(current_date)
    
    # Create filter array
    breakout_filter = hourly_df['trading_date'].dt.date.isin(breakout_dates).values
    
    return breakout_filter


# ==============================================================================
# VECTORBT BACKTEST
# ==============================================================================

def backtest_with_vectorbt(hourly_df, eod_df, token, config):
    """
    Run vectorized backtest using VectorBT
    """
    print(f"\n🔄 Backtesting Token: {token}")
    
    # Filter data by date range
    start_date = pd.to_datetime(config.START_DATE)
    end_date = pd.to_datetime(config.END_DATE)
    
    hourly_df = hourly_df[
        (hourly_df['exchange_timestamp'] >= start_date) &
        (hourly_df['exchange_timestamp'] <= end_date)
    ].copy()
    
    if len(hourly_df) == 0:
        print(f"  ⚠️  No data in date range")
        return None
    
    # Set index to timestamp for VectorBT
    hourly_df.set_index('exchange_timestamp', inplace=True)
    
    # Generate entry signals
    entry_signals = generate_entry_signals(hourly_df, config.RSI_COLUMN, config.RSI_ENTRY_THRESHOLD)
    
    # Generate exit signals (simplified - same day exit after RSI reversal)
    exit_signals = pd.Series(False, index=hourly_df.index)
    
    for i, entry in enumerate(entry_signals):
        if entry:
            # Look for exit after this entry
            exit_sig = generate_exit_signals(hourly_df, i, config.RSI_COLUMN, config.RSI_EXIT_THRESHOLD)
            exit_signals = exit_signals | exit_sig
    
    # Apply breakout filter
    breakout_filter = generate_breakout_filter(eod_df, hourly_df.reset_index())
    entry_signals = entry_signals & breakout_filter
    
    # Create portfolio with signals using close prices directly
    portfolio = vbt.Portfolio.from_signals(
        hourly_df['ohlc_close'],
        entries=entry_signals,
        exits=exit_signals,
        init_cash=config.INIT_CASH,
        fees=config.FEES,
        freq='1h'
    )
    
    return portfolio, entry_signals, exit_signals


# ==============================================================================
# RESULTS ANALYSIS
# ==============================================================================

def analyze_portfolio(portfolio, token):
    """
    Analyze portfolio performance using VectorBT stats and trade details
    """
    
    print(f"\n  📊 Performance Metrics for Token {token}:")
    
    try:
        # Get all stats from portfolio
        stats = portfolio.stats()
        
        # Extract key metrics
        total_return = stats.get('Return [%]', 0) / 100
        sharpe = stats.get('Sharpe Ratio', 0)
        max_drawdown = stats.get('Max Drawdown [%]', 0) / 100
        num_trades = stats.get('Total Trades', 0)
        win_rate = stats.get('Win Rate [%]', 0) / 100
        avg_pnl = stats.get('Avg Trade PnL', 0)
        total_pnl = stats.get('Total PnL', 0)
        
        print(f"    Total Return: {total_return*100:.2f}%")
        print(f"    Sharpe Ratio: {sharpe:.2f}")
        print(f"    Max Drawdown: {max_drawdown*100:.2f}%")
        print(f"    Win Rate: {win_rate*100:.2f}%")
        print(f"    Total Trades: {num_trades:.0f}")
        print(f"    Avg Trade PnL: {avg_pnl:.2f}")
        print(f"    Total PnL: {total_pnl:.2f}")
        
        # Get trade details
        trades_records = portfolio.trades.records
        
        if len(trades_records) > 0:
            print(f"\n  📋 Trade Details:")
            print(f"    {'Entry Time':<20} {'Entry Price':<12} {'Exit Time':<20} {'Exit Price':<12} {'PnL':<10} {'PnL %':<10}")
            print(f"    {'-'*94}")
            
            trades_list = []
            
            for trade in trades_records:
                entry_idx = trade['entry_idx']
                exit_idx = trade['exit_idx']
                entry_price = trade['entry_price']
                exit_price = trade['exit_price']
                pnl = trade['pnl']
                pnl_pct = (pnl / entry_price) * 100 if entry_price != 0 else 0
                
                # Get timestamps from portfolio index
                try:
                    entry_time = portfolio.index[int(entry_idx)]
                    exit_time = portfolio.index[int(exit_idx)] if exit_idx < len(portfolio.index) else 'N/A'
                except:
                    entry_time = f"Idx {entry_idx}"
                    exit_time = f"Idx {exit_idx}"
                
                print(f"    {str(entry_time):<20} {entry_price:<12.2f} {str(exit_time):<20} {exit_price:<12.2f} {pnl:<10.2f} {pnl_pct:<10.2f}")
                
                trades_list.append({
                    'token': token,
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'exit_time': exit_time,
                    'exit_price': exit_price,
                    'pnl': pnl,
                    'pnl_pct': pnl_pct
                })
            
            return {
                'token': token,
                'total_return': total_return,
                'sharpe_ratio': sharpe,
                'max_drawdown': max_drawdown,
                'win_rate': win_rate,
                'num_trades': num_trades,
                'avg_pnl': avg_pnl,
                'total_pnl': total_pnl,
                'trades': trades_list
            }
        else:
            print(f"    ⚠️  No trades executed")
            
            return {
                'token': token,
                'total_return': total_return,
                'sharpe_ratio': sharpe,
                'max_drawdown': max_drawdown,
                'win_rate': win_rate,
                'num_trades': 0,
                'avg_pnl': 0,
                'total_pnl': 0,
                'trades': []
            }
    
    except Exception as e:
        print(f"    ❌ Error analyzing portfolio: {e}")
        return None


# ==============================================================================
# MAIN BACKTEST ENGINE
# ==============================================================================

def run_backtest(config):
    """
    Run VectorBT-based backtest across all instruments
    """
    
    print("=" * 80)
    print("🚀 VECTORBT RSI MEAN REVERSION STRATEGY BACKTESTER")
    print("=" * 80)
    
    print(f"\n⚙️  Configuration:")
    print(f"  Instruments: {config.INSTRUMENTS_TO_PROCESS}")
    print(f"  Date Range: {config.START_DATE} to {config.END_DATE}")
    print(f"  Entry RSI Threshold: {config.RSI_ENTRY_THRESHOLD}")
    print(f"  Exit RSI Threshold: {config.RSI_EXIT_THRESHOLD}")
    print(f"  Initial Capital: ${config.INIT_CASH:,.0f}")
    print(f"  Fees: {config.FEES*100:.2f}%")
    
    results = []
    
    # Backtest each instrument
    for token in config.INSTRUMENTS_TO_PROCESS:
        try:
            eod_df = load_eod_data(token, config.EOD_DATA_PATH)
            hourly_df = load_hourly_data(token, config.HOURLY_DATA_PATH)
            
            if eod_df is None or hourly_df is None:
                print(f"  ⚠️  Skipping {token} - missing data")
                continue
            
            # Run backtest
            portfolio, entries, exits = backtest_with_vectorbt(hourly_df, eod_df, token, config)
            
            if portfolio is None:
                continue
            
            # Analyze results
            result = analyze_portfolio(portfolio, token)
            
            if result:
                results.append(result)
        
        except Exception as e:
            print(f"  ❌ Error backtesting {token}: {e}")
            continue
    
    # Aggregate results
    if not results:
        print("\n❌ No results generated")
        return None
    
    results_df = pd.DataFrame(results)
    
    # Extract all trades from results
    all_trades = []
    for result in results:
        if result and 'trades' in result and result['trades']:
            all_trades.extend(result['trades'])
    
    trades_df = pd.DataFrame(all_trades) if all_trades else None
    
    # Print aggregate statistics
    print("\n" + "=" * 80)
    print("📊 AGGREGATE RESULTS")
    print("=" * 80)
    
    print(f"\n✓ Instruments Tested: {len(results_df)}")
    print(f"✓ Average Total Return: {results_df['total_return'].mean()*100:.2f}%")
    print(f"✓ Average Sharpe Ratio: {results_df['sharpe_ratio'].mean():.2f}")
    print(f"✓ Average Max Drawdown: {results_df['max_drawdown'].mean()*100:.2f}%")
    print(f"✓ Average Win Rate: {results_df['win_rate'].mean()*100:.2f}%")
    print(f"✓ Total Trades (All): {results_df['num_trades'].sum():.0f}")
    print(f"✓ Cumulative PnL: {results_df['total_pnl'].sum():.2f}")
    
    print(f"\n📈 Results by Instrument:")
    summary_df = results_df[['token', 'total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate', 'num_trades', 'total_pnl']]
    print(summary_df.to_string(index=False))
    
    # Save results
    summary_file = "vectorbt_backtest_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"\n💾 Summary saved to: {summary_file}")
    
    # Save detailed trades
    if trades_df is not None and len(trades_df) > 0:
        trades_file = "vectorbt_backtest_trades.csv"
        trades_df.to_csv(trades_file, index=False)
        print(f"💾 Trade details saved to: {trades_file}")
    
    return results_df


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    config = BacktestConfig()
    print(f"config.rsi_entry_threshold: {config.RSI_ENTRY_THRESHOLD} ")
    # Run backtest
    results = run_backtest(config)
    
    if results is not None:
        print("\n" + "=" * 80)
        print("✅ BACKTEST COMPLETE")
        print("=" * 80)