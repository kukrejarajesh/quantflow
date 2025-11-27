import os
import pandas as pd
import numpy as np
import vectorbt as vbt
from pathlib import Path
from datetime import datetime
from config import example_tokens, instruments_to_process
import gc

# ==============================================================================
# CONFIGURATION
# ==============================================================================

#TICK_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\tick_data"
TICK_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\daily"
TRADING_DATES = ["2025-10-31","2025-11-03", "2025-11-04","2025-11-06", "2025-11-07", "2025-11-10", "2025-11-11","2025-11-12", "2025-11-13","2025-11-14", "2025-11-17","2025-11-18", "2025-11-19","2025-11-20", "2025-11-21","2025-11-22", "2025-11-23"]  # Add more dates as needed
#INSTRUMENTS = [424961]  # Add instrument tokens to backtest
#INSTRUMENTS = instruments_to_process
INSTRUMENTS = example_tokens
# Strategy Parameters (will be optimized)
DEFAULT_PARAMS = {
    'ema_period': 9,
    'stop_loss_pct': 2.5,
    'trailing_stop_pct': 2.5
}

# Processing configuration
BATCH_SIZE = 10  # Process N instruments at a time
SAVE_RESULTS = True  # Save results after each instrument

# ==============================================================================
# MEMORY-EFFICIENT DATA LOADING
# ==============================================================================

def load_instrument_data_generator(instrument_token, trading_dates, tick_data_root):
    """
    Generator that yields data one date at a time (memory efficient)
    """
    for date in trading_dates:
        file_path = os.path.join(tick_data_root, date, f"{instrument_token}.parquet")
        print(f" file path: {file_path}")
        if not os.path.exists(file_path):
            continue
        
        try:
            df = pd.read_parquet(
                file_path,
                columns=[
                    'instrument_token', 
                    'ohlc_open', 
                    'ohlc_high', 
                    'ohlc_low', 
                    'ohlc_close',
                    'volume_traded',
                    'exchange_timestamp',
                    'trading_date'
                ]
            )
            # ---------------------------------------------------------
            # ✅ SIMPLER FILTERING METHOD
            # ---------------------------------------------------------
            # 1. Ensure it is a datetime object (crucial if Parquet loaded it as string)
            df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
            # 1. Set timestamp as index (required for between_time)
            df.set_index('exchange_timestamp', inplace=True)
            
            # 2. Filter using simple strings
            #df = df.between_time('09:15', '15:30')
            #print("first few rows after time filter:\n", df.iloc[5:10])
            # 3. Reset index if you want 'exchange_timestamp' back as a column
            df.reset_index(inplace=True)
            # ---------------------------------------------------------

            yield df
            
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            continue


def load_instrument_data_chunked(instrument_token, trading_dates, tick_data_root):
    """
    Load data efficiently by streaming and concatenating
    Uses less memory than loading all at once
    """
    dfs = []
    
    for df_chunk in load_instrument_data_generator(instrument_token, trading_dates, tick_data_root):
        dfs.append(df_chunk)
    
    if not dfs:
        return None
    
    # Combine all dates
    combined_df = pd.concat(dfs, ignore_index=True)
    del dfs  # Free memory immediately
    gc.collect()
    
    # Convert timestamp to datetime and set as index
    combined_df['exchange_timestamp'] = pd.to_datetime(combined_df['exchange_timestamp'])
    combined_df = combined_df.sort_values('exchange_timestamp')
    combined_df = combined_df.set_index('exchange_timestamp')
    
    return combined_df


def process_instruments_in_batches(instruments, trading_dates, tick_data_root, 
                                   batch_size=10, process_func=None):
    """
    Process instruments in batches to avoid memory overflow
    
    Args:
        instruments: List of instrument tokens
        trading_dates: List of dates to process
        tick_data_root: Root directory of tick data
        batch_size: Number of instruments to process at once
        process_func: Function to apply to each instrument's data
    
    Yields:
        Results for each instrument
    """
    for i in range(0, len(instruments), batch_size):
        batch = instruments[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(instruments) + batch_size - 1) // batch_size
        
        print(f"\n{'='*80}")
        print(f"📦 Processing Batch {batch_num}/{total_batches} ({len(batch)} instruments)")
        print(f"{'='*80}")
        
        for instrument in batch:
            print(f"\n📊 Loading instrument: {instrument}")
            
            # Load data for this instrument
            df = load_instrument_data_chunked(instrument, trading_dates, tick_data_root)
            
            if df is None or len(df) == 0:
                print(f"   ❌ No data found")
                continue
            
            print(f"   ✅ Loaded {len(df)} rows across {len(trading_dates)} dates")
            
            # Process this instrument
            if process_func:
                result = process_func(instrument, df)
                yield instrument, result
            else:
                yield instrument, df
            
            # Clean up memory immediately after processing
            del df
            gc.collect()
        
        # Force garbage collection after each batch
        gc.collect()
        print(f"\n✅ Batch {batch_num} complete. Memory cleaned.")


# ==============================================================================
# STRATEGY IMPLEMENTATION
# ==============================================================================

def calculate_ema_crossover_signals(close, ema_period=9):
    """
    Entry Signal: Price crosses above EMA
    """
    ema = close.ewm(span=ema_period, adjust=False).mean()
    entries = close.vbt.crossed_above(ema)
    return entries, ema


def apply_stop_loss_and_trailing_stop(close, ema_period=9, stop_loss_pct=2.5, trailing_stop_pct=2.5):
    """
    Convert percentages to decimals for VectorBT
    """
    ema = close.ewm(span=ema_period, adjust=False).mean()
    sl_stop = stop_loss_pct / 100.0
    tsl_stop = trailing_stop_pct / 100.0
    return sl_stop, tsl_stop

def generate_trailing_stop_exits(close, trailing_pct):
    trailing_stop_prices = close * (1 - trailing_pct)
    rolling_max = close.cummax()
    tsl_exit = close < (rolling_max * (1 - trailing_pct))
    return tsl_exit
# ==============================================================================
# BACKTESTING FUNCTION
# ==============================================================================

def run_backtest(df, ema_period=9, stop_loss_pct=2.5, trailing_stop_pct=2.5, 
                 init_cash=100000, fees=0.001):
    """
    Run backtest for single instrument with given parameters
    """
    close = df['ohlc_close']
    
    # Generate entry signals
    entries, ema = calculate_ema_crossover_signals(close, ema_period)
    
    # Calculate stop loss values
    sl_stop, tsl_stop_pct = apply_stop_loss_and_trailing_stop(close,ema_period,stop_loss_pct, trailing_stop_pct)
    
    # --- TRAILING STOP EXIT SIGNAL ---
    tsl_exit = generate_trailing_stop_exits(close, tsl_stop_pct)
    # Run portfolio simulation
    portfolio = vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=tsl_exit,
        sl_stop=sl_stop,
        init_cash=init_cash,
        fees=fees,
        freq='1D'
    )
   
    all_file = os.path.join("backtest_results", "all_trade_logs.xlsx")

    # Convert dict-like trade_log to DataFrame if needed
    trade_log = portfolio.trades.records_readable.copy()

    # Add instrument column (IMPORTANT so logs can be separated in Excel)
    trade_log["instrument"] = instrument

    # If file exists, append to it
    if os.path.exists(all_file):
        existing = pd.read_excel(all_file)
        combined = pd.concat([existing, trade_log], ignore_index=True)
    else:
        combined = trade_log

    # Write combined result back
    combined.to_excel(all_file, index=False)

    print("Appended trade log to:", all_file)

    return portfolio, entries, ema


# ==============================================================================
# PARAMETER OPTIMIZATION (MEMORY EFFICIENT)
# ==============================================================================

def optimize_parameters(df, param_ranges, init_cash=100000, fees=0.001):
    """
    Memory-efficient parameter optimization
    """
    close = df['ohlc_close']
    
    ema_periods = param_ranges.get('ema_period', [9])
    sl_pcts = param_ranges.get('stop_loss_pct', [2.5])
    tsl_pcts = param_ranges.get('trailing_stop_pct', [2.5])
    
    total_combinations = len(ema_periods) * len(sl_pcts) * len(tsl_pcts)
    
    print(f"\n🔍 Optimizing {total_combinations} parameter combinations...")
    
    results = []
    processed = 0
    
    for ema_period in ema_periods:
        entries, ema = calculate_ema_crossover_signals(close, ema_period)
        
        for sl_pct in sl_pcts:
            for tsl_pct in tsl_pcts:
                processed += 1
                
                if processed % 10 == 0:
                    print(f"   Progress: {processed}/{total_combinations} ({processed/total_combinations*100:.1f}%)")
                
                # Calculate stop loss values
                sl_stop, tsl_stop_pct = apply_stop_loss_and_trailing_stop(close,ema_period,sl_pct, tsl_pct)
                # --- TRAILING STOP EXIT SIGNAL ---
                tsl_exit = generate_trailing_stop_exits(close, tsl_stop_pct)
                try:
                    portfolio = vbt.Portfolio.from_signals(
                        close=close,
                        entries=entries,
                        exits=tsl_exit,
                        sl_stop=sl_stop,
                        #sl_stop=sl_pct / 100.0,
                        #tsl_stop=tsl_pct / 100.0,
                        init_cash=init_cash,
                        fees=fees,
                        freq='1D'
                    )
                    
                    results.append({
                        'ema_period': ema_period,
                        'stop_loss_pct': sl_pct,
                        'trailing_stop_pct': tsl_pct,
                        'total_return': portfolio.total_return(),
                        'sharpe_ratio': portfolio.sharpe_ratio(),
                        'max_drawdown': portfolio.max_drawdown(),
                        'win_rate': portfolio.trades.win_rate(),
                        'total_trades': portfolio.trades.count(),
                        'avg_trade_duration': portfolio.trades.duration.mean() if portfolio.trades.count() > 0 else 0,
                        'profit_factor': portfolio.trades.profit_factor()
                    })
                    
                    # Clean up
                    del portfolio
                    
                except Exception as e:
                    continue
        
        # Periodic cleanup
        gc.collect()
    
    results_df = pd.DataFrame(results)
    return results_df


# ==============================================================================
# RESULTS MANAGEMENT
# ==============================================================================

class ResultsManager:
    """Manages saving and loading of backtest results"""
    
    def __init__(self, output_dir="backtest_results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def save_instrument_result(self, instrument, result_dict):
        """Save result for single instrument"""
        """Append result to a single consolidated CSV file."""
        filepath = os.path.join(self.output_dir, "all_results.csv")
        
        # Convert dict → DataFrame
        row_df = pd.DataFrame([result_dict])
        
        # If file exists -> append without header
        if os.path.exists(filepath):
            row_df.to_csv(filepath, mode='a', header=False, index=False)
        else:
            row_df.to_csv(filepath, index=False)
    
    def save_optimization_result(self, instrument, results_df):
        """Save optimization results"""
        
        """Append result to a single consolidated CSV file."""
        filepath = os.path.join(self.output_dir, "all_optimization.csv")
        
               
        # If file exists -> append without header
        if os.path.exists(filepath):
            results_df.to_csv(filepath, mode='a', header=False, index=False)
        else:
            results_df.to_csv(filepath, index=False)
    
    def save_summary(self, summary_df):
        """Save summary of all instruments"""
        filepath = os.path.join(self.output_dir, "summary_all_instruments.csv")
        summary_df.to_csv(filepath, index=False)
        print(f"\n💾 Summary saved: {filepath}")    
    
    def load_all_results(self):
        """Load all saved results"""
        results = []
        for file in os.listdir(self.output_dir):
            if file.startswith("result_") and file.endswith(".csv"):
                df = pd.read_csv(os.path.join(self.output_dir, file))
                results.append(df)
        
        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()


# ==============================================================================
# PROCESSING FUNCTIONS
# ==============================================================================

def process_single_instrument(instrument, df, params=None, optimize=False, 
                              param_ranges=None, results_manager=None):
    """
    Process a single instrument: backtest and optionally optimize
    """
    if params is None:
        params = DEFAULT_PARAMS.copy()
    
    result_dict = {
        'instrument_token': instrument,
        'data_points': len(df),
        'date_range': f"{df.index.min()} to {df.index.max()}"
    }
    
    try:
        # Run default backtest
        portfolio, _, _ = run_backtest(
            df,
            ema_period=params['ema_period'],
            stop_loss_pct=params['stop_loss_pct'],
            trailing_stop_pct=params['trailing_stop_pct']
        )
        
        # Extract metrics
        result_dict.update({
            'default_total_return': portfolio.total_return(),
            'default_sharpe_ratio': portfolio.sharpe_ratio(),
            'default_max_drawdown': portfolio.max_drawdown(),
            'default_win_rate': portfolio.trades.win_rate(),
            'default_total_trades': portfolio.trades.count(),
            'default_profit_factor': portfolio.trades.profit_factor()
        })
        
        print(f"   📈 Return: {portfolio.total_return():.2%} | Sharpe: {portfolio.sharpe_ratio():.2f} | Trades: {portfolio.trades.count()}")
        
        del portfolio
        gc.collect()
        
        # Run optimization if requested
        if optimize and param_ranges:
            print(f"   🔬 Running optimization...")
            results_df = optimize_parameters(df, param_ranges)
            
            if len(results_df) > 0:
                best = results_df.sort_values('sharpe_ratio', ascending=False).iloc[0]
                
                result_dict.update({
                    'best_ema_period': best['ema_period'],
                    'best_stop_loss_pct': best['stop_loss_pct'],
                    'best_trailing_stop_pct': best['trailing_stop_pct'],
                    'best_total_return': best['total_return'],
                    'best_sharpe_ratio': best['sharpe_ratio'],
                    'best_max_drawdown': best['max_drawdown'],
                    'best_win_rate': best['win_rate'],
                    'best_total_trades': best['total_trades']
                })
                
                print(f"   🏆 Best Sharpe: {best['sharpe_ratio']:.2f} (EMA={best['ema_period']:.0f}, SL={best['stop_loss_pct']:.1f}%, TSL={best['trailing_stop_pct']:.1f}%)")
                
                # Save optimization details
                if results_manager:
                    results_manager.save_optimization_result(instrument, results_df)
            
            del results_df
            gc.collect()
        
        # Save individual result
        if results_manager:
            results_manager.save_instrument_result(instrument, result_dict)
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        result_dict['error'] = str(e)
    
    return result_dict


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    
    print("="*80)
    print("🚀 MEMORY-EFFICIENT EMA BREAKOUT BACKTESTER")
    print("="*80)
    
    # Initialize results manager
    results_mgr = ResultsManager(output_dir="backtest_results")
    
    # Configuration
    RUN_OPTIMIZATION = True  # Set to False for faster execution
    
    param_ranges = {
        'ema_period': [5, 9, 13, 17],
        'stop_loss_pct': [1.5, 2.0, 2.5, 3.0, 4.0],
        'trailing_stop_pct': [1.5, 2.0, 2.5, 3.0, 4.0]
    } if RUN_OPTIMIZATION else None
    
    # Process instruments in batches
    all_results = []
    
    for instrument, df in process_instruments_in_batches(
        INSTRUMENTS, 
        TRADING_DATES, 
        TICK_DATA_ROOT,
        batch_size=BATCH_SIZE
    ):
        result = process_single_instrument(
            instrument, 
            df, 
            params=DEFAULT_PARAMS,
            optimize=RUN_OPTIMIZATION,
            param_ranges=param_ranges,
            results_manager=results_mgr
        )
        
        all_results.append(result)
    
    # Create and save summary
    if all_results:
        summary_df = pd.DataFrame(all_results)
        results_mgr.save_summary(summary_df)
        
        print("\n" + "="*80)
        print("📊 FINAL SUMMARY - ALL INSTRUMENTS")
        print("="*80)
        print(summary_df.to_string(index=False))
        
        if RUN_OPTIMIZATION and 'best_sharpe_ratio' in summary_df.columns:
            print("\n" + "="*80)
            print("🏆 TOP 5 INSTRUMENTS BY OPTIMIZED SHARPE RATIO")
            print("="*80)
            top5 = summary_df.nlargest(5, 'best_sharpe_ratio')
            print(top5[['instrument_token', 'best_sharpe_ratio', 'best_total_return', 
                       'best_ema_period', 'best_stop_loss_pct', 'best_trailing_stop_pct']].to_string(index=False))
    
    print("\n" + "="*80)
    print("✅ BACKTEST COMPLETE!")
    print(f"📁 Results saved in: {results_mgr.output_dir}/")
    print("="*80)