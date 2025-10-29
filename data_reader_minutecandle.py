#!/usr/bin/env python3
# %% 
"""
1-Minute Candle Parquet Data Reader
Reads and analyzes 1-minute OHLCV data from Parquet files
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
import argparse
from datetime import datetime, timedelta

class MinuteDataReader:
    def __init__(self, data_dir="historicaldata"):
        self.data_dir = Path("E:/working/historicaldata/1mincandles")
        self.data = {}
        
    def list_available_symbols(self):
        """List all available stock symbols with Parquet files"""
        parquet_files = list(self.data_dir.glob("*.parquet"))
        symbols = [f.stem for f in parquet_files]
        return sorted(symbols)
    
    def load_symbol_data(self, symbol, start_date=None, end_date=None):
        """
        Load 1-minute data for a specific symbol
        Optionally filter by date range
        """
        file_path = self.data_dir / f"{symbol}.parquet"
        
        if not file_path.exists():
            print(f"Data file for {symbol} not found: {file_path}")
            return None
        
        try:
            # Read the Parquet file
            df = pd.read_parquet(file_path)
            
            # Ensure the index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.set_index('datetime', inplace=True)
                else:
                    # Try to convert the index to datetime
                    df.index = pd.to_datetime(df.index)
            
            # Filter by date range if specified
            if start_date:
                if isinstance(start_date, str):
                    start_date = pd.to_datetime(start_date)
                # Set time to beginning of day for proper comparison
                start_date = start_date.replace(hour=0, minute=0, second=0)    
                df = df[df.index >= start_date]
            
            if end_date:
                if isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date)

                # Set time to end of day for proper comparison
                end_date = end_date.replace(hour=23, minute=59, second=59)
                df = df[df.index <= end_date]
            
            # Store in memory
            self.data[symbol] = df
            
            print(f"Loaded {len(df)} records for {symbol}")
            if not df.empty:
                print(f"Date range: {df.index.min()} to {df.index.max()}")
            
            return df
            
        except Exception as e:
            print(f"Error loading data for {symbol}: {e}")
            return None
    
    def get_symbol_data(self, symbol):
        """Get previously loaded data for a symbol"""
        return self.data.get(symbol, None)
    
    def get_multiple_symbols(self, symbols, start_date=None, end_date=None):
        """Load data for multiple symbols"""
        result = {}
        for symbol in symbols:
            df = self.load_symbol_data(symbol, start_date, end_date)
            if df is not None:
                result[symbol] = df
        return result
    
    def describe_data(self, symbol):
        """Generate basic statistics for a symbol's data"""
        if symbol not in self.data:
            print(f"No data loaded for {symbol}")
            return
        
        df = self.data[symbol]
        print(f"\n=== Statistics for {symbol} ===")
        print(f"Time period: {df.index.min()} to {df.index.max()}")
        print(f"Total records: {len(df):,}")
        
        # Basic statistics
        print("\nBasic Statistics:")
        print(df[['open', 'high', 'low', 'close', 'volume']].describe())
        
        # Count trading days
        trading_days = df.index.normalize().nunique()
        print(f"\nTrading days: {trading_days}")
        
        # Average records per day
        avg_records_per_day = len(df) / trading_days if trading_days > 0 else 0
        print(f"Average records per day: {avg_records_per_day:.1f}")
        
        # Check for missing dates/times
        full_date_range = pd.date_range(start=df.index.min().floor('D'), 
                                       end=df.index.max().ceil('D'), 
                                       freq='1min')
        market_hours_only = full_date_range[full_date_range.indexer_between_time('9:15', '15:30')]
        missing = market_hours_only.difference(df.index)
        print(f"Missing intervals: {len(missing)}")
        
        return df
    
    def plot_symbol(self, symbol, period='1D', indicators=True):
        """Plot price and volume data for a symbol"""
        if symbol not in self.data:
            print(f"No data loaded for {symbol}")
            return
        
        df = self.data[symbol]
        
        if df.empty:
            print(f"No data available for {symbol}")
            return
        
        # Resample based on the requested period
        if period == '1min':
            plot_df = df
            title_period = "1-Minute"
        else:
            # Convert period to pandas resample string
            if period.endswith('D'):
                days = int(period[:-1])
                resample_str = f'{days}D'
                title_period = f"{days}-Day"
            elif period.endswith('H'):
                hours = int(period[:-1])
                resample_str = f'{hours}H'
                title_period = f"{hours}-Hour"
            else:
                resample_str = '1D'
                title_period = "Daily"
            
            # Resample to the requested period
            plot_df = df.resample(resample_str).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
        
        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
        
        # Plot OHLC data
        ax1.plot(plot_df.index, plot_df['close'], label='Close', linewidth=1)
        ax1.set_title(f'{symbol} - {title_period} Price')
        ax1.set_ylabel('Price')
        ax1.grid(True, linestyle='--', alpha=0.7)
        ax1.legend()
        
        # Add moving averages if requested
        if indicators:
            for ma_period in [20, 50]:
                ma = plot_df['close'].rolling(window=ma_period).mean()
                ax1.plot(plot_df.index, ma, label=f'MA{ma_period}', alpha=0.7)
        
        # Plot volume
        ax2.bar(plot_df.index, plot_df['volume'], alpha=0.7, color='orange')
        ax2.set_title('Volume')
        ax2.set_ylabel('Volume')
        ax2.grid(True, linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        plt.show()
    
    def export_to_csv(self, symbol, output_path=None):
        """Export data to CSV file"""
        if symbol not in self.data:
            print(f"No data loaded for {symbol}")
            return
        
        if output_path is None:
            output_path = f"{symbol}_minute_data.csv"
        
        try:
            self.data[symbol].to_csv(output_path)
            print(f"Exported {symbol} data to {output_path}")
        except Exception as e:
            print(f"Error exporting data: {e}")
    
    def find_missing_periods(self, symbol):
        """Identify missing time periods in the data"""
        if symbol not in self.data:
            print(f"No data loaded for {symbol}")
            return
        
        df = self.data[symbol]
        
        # Create a complete datetime index for market hours
        start_date = df.index.min().floor('D')
        end_date = df.index.max().ceil('D')
        
        # Generate all possible 1-minute intervals during market hours
        all_minutes = pd.date_range(start=start_date, end=end_date, freq='1min')
        market_hours = all_minutes[all_minutes.indexer_between_time('9:15', '15:30')]
        
        # Find missing minutes
# %% 
def main_jupyter(args_list=None):
    """Main function for Jupyter notebooks that accepts arguments as a list"""
    parser = argparse.ArgumentParser(description="1-Minute Candle Parquet Data Reader")
    parser.add_argument('--list', action='store_true', help="List all available stock symbols.")
    parser.add_argument('--describe', type=str, metavar='SYMBOL', help="Generate statistics for a given symbol.")
    parser.add_argument('--plot', type=str, metavar='SYMBOL', help="Plot price and volume data for a symbol.")
    parser.add_argument('--period', type=str, default='1D', help="Plot period (e.g., 1D, 5D, 1H). Requires --plot.")
    parser.add_argument('--export', type=str, metavar='SYMBOL', help="Export data to a CSV file.")
    
    # Parse arguments from list instead of command line
    if args_list is None:
        args_list = []
    args = parser.parse_args(args_list)
    
    reader = MinuteDataReader()
    
    if args.list:
        symbols = reader.list_available_symbols()
        print("Available symbols:")
        for symbol in symbols:
            print(f"- {symbol}")
            
    elif args.describe:
        reader.load_symbol_data(args.describe)
        reader.describe_data(args.describe)
        
    elif args.plot:
        reader.load_symbol_data(args.plot)
        reader.plot_symbol(args.plot, period=args.period)

    elif args.export:
        reader.load_symbol_data(args.export)
        reader.export_to_csv(args.export)
    
    else:
        parser.print_help()

def main():
    """Main function for command line usage"""
    main_jupyter()
#  added main function to run from command line (Gemini)
# def main():  
#     """Main function to parse arguments and run the data analysis tool."""
#     parser = argparse.ArgumentParser(description="1-Minute Candle Parquet Data Reader")
#     parser.add_argument('--list', action='store_true', help="List all available stock symbols.")
#     parser.add_argument('--describe', type=str, metavar='SYMBOL', help="Generate statistics for a given symbol.")
#     parser.add_argument('--plot', type=str, metavar='SYMBOL', help="Plot price and volume data for a symbol.")
#     parser.add_argument('--period', type=str, default='1D', help="Plot period (e.g., 1D, 5D, 1H). Requires --plot.")
#     parser.add_argument('--export', type=str, metavar='SYMBOL', help="Export data to a CSV file.")
    
#     args = parser.parse_args()
    
#     reader = MinuteDataReader()
    
#     if args.list:
#         symbols = reader.list_available_symbols()
#         print("Available symbols:")
#         for symbol in symbols:
#             print(f"- {symbol}")
            
#     elif args.describe:
#         reader.load_symbol_data(args.describe)
#         reader.describe_data(args.describe)
        
#     elif args.plot:
#         reader.load_symbol_data(args.plot)
#         reader.plot_symbol(args.plot, period=args.period)

#     elif args.export:
#         reader.load_symbol_data(args.export)
#         reader.export_to_csv(args.export)
    
#     else:
#         parser.print_help()
# uncomment to run directly from command prompt
if __name__ == "__main__":
    main()       


# ... (main function defined as above) ...
#funnction to call main with args so that can run in jupyter
# Call the function with a list of arguments
# if __name__ == "__main__":
#     main(['--plot', 'TVSMOT', '--period', '1min'])
# %%
