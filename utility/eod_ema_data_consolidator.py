"""
EOD Data Consolidator - Consolidates daily metrics across dates and calculates EMAs
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import gc
from tqdm import tqdm

# ==============================================================================
# CONFIGURATION
# ==============================================================================

class ConsolidatorConfig:
    # Input/Output paths
    DAILY_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\daily"
    OUTPUT_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_eod"
    
    # EMA periods to calculate
    EMA_PERIODS = [5, 9, 13, 21, 34, 50, 200]
    
    # Date range (leave None to process all available dates)
    START_DATE = None  # e.g., "2025-11-01" or None for all
    END_DATE = None    # e.g., "2025-11-25" or None for all
    
    # Processing options
    OVERWRITE_EXISTING = False  # If False, only update with new dates
    BATCH_SIZE = 50  # Process N instruments at a time


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_all_dates(root_path, start_date=None, end_date=None):
    """
    Get all date folders available in the root path
    """
    all_dates = []
    
    for item in os.listdir(root_path):
        item_path = os.path.join(root_path, item)
        if os.path.isdir(item_path):
            try:
                # Validate it's a date folder
                date_obj = datetime.strptime(item, "%Y-%m-%d")
                
                # Filter by date range if specified
                if start_date and date_obj < datetime.strptime(start_date, "%Y-%m-%d"):
                    continue
                if end_date and date_obj > datetime.strptime(end_date, "%Y-%m-%d"):
                    continue
                
                all_dates.append(item)
            except ValueError:
                continue
    
    # Sort dates
    all_dates.sort()
    return all_dates


def get_all_instruments(root_path, dates):
    """
    Get set of all unique instrument tokens across all dates
    """
    all_instruments = set()
    
    print("📊 Scanning for instruments across all dates...")
    for date in tqdm(dates, desc="Scanning dates"):
        date_path = os.path.join(root_path, date)
        
        if not os.path.exists(date_path):
            continue
        
        for file in os.listdir(date_path):
            if file.endswith('.parquet'):
                # Extract instrument token from filename
                token = file.replace('.parquet', '')
                try:
                    all_instruments.add(int(token))
                except ValueError:
                    continue
    
    return sorted(list(all_instruments))


def calculate_ema(series, period):
    """
    Calculate EMA for a given period
    """
    return series.ewm(span=period, adjust=False).mean()


# ==============================================================================
# CONSOLIDATION FUNCTIONS
# ==============================================================================

def load_instrument_data_across_dates(instrument_token, dates, root_path):
    """
    Load EOD data for a single instrument across all specified dates
    """
    dfs = []
    
    for date in dates:
        file_path = os.path.join(root_path, date, f"{instrument_token}.parquet")
        
        if not os.path.exists(file_path):
            continue
        
        try:
            df = pd.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            print(f"  ⚠️  Error loading {date}/{instrument_token}.parquet: {e}")
            continue
    
    if not dfs:
        return None
    
    # Combine all dates
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Convert timestamp and sort
    combined_df['exchange_timestamp'] = pd.to_datetime(combined_df['exchange_timestamp'])
    combined_df['trading_date'] = pd.to_datetime(combined_df['trading_date'])
    combined_df = combined_df.sort_values('trading_date')
    
    # Remove duplicates (keep latest)
    combined_df = combined_df.drop_duplicates(subset=['trading_date'], keep='last')
    
    return combined_df


def calculate_all_emas(df, ema_periods, price_column='ohlc_close'):
    """
    Calculate all EMAs for the dataframe
    """
    for period in ema_periods:
        column_name = f'EMA_{period}'
        df[column_name] = calculate_ema(df[price_column], period)
    
    return df


def consolidate_instrument(instrument_token, dates, config):
    """
    Consolidate EOD data for a single instrument and calculate EMAs
    """
    # Load data across all dates
    df = load_instrument_data_across_dates(
        instrument_token, 
        dates, 
        config.DAILY_DATA_ROOT
    )
    
    if df is None or len(df) == 0:
        return None
    
    # Calculate EMAs
    df = calculate_all_emas(df, config.EMA_PERIODS)
    
    # Add metadata
    df['last_updated'] = datetime.now()
    
    return df


def save_consolidated_data(df, instrument_token, output_path):
    """
    Save consolidated data to parquet
    """
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, f"{instrument_token}.parquet")
    df.to_parquet(output_file, index=False)


def update_existing_consolidated_data(instrument_token, new_dates, config):
    """
    Update existing consolidated data with new dates only
    """
    output_file = os.path.join(config.OUTPUT_ROOT, f"{instrument_token}.parquet")
    
    # Load existing data
    if os.path.exists(output_file):
        existing_df = pd.read_parquet(output_file)
        existing_df['trading_date'] = pd.to_datetime(existing_df['trading_date'])
        
        # Get dates already in file
        existing_dates = set(existing_df['trading_date'].dt.strftime('%Y-%m-%d').unique())
        
        # Filter to only new dates
        dates_to_add = [d for d in new_dates if d not in existing_dates]
        
        if not dates_to_add:
            return existing_df, 0  # No new dates to add
        
        # Load new data
        new_df = load_instrument_data_across_dates(
            instrument_token,
            dates_to_add,
            config.DAILY_DATA_ROOT
        )
        
        if new_df is None or len(new_df) == 0:
            return existing_df, 0
        
        # Combine with existing
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.sort_values('trading_date')
        combined_df = combined_df.drop_duplicates(subset=['trading_date'], keep='last')
        
        # Recalculate all EMAs on combined data
        # Drop old EMA columns
        ema_cols = [col for col in combined_df.columns if col.startswith('EMA_')]
        combined_df = combined_df.drop(columns=ema_cols, errors='ignore')
        
        # Recalculate
        combined_df = calculate_all_emas(combined_df, config.EMA_PERIODS)
        combined_df['last_updated'] = datetime.now()
        
        return combined_df, len(dates_to_add)
    
    else:
        # No existing file, create fresh
        df = consolidate_instrument(instrument_token, new_dates, config)
        return df, len(new_dates) if df is not None else 0


# ==============================================================================
# MAIN CONSOLIDATION ENGINE
# ==============================================================================

def consolidate_all_instruments(config):
    """
    Main function to consolidate EOD data for all instruments
    """
    print("="*80)
    print("🚀 EOD DATA CONSOLIDATION ENGINE")
    print("="*80)
    
    # Get all available dates
    all_dates = get_all_dates(
        config.DAILY_DATA_ROOT,
        config.START_DATE,
        config.END_DATE
    )
    
    if not all_dates:
        print("❌ No dates found to process")
        return
    
    print(f"\n📅 Found {len(all_dates)} dates to process:")
    print(f"   From: {all_dates[0]}")
    print(f"   To: {all_dates[-1]}")
    
    # Get all instruments
    all_instruments = get_all_instruments(config.DAILY_DATA_ROOT, all_dates)
    
    print(f"\n📊 Found {len(all_instruments)} unique instruments")
    
    # Statistics
    stats = {
        'total_instruments': len(all_instruments),
        'processed': 0,
        'updated': 0,
        'new': 0,
        'failed': 0,
        'skipped': 0
    }
    
    # Process in batches
    for i in range(0, len(all_instruments), config.BATCH_SIZE):
        batch = all_instruments[i:i + config.BATCH_SIZE]
        batch_num = i // config.BATCH_SIZE + 1
        total_batches = (len(all_instruments) + config.BATCH_SIZE - 1) // config.BATCH_SIZE
        
        print(f"\n{'='*80}")
        print(f"📦 Processing Batch {batch_num}/{total_batches} ({len(batch)} instruments)")
        print(f"{'='*80}")
        
        for instrument_token in tqdm(batch, desc=f"Batch {batch_num}"):
            try:
                output_file = os.path.join(config.OUTPUT_ROOT, f"{instrument_token}.parquet")
                
                # Check if should overwrite or update
                if config.OVERWRITE_EXISTING or not os.path.exists(output_file):
                    # Full consolidation
                    df = consolidate_instrument(instrument_token, all_dates, config)
                    
                    if df is not None and len(df) > 0:
                        save_consolidated_data(df, instrument_token, config.OUTPUT_ROOT)
                        stats['new' if not os.path.exists(output_file) else 'updated'] += 1
                        stats['processed'] += 1
                    else:
                        stats['skipped'] += 1
                
                else:
                    # Update with new dates only
                    df, new_dates_count = update_existing_consolidated_data(
                        instrument_token,
                        all_dates,
                        config
                    )
                    
                    if new_dates_count > 0:
                        save_consolidated_data(df, instrument_token, config.OUTPUT_ROOT)
                        stats['updated'] += 1
                        stats['processed'] += 1
                    else:
                        stats['skipped'] += 1
                
            except Exception as e:
                print(f"  ❌ Error processing {instrument_token}: {e}")
                stats['failed'] += 1
                continue
        
        # Cleanup after each batch
        gc.collect()
    
    # Print final statistics
    print("\n" + "="*80)
    print("✅ CONSOLIDATION COMPLETE")
    print("="*80)
    print(f"📊 Statistics:")
    print(f"   Total Instruments: {stats['total_instruments']}")
    print(f"   Processed: {stats['processed']}")
    print(f"   New Files: {stats['new']}")
    print(f"   Updated: {stats['updated']}")
    print(f"   Skipped (no new data): {stats['skipped']}")
    print(f"   Failed: {stats['failed']}")
    print(f"\n📁 Output Location: {config.OUTPUT_ROOT}")
    print("="*80)
    
    return stats


# ==============================================================================
# ANALYSIS & VERIFICATION
# ==============================================================================

def verify_consolidated_data(config, sample_instruments=5):
    """
    Verify the consolidated data by checking a few sample instruments
    """
    print("\n" + "="*80)
    print("🔍 VERIFICATION - Sampling Consolidated Data")
    print("="*80)
    
    # Get list of consolidated files
    consolidated_files = [
        f.replace('.parquet', '') 
        for f in os.listdir(config.OUTPUT_ROOT) 
        if f.endswith('.parquet')
    ]
    
    if not consolidated_files:
        print("❌ No consolidated files found")
        return
    
    # Sample random instruments
    sample = np.random.choice(
        consolidated_files, 
        min(sample_instruments, len(consolidated_files)), 
        replace=False
    )
    
    for instrument_token in sample:
        file_path = os.path.join(config.OUTPUT_ROOT, f"{instrument_token}.parquet")
        df = pd.read_parquet(file_path)
        
        print(f"\n📊 Instrument: {instrument_token}")
        print(f"   Total Records: {len(df)}")
        print(f"   Date Range: {df['trading_date'].min()} to {df['trading_date'].max()}")
        print(f"   Columns: {list(df.columns)}")
        print(f"\n   Latest Record:")
        print(df.tail(1).to_string(index=False))


def get_consolidated_summary(config):
    """
    Get summary statistics of all consolidated files
    """
    print("\n" + "="*80)
    print("📊 CONSOLIDATED DATA SUMMARY")
    print("="*80)
    
    consolidated_files = [
        f for f in os.listdir(config.OUTPUT_ROOT) 
        if f.endswith('.parquet')
    ]
    
    if not consolidated_files:
        print("❌ No consolidated files found")
        return
    
    summary_data = []
    
    print(f"📁 Analyzing {len(consolidated_files)} consolidated files...")
    
    for file in tqdm(consolidated_files[:100], desc="Sampling files"):  # Sample first 100
        file_path = os.path.join(config.OUTPUT_ROOT, file)
        
        try:
            df = pd.read_parquet(file_path)
            instrument_token = file.replace('.parquet', '')
            
            summary_data.append({
                'instrument_token': instrument_token,
                'total_days': len(df),
                'start_date': df['trading_date'].min(),
                'end_date': df['trading_date'].max(),
                'file_size_mb': os.path.getsize(file_path) / (1024 * 1024),
                'has_ema_5': 'EMA_5' in df.columns,
                'has_ema_200': 'EMA_200' in df.columns
            })
        except Exception as e:
            continue
    
    summary_df = pd.DataFrame(summary_data)
    
    print(f"\n📈 Summary Statistics:")
    print(f"   Total Files: {len(consolidated_files)}")
    print(f"   Average Days per Instrument: {summary_df['total_days'].mean():.1f}")
    print(f"   Date Range: {summary_df['start_date'].min()} to {summary_df['end_date'].max()}")
    print(f"   Total Storage: {summary_df['file_size_mb'].sum():.2f} MB")
    print(f"   EMA Columns Present: {summary_df['has_ema_5'].mean() * 100:.1f}%")
    
    return summary_df


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    
    # Initialize configuration
    config = ConsolidatorConfig()
    
    print("="*80)
    print("📋 CONFIGURATION")
    print("="*80)
    print(f"Input Path: {config.DAILY_DATA_ROOT}")
    print(f"Output Path: {config.OUTPUT_ROOT}")
    print(f"EMA Periods: {config.EMA_PERIODS}")
    print(f"Date Range: {config.START_DATE or 'All'} to {config.END_DATE or 'All'}")
    print(f"Overwrite Existing: {config.OVERWRITE_EXISTING}")
    print(f"Batch Size: {config.BATCH_SIZE}")
    
    # Confirm before proceeding
    print("\n" + "="*80)
    response = input("Proceed with consolidation? (yes/no): ").strip().lower()
    
    if response != 'yes':
        print("❌ Consolidation cancelled")
        exit()
    
    # Run consolidation
    stats = consolidate_all_instruments(config)
    
    # Verify results
    verify_consolidated_data(config, sample_instruments=3)
    
    # Get summary
    summary_df = get_consolidated_summary(config)
    
    # Save summary
    if summary_df is not None:
        summary_path = os.path.join(config.OUTPUT_ROOT, "consolidation_summary.parquet")
        summary_df.to_parquet(summary_path, index=False)
        print(f"\n💾 Summary saved to: {summary_path}")