import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict
import threading
import time # Added for timed flushing

# --- Configuration ---
# The root directory where data will be stored (e.g., tick_data/2025-10-28/123.parquet)
BASE_TICK_DATA_DIR = "tick_data"
PARQUET_ENGINE = 'pyarrow'
PARQUET_COMPRESSION = 'snappy' 
# Define the maximum number of ticks to store in the buffer before flushing to disk
FLUSH_THRESHOLD = 2000 
# NEW: Define how often to force a flush of all pending buffers (e.g., every 30 seconds)
FLUSH_INTERVAL_SECONDS = 30 

def write_ticks_to_parquet(tick_data_list: List[Dict[str, Any]], base_dir: str = BASE_TICK_DATA_DIR):
    """
    Writes a list of flattened tick dictionaries to separate Parquet files, 
    partitioned by date and instrument token.

    Directory Structure: <base_dir>/<Current_Date>/<Token>.parquet
    
    This function should be called on a buffered list of ticks to ensure high 
    performance and efficient columnar storage.

    Args:
        tick_data_list: A list of dictionaries, where each dictionary represents a single tick.
                        Must contain 'timestamp' and 'token' fields.
        base_dir: The root directory for all tick data storage.
    """
    # for i, tick in enumerate(tick_data_list):
    #     print(i, type(tick), tick)

    if not tick_data_list:
        return
        
    print(f"Starting write process for {len(tick_data_list)} ticks...")

    # 1. Convert to DataFrame
    df = pd.DataFrame(tick_data_list)
    #print(f"dataframe columns: {df.columns.tolist()}")
    # Ensure mandatory columns exist
    if 'instrument_token' not in df.columns or 'exchange_timestamp' not in df.columns:
        print("Error: DataFrame must contain 'instrument_token' and 'excahnge_timestamp' columns.")
        return

    # 2. Prepare Timestamps and Date Keys
    df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
    df['trading_date'] = df['exchange_timestamp'].dt.strftime('%Y-%m-%d')
    
    # 3. Iterate and Write Partitions
    grouped = df.groupby(['trading_date', 'instrument_token'])
    #print(f"grouped columns: {grouped.groups.keys()}")
    for (date_key, token_key), group in grouped:
        try:
            date_dir = os.path.join(base_dir, date_key)
            file_name = f"{token_key}.parquet"
            file_path = os.path.join(date_dir, file_name)
            
            os.makedirs(date_dir, exist_ok=True)
            file_exists = os.path.exists(file_path) and os.path.getsize(file_path) > 0
            
            if file_exists:
                existing_df = pd.read_parquet(file_path, engine=PARQUET_ENGINE)
                df_to_write = pd.concat([existing_df, group], ignore_index=True)
            else:
                df_to_write = group


            # Write to Parquet file
            df_to_write.to_parquet(
                file_path,
                engine=PARQUET_ENGINE,
                compression=PARQUET_COMPRESSION,
                index=False
                
            )
            
        except Exception as e:
            print(f"Error writing partition {date_key}/{token_key}: {e}")
            
# --- Tick Buffer Manager Class ---

class TickBufferManager:
    """
    Manages tick buffers per instrument token and flushes data to Parquet 
    when the FLUSH_THRESHOLD is reached or the FLUSH_INTERVAL_SECONDS has passed.
    
    This class should be instantiated once in your main streaming loop.
    """
    def __init__(self, threshold: int = FLUSH_THRESHOLD, base_dir: str = BASE_TICK_DATA_DIR, interval: int = FLUSH_INTERVAL_SECONDS):
        self.buffers: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        self.threshold = threshold
        self.base_dir = base_dir
        self.total_ticks_received = 0
        self.total_flushes = 0
        self._running = True
        
        # 💡 CRITICAL FIX: Initialize a Lock to protect the shared 'buffers' dictionary
        self._lock = threading.Lock()
        # New timed flush mechanism
        self._flush_interval = interval
        self._flush_thread = threading.Thread(target=self._start_timed_flush, daemon=True)
        self._flush_thread.start()
        
        print(f"Tick Buffer Manager initialized with threshold: {self.threshold} ticks/token and timed flush every {self._flush_interval} seconds.")

    def _start_timed_flush(self):
        """Runs in a separate thread to periodically call flush_all."""
        while self._running:
            time.sleep(self._flush_interval)
            if self._running:
                
                with self._lock: 
                    has_data = any(self.buffers.values())
                # Only flush if there is data to be flushed
                if has_data:
                    print(f"\n--- TIMED FLUSH ({self._flush_interval}s interval) ---")
                    # is_final=False tells flush_all not to print the final summary or stop the thread
                    try: 
                        self.flush_all(is_final=False) 
                        print("--- TIMED FLUSH COMPLETE ---")
                    except Exception as e:
                        # Catch the error here so the thread doesn't die completely
                        print(f"Timed flush failed: {e}")

    def add_tick(self, tick: Dict[str, Any]):
        """
        Adds a single tick to the appropriate instrument buffer and checks the threshold.
        """
        
        token = tick.get('instrument_token')
        if token is None:
            print("Warning: Tick missing 'token' key. Skipping.")
            return
        # 🔑 ACQUIRE LOCK: Ensure no other thread can access the buffer while adding a tick
        with self._lock:
            self.buffers[token].append(tick)
            self.total_ticks_received += 1
        
        # Check if the buffer for this token is full
            if len(self.buffers[token]) >= self.threshold:
                self.flush_token_buffer(token)
         # 🔓 RELEASE LOCK: Lock is automatically released here
    def flush_token_buffer(self, token: int):
        """
        Flushes the buffer for a single instrument token to Parquet.
        """
        if self.buffers[token]:
            ticks_to_write = self.buffers[token]
            write_ticks_to_parquet(ticks_to_write, self.base_dir)
            
            self.buffers[token] = []
            self.total_flushes += 1
            print(f"--- Flushed {len(ticks_to_write)} ticks for Token {token}. Total flushes: {self.total_flushes}")

    def flush_all(self, is_final: bool = True):
        """
        Flushes all remaining buffers. Called at session end or by timed thread.
        """
        if is_final:
            print("\n--- FINAL FLUSH: Processing all remaining buffers... ---")
            self._running = False # Stop the timed flush thread
        
        flushed_count = 0
        # Iterate over a copy of keys, as the dict size may change during iteration
        for token in list(self.buffers.keys()): 
            if self.buffers[token]:
                self.flush_token_buffer(token)
                flushed_count += 1
        
        if is_final:
            print(f"Final flush complete. {flushed_count} buffers written.")
            print(f"Total Ticks Received during session: {self.total_ticks_received}")
