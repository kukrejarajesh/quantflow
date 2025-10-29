import re
import pandas as pd
import time
import os # Added for robust path handling
from datetime import datetime # Added for the example usage timestamp
from typing import List, Dict, Union, Optional

class RealTimeLogMonitor:
    """
    Monitors a log file that is being continuously written to by another program.
    Reads new signal entries and stores them in a DataFrame for analysis.
    """
    
    # Regex to capture the required fields from the log entry.
    # It handles both OHLC signals (with bar timestamp) and TICK signals (without bar timestamp).
    # Group 1: Log Timestamp (YYYY-MM-DD HH:MM:SS,mmm)
    # Group 2: Bar Timestamp (YYYY-MM-DD HH:MM:SS) - OPTIONAL for tick signals
    # Group 3: Token/Symbol ID
    # Group 4: Signal Direction (LONG or SHORT)
    # Group 5: Confidence Value (e.g., 0.70)
    # Group 6: Signal Source (ohlc, tick, etc.)
    LOG_PATTERN = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - INFO - 🎯 "
        # Group 2 (Bar Timestamp) is optional, captured via a non-capturing group
        r"(?:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s)?" 
        r"(\d+) "
        r"(LONG|SHORT) signal "
        r"\(Confidence: ([0-9.]+)\) "
        r"from (\w+)"
    )

    def __init__(self, log_filepath: str):
        """
        Initialize the monitor and sets the starting read position to the end of the file.
        """
        self.filepath = log_filepath
        self.last_read_position = 0
        self.signal_history = pd.DataFrame(
            columns=['log_timestamp', 'bar_timestamp', 'token_id', 'signal', 'confidence', 'source']
        )
        
        # Read to end initially so we only process new entries
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                f.seek(0, 2) # Move cursor to end of file
                self.last_read_position = f.tell()
                print(f"Monitoring log file: {self.filepath}. Starting from position {self.last_read_position}.")
        except FileNotFoundError:
            print(f"File not found: {self.filepath}. Will start monitoring when it is created.")
        except Exception as e:
            print(f"Error during initial file setup: {e}")


    def _parse_new_data(self, new_data: str) -> List[Dict[str, Union[str, float, int, pd.Timestamp, Optional[pd.Timestamp]]]]:
        """Parses new lines for signal entries."""
        parsed_data = []
        
        for line in new_data.splitlines():
            safe_line = line.strip()
            if not safe_line:
                continue
                
            match = self.LOG_PATTERN.search(safe_line)
            
            if match:
                # Groups: 1=LogTime, 2=BarTime (Optional), 3=Token, 4=Direction, 5=Confidence, 6=Source
                groups = match.groups()
                
                log_time_str = groups[0]
                bar_time_str = groups[1] # This will be None if the bar timestamp is missing (tick signal)
                token_id = groups[2]
                direction = groups[3]
                confidence_str = groups[4]
                source = groups[5]
                
                # Type Conversion
                log_timestamp = pd.to_datetime(log_time_str, format='%Y-%m-%d %H:%M:%S,%f')
                
                # Handle optional bar timestamp
                bar_timestamp = None
                if bar_time_str:
                    bar_timestamp = pd.to_datetime(bar_time_str, format='%Y-%m-%d %H:%M:%S')

                data_point = {
                    'log_timestamp': log_timestamp,
                    'bar_timestamp': bar_timestamp,
                    'token_id': int(token_id),
                    'signal': direction,
                    'confidence': float(confidence_str),
                    'source': source
                }
                parsed_data.append(data_point)
                
        return parsed_data

    def poll_for_new_signals(self) -> pd.DataFrame:
        """
        Checks the log file for new entries since the last read position,
        processes them, updates the history, and returns the new signals.
        """
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                # Move to the last known position
                f.seek(self.last_read_position)
                
                # Read all new content
                new_data = f.read()
                
                # Update the position to the end of the file (ready for the next poll)
                self.last_read_position = f.tell()
                
                if not new_data:
                    return pd.DataFrame() # No new data
                
                # Parse the new lines
                new_signals_list = self._parse_new_data(new_data)
                new_signals_df = pd.DataFrame(new_signals_list)
                
                if new_signals_df.empty:
                    return pd.DataFrame()
                
                # Append to history
                self.signal_history = pd.concat(
                    [self.signal_history, new_signals_df], 
                    ignore_index=True
                )
                
                return new_signals_df

        except FileNotFoundError:
            # If the file hasn't been created yet, just return empty
            return pd.DataFrame()
        except Exception as e:
            print(f"Error during polling: {e}")
            return pd.DataFrame()


if __name__ == '__main__':
    # --- Example Usage ---
    # Define the log directory and file name relative to the script's execution path
    log_dir = "logs"
    log_file_name = "orderflow_logs_20251024.log"
    
    # Construct the full path using os.path.join for robustness across operating systems
    # Script runs from F:\working\2024\Zerodha\breeze_data_service
    # Logs are in F:\working\2024\Zerodha\breeze_data_service\logs
    log_file_path = os.path.join(log_dir, log_file_name) 

    monitor = RealTimeLogMonitor(log_file_path)
    
    print("\n--- Starting Real-Time Log Monitoring ---")
    print("This program will poll the log file every 5 seconds.")
    print("Press Ctrl+C to stop.")
    
    poll_interval = 5  # seconds
    
    try:
        while True:
            new_signals = monitor.poll_for_new_signals()
            
            if not new_signals.empty:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] --- NEW SIGNALS FOUND ({len(new_signals)}) ---")
                
                # Print the new signals that were just processed
                print(new_signals[['log_timestamp', 'token_id', 'signal', 'confidence', 'source']].to_string(index=False))
                
                # You can now access the full history:
                # print("\nTotal signals tracked:", len(monitor.signal_history))
            
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        print("\n--- Monitoring stopped by user. ---")
        print(f"Total signals processed: {len(monitor.signal_history)}")
        print("\nFinal Signal Counts:")
        if not monitor.signal_history.empty:
            print(monitor.signal_history['signal'].value_counts())
