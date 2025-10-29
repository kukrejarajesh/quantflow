import time
import os

# --- Configuration ---
SIGNAL_FILE = 'trade_signals.txt'
# Delay between checking the file for new lines. 0.1 seconds is fast enough.
TAIL_DELAY_SECONDS = 0.1 

def process_signal(signal_line):
    """
    Function to process the new signal line.
    This function expects the line to be in the format: TIMESTAMP,TOKEN,ACTION,PRICE,QTY
    """
    try:
        # Expected format: TIMESTAMP,TOKEN,ACTION,PRICE,QTY
        parts = signal_line.strip().split(',')
        
        # We expect exactly 5 parts
        if len(parts) != 5:
            print(f"[ERROR] Malformed signal (Expected 5 fields, got {len(parts)}): {signal_line.strip()}")
            return

        timestamp, token, action, price, quantity = parts
        
        # --- Display and Execution ---
        print(f"\n|--------------------------------------|")
        print(f"| NEW SIGNAL RECEIVED: {action.ljust(4)}")
        print(f"|--------------------------------------|")
        print(f"| Time: {timestamp.split(' ')[1]}")
        print(f"| Token: {token}")
        print(f"| Price: {price}")
        print(f"| Qty: {quantity}")
        print(f"|--------------------------------------|")
        
        # --- YOUR ORDER PLACEMENT/EXECUTION LOGIC GOES HERE ---
        # Example: if action == 'BUY': place_buy_order(token, price, quantity)
        
    except Exception as e:
        print(f"[CRITICAL ERROR] Failed to process line: {e}")
        
def run_reader():
    """Continuously reads and processes new lines written to the file (file tailing)."""
    print(f"--- Signal Reader Running. Tailing {SIGNAL_FILE} ---")
    
    # 1. Wait for the writer to create the file if it doesn't exist
    while not os.path.exists(SIGNAL_FILE):
        print(f"Waiting for signal file '{SIGNAL_FILE}' to be created by the writer...")
        time.sleep(1)

    try:
        # Open the file for reading ('r')
        with open(SIGNAL_FILE, 'r') as f:
            # IMPORTANT: Move the cursor to the end of the file to ignore old, processed content
            f.seek(0, 2) 
            
            while True:
                # Read a new line. This is non-blocking; if no new data is present, it returns an empty string.
                line = f.readline()
                
                if line:
                    # New line found, process it
                    process_signal(line)
                else:
                    # No new line found, pause briefly and check again (tailing)
                    time.sleep(TAIL_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\nSignal Reader stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred in the reader: {e}")

if __name__ == "__main__":
    run_reader()