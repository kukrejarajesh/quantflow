"""
Streamlit RSI Opportunity Monitor
Displays hourly data with RSI-based buy/sell opportunities
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
from config import token_map, instruments_to_process

# ==============================================================================
# CONFIGURATION
# ==============================================================================

HOURLY_DATA_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_hourly"

# Token mapping
TOKEN_MAP = token_map

# Instruments to process
INSTRUMENTS_TO_PROCESS = instruments_to_process

# Key columns to display
DISPLAY_COLUMNS = [
    'exchange_timestamp',
    'ohlc_open',
    'ohlc_high',
    'ohlc_low',
    'ohlc_close',
    'volume_traded',
    'RSI_6'
]

# ==============================================================================
# STREAMLIT CONFIG
# ==============================================================================

st.set_page_config(page_title="RSI Opportunity Monitor", layout="wide")

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

@st.cache_data(ttl=60)
def load_instrument_data(token):
    """Load hourly parquet data for a token"""
    file_path = os.path.join(HOURLY_DATA_PATH, f"{token}.parquet")
    
    if not os.path.exists(file_path):
        return None
    
    try:
        df = pd.read_parquet(file_path)
        df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
        df = df.sort_values('exchange_timestamp')
        return df
    except Exception as e:
        st.error(f"Error loading {token}: {e}")
        return None


def detect_buy_opportunities(df):
    """
    Detect buy opportunities:
    - RSI_6 goes below 10 (Red)
    - Next record RSI_6 goes above 10 (Green)
    """
    opportunities = []
    
    if len(df) < 2:
        return pd.DataFrame(opportunities)
    
    rsi = df['RSI_6'].values
    
    for i in range(1, len(df)):
        # Check if previous RSI was below 10 and current is above 10
        if rsi[i-1] < 10 and rsi[i] >= 10:
            opportunities.append({
                'timestamp': df.iloc[i]['exchange_timestamp'],
                'close': df.iloc[i]['ohlc_close'],
                'rsi_prev': rsi[i-1],
                'rsi_current': rsi[i],
                'type': 'Buy Signal'
            })
    
    return pd.DataFrame(opportunities)


def detect_sell_opportunities(df):
    """
    Detect sell opportunities:
    - RSI_6 goes above 90 (Blue)
    - Next record RSI_6 goes below 90 (Gold)
    """
    opportunities = []
    
    if len(df) < 2:
        return pd.DataFrame(opportunities)
    
    rsi = df['RSI_6'].values
    
    for i in range(1, len(df)):
        # Check if previous RSI was above 90 and current is below 90
        if rsi[i-1] > 90 and rsi[i] <= 90:
            opportunities.append({
                'timestamp': df.iloc[i]['exchange_timestamp'],
                'close': df.iloc[i]['ohlc_close'],
                'rsi_prev': rsi[i-1],
                'rsi_current': rsi[i],
                'type': 'Sell Signal'
            })
    
    return pd.DataFrame(opportunities)


def get_token_name(token):
    """Get friendly name for token"""
    return TOKEN_MAP.get(token, f"Token {token}")


def style_dataframe_rsi(df):
    """
    Style dataframe based on RSI values:
    - RSI < 10: Red background
    - RSI > 90: Blue background
    """
    def highlight_rsi(val):
        if pd.isna(val):
            return ''
        if val < 10:
            return 'background-color: #ff6b6b'  # Red
        elif val > 90:
            return 'background-color: #4dabf7'  # Blue
        return ''
    
    # Apply styling to RSI_6 column if it exists
    if 'RSI_6' in df.columns:
        return df.style.applymap(highlight_rsi, subset=['RSI_6'])
    return df.style


# ==============================================================================
# AUTO-REFRESH SETUP
# ==============================================================================

# Initialize refresh timer
if "refresh_counter" not in st.session_state:
    st.session_state.refresh_counter = 0

# Auto-refresh using placeholder
refresh_placeholder = st.empty()


# ==============================================================================
# STREAMLIT APP
# ==============================================================================

st.title("📊 RSI Opportunity Monitor")
st.markdown("---")

# Sidebar for instrument selection
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Create options list
    options = ["All"] + [f"{get_token_name(t)} ({t})" for t in INSTRUMENTS_TO_PROCESS]
    
    selected = st.selectbox(
        "Select Instrument:",
        options,
        index=0
    )
    
    st.divider()
    
    # Auto-refresh countdown
    @st.fragment(run_every=60)
    def countdown_timer():
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        with col2:
            st.success("🔄 Auto-refreshing...")
    
    countdown_timer()

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📈 Hourly Data")

with col2:
    st.metric("Total Instruments", len(INSTRUMENTS_TO_PROCESS))

# Process based on selection
if selected == "All":
    # Show last 2 records for all instruments in single table
    st.markdown("### Last 2 Records for All Instruments")
    all_data = []
    
    for token in INSTRUMENTS_TO_PROCESS:
        df = load_instrument_data(token)
        
        if df is None or len(df) == 0:
            continue
        
        # Get last 2 records
        display_df = df[DISPLAY_COLUMNS].tail(2).copy()
        display_df['instrument'] = get_token_name(token)
        display_df['token'] = token
        all_data.append(display_df)
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df['exchange_timestamp'] = combined_df['exchange_timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    
    # Reorder columns - instrument first
    cols = ['instrument', 'token'] + [col for col in combined_df.columns if col not in ['instrument', 'token']]
    combined_df = combined_df[cols]
    
    # Format numeric columns
    for col in ['ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close']:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(lambda x: f"{x:.2f}")
    
    combined_df['RSI_6'] = combined_df['RSI_6'].apply(lambda x: f"{x:.2f}")
    # Apply styling based on RSI values
    def highlight_rsi(row):
        rsi_val = float(row['RSI_6'])
        if rsi_val < 10:
            return ['background-color: #ff6b6b'] * len(row) if row.name % 2 == 0 else [''] * len(row)
        elif rsi_val > 90:
            return ['background-color: #4dabf7'] * len(row) if row.name % 2 == 0 else [''] * len(row)
        return [''] * len(row)
    
    styled_df = combined_df.style.apply(highlight_rsi, axis=1)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    
else:
    # Single instrument selected
    token = int(selected.split('(')[-1].rstrip(')'))
    df = load_instrument_data(token)
    
    if df is not None and len(df) > 0:
        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["📊 Data", "🟢 Buy Signals", "🔴 Sell Signals"])
        
        # Tab 1: Last 30 records
        with tab1:
            st.subheader(f"Last 30 Records - {get_token_name(token)}")
            
            display_df = df[DISPLAY_COLUMNS].tail(30).copy()
            display_df['exchange_timestamp'] = display_df['exchange_timestamp'].dt.strftime('%Y-%m-%d %H:%M')
            
            # Format numeric columns
            for col in ['ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close']:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}")
            
            display_df['RSI_6'] = display_df['RSI_6'].apply(lambda x: f"{x:.2f}")
            # Apply styling based on RSI values
            def highlight_rsi_row(row):
                rsi_val = float(row['RSI_6'])
                if rsi_val < 10:
                    return ['background-color: #ff6b6b'] * len(row)  # Red
                elif rsi_val > 90:
                    return ['background-color: #4dabf7'] * len(row)  # Blue
                return [''] * len(row)
            
            styled_df = display_df.style.apply(highlight_rsi_row, axis=1)
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
           
        
        # Tab 2: Buy Opportunities
        with tab2:
            st.subheader("🟢 Buy Opportunities (RSI < 10 → RSI >= 10)")
            
            buy_opp = detect_buy_opportunities(df)
            
            if len(buy_opp) > 0:
                buy_opp['timestamp'] = buy_opp['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                buy_opp['close'] = buy_opp['close'].apply(lambda x: f"{x:.2f}")
                buy_opp['rsi_prev'] = buy_opp['rsi_prev'].apply(lambda x: f"{x:.2f}")
                buy_opp['rsi_current'] = buy_opp['rsi_current'].apply(lambda x: f"{x:.2f}")
                
                # Color code the rows
                st.markdown(
                    """
                    <style>
                    .buy-row { background-color: #d4edda; }
                    </style>
                    """,
                    unsafe_allow_html=True
                )
                
                st.dataframe(buy_opp, use_container_width=True, hide_index=True)
            else:
                st.info("No buy opportunities detected")
        
        # Tab 3: Sell Opportunities
        with tab3:
            st.subheader("🔴 Sell Opportunities (RSI > 90 → RSI <= 90)")
            
            sell_opp = detect_sell_opportunities(df)
            
            if len(sell_opp) > 0:
                sell_opp['timestamp'] = sell_opp['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                sell_opp['close'] = sell_opp['close'].apply(lambda x: f"{x:.2f}")
                sell_opp['rsi_prev'] = sell_opp['rsi_prev'].apply(lambda x: f"{x:.2f}")
                sell_opp['rsi_current'] = sell_opp['rsi_current'].apply(lambda x: f"{x:.2f}")
                
                st.dataframe(sell_opp, use_container_width=True, hide_index=True)
            else:
                st.info("No sell opportunities detected")
    else:
        st.error(f"No data found for {selected}")

st.markdown("---")
st.caption("Data sourced from consolidated_hourly parquet files | Auto-refreshes every 60 seconds")