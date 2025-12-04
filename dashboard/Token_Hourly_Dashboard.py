"""
Streamlit Application - Hourly Consolidated Data Viewer
Display parquet data for selected tokens with interactive exploration
"""

import streamlit as st
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
from config import token_map, instruments_to_process
import time

# ==============================================================================
# CONFIGURATION
# ==============================================================================

CONSOLIDATED_HOURLY_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\consolidated_hourly"

# Define token mappings (instrument_token: friendly_name)
TOKEN_MAPPING = token_map
INSTRUMENTS_TO_PROCESS = instruments_to_process
# ==============================================================================
# PAGE CONFIGURATION
# ==============================================================================

st.set_page_config(
    page_title="Hourly Data Viewer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Add this - Auto-refresh every 60 seconds (adjust as needed)
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

refresh_interval = 60  # seconds
if time.time() - st.session_state.last_refresh > refresh_interval:
    st.session_state.last_refresh = time.time()
    st.rerun()

st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

@st.cache_data
def get_available_tokens(path):
    """
    Get list of available token files from INSTRUMENTS_TO_PROCESS
    """
    tokens = []
    
    if not os.path.exists(path):
        st.error(f"❌ Path not found: {path}")
        return tokens
    
    # Only process tokens defined in INSTRUMENTS_TO_PROCESS
    for token in INSTRUMENTS_TO_PROCESS:
        file_path = os.path.join(path, f"{token}.parquet")
        
        if os.path.exists(file_path):
            tokens.append(token)
        else:
            st.warning(f"⚠️  Token {token} file not found in directory")
    
    return sorted(tokens)

# CHANGE 3: Add this helper function to safely convert filenames to tokens (optional, if not using INSTRUMENTS_TO_PROCESS)
def safe_get_available_tokens(path):
    """
    Get all token files from folder, safely handling non-numeric filenames like 'consolidation_summary.parquet'
    Use this if you want to scan ALL tokens in folder instead of using INSTRUMENTS_TO_PROCESS
    """
    tokens = []
    
    if not os.path.exists(path):
        st.error(f"❌ Path not found: {path}")
        return tokens
    
    for file in os.listdir(path):
        if file.endswith('.parquet'):
            filename = file.replace('.parquet', '')
            
            # Only process if filename is a valid integer (token)
            try:
                token = int(filename)
                tokens.append(token)
            except ValueError:
                # Skip files that are not valid token numbers (like consolidation_summary.parquet)
                continue
    
    return sorted(tokens)

@st.cache_data(ttl=60)  # Cache expires after 60 seconds
def load_token_data(token):
    """
    Load parquet data for a specific token
    """
    file_path = os.path.join(CONSOLIDATED_HOURLY_PATH, f"{token}.parquet")
    
    if not os.path.exists(file_path):
        st.error(f"❌ File not found: {file_path}")
        return None
    
    try:
        df = pd.read_parquet(file_path)
        
        # Convert timestamp columns
        if 'exchange_timestamp' in df.columns:
            df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
        if 'trading_date' in df.columns:
            df['trading_date'] = pd.to_datetime(df['trading_date'])
        
        return df
    
    except Exception as e:
        st.error(f"❌ Error loading file: {e}")
        return None


def get_token_name(token):
    """
    Get friendly name for token, fallback to token number
    """
    return TOKEN_MAPPING.get(token, f"Token {token}")


# ==============================================================================
# MAIN APPLICATION
# ==============================================================================

def main():
    st.title("📊 Hourly Consolidated Data Viewer")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        st.write(f"📁 Data Path: `{CONSOLIDATED_HOURLY_PATH}`")
        
        # Get available tokens
        available_tokens = get_available_tokens(CONSOLIDATED_HOURLY_PATH)
        
        #to get all tokens in folder, use safe_get_available_tokens
        #available_tokens = safe_get_available_tokens(CONSOLIDATED_HOURLY_PATH)

        if not available_tokens:
            st.error("❌ No token files found in the directory")
            return
        
        st.write(f"✓ Found {len(available_tokens)} tokens")
        
        # Create token options with friendly names
        token_options = {get_token_name(t): t for t in available_tokens}
        
        # Token selection dropdown
        selected_name = st.selectbox(
            "Select Token:",
            options=list(token_options.keys()),
            key="token_selector"
        )
        
        selected_token = token_options[selected_name]
        
        st.divider()
        
        # Display options
        st.subheader("Display Options")
        
        rows_to_show = st.slider(
            "Rows to display:",
            min_value=10,
            max_value=500,
            value=50,
            step=10
        )
        
        show_statistics = st.checkbox("Show Statistics", value=True)
        show_chart = st.checkbox("Show Charts", value=True)
        show_raw_data = st.checkbox("Show Raw Data", value=True)
    
    # Load data
    with st.spinner(f"🔄 Loading data for {selected_name}..."):
        df = load_token_data(selected_token)
    
    if df is None:
        return
    
    # Display header info
    st.markdown(f"### {selected_name} - Token: `{selected_token}`")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Total Records", len(df))
    with col2:
        st.metric("📅 Start Date", df['exchange_timestamp'].min().strftime('%Y-%m-%d %H:%M'))
    with col3:
        st.metric("📅 End Date", df['exchange_timestamp'].max().strftime('%Y-%m-%d %H:%M'))
    with col4:
        st.metric("📦 Columns", len(df.columns))
    
    st.divider()
    
    # Statistics section
    if show_statistics:
        st.subheader("📈 Statistics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Price Statistics (Close)**")
            price_stats = df['ohlc_close'].describe()
            st.dataframe(price_stats, use_container_width=True)
        
        with col2:
            st.write("**Volume Statistics**")
            if 'volume_traded' in df.columns:
                vol_stats = df['volume_traded'].describe()
                st.dataframe(vol_stats, use_container_width=True)
        
        # Indicator statistics
        indicator_cols = [col for col in df.columns if col.startswith('EMA_') or col in ['ATR', 'ATR_EMA', 'RSI']]
        
        if indicator_cols:
            st.write("**Latest Indicator Values**")
            latest_indicators = df[['exchange_timestamp'] + indicator_cols].tail(1)
            st.dataframe(latest_indicators.iloc[0], use_container_width=True)
    
    st.divider()
    
    # Charts section
    if show_chart:
        st.subheader("📊 Charts")
        
        col1, col2 = st.columns(2)
        
        # Price chart with EMAs
        with col1:
            st.write("**Price & EMAs**")
            
            fig = go.Figure()
            
            # Add close price
            fig.add_trace(go.Scatter(
                x=df['exchange_timestamp'],
                y=df['ohlc_close'],
                mode='lines',
                name='Close',
                line=dict(color='blue', width=2)
            ))
            
            # Add EMAs if available
            ema_cols = [col for col in df.columns if col.startswith('EMA_')]
            colors = ['red', 'green', 'orange', 'purple']
            
            for i, ema_col in enumerate(ema_cols[:4]):
                fig.add_trace(go.Scatter(
                    x=df['exchange_timestamp'],
                    y=df[ema_col],
                    mode='lines',
                    name=ema_col,
                    line=dict(width=1, dash='dash')
                ))
            
            fig.update_layout(
                title="Price & EMAs",
                xaxis_title="Timestamp",
                yaxis_title="Price",
                hovermode='x unified',
                height=400,
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Volume chart
        with col2:
            if 'volume_traded' in df.columns:
                st.write("**Trading Volume**")
                
                fig = go.Figure()
                
                fig.add_trace(go.Bar(
                    x=df['exchange_timestamp'],
                    y=df['volume_traded'],
                    name='Volume',
                    marker_color='lightblue'
                ))
                
                fig.update_layout(
                    title="Trading Volume",
                    xaxis_title="Timestamp",
                    yaxis_title="Volume",
                    hovermode='x unified',
                    height=400,
                    template='plotly_white'
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        # RSI chart
        rsi_cols = [col for col in df.columns if col.startswith('RSI_')]
        if rsi_cols:
            st.write("**RSI (Relative Strength Index)**")
            
            fig = go.Figure()
            
            for rsi_col in rsi_cols:
                fig.add_trace(go.Scatter(
                    x=df['exchange_timestamp'],
                    y=df[rsi_col],
                    mode='lines',
                    name=rsi_col,
                    line=dict(width=2)
                ))
            
            # Add overbought/oversold lines
            fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)")
            fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)")
            
            fig.update_layout(
                title="RSI Indicator",
                xaxis_title="Timestamp",
                yaxis_title="RSI",
                hovermode='x unified',
                height=350,
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # ATR chart
        if 'ATR' in df.columns:
            st.write("**Average True Range (ATR)**")
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['exchange_timestamp'],
                y=df['ATR'],
                mode='lines',
                name='ATR',
                line=dict(color='orange', width=2)
            ))
            
            if 'ATR_EMA' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['exchange_timestamp'],
                    y=df['ATR_EMA'],
                    mode='lines',
                    name='ATR EMA',
                    line=dict(color='red', width=2, dash='dash')
                ))
            
            fig.update_layout(
                title="ATR Indicator",
                xaxis_title="Timestamp",
                yaxis_title="ATR Value",
                hovermode='x unified',
                height=350,
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Raw data section
    if show_raw_data:
        st.subheader("📋 Raw Data")
        
        # Filter options
        col1, col2 = st.columns(2)
        
        with col1:
            show_latest = st.radio(
                "Show:",
                ("Latest N rows", "Filter by date range"),
                horizontal=True
            )
        
        if show_latest == "Latest N rows":
            display_df = df.tail(rows_to_show).copy()
        else:
            date_range = st.date_input(
                "Select date range:",
                value=(df['exchange_timestamp'].min().date(), df['exchange_timestamp'].max().date()),
                key="date_range"
            )
            
            if len(date_range) == 2:
                mask = (df['exchange_timestamp'].dt.date >= date_range[0]) & \
                       (df['exchange_timestamp'].dt.date <= date_range[1])
                display_df = df[mask].copy()
        
        # Column selection
        col_selection = st.multiselect(
            "Select columns to display:",
            options=df.columns.tolist(),
            default=['exchange_timestamp', 'ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close', 'volume_traded']
        )
        
        if col_selection:
            display_df = display_df[col_selection]
        
        st.dataframe(display_df, use_container_width=True, height=400)
        
        # Download button
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"{selected_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    st.divider()
    
    # Footer
    st.markdown("""
    ---
    📊 **Hourly Consolidated Data Viewer** | Data sourced from consolidated_hourly directory
    """)

    # Auto-refresh logic
    refresh_enabled = True
    if refresh_enabled:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()