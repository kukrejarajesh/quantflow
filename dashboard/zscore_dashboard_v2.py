import streamlit as st
import pandas as pd
import os
import time
import altair as alt
from config import token_map, instruments_to_process

allowed_tokens = instruments_to_process
token_to_name = token_map
name_to_token = {v: k for k, v in token_map.items()}

# === CONFIG ===
DATA_PATH = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly\dashboard_zscores.parquet"

# === PAGE SETUP ===
st.set_page_config(
    page_title="Z-Score Analytics Dashboard",
    layout="wide"
)

st.title("📊 Real-Time Z-Score Dashboard (Auto-Refresh + Alerts)")

# === HELPER FUNCTIONS ===
@st.cache_data
def load_data(path, mod_time):
    """Load parquet and cache until file changes."""
    df = pd.read_parquet(path)
    df["exchange_timestamp"] = pd.to_datetime(df["exchange_timestamp"])
    df = df.sort_values(["exchange_timestamp", "instrument_token"])
    return df


def highlight_z_alerts(row):
    """Highlight entire row green if z>2, red if z<-2."""
    if row["z_score"] > 2:
        color = "background-color: lightgreen"
    elif row["z_score"] < -2:
        color = "background-color: lightcoral"
    else:
        color = ""
    return [color] * len(row)


def display_dashboard(df, selected_instrument, selected_time):
    """Render dashboard based on user selections."""
    if df.empty:
        st.warning("No data to display.")
        return

    if selected_instrument == "(All)":
        recent_times = sorted(df["exchange_timestamp"].unique())[-2:]
        st.subheader(f"📅 Latest Snapshot — {recent_times[-1]}")

        
        latest_df = df[df["exchange_timestamp"].isin(recent_times)].copy()

        styled_df = (
            latest_df.style
            .apply(highlight_z_alerts, axis=1)  # ✅ green/red highlight
            .format({
                "ohlc_open": "{:,.2f}",
                "ohlc_high": "{:,.2f}",
                "ohlc_low": "{:,.2f}",
                "ohlc_close": "{:,.2f}",
                "vwap": "{:,.2f}",
                "Mean": "{:,.2f}",
                "S.D.": "{:,.2f}",
                "15min_Volume": "{:,.0f}",
                "z_score": "{:,.2f}"
            })
        )

        st.dataframe(styled_df, use_container_width=True, height=300)

    else:
        # token = int(selected_instrument)
        token = name_to_token.get(selected_instrument)
        inst_df = df[df["instrument_token"] == token]
        inst_df = df[df["instrument_token"] == token].copy()
        if inst_df.empty:
            st.warning(f"No data found for instrument {token}")
            return

        if selected_time != "(Latest)":
            selected_timestamp = pd.to_datetime(selected_time)
            record = inst_df[inst_df["exchange_timestamp"] == selected_timestamp]
            if record.empty:
                st.warning("No record found for this timestamp.")
            else:
                st.subheader(f"📈 Instrument {token} — {selected_time}")
                st.dataframe(record, use_container_width=True)
        else:
            st.subheader(f"📈 Historical view — Instrument {token} (latest first)")
            inst_df = inst_df.sort_values("exchange_timestamp", ascending=False)

            styled_df = (
                inst_df.style
                .apply(highlight_z_alerts, axis=1)
                .format({
                    "Mean": "{:,.2f}",
                    "S.D.": "{:,.2f}",
                    "15min_Volume": "{:,.0f}",
                    "z_score": "{:,.2f}"
                })
            )

            st.dataframe(styled_df, use_container_width=True, height=300)
            
            chart_data = inst_df.copy()
            chart_data["exchange_timestamp"] = pd.to_datetime(chart_data["exchange_timestamp"])
            chart_data["exchange_timestamp"] = pd.to_datetime(chart_data["exchange_timestamp"])
            chart_data = chart_data[
                chart_data["exchange_timestamp"].dt.time.between(
                    pd.to_datetime("09:15:00").time(),
                    pd.to_datetime("15:30:00").time()
                )
            ]

            # --- Shared X-axis scale for syncing ---
            if not chart_data["exchange_timestamp"].empty:
                # Ensure timestamps are datetime
                chart_data["exchange_timestamp"] = pd.to_datetime(chart_data["exchange_timestamp"])

                # Determine earliest and latest dates in dataset
                start_date = chart_data["exchange_timestamp"].dt.normalize().min()
                end_date = chart_data["exchange_timestamp"].dt.normalize().max()

                # NSE market open/close times for first and last days
                market_open = start_date + pd.Timedelta(hours=9, minutes=15)
                market_close = end_date + pd.Timedelta(hours=15, minutes=30)

                # Define the full x-axis range across multiple days
                x_scale = alt.Scale(domain=[market_open, market_close])
            else:
                x_scale = alt.Scale()

            x = alt.X("exchange_timestamp:T", scale=x_scale, title="Time")
            # --- Chart 1: VWAP and Close Price ---
            # --- Dynamic Y-axis range for Price chart ---
            v_min = chart_data[["vwap", "ohlc_close"]].min().min()
            v_max = chart_data[["vwap", "ohlc_close"]].max().max()

            # Add a small 1% padding for nice spacing
            v_pad = (v_max - v_min) * 0.01 if v_max != v_min else 1
            v_domain = [v_min - v_pad, v_max + v_pad]
            price_chart = (
                alt.Chart(chart_data)
                .transform_fold(
                    ["vwap", "ohlc_close"],
                    as_=["Metric", "Value"]
                )
                .mark_line()
                .encode(
                    x=x,
                    y=alt.Y("Value:Q", title="Price (VWAP / Close)", scale=alt.Scale(domain=v_domain)),
                    color=alt.Color("Metric:N", legend=alt.Legend(title="Metric")),
                    tooltip=["exchange_timestamp:T", "Metric:N", "Value:Q"]
                )
                .properties(
                    title="💰 VWAP & Close Price Trend",
                    height=300
                )
            )
            # --- Chart 2: Z-Score ---
            zscore_chart = (
                alt.Chart(chart_data)
                .mark_line(color="red")
                .encode(
                    x=x,
                    y=alt.Y("z_score:Q", title="Z-Score"),
                    tooltip=["exchange_timestamp:T", "z_score:Q"]
                )
                .properties(
                    title="📊 Z-Score Trend",
                    height=200
                )
            )
            
           # --- Link charts with shared X-axis ---
            combined_chart = alt.vconcat(price_chart, zscore_chart).resolve_scale(x="shared")

            st.markdown("### 📈 Instrument Trend Overview (Synced Charts)")
            st.altair_chart(combined_chart.interactive().configure_axis(grid=True), use_container_width=True)

            # st.markdown("### 📈 VWAP, Close & Z-Score (Dual Axis)")
            # st.altair_chart(final_chart, use_container_width=True)
            # #st.altair_chart(price_lines + zscore_line, use_container_width=True)
            # # st.line_chart(
            # #     inst_df.set_index("exchange_timestamp")[["z_score", "vwap", "ohlc_close"]],
            # #     height=300
            # # )


# === INITIAL LOAD ===
if os.path.exists(DATA_PATH):
    # Keep only configured instruments
    
    

    mod_time = os.path.getmtime(DATA_PATH)
    df = load_data(DATA_PATH, mod_time)
    df = df[df["instrument_token"].isin(allowed_tokens)]
    df["instrument_name"] = df["instrument_token"].map(token_to_name)
    # # Ensure exchange_timestamp is proper datetime (not string)
    # df["exchange_timestamp"] = pd.to_datetime(df["exchange_timestamp"], errors="coerce")

    # # Create a pure date column for filtering
    # df["date_only"] = df["exchange_timestamp"].dt.date


else:
    st.warning("⚠️ dashboard_zscores.parquet not found.")
    df = pd.DataFrame()

if df.empty:
    st.stop()

# Sidebar filters
# instruments = sorted(df["instrument_token"].unique())
# selected_instrument = st.sidebar.selectbox("🎯 Select Instrument", ["(All)"] + list(map(str, instruments)))

instrument_list = ["(All)"] + sorted(df["instrument_name"].dropna().unique())
selected_instrument = st.sidebar.selectbox("🎯 Select Instrument", instrument_list)
# === Date Filter ===
dates = sorted(df["exchange_timestamp"].dt.date.unique(), reverse=True)
#dates = sorted(df["date_only"].unique(), reverse=True)
selected_date = st.sidebar.selectbox("📅 Select Date", ["(All)"] + [str(d) for d in dates])

# Apply date filter
if selected_date != "(All)":
    df = df[df["exchange_timestamp"].dt.date == pd.to_datetime(selected_date).date()]
    # selected_date = pd.to_datetime(selected_date).date()
    # df = df[df["date_only"] == selected_date]

# Time options (descending)
times = sorted(df["exchange_timestamp"].unique(), reverse=True)
time_options = ["(Latest)"] + [t.strftime("%Y-%m-%d %H:%M:%S") for t in times]
selected_time = st.sidebar.selectbox("🕒 Select Time", time_options)

st.markdown("---")

# === AUTO REFRESH CONTROLS ===
auto_refresh = st.sidebar.toggle("🔄 Auto-refresh", value=True)
refresh_interval = st.sidebar.slider("⏱️ Refresh Interval (seconds)", 10, 120, 30)

# === DASHBOARD LOOP ===
placeholder = st.empty()

def run_dashboard():
    if os.path.exists(DATA_PATH):
        mod_time = os.path.getmtime(DATA_PATH)
        df = load_data(DATA_PATH, mod_time)
        df = df[df["instrument_token"].isin(allowed_tokens)]
        df["instrument_name"] = df["instrument_token"].map(token_to_name)
        # Ensure exchange_timestamp is proper datetime (not string)
        # df["exchange_timestamp"] = pd.to_datetime(df["exchange_timestamp"], errors="coerce")

        # # Create a pure date column for filtering
        # df["date_only"] = df["exchange_timestamp"].dt.date
    else:
        st.warning("⚠️ dashboard_zscores.parquet not found.")
        df = pd.DataFrame()

    with placeholder.container():
        display_dashboard(df, selected_instrument, selected_time)

if auto_refresh:
    last_modified = 0
    while True:
        if os.path.exists(DATA_PATH):
            mod_time = os.path.getmtime(DATA_PATH)
            if mod_time != last_modified:
                run_dashboard()
                last_modified = mod_time
        else:
            st.warning("dashboard_zscores.parquet not found yet.")
        time.sleep(refresh_interval)
else:
    run_dashboard()

st.markdown("---")
st.caption(
    "🟩 Z-score > 2 → unusually high activity | 🟥 Z-score < -2 → unusually low activity"
)
