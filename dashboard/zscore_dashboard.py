import streamlit as st
import pandas as pd
import os
import time

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
        latest_time = df["exchange_timestamp"].max()
        st.subheader(f"📅 Latest Snapshot — {latest_time}")

        latest_df = df[df["exchange_timestamp"] == latest_time].copy()
        latest_df = latest_df.sort_values("z_score", ascending=False)

        styled_df = (
            latest_df.style
            .apply(highlight_z_alerts, axis=1)  # ✅ green/red highlight
            .format({
                "Mean": "{:,.2f}",
                "S.D.": "{:,.2f}",
                "15min_Volume": "{:,.0f}",
                "z_score": "{:,.2f}"
            })
        )

        st.dataframe(styled_df, use_container_width=True, height=500)

    else:
        token = int(selected_instrument)
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

            st.dataframe(styled_df, use_container_width=True, height=600)
            st.line_chart(
                inst_df.set_index("exchange_timestamp")[["z_score"]],
                height=300
            )


# === INITIAL LOAD ===
if os.path.exists(DATA_PATH):
    mod_time = os.path.getmtime(DATA_PATH)
    df = load_data(DATA_PATH, mod_time)
else:
    st.warning("⚠️ dashboard_zscores.parquet not found.")
    df = pd.DataFrame()

if df.empty:
    st.stop()

# Sidebar filters
instruments = sorted(df["instrument_token"].unique())
selected_instrument = st.sidebar.selectbox("🎯 Select Instrument", ["(All)"] + list(map(str, instruments)))

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
