import subprocess
import sys
import time
from datetime import datetime, timedelta

# === CONFIGURATION ===
SCRIPT_1 = "src/datastream/TickLevelMetrics.py"
SCRIPT_2 = "src/datastream/Rollup_Metrics_Batch.py"
SCRIPT_3 = "src/utility/hourly_indicators_data_consolidator.py"
SCRIPT_4 = "src/utility/eod_ema_data_consolidator.py"
SCRIPT_5 = "src/datastream/Generate_z_score.py"


INTERVAL_MINUTES = 2  # Run every 15 minutes


def run_script(script_name):
    """Run a python script and return True if it succeeds, else False."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] ▶ Running {script_name}...")
    result = subprocess.run(
        [sys.executable, script_name],
        capture_output=True,
        text=True,
        encoding="utf-8"
    )

    if result.returncode == 0:
        print(f"✅ {script_name} completed successfully.")
    else:
        print(f"❌ {script_name} failed with return code {result.returncode}")

    # Optional: log outputs
    if result.stdout:
        print(f"--- STDOUT ---\n{result.stdout.strip()}")
    if result.stderr:
        print(f"--- STDERR ---\n{result.stderr.strip()}")

    return result.returncode == 0


def seconds_until_next_interval(minutes=15):
    """Compute seconds until the next time aligned to the system clock (e.g. 9:00, 9:15, 9:30...)."""
    now = datetime.now()
    next_minute = (now.minute // minutes + 1) * minutes
    next_run_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_minute)
    if next_run_time <= now:
        next_run_time += timedelta(minutes=minutes)
    return (next_run_time - now).total_seconds(), next_run_time


if __name__ == "__main__":
    print("🔁 Scheduler started. Press Ctrl+C to exit.\n")

    try:
        while True:
            # Calculate when to run next
            wait_seconds, next_run_time = seconds_until_next_interval(INTERVAL_MINUTES)
            print(f"🕒 Next run at: {next_run_time.strftime('%H:%M:%S')} (in {int(wait_seconds)} seconds)")
            time.sleep(wait_seconds)

            print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] === Starting 15-min cycle ===")

            # 1️⃣ Run first script
            success = run_script(SCRIPT_1)

            # 2️⃣ Run second script only if first succeeded
            if success:
                success = run_script(SCRIPT_2)
            else:
                print("⚠️ Skipping second script because first script failed.")

            if success:
                run_script(SCRIPT_3)
            else:
                print("⚠️ Skipping third script because second script failed.")
            if success:
                success = run_script(SCRIPT_4)
            else:
                print("⚠️ Skipping fourth script because third script failed.")

            if success:
                run_script(SCRIPT_5)
            else:
                print("⚠️ Skipping fifth script because fourth script failed.")

            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] === Cycle complete ===\n")

    except KeyboardInterrupt:
        print("\n🛑 Scheduler stopped by user. Exiting gracefully...")
