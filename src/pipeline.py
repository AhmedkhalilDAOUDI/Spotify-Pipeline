import schedule
import time
from extract import run_extraction
from transform import run_transformation, load_latest_parquet
from load import run_load

def run_pipeline():
    print("\n--- Pipeline started ---")
    try:
        run_extraction()
        recent_df, top_df = run_transformation()
        run_load(recent_df, top_df)
        print("--- Pipeline complete ---\n")
    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()
    schedule.every(1).hours.do(run_pipeline)
    print("Scheduler running. Pipeline will execute every hour.")
    while True:
        schedule.run_pending()
        time.sleep(60)