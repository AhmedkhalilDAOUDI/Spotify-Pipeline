import pandas as pd
import pyarrow.parquet as pq
import glob
import os

def load_latest_parquet(name):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pattern = os.path.join(base_dir, "data", "raw", f"{name}_*.parquet")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No parquet files found for {name}")
    latest = max(files, key=os.path.getctime)
    return pq.read_table(latest).to_pandas()

def transform_recently_played(df):
    df = df.drop_duplicates(subset=["played_at", "track_id"])
    df["played_at"] = pd.to_datetime(df["played_at"], utc=True)
    df["played_date"] = df["played_at"].dt.date
    df["played_hour"] = df["played_at"].dt.hour
    df["played_day_of_week"] = df["played_at"].dt.day_name()
    df["duration_min"] = (df["duration_ms"] / 60000).round(2)
    df = df.drop(columns=["duration_ms"])
    return df

def transform_top_tracks(df):
    df = df.drop_duplicates(subset=["track_id", "time_range"])
    df["duration_min"] = (df["duration_ms"] / 60000).round(2)
    df = df.drop(columns=["duration_ms"])
    return df

def run_transformation():
    print("Transforming recently played...")
    recent_df = load_latest_parquet("recently_played")
    recent_df = transform_recently_played(recent_df)

    print("Transforming top tracks...")
    top_df = load_latest_parquet("top_tracks")
    top_df = transform_top_tracks(top_df)

    print(f"Recently played: {len(recent_df)} rows")
    print(f"Top tracks: {len(top_df)} rows")

    return recent_df, top_df

if __name__ == "__main__":
    recent_df, top_df = run_transformation()
    print("\nRecently played sample:")
    print(recent_df.head())
    print("\nTop tracks sample:")
    print(top_df.head())