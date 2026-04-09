# Spotify Listening Pipeline

An end-to-end data pipeline that extracts personal Spotify listening data, transforms it, and loads it into PostgreSQL for analysis.

## Architecture

```
Spotify API
    ↓
Extract (Spotipy) → Raw Parquet files (Bronze layer)
    ↓
Transform (Pandas) → Cleaned, enriched DataFrames
    ↓
Load → PostgreSQL (Silver layer)
    ↓
Schedule → Runs every hour automatically
```

## Data Collected

- **Recently Played** — last 50 tracks with timestamps, enriched with hour, day of week, and duration
- **Top Tracks** — top 50 tracks across three time ranges (short, medium, long term)

## Tech Stack

- Python 3.11
- Spotipy — Spotify Web API wrapper
- Pandas — data transformation
- PyArrow — Parquet file format
- SQLAlchemy + psycopg2 — PostgreSQL interface
- Schedule — pipeline orchestration

## Project Structure

```
spotify-pipeline/
├── .env                  # credentials (not committed)
├── .env.example          # credentials template
├── requirements.txt
├── src/
│   ├── extract.py        # Spotify API extraction
│   ├── transform.py      # cleaning and feature engineering
│   ├── load.py           # PostgreSQL loading with upsert
│   └── pipeline.py       # orchestrator + scheduler
├── data/
│   └── raw/              # Parquet bronze layer
├── notebooks/
│   └── analysis.ipynb    # listening pattern analysis
└── README.md
```

## Setup

1. Clone the repo
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Create a Spotify Developer app at [developer.spotify.com](https://developer.spotify.com) and set the redirect URI to `http://127.0.0.1:8888/callback`
4. Copy `.env.example` to `.env` and fill in your credentials
5. Run the pipeline:
   ```bash
   cd src
   python pipeline.py
   ```

## Schema

**recently_played**

| Column | Type | Description |
|---|---|---|
| played_at | TIMESTAMPTZ | exact play timestamp (PK) |
| track_id | TEXT | Spotify track ID (PK) |
| track_name | TEXT | track title |
| artist_name | TEXT | primary artist |
| album_name | TEXT | album title |
| popularity | INTEGER | Spotify popularity score 0–100 |
| played_date | DATE | date of play |
| played_hour | INTEGER | hour of play (0–23) |
| played_day_of_week | TEXT | day name |
| duration_min | FLOAT | track duration in minutes |

**top_tracks**

| Column | Type | Description |
|---|---|---|
| track_id | TEXT | Spotify track ID (PK) |
| time_range | TEXT | short / medium / long term (PK) |
| track_name | TEXT | track title |
| artist_name | TEXT | primary artist |
| album_name | TEXT | album title |
| popularity | INTEGER | Spotify popularity score 0–100 |
| duration_min | FLOAT | track duration in minutes |

## Analysis

See `notebooks/analysis.ipynb` for listening pattern analysis including:
- Most played artists and tracks
- Listening activity by hour and day of week
- Popularity distribution
- Short vs long term taste comparison
