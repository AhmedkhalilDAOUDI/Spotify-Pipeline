import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

def create_database_if_not_exists():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres")
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='spotify_pipeline'"))
        if not result.fetchone():
            conn.execute(text("CREATE DATABASE spotify_pipeline"))
            print("Created database: spotify_pipeline")
        else:
            print("Database already exists.")

def create_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS recently_played (
                played_at TIMESTAMPTZ,
                track_id TEXT,
                track_name TEXT,
                artist_id TEXT,
                artist_name TEXT,
                album_name TEXT,
                popularity INTEGER,
                played_date DATE,
                played_hour INTEGER,
                played_day_of_week TEXT,
                duration_min FLOAT,
                PRIMARY KEY (played_at, track_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS top_tracks (
                track_id TEXT,
                track_name TEXT,
                artist_id TEXT,
                artist_name TEXT,
                album_name TEXT,
                popularity INTEGER,
                time_range TEXT,
                duration_min FLOAT,
                PRIMARY KEY (track_id, time_range)
            )
        """))
        conn.commit()
    print("Tables ready.")

def load_to_postgres(recent_df, top_df, engine):
    # Recently played — upsert by (played_at, track_id)
    with engine.connect() as conn:
        for _, row in recent_df.iterrows():
            conn.execute(text("""
                INSERT INTO recently_played
                VALUES (:played_at, :track_id, :track_name, :artist_id, :artist_name,
                        :album_name, :popularity, :played_date, :played_hour,
                        :played_day_of_week, :duration_min)
                ON CONFLICT (played_at, track_id) DO NOTHING
            """), row.to_dict())
        conn.commit()
    print(f"Loaded {len(recent_df)} rows into recently_played.")

    # Top tracks — upsert by (track_id, time_range)
    with engine.connect() as conn:
        for _, row in top_df.iterrows():
            conn.execute(text("""
                INSERT INTO top_tracks
                VALUES (:track_id, :track_name, :artist_id, :artist_name,
                        :album_name, :popularity, :time_range, :duration_min)
                ON CONFLICT (track_id, time_range) DO UPDATE SET
                    popularity = EXCLUDED.popularity
            """), row.to_dict())
        conn.commit()
    print(f"Loaded {len(top_df)} rows into top_tracks.")

def run_load(recent_df, top_df):
    create_database_if_not_exists()
    engine = get_engine()
    create_tables(engine)
    load_to_postgres(recent_df, top_df, engine)

if __name__ == "__main__":
    from transform import run_transformation
    recent_df, top_df = run_transformation()
    run_load(recent_df, top_df)