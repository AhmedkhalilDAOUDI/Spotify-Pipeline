import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

load_dotenv()

def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIPY_REDIRECT_URI"),
        scope="user-read-recently-played user-top-read",
        cache_path=".spotify_cache"
    ))

def extract_recently_played(sp, limit=50):
    results = sp.current_user_recently_played(limit=limit)
    tracks = []
    for item in results["items"]:
        track = item["track"]
        tracks.append({
            "played_at": item["played_at"],
            "track_id": track["id"],
            "track_name": track["name"],
            "artist_id": track["artists"][0]["id"],
            "artist_name": track["artists"][0]["name"],
            "album_name": track["album"]["name"],
            "duration_ms": track["duration_ms"],
            "popularity": track.get("popularity")
        })
    return pd.DataFrame(tracks)

def extract_top_tracks(sp, time_range="medium_term", limit=50):
    results = sp.current_user_top_tracks(time_range=time_range, limit=limit)
    tracks = []
    for item in results["items"]:
        tracks.append({
            "track_id": item["id"],
            "track_name": item["name"],
            "artist_id": item["artists"][0]["id"],
            "artist_name": item["artists"][0]["name"],
            "album_name": item["album"]["name"],
            "duration_ms": item["duration_ms"],
            "popularity": item.get("popularity"),
            "time_range": time_range
        })
    return pd.DataFrame(tracks)

def extract_artist_details(sp, artist_ids):
    artists = []
    # Spotify allows max 50 per request
    for i in range(0, len(artist_ids), 50):
        batch = list(artist_ids)[i:i+50]
        results = sp.artists(batch)
        for artist in results["artists"]:
            if artist:
                artists.append({
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "genres": ", ".join(artist.get("genres", [])),
                    "followers": artist["followers"]["total"],
                    "popularity": artist.get("popularity")
                })
    return pd.DataFrame(artists)

def save_to_parquet(df, name):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_dir, "data", "raw", f"{name}_{timestamp}.parquet")
    pq.write_table(pa.Table.from_pandas(df), path)
    print(f"Saved {name} → {path}")
    return path

def run_extraction():
    sp = get_spotify_client()

    print("Extracting recently played...")
    recent_df = extract_recently_played(sp)

    print("Extracting top tracks (short term)...")
    top_short_df = extract_top_tracks(sp, time_range="short_term")

    print("Extracting top tracks (medium term)...")
    top_medium_df = extract_top_tracks(sp, time_range="medium_term")

    print("Extracting top tracks (long term)...")
    top_long_df = extract_top_tracks(sp, time_range="long_term")

    top_df = pd.concat([top_short_df, top_medium_df, top_long_df], ignore_index=True)

    save_to_parquet(recent_df, "recently_played")
    save_to_parquet(top_df, "top_tracks")

    return recent_df, top_df

if __name__ == "__main__":
    run_extraction()