"""Application settings loaded from environment variables."""

from __future__ import annotations

import os

DB_NAME = os.getenv("DB_NAME", "spotify")
DB_PASSWORD = os.getenv("DB_PASSWORD", "test")
DB_URL = os.getenv("ARANGO_DB_URL", os.getenv("DB_URL", "http://127.0.0.1:8529"))
DB_USER = os.getenv("ARANGO_USER", os.getenv("DB_USER", "root"))

PLAYLIST_NAME_FALLBACK = os.getenv("PLAYLIST_NAME_FALLBACK", "playlist")
GRAPH_NAME_PREFIX = os.getenv("GRAPH_NAME_PREFIX", "spotify_kg")
GRAPH_JOBS_COLLECTION = os.getenv("GRAPH_JOBS_COLLECTION", "graph_jobs")
PLAYLIST_URL = os.getenv(
    "PLAYLIST_URL",
    "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF",
)

ENRICH_MAX_WORKERS = int(os.getenv("ENRICH_MAX_WORKERS", "6"))

DISCOGS_TOKEN_ENV = "DISCOGS_TOKEN"
LASTFM_API_KEY_ENV = "LASTFM_API_KEY"
LASTFM_SHARED_SECRET_ENV = "LASTFM_SHARED_SECRET"
GENIUS_ACCESS_TOKEN_ENV = "GENIUS_ACCESS_TOKEN"
AUDIODB_API_KEY_ENV = "THEAUDIODB_API_KEY"

DEFAULT_TIMEOUT = 10
DEFAULT_HEADERS = {
    "User-Agent": "SpotifyKG/1.0 (https://github.com)",
}
