"""Graph collection naming and schema helpers."""

from __future__ import annotations

from typing import Dict

from config import settings

# Node collections (snake_case)
ARTISTS_COLLECTION = "artists"
SONGS_COLLECTION = "songs"
ALBUMS_COLLECTION = "albums"
LABELS_COLLECTION = "record_labels"
PLAYLISTS_COLLECTION = "playlists"
GENRES_COLLECTION = "genres"
LOCATIONS_COLLECTION = "locations"
MOODS_COLLECTION = "moods"
INSTRUMENTS_COLLECTION = "instruments"
LANGUAGES_COLLECTION = "languages"

# Edge collections (snake_case)
ARTISTS_SONGS = "artists_songs"
ARTISTS_ALBUMS = "artists_albums"
SONGS_ALBUMS = "songs_albums"
ALBUMS_LABELS = "albums_record_labels"
ARTISTS_GENRES = "artists_genres"
ARTISTS_LOCATIONS = "artists_locations"
ARTISTS_LABELS = "artists_record_labels"
ARTISTS_ACTS = "artists_associated_acts"
SONGS_WRITERS = "songs_songwriters"
SONGS_PRODUCERS = "songs_producers"
SONGS_FEATURES = "songs_features"
SONGS_MOODS = "songs_moods"
SONGS_INSTRUMENTS = "songs_instruments"
SONGS_LANGUAGES = "songs_languages"
SONGS_CONTRIBUTORS = "songs_contributors"


def collection_prefix(graph_id: str) -> str:
    return f"g_{graph_id}"


def graph_name(graph_id: str) -> str:
    return f"{settings.GRAPH_NAME_PREFIX}_{collection_prefix(graph_id)}"


def collection_map(graph_id: str) -> Dict[str, Dict[str, str]]:
    prefix = collection_prefix(graph_id)
    nodes = {
        "artists": f"{prefix}_{ARTISTS_COLLECTION}",
        "songs": f"{prefix}_{SONGS_COLLECTION}",
        "albums": f"{prefix}_{ALBUMS_COLLECTION}",
        "record_labels": f"{prefix}_{LABELS_COLLECTION}",
        "playlists": f"{prefix}_{PLAYLISTS_COLLECTION}",
        "genres": f"{prefix}_{GENRES_COLLECTION}",
        "locations": f"{prefix}_{LOCATIONS_COLLECTION}",
        "moods": f"{prefix}_{MOODS_COLLECTION}",
        "instruments": f"{prefix}_{INSTRUMENTS_COLLECTION}",
        "languages": f"{prefix}_{LANGUAGES_COLLECTION}",
    }
    edges = {
        "artists_songs": f"{prefix}_{ARTISTS_SONGS}",
        "artists_albums": f"{prefix}_{ARTISTS_ALBUMS}",
        "songs_albums": f"{prefix}_{SONGS_ALBUMS}",
        "albums_record_labels": f"{prefix}_{ALBUMS_LABELS}",
        "artists_genres": f"{prefix}_{ARTISTS_GENRES}",
        "artists_locations": f"{prefix}_{ARTISTS_LOCATIONS}",
        "artists_record_labels": f"{prefix}_{ARTISTS_LABELS}",
        "artists_associated_acts": f"{prefix}_{ARTISTS_ACTS}",
        "songs_songwriters": f"{prefix}_{SONGS_WRITERS}",
        "songs_producers": f"{prefix}_{SONGS_PRODUCERS}",
        "songs_features": f"{prefix}_{SONGS_FEATURES}",
        "songs_moods": f"{prefix}_{SONGS_MOODS}",
        "songs_instruments": f"{prefix}_{SONGS_INSTRUMENTS}",
        "songs_languages": f"{prefix}_{SONGS_LANGUAGES}",
        "songs_contributors": f"{prefix}_{SONGS_CONTRIBUTORS}",
    }
    return {"nodes": nodes, "edges": edges}
